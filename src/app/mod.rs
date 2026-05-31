use anyhow::{anyhow, Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use crossbeam_channel::bounded;
use osmpbf::{BlobDecode, BlobReader, Element, HeaderBlock, RelMemberType};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::config::{CompiledConfig, CompiledTable, NodeCacheMode, RuntimeConfig};
use crate::pipeline::{BlockProcessor, NodesOnlyProcessor, StandardProcessor, WayStore};
use crate::sinks::{
    ColumnSpec, CsvWktSink, DataSink, FeatureRow, GeoJsonSink, GeoJsonlSink, GeoParquetSink,
    MapRouletteSink,
};
use crate::storage::{NodeStoreReader, NodeStoreWriter};
use crate::utils::ProgressCounter;

pub type SinkMap = HashMap<String, Arc<Mutex<Box<dyn DataSink + Send>>>>;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Enable verbose output
    #[arg(short, long, env = "COSMO_VERBOSE", global = true)]
    pub verbose: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Convert OSM PBF data to other formats
    Convert(ConvertArgs),
    /// Validate filter configuration
    Validate(ValidateArgs),
}

#[derive(Args)]
pub struct ValidateArgs {
    /// Filter configuration file (YAML)
    #[arg(env = "COSMO_FILTERS")]
    pub filters: PathBuf,
}

#[derive(Args)]
pub struct ConvertArgs {
    /// Input PBF file
    #[arg(short, long, env = "COSMO_INPUT")]
    pub input: PathBuf,

    /// Output file or directory (.geojson, .geojsonl, .parquet, .csv)
    #[arg(short, long, env = "COSMO_OUTPUT")]
    pub output: PathBuf,

    /// Filter configuration file (YAML)
    #[arg(short, long, env = "COSMO_FILTERS")]
    pub filters: PathBuf,

    /// Force specific node cache mode
    #[arg(long, value_enum, env = "COSMO_NODE_CACHE_MODE")]
    pub node_cache_mode: Option<NodeCacheMode>,

    /// Node cache directory (for dense mode)
    #[arg(long, env = "COSMO_NODE_CACHE")]
    pub node_cache: Option<PathBuf>,

    /// Maximum nodes for dense cache (default: 16B)
    #[arg(long, env = "COSMO_NODE_CACHE_MAX_NODES")]
    pub node_cache_max_nodes: Option<u64>,

    /// Number of parallel jobs (default: all cores)
    #[arg(short, long, env = "COSMO_JOBS")]
    pub jobs: Option<usize>,

    /// Output format (auto-detected if omitted)
    #[arg(long, value_enum, env = "COSMO_FORMAT")]
    pub format: Option<OutputFormat>,

    /// Include all tags in a 'tags' JSON column
    #[arg(long, env = "COSMO_ALL_TAGS")]
    pub all_tags: bool,

    /// Bounding box filter: min_lon,min_lat,max_lon,max_lat
    #[arg(long, value_parser = parse_bbox, allow_hyphen_values = true, env = "COSMO_BBOX")]
    pub bbox: Option<[f64; 4]>,
}

/// Parse a bounding box from a comma-separated string: min_lon,min_lat,max_lon,max_lat
fn parse_bbox(s: &str) -> Result<[f64; 4], String> {
    let parts: Vec<&str> = s.split(',').collect();
    if parts.len() != 4 {
        return Err(format!(
            "bbox must have 4 values (min_lon,min_lat,max_lon,max_lat), got {}",
            parts.len()
        ));
    }
    let coords: Result<Vec<f64>, _> = parts.iter().map(|p| p.trim().parse::<f64>()).collect();
    let coords = coords.map_err(|e| format!("invalid bbox coordinate: {}", e))?;

    let [min_lon, min_lat, max_lon, max_lat] = [coords[0], coords[1], coords[2], coords[3]];

    // Validate
    if min_lon > max_lon {
        return Err(format!(
            "bbox min_lon ({}) must be <= max_lon ({})",
            min_lon, max_lon
        ));
    }
    if min_lat > max_lat {
        return Err(format!(
            "bbox min_lat ({}) must be <= max_lat ({})",
            min_lat, max_lat
        ));
    }
    if min_lat < -90.0 || max_lat > 90.0 {
        return Err("bbox latitude must be between -90 and 90".to_string());
    }
    if min_lon < -180.0 || max_lon > 180.0 {
        return Err("bbox longitude must be between -180 and 180".to_string());
    }

    Ok([min_lon, min_lat, max_lon, max_lat])
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum OutputFormat {
    #[value(name = "geojson")]
    GeoJson,
    #[value(name = "geojsonl")]
    GeoJsonl,
    #[value(name = "geoparquet", alias = "parquet")]
    GeoParquet,
    #[value(name = "csv")]
    Csv,
    #[value(name = "maproulette")]
    MapRoulette,
}

pub fn resolve_node_cache_mode(
    requested: NodeCacheMode,
    input_path: &Path,
) -> (NodeCacheMode, String) {
    match requested {
        NodeCacheMode::Auto => {
            let file_size = std::fs::metadata(input_path).map(|m| m.len()).unwrap_or(0);
            let size_gb = file_size as f64 / (1024.0 * 1024.0 * 1024.0);

            let dense_threshold = crate::config::DENSE_THRESHOLD_BYTES;
            if file_size >= dense_threshold {
                (
                    NodeCacheMode::Dense,
                    format!("dense (auto-selected for {:.1} GB input)", size_gb),
                )
            } else {
                (
                    NodeCacheMode::Sparse,
                    format!("sparse (auto-selected for {:.1} GB input)", size_gb),
                )
            }
        }
        requested => (requested, requested.label().to_string()),
    }
}

pub fn summarize_filters_compiled(config: &CompiledConfig) -> String {
    let table_count = config.tables.len();
    let mut total_cols = 0;
    let mut has_node = false;
    let mut has_way = false;
    let mut has_rel = false;

    for table in config.tables.values() {
        total_cols += table.columns.len();
        has_node |= table.geometry.node;
        has_way |= table.geometry.processes_ways();
        has_rel |= table.geometry.relation;
    }

    format!(
        "{} table(s), {} total columns (nodes: {}, ways: {}, relations: {})",
        table_count, total_cols, has_node, has_way, has_rel
    )
}

pub fn output_format_label(format: &OutputFormat) -> &'static str {
    match format {
        OutputFormat::GeoJson => "geojson",
        OutputFormat::GeoJsonl => "geojsonl",
        OutputFormat::GeoParquet => "geoparquet",
        OutputFormat::Csv => "csv",
        OutputFormat::MapRoulette => "maproulette",
    }
}

pub fn log_sorted_header(header: &HeaderBlock, logged: &Arc<AtomicBool>) {
    let mut found = Vec::new();
    // HeaderBlock in osmpbf has required_features() and optional_features()
    for feature in header.required_features() {
        let feature_trim = feature.trim();
        if ["Sort.Nodes", "Sort.Ways", "Sort.Relations"]
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(feature_trim))
        {
            found.push(feature_trim.to_string());
        }
    }
    for feature in header.optional_features() {
        let feature_trim = feature.trim();
        if ["Sort.Nodes", "Sort.Ways", "Sort.Relations"]
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(feature_trim))
        {
            found.push(feature_trim.to_string());
        }
    }

    if found.is_empty() {
        return;
    }
    if !logged.swap(true, Ordering::SeqCst) {
        tracing::info!("Detected PBF sort header(s): {}", found.join(", "));
    }
}

pub fn init_sink(
    format: &OutputFormat,
    output: &Path,
    config: &CompiledConfig,
) -> Result<Box<dyn DataSink + Send>> {
    match format {
        OutputFormat::GeoJson => {
            if output == Path::new("-") {
                anyhow::bail!(
                    "CLI: GeoJSON output to stdout is not supported; use geojsonl instead"
                );
            }
            tracing::info!("Sink: {} -> {:?}", output_format_label(format), output);
            Ok(Box::new(GeoJsonSink::new(output)?))
        }
        OutputFormat::GeoJsonl => {
            if output == Path::new("-") {
                tracing::info!("Sink: {} -> stdout", output_format_label(format));
                Ok(Box::new(GeoJsonlSink::stdout()?))
            } else {
                tracing::info!("Sink: {} -> {:?}", output_format_label(format), output);
                Ok(Box::new(GeoJsonlSink::new(output)?))
            }
        }
        OutputFormat::GeoParquet => {
            if output == Path::new("-") {
                anyhow::bail!("CLI: Parquet output to stdout is not supported");
            }
            let columns = collect_columns(config)?;
            tracing::info!(
                "Sink: {} -> {:?} ({} columns)",
                output_format_label(format),
                output,
                columns.len()
            );
            Ok(Box::new(GeoParquetSink::new(output, columns)?))
        }
        OutputFormat::Csv => {
            let columns = collect_columns(config)?;
            if output == Path::new("-") {
                tracing::info!(
                    "Sink: {} -> stdout ({} columns)",
                    output_format_label(format),
                    columns.len()
                );
                Ok(Box::new(CsvWktSink::stdout(columns)?))
            } else {
                tracing::info!(
                    "Sink: {} -> {:?} ({} columns)",
                    output_format_label(format),
                    output,
                    columns.len()
                );
                Ok(Box::new(CsvWktSink::new(output, columns)?))
            }
        }
        OutputFormat::MapRoulette => {
            if output == Path::new("-") {
                tracing::info!("Sink: {} -> stdout", output_format_label(format));
                Ok(Box::new(MapRouletteSink::stdout()?))
            } else {
                tracing::info!("Sink: {} -> {:?}", output_format_label(format), output);
                Ok(Box::new(MapRouletteSink::new(output)?))
            }
        }
    }
}

pub fn init_sinks(
    format: &OutputFormat,
    output_dir: &Path,
    config: &CompiledConfig,
) -> Result<SinkMap> {
    // Validate output is not stdout for multi-table
    if output_dir == Path::new("-") {
        anyhow::bail!("Multi-table output to stdout is not supported; specify a directory path");
    }

    // Create output directory if it doesn't exist
    if !output_dir.exists() {
        std::fs::create_dir_all(output_dir)
            .with_context(|| format!("Failed to create output directory: {:?}", output_dir))?;
        tracing::info!("Created output directory: {:?}", output_dir);
    } else if !output_dir.is_dir() {
        anyhow::bail!(
            "Output path exists but is not a directory: {:?}",
            output_dir
        );
    }

    let mut sinks = HashMap::new();
    let extension = match format {
        OutputFormat::GeoJson => "geojson",
        OutputFormat::GeoJsonl => "geojsonl",
        OutputFormat::GeoParquet => "parquet",
        OutputFormat::Csv => "csv",
        OutputFormat::MapRoulette => "geojson",
    };

    for (table_name, table_config) in &config.tables {
        let file_path = output_dir.join(format!("{}.{}", table_name, extension));

        let sink: Box<dyn DataSink + Send> = match format {
            OutputFormat::GeoJson => Box::new(GeoJsonSink::new(&file_path)?),
            OutputFormat::GeoJsonl => Box::new(GeoJsonlSink::new(&file_path)?),
            OutputFormat::GeoParquet => {
                let columns = collect_columns_for_table(table_config)?;
                Box::new(GeoParquetSink::new(&file_path, columns)?)
            }
            OutputFormat::Csv => {
                let columns = collect_columns_for_table(table_config)?;
                Box::new(CsvWktSink::new(&file_path, columns)?)
            }
            OutputFormat::MapRoulette => Box::new(MapRouletteSink::new(&file_path)?),
        };

        tracing::info!(
            "Sink: {} -> {:?} ({} columns)",
            table_name,
            file_path,
            table_config.columns.len()
        );

        sinks.insert(table_name.clone(), Arc::new(Mutex::new(sink)));
    }

    Ok(sinks)
}

pub fn collect_columns(config: &CompiledConfig) -> Result<Vec<ColumnSpec>> {
    // For single table compatibility, get the first (and only) table
    let table = config
        .tables
        .values()
        .next()
        .ok_or_else(|| anyhow!("No tables defined in config"))?;
    collect_columns_for_table(table)
}

fn collect_columns_for_table(table: &CompiledTable) -> Result<Vec<ColumnSpec>> {
    let mut columns: Vec<ColumnSpec> = table
        .columns
        .iter()
        .map(|col| ColumnSpec {
            name: col.name.clone(),
            col_type: col.col_type,
        })
        .collect();

    columns.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(columns)
}

pub fn needs_node_store_compiled(config: &CompiledConfig) -> bool {
    config
        .tables
        .values()
        .any(|table| table.geometry.processes_ways() || table.geometry.relation)
}

pub fn needs_way_store(config: &CompiledConfig) -> bool {
    config
        .tables
        .values()
        .any(|table| table.geometry.relation)
}

/// Pass 0: Scan relations to collect the set of member way IDs that will be
/// needed for relation geometry assembly. This allows pass 1 to collect only
/// the referenced ways instead of all ways, keeping memory bounded for
/// planet-scale inputs.
pub fn pass0_scan_relations(path: &Path) -> Result<HashSet<i64>> {
    let reader = BlobReader::from_path(path)?;
    let way_ids: Arc<Mutex<HashSet<i64>>> = Arc::new(Mutex::new(HashSet::new()));
    let progress = Arc::new(ProgressCounter::new("Pass 0: scanning relations", 10_000));

    let way_ids_ref = Arc::clone(&way_ids);
    let progress_ref = Arc::clone(&progress);
    reader
        .par_bridge()
        .try_for_each(move |blob_result| -> Result<()> {
            let blob = blob_result?;
            match blob.decode() {
                Ok(BlobDecode::OsmData(block)) => {
                    let mut local_ids: Vec<i64> = Vec::new();
                    for element in block.elements() {
                        if let Element::Relation(rel) = element {
                            let mut is_polygon_type = false;
                            for (key, value) in rel.tags() {
                                if key == "type"
                                    && (value == "multipolygon" || value == "boundary")
                                {
                                    is_polygon_type = true;
                                    break;
                                }
                            }
                            if is_polygon_type {
                                for member in rel.members() {
                                    if member.member_type == RelMemberType::Way {
                                        local_ids.push(member.member_id);
                                    }
                                }
                                progress_ref.inc(1);
                            }
                        }
                    }
                    if !local_ids.is_empty() {
                        let mut set = way_ids_ref.lock().unwrap();
                        set.extend(local_ids);
                    }
                    Ok(())
                }
                Ok(_) => Ok(()),
                Err(error) => Err(error.into()),
            }
        })?;

    progress.finish();
    let way_ids = Arc::try_unwrap(way_ids)
        .expect("pass0 way_ids Arc should have single owner")
        .into_inner()
        .unwrap();
    tracing::info!(
        "Pass 0: found {} unique way IDs referenced by relations.",
        way_ids.len()
    );
    Ok(way_ids)
}

pub fn pass1_index_nodes(
    path: &Path,
    node_store: NodeStoreWriter,
    use_parallel: bool,
    way_filter: Option<Arc<HashSet<i64>>>,
) -> Result<(NodeStoreWriter, u64, WayStore)> {
    let mut reader = BlobReader::from_path(path)?;
    let (tx, rx) = bounded::<Vec<(u64, f64, f64)>>(64);
    let way_store: Arc<Mutex<WayStore>> = Arc::new(Mutex::new(HashMap::new()));
    let header_logged = Arc::new(AtomicBool::new(false));

    let writer = std::thread::spawn(move || -> Result<(NodeStoreWriter, u64)> {
        let mut node_store = node_store;
        let mut node_count = 0u64;
        let progress = ProgressCounter::new("Pass 1/2: indexing", 100_000);

        for batch in rx {
            let batch_len = batch.len() as u64;
            for (id, lat, lon) in batch {
                node_store
                    .put(id, lat, lon)
                    .with_context(|| format!("Pipeline: Failed writing node {}", id))?;
                node_count += 1;
            }
            if batch_len > 0 {
                progress.inc(batch_len);
            }
        }

        progress.finish();
        Ok((node_store, node_count))
    });

    let decode_result = if use_parallel {
        let header_logged = Arc::clone(&header_logged);
        let tx = tx.clone();
        let way_store = Arc::clone(&way_store);
        let way_filter = way_filter.clone();
        reader
            .par_bridge()
            .try_for_each(move |blob_result| -> Result<()> {
                let blob = blob_result?;
                match blob.decode() {
                    Ok(BlobDecode::OsmHeader(header)) => {
                        log_sorted_header(&header, &header_logged);
                        Ok(())
                    }
                    Ok(BlobDecode::OsmData(block)) => {
                        let mut node_batch = Vec::new();
                        let mut way_batch: Vec<(i64, Vec<i64>)> = Vec::new();
                        for element in block.elements() {
                            match element {
                                Element::Node(node) => {
                                    node_batch.push((node.id() as u64, node.lat(), node.lon()));
                                }
                                Element::DenseNode(node) => {
                                    node_batch.push((node.id() as u64, node.lat(), node.lon()));
                                }
                                Element::Way(way) if way_filter.is_some() => {
                                    let id = way.id();
                                    if way_filter.as_ref().unwrap().contains(&id) {
                                        way_batch.push((id, way.refs().collect()));
                                    }
                                }
                                _ => {}
                            }
                        }

                        if !node_batch.is_empty() {
                            tx.send(node_batch).map_err(|err| {
                                anyhow!("Pipeline: Failed to send node batch: {}", err)
                            })?;
                        }
                        if !way_batch.is_empty() {
                            let mut ws = way_store.lock().unwrap();
                            for (id, refs) in way_batch {
                                ws.insert(id, refs);
                            }
                        }
                        Ok(())
                    }
                    Ok(BlobDecode::Unknown(unknown)) => {
                        tracing::info!("Unknown blob: {}", unknown);
                        Ok(())
                    }
                    Err(error) => Err(error.into()),
                }
            })
    } else {
        // Sequential processing to preserve node ID order (for sparse mode)
        reader.try_for_each(|blob_result| -> Result<()> {
            let blob = blob_result?;
            match blob.decode() {
                Ok(BlobDecode::OsmHeader(header)) => {
                    log_sorted_header(&header, &header_logged);
                    Ok(())
                }
                Ok(BlobDecode::OsmData(block)) => {
                    let mut node_batch = Vec::new();
                    for element in block.elements() {
                        match element {
                            Element::Node(node) => {
                                node_batch.push((node.id() as u64, node.lat(), node.lon()));
                            }
                            Element::DenseNode(node) => {
                                node_batch.push((node.id() as u64, node.lat(), node.lon()));
                            }
                            Element::Way(way) if way_filter.is_some() => {
                                let id = way.id();
                                if way_filter.as_ref().unwrap().contains(&id) {
                                    let mut ws = way_store.lock().unwrap();
                                    ws.insert(id, way.refs().collect());
                                }
                            }
                            _ => {}
                        }
                    }

                    if !node_batch.is_empty() {
                        tx.send(node_batch).map_err(|err| {
                            anyhow!("Pipeline: Failed to send node batch: {}", err)
                        })?;
                    }
                    Ok(())
                }
                Ok(BlobDecode::Unknown(unknown)) => {
                    tracing::info!("Unknown blob: {}", unknown);
                    Ok(())
                }
                Err(error) => Err(error.into()),
            }
        })
    };

    drop(tx);

    // Get writer thread result - it contains the root cause if there was an error
    let writer_join = writer.join();

    // Check writer thread first - it has the real error if the channel disconnected
    let (node_store, node_count) = match writer_join {
        Ok(Ok(result)) => result,
        Ok(Err(writer_err)) => {
            // Writer had an error - this is the root cause
            return if decode_result.is_err() {
                Err(writer_err.context("writer thread failed (caused channel disconnect)"))
            } else {
                Err(writer_err)
            };
        }
        Err(panic_payload) => {
            // Thread panicked - try to extract useful info
            let panic_msg = panic_payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic_payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_string());
            return Err(anyhow!(
                "Pipeline: Node writer thread panicked: {}",
                panic_msg
            ));
        }
    };

    // Only check decode_result if writer succeeded
    decode_result?;

    let way_store = Arc::try_unwrap(way_store)
        .expect("way_store Arc should have single owner after decode")
        .into_inner()
        .unwrap();
    Ok((node_store, node_count, way_store))
}

pub fn run_pass<P>(
    path: &Path,
    processor: Arc<P>,
    sinks: SinkMap,
    label: &'static str,
) -> Result<HashMap<String, u64>>
where
    P: BlockProcessor + 'static,
{
    let reader = BlobReader::from_path(path)?;
    let (tx, rx) = bounded::<Vec<(String, FeatureRow)>>(64);
    let progress = Arc::new(ProgressCounter::new(label, 100));

    let sinks_clone = sinks.clone();
    let writer = std::thread::spawn(move || -> Result<HashMap<String, u64>> {
        let mut match_counts: HashMap<String, u64> = HashMap::new();

        for batch in rx {
            for (table_name, row) in batch {
                if let Some(sink_handle) = sinks_clone.get(&table_name) {
                    let mut sink = sink_handle
                        .lock()
                        .map_err(|e| anyhow!("Sink lock poisoned: {}", e))?;
                    sink.add_feature(row)?;
                    *match_counts.entry(table_name).or_insert(0) += 1;
                } else {
                    tracing::warn!("Unknown table name in feature batch: {}", table_name);
                }
            }
        }
        Ok(match_counts)
    });

    let processor = processor.clone();
    let decode_result = reader
        .par_bridge()
        .try_for_each(|blob_result| -> Result<()> {
            let blob = blob_result?;
            let block = match blob.decode() {
                Ok(BlobDecode::OsmHeader(_)) => return Ok(()),
                Ok(BlobDecode::OsmData(block)) => block,
                Ok(BlobDecode::Unknown(unknown)) => {
                    tracing::info!("Unknown blob: {}", unknown);
                    return Ok(());
                }
                Err(error) => return Err(error.into()),
            };

            progress.inc(1);

            let batch = processor.process_block(block)?;
            if !batch.is_empty() {
                tx.send(batch)
                    .map_err(|err| anyhow!("Pipeline: Failed to send feature batch: {}", err))?;
            }

            Ok(())
        });

    drop(tx);

    // Get writer thread result - it contains the root cause if there was an error
    let writer_join = writer.join();

    // Check writer thread first - it has the real error if the channel disconnected
    let match_counts = match writer_join {
        Ok(Ok(result)) => result,
        Ok(Err(writer_err)) => {
            // Writer had an error - this is the root cause
            return if decode_result.is_err() {
                Err(writer_err
                    .context("Pipeline: Sink writer thread failed (caused channel disconnect)"))
            } else {
                Err(writer_err)
            };
        }
        Err(panic_payload) => {
            // Thread panicked - try to extract useful info
            let panic_msg = panic_payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic_payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_string());
            return Err(anyhow!(
                "Pipeline: Sink writer thread panicked: {}",
                panic_msg
            ));
        }
    };

    // Only check decode_result if writer succeeded
    decode_result?;

    progress.finish();
    Ok(match_counts)
}

pub fn pass2_process(
    path: &Path,
    config: Arc<CompiledConfig>,
    runtime: Arc<RuntimeConfig>,
    node_store: Arc<NodeStoreReader>,
    way_store: Arc<WayStore>,
    sinks: SinkMap,
) -> Result<HashMap<String, u64>> {
    let processor = Arc::new(StandardProcessor {
        config,
        runtime,
        node_store,
        way_store,
    });
    run_pass(path, processor, sinks, "Pass 2/2: blocks")
}

pub fn pass_nodes_only(
    path: &Path,
    config: Arc<CompiledConfig>,
    runtime: Arc<RuntimeConfig>,
    sinks: SinkMap,
) -> Result<HashMap<String, u64>> {
    let processor = Arc::new(NodesOnlyProcessor { config, runtime });
    run_pass(path, processor, sinks, "Single pass: blocks")
}

pub fn process_pbf(
    args: &ConvertArgs,
    verbose: bool,
    config: Arc<CompiledConfig>,
    runtime: Arc<RuntimeConfig>,
    sinks: SinkMap,
    needs_nodes: bool,
) -> Result<HashMap<String, u64>> {
    tracing::info!("Node cache required: {}", needs_nodes);
    if needs_nodes {
        if verbose {
            if let Ok(metadata) = std::fs::metadata(&args.input) {
                let size_gb = metadata.len() as f64 / (1024.0 * 1024.0 * 1024.0);
                tracing::info!("Input size: {:.2} GB", size_gb);
            }
        }
        // Resolve auto mode to concrete mode based on input file size
        let (resolved_mode, mode_desc) =
            resolve_node_cache_mode(runtime.node_cache_mode, &args.input);

        // Create node store based on resolved mode
        let node_store = match resolved_mode {
            NodeCacheMode::Sparse => {
                tracing::info!("Node cache: {} (temp file)", mode_desc);
                NodeStoreWriter::new_sparse()
                    .context("Pipeline: Failed to create sparse node store")?
            }
            NodeCacheMode::Dense => {
                if let Some(ref path) = args.node_cache {
                    // User provided explicit path - no auto-cleanup
                    tracing::info!(
                        "Node cache: {} at {:?} (max {} nodes)",
                        mode_desc,
                        path,
                        runtime.node_cache_max_nodes
                    );
                    NodeStoreWriter::new_dense(path, runtime.node_cache_max_nodes)
                        .context("Pipeline: Failed to create dense node store")?
                } else {
                    // Use temp file with auto-cleanup on drop
                    tracing::info!(
                        "Node cache: {} (temp file, max {} nodes)",
                        mode_desc,
                        runtime.node_cache_max_nodes
                    );
                    NodeStoreWriter::new_dense_temp(runtime.node_cache_max_nodes)
                        .context("Pipeline: Failed to create temporary dense node store")?
                }
            }
            NodeCacheMode::Memory => {
                tracing::info!("Node cache: {}", mode_desc);
                NodeStoreWriter::new_memory()
            }
            NodeCacheMode::Auto => {
                unreachable!("Auto mode should have been resolved")
            }
        };

        // Use sequential processing for sparse mode to preserve sort order and avoid in-memory sort
        let use_parallel = !matches!(resolved_mode, NodeCacheMode::Sparse);
        let pass1_mode = if use_parallel {
            "parallel"
        } else {
            "sequential"
        };
        tracing::info!(
            "Pass 1: Indexing nodes from {:?} ({})...",
            args.input,
            pass1_mode
        );
        let needs_relations = needs_way_store(&config);
        if needs_relations {
            if let Ok(metadata) = std::fs::metadata(&args.input) {
                if metadata.len() >= crate::config::DENSE_THRESHOLD_BYTES {
                    let size_gb = metadata.len() as f64 / (1024.0 * 1024.0 * 1024.0);
                    tracing::warn!(
                        "Input is {:.1} GB with relation processing enabled. \
                         Way store memory usage depends on the number of relation members. \
                         Pass 0 pre-scan will filter to only needed ways.",
                        size_gb
                    );
                }
            }
        }
        let way_filter = if needs_relations {
            tracing::info!("Pass 0: Scanning relations for member way IDs...");
            let way_ids = pass0_scan_relations(&args.input)?;
            Some(Arc::new(way_ids))
        } else {
            None
        };

        let (node_store, node_count, way_store) =
            pass1_index_nodes(&args.input, node_store, use_parallel, way_filter)?;
        tracing::info!("Indexed {} nodes.", node_count);
        if needs_relations {
            tracing::info!("Indexed {} ways for relation assembly.", way_store.len());
        }

        let finalize_step = match resolved_mode {
            NodeCacheMode::Sparse => "Finalizing node cache (flush + mmap)...",
            NodeCacheMode::Dense => "Finalizing node cache (mmap read-only)...",
            NodeCacheMode::Memory => "Finalizing node cache (in-memory)...",
            NodeCacheMode::Auto => "Finalizing node cache...",
        };
        tracing::info!("{}", finalize_step);
        let node_store = Arc::new(node_store.finalize()?);
        tracing::info!("Node cache ready.");

        tracing::info!("Pass 2: Processing elements (parallel)...");
        let way_store = Arc::new(way_store);
        let result = pass2_process(&args.input, config, runtime, node_store, way_store, sinks)?;

        // Temp file (if any) is cleaned up when node_store is dropped

        Ok(result)
    } else {
        tracing::info!("Single pass: Processing nodes (parallel)...");
        pass_nodes_only(&args.input, config, runtime, sinks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bbox_valid_utah() {
        let result = parse_bbox("-114.0556,36.9963,-109.0394,42.00323");
        assert!(result.is_ok());
        let bbox = result.unwrap();
        assert!((bbox[0] - -114.0556).abs() < 1e-6);
        assert!((bbox[1] - 36.9963).abs() < 1e-6);
        assert!((bbox[2] - -109.0394).abs() < 1e-6);
        assert!((bbox[3] - 42.00323).abs() < 1e-6);
    }

    #[test]
    fn parse_bbox_with_spaces() {
        let result = parse_bbox("-114.05, 36.99, -109.03, 42.00");
        assert!(result.is_ok());
    }

    #[test]
    fn parse_bbox_wrong_count() {
        let result = parse_bbox("-114.05,36.99,-109.03");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("4 values"));
    }

    #[test]
    fn parse_bbox_invalid_number() {
        let result = parse_bbox("-114.05,foo,-109.03,42.00");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid"));
    }

    #[test]
    fn parse_bbox_min_greater_than_max_lon() {
        let result = parse_bbox("-109.0,36.99,-114.0,42.00");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("min_lon"));
    }

    #[test]
    fn parse_bbox_min_greater_than_max_lat() {
        let result = parse_bbox("-114.0,42.00,-109.0,36.99");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("min_lat"));
    }

    #[test]
    fn parse_bbox_lat_out_of_range() {
        let result = parse_bbox("-114.0,-91.0,-109.0,42.0");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("latitude"));
    }

    #[test]
    fn parse_bbox_lon_out_of_range() {
        let result = parse_bbox("-181.0,36.99,-109.0,42.0");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("longitude"));
    }
}
