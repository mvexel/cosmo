use anyhow::{Context, Result, anyhow};
use clap::{Parser, ValueEnum};
use crossbeam_channel::bounded;
use geo::algorithm::centroid::Centroid;
use geo_types::{Geometry, LineString, Point, Polygon};
use osmpbf::{BlobDecode, BlobReader, Element, Info, PrimitiveBlock};
use rayon::prelude::*;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

mod config;
mod sinks;
mod storage;

use config::{
    ClosedWayMode, FilterExpr, FiltersConfig, NodeCacheMode, RuntimeConfig, TagMatch,
    WayGeometryMode,
};
use sinks::{
    ColumnSpec, ColumnType, ColumnValue, DataSink, FeatureRow, GeoJsonSink, GeoJsonlSink,
    GeoParquetSink,
};
use storage::{NodeStoreReader, NodeStoreWriter};

type SinkHandle = Arc<Mutex<Box<dyn DataSink + Send>>>;

/// Threshold for auto-selecting between sparse and dense node cache modes.
/// Based on osmium documentation: files >= 5GB are typically continent/planet scale.
const DENSE_THRESHOLD_BYTES: u64 = 5 * 1024 * 1024 * 1024; // 5 GB

/// Resolve Auto mode to a concrete mode based on input file size.
/// Returns the resolved mode and a description for logging.
fn resolve_node_cache_mode(
    requested: NodeCacheMode,
    input_path: &Path,
) -> (NodeCacheMode, String) {
    match requested {
        NodeCacheMode::Auto => {
            let file_size = std::fs::metadata(input_path)
                .map(|m| m.len())
                .unwrap_or(0);

            let size_gb = file_size as f64 / (1024.0 * 1024.0 * 1024.0);

            if file_size >= DENSE_THRESHOLD_BYTES {
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
        NodeCacheMode::Sparse => (NodeCacheMode::Sparse, "sparse".to_string()),
        NodeCacheMode::Dense => (NodeCacheMode::Dense, "dense".to_string()),
        NodeCacheMode::Memory => (NodeCacheMode::Memory, "memory".to_string()),
    }
}

macro_rules! vprintln {
    ($verbose:expr, $($arg:tt)*) => {
        if $verbose {
            eprintln!($($arg)*);
        }
    };
}

struct ProgressCounter {
    label: &'static str,
    interval: u64,
    count: u64,
}

impl ProgressCounter {
    fn new(label: &'static str, interval: u64) -> Self {
        let counter = Self {
            label,
            interval: interval.max(1),
            count: 0,
        };
        counter.print();
        counter
    }

    fn inc(&mut self, delta: u64) {
        self.count = self.count.saturating_add(delta);
        if self.count % self.interval == 0 {
            self.print();
        }
    }

    fn finish(&self) {
        self.print();
        eprintln!();
    }

    fn print(&self) {
        use std::io::Write;

        eprint!("\r{}: {}", self.label, self.count);
        let _ = std::io::stderr().flush();
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Input OSM PBF file
    #[arg(short, long, env = "COSMO_INPUT")]
    input: PathBuf,

    /// Output file path
    #[arg(short, long, env = "COSMO_OUTPUT")]
    output: PathBuf,

    /// Output format
    #[arg(short = 'f', long = "format", env = "COSMO_FORMAT")]
    format: OutputFormat,

    /// Filters YAML file
    #[arg(long, default_value = "filters.yaml", env = "COSMO_FILTERS")]
    filters: PathBuf,

    /// Node cache file path
    #[arg(long, env = "COSMO_NODE_CACHE")]
    node_cache: Option<PathBuf>,

    /// Node cache mode: auto (default), sparse, dense, or memory
    #[arg(long, env = "COSMO_NODE_CACHE_MODE")]
    node_cache_mode: Option<NodeCacheMode>,

    /// Node cache max nodes override (default: 11B for full planet support)
    #[arg(long, env = "COSMO_NODE_CACHE_MAX_NODES")]
    node_cache_max_nodes: Option<u64>,

    /// Include all tags in output properties
    #[arg(long, env = "COSMO_ALL_TAGS")]
    all_tags: bool,

    /// Show detailed log output
    #[arg(short, long, env = "COSMO_VERBOSE")]
    verbose: bool,
}

#[derive(ValueEnum, Clone, Debug)]
enum OutputFormat {
    #[value(name = "geojson")]
    GeoJson,
    #[value(name = "geojsonl")]
    GeoJsonl,
    #[value(name = "parquet")]
    #[value(alias = "geoparquet")]
    GeoParquet,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let filters = Arc::new(FiltersConfig::load(&cli.filters)?);

    let mut runtime = RuntimeConfig::default();

    if let Some(mode) = cli.node_cache_mode {
        runtime.node_cache_mode = mode;
    }
    if let Some(max_nodes) = cli.node_cache_max_nodes {
        runtime.node_cache_max_nodes = max_nodes;
    }
    if cli.all_tags {
        runtime.all_tags = true;
    }

    if cli.input == Path::new("-") {
        anyhow::bail!("--input - is not supported without buffering; use a file path");
    }

    let runtime = Arc::new(runtime);

    // 1. Initialize Sink
    let sink = Arc::new(Mutex::new(init_sink(&cli.format, &cli.output, &filters)?));

    let needs_nodes = needs_node_store(&filters);
    let match_count = process_pbf(
        &cli,
        filters.clone(),
        runtime.clone(),
        sink.clone(),
        needs_nodes,
    )?;

    // 5. Finish Sink
    vprintln!(cli.verbose, "Finalizing sink: {:?}", cli.output);
    let mut sink = sink.lock().unwrap();
    sink.finish()?;

    eprintln!("Done! Processed {} matching features.", match_count);

    Ok(())
}

/// Process w rayon
fn process_pbf(
    cli: &Cli,
    filters: Arc<FiltersConfig>,
    runtime: Arc<RuntimeConfig>,
    sink: SinkHandle,
    needs_nodes: bool,
) -> Result<u64> {
    if needs_nodes {
        // Resolve auto mode to concrete mode based on input file size
        let (resolved_mode, mode_desc) =
            resolve_node_cache_mode(runtime.node_cache_mode, &cli.input);

        // Create node store based on resolved mode
        let node_store = match resolved_mode {
            NodeCacheMode::Sparse => {
                eprintln!("Node cache: {}", mode_desc);
                NodeStoreWriter::new_sparse()
            }
            NodeCacheMode::Dense => {
                if let Some(ref path) = cli.node_cache {
                    // User provided explicit path - no auto-cleanup
                    eprintln!(
                        "Node cache: {} at {:?} (max {} nodes)",
                        mode_desc,
                        path,
                        runtime.node_cache_max_nodes
                    );
                    NodeStoreWriter::new_dense(path, runtime.node_cache_max_nodes)
                        .context("Failed to create dense node store")?
                } else {
                    // Use temp file with auto-cleanup on drop
                    eprintln!(
                        "Node cache: {} (temp file, max {} nodes)",
                        mode_desc,
                        runtime.node_cache_max_nodes
                    );
                    NodeStoreWriter::new_dense_temp(runtime.node_cache_max_nodes)
                        .context("Failed to create temporary dense node store")?
                }
            }
            NodeCacheMode::Memory => {
                eprintln!("Node cache: {}", mode_desc);
                NodeStoreWriter::new_memory()
            }
            NodeCacheMode::Auto => {
                unreachable!("Auto mode should have been resolved")
            }
        };

        vprintln!(
            cli.verbose,
            "Pass 1: Indexing nodes from {:?}...",
            cli.input
        );
        let (node_store, node_count) = pass1_index_nodes(&cli.input, node_store)?;
        vprintln!(cli.verbose, "Indexed {} nodes.", node_count);

        let node_store = Arc::new(node_store.finalize()?);

        vprintln!(cli.verbose, "Pass 2: Processing elements...");
        let result = pass2_process(&cli.input, filters, runtime, node_store, sink)?;

        // Temp file (if any) is cleaned up when node_store is dropped

        Ok(result)
    } else {
        vprintln!(cli.verbose, "Single pass: Processing nodes...");
        pass_nodes_only(&cli.input, filters, runtime, sink)
    }
}

fn init_sink(
    format: &OutputFormat,
    output: &Path,
    filters: &FiltersConfig,
) -> Result<Box<dyn DataSink + Send>> {
    match format {
        OutputFormat::GeoJson => {
            if output == Path::new("-") {
                anyhow::bail!("geojson output to stdout is not supported; use geojsonl instead");
            }
            Ok(Box::new(GeoJsonSink::new(output)?))
        }
        OutputFormat::GeoJsonl => {
            if output == Path::new("-") {
                Ok(Box::new(GeoJsonlSink::stdout()?))
            } else {
                Ok(Box::new(GeoJsonlSink::new(output)?))
            }
        }
        OutputFormat::GeoParquet => {
            if output == Path::new("-") {
                anyhow::bail!("parquet output to stdout is not supported");
            }
            let columns = collect_columns(filters)?;
            Ok(Box::new(GeoParquetSink::new(output, columns)?))
        }
    }
}

fn collect_columns(filters: &FiltersConfig) -> Result<Vec<ColumnSpec>> {
    let mut columns: HashMap<String, ColumnType> = HashMap::new();
    for table in filters.tables.values() {
        for col in &table.columns {
            let col_type = parse_column_type(&col.col_type)?;
            match columns.get(&col.name) {
                Some(existing) if *existing != col_type => {
                    return Err(anyhow::anyhow!(
                        "conflicting column types for {}: {:?} vs {:?}",
                        col.name,
                        existing,
                        col_type
                    ));
                }
                Some(_) => {}
                None => {
                    columns.insert(col.name.clone(), col_type);
                }
            }
        }
    }

    let mut result: Vec<ColumnSpec> = columns
        .into_iter()
        .map(|(name, col_type)| ColumnSpec { name, col_type })
        .collect();
    result.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(result)
}

fn parse_column_type(value: &str) -> Result<ColumnType> {
    match value.to_ascii_lowercase().as_str() {
        "string" => Ok(ColumnType::String),
        "integer" => Ok(ColumnType::Integer),
        "float" => Ok(ColumnType::Float),
        _ => Err(anyhow::anyhow!("unsupported column type: {}", value)),
    }
}

fn process_block_collect(
    block: PrimitiveBlock,
    filters: &FiltersConfig,
    runtime: &RuntimeConfig,
    node_store: &NodeStoreReader,
) -> Result<Vec<FeatureRow>> {
    let mut rows = Vec::new();
    for element in block.elements() {
        match element {
            Element::Node(node) => {
                let tag_map = build_tag_map(node.tags());
                for table in filters.tables.values() {
                    if !table.geometry.node {
                        continue;
                    }
                    if matches_filter(&table.filter, &tag_map) {
                        let row = build_feature_row(
                            Geometry::Point(Point::new(node.lon(), node.lat())),
                            &tag_map,
                            &table.columns,
                            runtime,
                            Some(build_metadata_from_info(node.id(), &node.info())),
                        );
                        rows.push(row);
                    }
                }
            }
            Element::DenseNode(node) => {
                let tag_map = build_tag_map(node.tags());
                for table in filters.tables.values() {
                    if !table.geometry.node {
                        continue;
                    }
                    if matches_filter(&table.filter, &tag_map) {
                        let metadata = node
                            .info()
                            .map(|info| build_metadata_from_dense_info(node.id(), info));
                        let row = build_feature_row(
                            Geometry::Point(Point::new(node.lon(), node.lat())),
                            &tag_map,
                            &table.columns,
                            runtime,
                            metadata,
                        );
                        rows.push(row);
                    }
                }
            }
            Element::Way(way) => {
                let tag_map = build_tag_map(way.tags());
                for table in filters.tables.values() {
                    if !table.geometry.way.enabled() {
                        continue;
                    }
                    if matches_filter(&table.filter, &tag_map) {
                        let coords: Vec<(f64, f64)> = way
                            .refs()
                            .filter_map(|id| node_store.get(id as u64))
                            .collect();

                        if coords.len() < 2 {
                            continue;
                        }

                        let line_string = LineString::from(coords.clone());
                        let geometry = build_way_geometry(&table.geometry, line_string, &coords);
                        let row = build_feature_row(
                            geometry,
                            &tag_map,
                            &table.columns,
                            runtime,
                            Some(build_metadata_from_info(way.id(), &way.info())),
                        );
                        rows.push(row);
                    }
                }
            }
            Element::Relation(_) => {
                // TODO: Relation support
            }
        }
    }

    Ok(rows)
}

fn pass1_index_nodes(path: &Path, node_store: NodeStoreWriter) -> Result<(NodeStoreWriter, u64)> {
    let reader = BlobReader::from_path(path)?;
    let (tx, rx) = bounded::<Vec<(u64, f64, f64)>>(64);

    let writer = std::thread::spawn(move || -> Result<(NodeStoreWriter, u64)> {
        let mut node_store = node_store;
        let mut node_count = 0u64;
        let mut progress = ProgressCounter::new("Pass 1/2: indexing nodes", 100_000);

        for batch in rx {
            let batch_len = batch.len() as u64;
            for (id, lat, lon) in batch {
                node_store
                    .put(id, lat, lon)
                    .with_context(|| format!("failed writing node {}", id))?;
                node_count += 1;
            }
            if batch_len > 0 {
                progress.inc(batch_len);
            }
        }

        progress.finish();
        Ok((node_store, node_count))
    });

    let decode_result = reader
        .par_bridge()
        .try_for_each(|blob_result| -> Result<()> {
            let blob = blob_result?;
            match blob.decode() {
                Ok(BlobDecode::OsmHeader(_)) => Ok(()),
                Ok(BlobDecode::OsmData(block)) => {
                    let mut batch = Vec::new();
                    for element in block.elements() {
                        match element {
                            Element::Node(node) => {
                                batch.push((node.id() as u64, node.lat(), node.lon()));
                            }
                            Element::DenseNode(node) => {
                                batch.push((node.id() as u64, node.lat(), node.lon()));
                            }
                            _ => {}
                        }
                    }

                    if !batch.is_empty() {
                        tx.send(batch).map_err(|err| anyhow!(err))?;
                    }
                    Ok(())
                }
                Ok(BlobDecode::Unknown(unknown)) => {
                    eprintln!("Unknown blob: {}", unknown);
                    Ok(())
                }
                Err(error) => Err(error.into()),
            }
        });

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
            return Err(anyhow!("node writer thread panicked: {}", panic_msg));
        }
    };

    // Only check decode_result if writer succeeded
    decode_result?;

    Ok((node_store, node_count))
}

fn pass2_process(
    path: &Path,
    filters: Arc<FiltersConfig>,
    runtime: Arc<RuntimeConfig>,
    node_store: Arc<NodeStoreReader>,
    sink: SinkHandle,
) -> Result<u64> {
    let reader = BlobReader::from_path(path)?;
    let (tx, rx) = bounded::<Vec<FeatureRow>>(64);
    let progress = Arc::new(Mutex::new(ProgressCounter::new("Pass 2/2: blocks", 100)));

    let sink_handle = sink.clone();
    let writer = std::thread::spawn(move || -> Result<u64> {
        let mut sink = sink_handle.lock().unwrap();
        let mut match_count = 0u64;
        for batch in rx {
            for row in batch {
                sink.add_feature(row)?;
                match_count += 1;
            }
        }
        Ok(match_count)
    });

    let decode_result = reader
        .par_bridge()
        .try_for_each(|blob_result| -> Result<()> {
            let blob = blob_result?;
            let block = match blob.decode() {
                Ok(BlobDecode::OsmHeader(_)) => return Ok(()),
                Ok(BlobDecode::OsmData(block)) => block,
                Ok(BlobDecode::Unknown(unknown)) => {
                    eprintln!("Unknown blob: {}", unknown);
                    return Ok(());
                }
                Err(error) => return Err(error.into()),
            };

            {
                let mut p = progress.lock().unwrap();
                p.inc(1);
            }

            let batch = process_block_collect(block, &filters, &runtime, &node_store)?;
            if !batch.is_empty() {
                tx.send(batch).map_err(|err| anyhow!(err))?;
            }

            Ok(())
        });

    drop(tx);

    // Get writer thread result - it contains the root cause if there was an error
    let writer_join = writer.join();

    // Check writer thread first - it has the real error if the channel disconnected
    let match_count = match writer_join {
        Ok(Ok(result)) => result,
        Ok(Err(writer_err)) => {
            // Writer had an error - this is the root cause
            return if decode_result.is_err() {
                Err(writer_err.context("sink writer thread failed (caused channel disconnect)"))
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
            return Err(anyhow!("sink writer thread panicked: {}", panic_msg));
        }
    };

    // Only check decode_result if writer succeeded
    decode_result?;

    progress.lock().unwrap().finish();
    Ok(match_count)
}

fn pass_nodes_only(
    path: &Path,
    filters: Arc<FiltersConfig>,
    runtime: Arc<RuntimeConfig>,
    sink: SinkHandle,
) -> Result<u64> {
    let reader = BlobReader::from_path(path)?;
    let (tx, rx) = bounded::<Vec<FeatureRow>>(64);
    let progress = Arc::new(Mutex::new(ProgressCounter::new("Single pass: blocks", 100)));

    let sink_handle = sink.clone();
    let writer = std::thread::spawn(move || -> Result<u64> {
        let mut sink = sink_handle.lock().unwrap();
        let mut match_count = 0u64;
        for batch in rx {
            for row in batch {
                sink.add_feature(row)?;
                match_count += 1;
            }
        }
        Ok(match_count)
    });

    let decode_result = reader
        .par_bridge()
        .try_for_each(|blob_result| -> Result<()> {
            let blob = blob_result?;
            let block = match blob.decode() {
                Ok(BlobDecode::OsmHeader(_)) => return Ok(()),
                Ok(BlobDecode::OsmData(block)) => block,
                Ok(BlobDecode::Unknown(unknown)) => {
                    eprintln!("Unknown blob: {}", unknown);
                    return Ok(());
                }
                Err(error) => return Err(error.into()),
            };

            {
                let mut p = progress.lock().unwrap();
                p.inc(1);
            }

            let batch = process_block_nodes_only_collect(block, &filters, &runtime)?;
            if !batch.is_empty() {
                tx.send(batch).map_err(|err| anyhow!(err))?;
            }

            Ok(())
        });

    drop(tx);

    // Get writer thread result - it contains the root cause if there was an error
    let writer_join = writer.join();

    // Check writer thread first - it has the real error if the channel disconnected
    let match_count = match writer_join {
        Ok(Ok(result)) => result,
        Ok(Err(writer_err)) => {
            // Writer had an error - this is the root cause
            return if decode_result.is_err() {
                Err(writer_err.context("sink writer thread failed (caused channel disconnect)"))
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
            return Err(anyhow!("sink writer thread panicked: {}", panic_msg));
        }
    };

    // Only check decode_result if writer succeeded
    decode_result?;

    progress.lock().unwrap().finish();
    Ok(match_count)
}

fn matches_filter(filter: &FilterExpr, tags: &HashMap<String, String>) -> bool {
    match filter {
        FilterExpr::Simple(map) => map
            .iter()
            .all(|(k, v)| tags.get(k).map_or(false, |tag_val| tag_val == v)),
        FilterExpr::Any { any } => any.iter().any(|expr| matches_filter(expr, tags)),
        FilterExpr::All { all } => all.iter().all(|expr| matches_filter(expr, tags)),
        FilterExpr::Not { not } => !matches_filter(not, tags),
        FilterExpr::Tag(tag_match) => matches_tag(tag_match, tags),
    }
}

fn build_feature_row(
    geometry: Geometry<f64>,
    tags: &HashMap<String, String>,
    columns: &[config::ColumnConfig],
    runtime: &RuntimeConfig,
    metadata: Option<MetadataFields>,
) -> FeatureRow {
    let mut column_values: HashMap<String, ColumnValue> = HashMap::new();

    for col in columns {
        if col.source.starts_with("tag:") {
            let tag_key = &col.source[4..];
            if let Some(val) = tags.get(tag_key) {
                if let Some(value) = parse_column_value(val, &col.col_type) {
                    column_values.insert(col.name.clone(), value);
                }
            }
        } else if col.source.starts_with("meta:") {
            if let Some(meta_val) = extract_meta_value(&col.source[5..], metadata.as_ref()) {
                if let Some(value) = parse_column_value(&meta_val, &col.col_type) {
                    column_values.insert(col.name.clone(), value);
                }
            }
        }
    }

    let mut extras = Map::new();
    if runtime.all_tags {
        let tags_map = tags
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect();
        extras.insert("tags".to_string(), Value::Object(tags_map));
    }

    FeatureRow {
        geometry,
        columns: column_values,
        extras,
    }
}

fn parse_column_value(value: &str, col_type: &str) -> Option<ColumnValue> {
    match col_type.to_ascii_lowercase().as_str() {
        "integer" => value.parse::<i64>().ok().map(ColumnValue::Integer),
        "float" => value.parse::<f64>().ok().map(ColumnValue::Float),
        _ => Some(ColumnValue::String(value.to_string())),
    }
}

fn extract_meta_value(key: &str, metadata: Option<&MetadataFields>) -> Option<String> {
    let meta = metadata?;
    match key {
        "id" => Some(meta.id.to_string()),
        "visible" => meta.visible.map(|v| v.to_string()),
        "version" => meta.version.map(|v| v.to_string()),
        "changeset" => meta.changeset.map(|v| v.to_string()),
        "timestamp" => meta.timestamp.clone(),
        "uid" => meta.uid.map(|v| v.to_string()),
        "user" => meta.user.clone(),
        _ => None,
    }
}

fn build_tag_map<'a, I>(tags: I) -> HashMap<String, String>
where
    I: Iterator<Item = (&'a str, &'a str)>,
{
    tags.map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

fn matches_tag(tag_match: &TagMatch, tags: &HashMap<String, String>) -> bool {
    let Some(tag_val) = tags.get(&tag_match.tag) else {
        return false;
    };

    if let Some(value) = &tag_match.value {
        return tag_val == value;
    }

    if !tag_match.values.is_empty() {
        return tag_match
            .values
            .iter()
            .any(|pattern| glob_match(pattern, tag_val));
    }

    true
}

fn glob_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == value;
    }

    let mut remaining = value;
    if !pattern.starts_with('*') {
        let prefix = parts.first().unwrap();
        if !remaining.starts_with(prefix) {
            return false;
        }
        remaining = &remaining[prefix.len()..];
    }

    if !pattern.ends_with('*') {
        let suffix = parts.last().unwrap();
        if !remaining.ends_with(suffix) {
            return false;
        }
    }

    for part in parts.iter().filter(|p| !p.is_empty()) {
        match remaining.find(part) {
            Some(idx) => {
                remaining = &remaining[idx + part.len()..];
            }
            None => return false,
        }
    }

    true
}

fn build_way_geometry(
    geometry_cfg: &config::GeometryConfig,
    line_string: LineString<f64>,
    coords: &[(f64, f64)],
) -> Geometry<f64> {
    if line_string.is_closed() {
        return match geometry_cfg.closed_way {
            ClosedWayMode::Polygon => Geometry::Polygon(Polygon::new(line_string, vec![])),
            ClosedWayMode::Centroid => {
                let polygon = Polygon::new(line_string, vec![]);
                let centroid = polygon
                    .centroid()
                    .unwrap_or_else(|| Point::new(coords[0].0, coords[0].1));
                Geometry::Point(centroid)
            }
            ClosedWayMode::Linestring => Geometry::LineString(line_string),
        };
    }

    match geometry_cfg.way.mode() {
        WayGeometryMode::Linestring => Geometry::LineString(line_string),
        WayGeometryMode::Polygon => Geometry::Polygon(Polygon::new(line_string, vec![])),
        WayGeometryMode::Centroid => {
            let polygon = Polygon::new(line_string, vec![]);
            let centroid = polygon
                .centroid()
                .unwrap_or_else(|| Point::new(coords[0].0, coords[0].1));
            Geometry::Point(centroid)
        }
    }
}

fn process_block_nodes_only_collect(
    block: PrimitiveBlock,
    filters: &FiltersConfig,
    runtime: &RuntimeConfig,
) -> Result<Vec<FeatureRow>> {
    let mut rows = Vec::new();
    for element in block.elements() {
        match element {
            Element::Node(node) => {
                let tag_map = build_tag_map(node.tags());
                for table in filters.tables.values() {
                    if !table.geometry.node {
                        continue;
                    }
                    if matches_filter(&table.filter, &tag_map) {
                        let row = build_feature_row(
                            Geometry::Point(Point::new(node.lon(), node.lat())),
                            &tag_map,
                            &table.columns,
                            runtime,
                            Some(build_metadata_from_info(node.id(), &node.info())),
                        );
                        rows.push(row);
                    }
                }
            }
            Element::DenseNode(node) => {
                let tag_map = build_tag_map(node.tags());
                for table in filters.tables.values() {
                    if !table.geometry.node {
                        continue;
                    }
                    if matches_filter(&table.filter, &tag_map) {
                        let metadata = node
                            .info()
                            .map(|info| build_metadata_from_dense_info(node.id(), info));
                        let row = build_feature_row(
                            Geometry::Point(Point::new(node.lon(), node.lat())),
                            &tag_map,
                            &table.columns,
                            runtime,
                            metadata,
                        );
                        rows.push(row);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(rows)
}

fn needs_node_store(filters: &FiltersConfig) -> bool {
    filters
        .tables
        .values()
        .any(|table| table.geometry.way.enabled() || table.geometry.relation)
}

struct MetadataFields {
    id: i64,
    visible: Option<bool>,
    version: Option<i64>,
    changeset: Option<i64>,
    timestamp: Option<String>,
    uid: Option<i64>,
    user: Option<String>,
}

fn build_metadata_from_info(id: i64, info: &Info) -> MetadataFields {
    MetadataFields {
        id,
        visible: Some(info.visible()),
        version: info.version().map(i64::from),
        changeset: info.changeset(),
        timestamp: info.milli_timestamp().and_then(format_timestamp_millis),
        uid: info.uid().map(i64::from),
        user: info
            .user()
            .and_then(|user| user.ok())
            .map(|s| s.to_string()),
    }
}

fn build_metadata_from_dense_info(id: i64, info: &osmpbf::DenseNodeInfo) -> MetadataFields {
    MetadataFields {
        id,
        visible: Some(info.visible()),
        version: Some(i64::from(info.version())),
        changeset: Some(info.changeset()),
        timestamp: format_timestamp_millis(info.milli_timestamp()),
        uid: Some(i64::from(info.uid())),
        user: info.user().ok().map(|s| s.to_string()),
    }
}

fn format_timestamp_millis(millis: i64) -> Option<String> {
    let nanos = i128::from(millis) * 1_000_000;
    let dt = OffsetDateTime::from_unix_timestamp_nanos(nanos).ok()?;
    dt.format(&Rfc3339).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_match_supports_star_suffix() {
        assert!(glob_match("*_link", "motorway_link"));
        assert!(!glob_match("*_link", "motorway"));
    }

    #[test]
    fn matches_tag_values_with_glob() {
        let mut tags = HashMap::new();
        tags.insert("highway".to_string(), "trunk_link".to_string());
        let tag_match = TagMatch {
            tag: "highway".to_string(),
            value: None,
            values: vec!["primary".to_string(), "*_link".to_string()],
        };
        let filter = FilterExpr::Tag(tag_match);
        assert!(matches_filter(&filter, &tags));
    }

    #[test]
    fn closed_way_can_be_linestring() {
        let geometry_cfg = config::GeometryConfig {
            way: config::WaySetting::Enabled(config::WayGeometryMode::Linestring),
            closed_way: ClosedWayMode::Linestring,
            node: true,
            relation: false,
        };
        let coords = vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)];
        let line_string = LineString::from(coords.clone());
        let geometry = build_way_geometry(&geometry_cfg, line_string, &coords);
        assert!(matches!(geometry, Geometry::LineString(_)));
    }

    #[test]
    fn meta_columns_populate_feature_row() {
        let columns = vec![config::ColumnConfig {
            name: "timestamp".to_string(),
            source: "meta:timestamp".to_string(),
            col_type: "string".to_string(),
        }];
        let metadata = MetadataFields {
            id: 1,
            visible: Some(true),
            version: Some(1),
            changeset: Some(2),
            timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            uid: Some(3),
            user: Some("tester".to_string()),
        };
        let row = build_feature_row(
            Geometry::Point(Point::new(0.0, 0.0)),
            &HashMap::new(),
            &columns,
            &RuntimeConfig::default(),
            Some(metadata),
        );
        assert!(matches!(
            row.columns.get("timestamp"),
            Some(ColumnValue::String(value)) if value == "2024-01-01T00:00:00Z"
        ));
    }
}
