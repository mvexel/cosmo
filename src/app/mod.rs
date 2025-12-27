use anyhow::{Context, Result, anyhow};
use clap::{Parser, ValueEnum};
use crossbeam_channel::bounded;
use osmpbf::{BlobDecode, BlobReader, Element, HeaderBlock};
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::config::{CompiledConfig, NodeCacheMode, RuntimeConfig};
use crate::pipeline::{BlockProcessor, NodesOnlyProcessor, StandardProcessor};
use crate::sinks::{
    ColumnSpec, DataSink, FeatureRow, GeoJsonSink, GeoJsonlSink, GeoParquetSink,
};
use crate::storage::{NodeStoreReader, NodeStoreWriter};
use crate::utils::ProgressCounter;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Input PBF file
    #[arg(short, long)]
    pub input: PathBuf,

    /// Output file (.geojson, .geojsonl, .parquet)
    #[arg(short, long)]
    pub output: PathBuf,

    /// Filter configuration file (YAML)
    #[arg(short, long)]
    pub filters: PathBuf,

    /// Force specific node cache mode
    #[arg(long, value_enum)]
    pub node_cache_mode: Option<NodeCacheMode>,

    /// Node cache directory (for dense mode)
    #[arg(long)]
    pub node_cache: Option<PathBuf>,

    /// Maximum nodes for dense cache (default: 16B)
    #[arg(long)]
    pub node_cache_max_nodes: Option<u64>,

    /// Number of threads (default: all cores)
    #[arg(short, long)]
    pub threads: Option<usize>,

    /// Enable verbose output
    #[arg(short, long)]
    pub verbose: bool,

    /// Output format (auto-detected if omitted)
    #[arg(long, value_enum)]
    pub format: Option<OutputFormat>,

    /// Include all tags in a 'tags' JSON column
    #[arg(long)]
    pub all_tags: bool,
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
}

pub type SinkHandle = Arc<Mutex<Box<dyn DataSink + Send>>>;

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

pub fn summarize_filters_compiled(config: &CompiledConfig) -> (String, usize, bool, bool, bool) {
    let table = &config.table;

    (
        table.name.clone(),
        table.columns.len(),
        table.geometry.node,
        table.geometry.way.enabled(),
        table.geometry.relation,
    )
}

pub fn output_format_label(format: &OutputFormat) -> &'static str {
    match format {
        OutputFormat::GeoJson => "geojson",
        OutputFormat::GeoJsonl => "geojsonl",
        OutputFormat::GeoParquet => "geoparquet",
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
    }
}

pub fn collect_columns(config: &CompiledConfig) -> Result<Vec<ColumnSpec>> {
    let table = &config.table;
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
    config.table.geometry.way.enabled() || config.table.geometry.relation
}

pub fn pass1_index_nodes(
    path: &Path,
    node_store: NodeStoreWriter,
    use_parallel: bool,
) -> Result<(NodeStoreWriter, u64)> {
    let mut reader = BlobReader::from_path(path)?;
    let (tx, rx) = bounded::<Vec<(u64, f64, f64)>>(64);
    let header_logged = Arc::new(AtomicBool::new(false));

    let writer = std::thread::spawn(move || -> Result<(NodeStoreWriter, u64)> {
        let mut node_store = node_store;
        let mut node_count = 0u64;
        let progress = ProgressCounter::new("Pass 1/2: indexing nodes", 100_000);

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
                            tx.send(batch).map_err(|err| {
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
                        tx.send(batch).map_err(|err| {
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

    Ok((node_store, node_count))
}

pub fn run_pass<P>(
    path: &Path,
    processor: Arc<P>,
    sink: SinkHandle,
    label: &'static str,
) -> Result<u64>
where
    P: BlockProcessor + 'static,
{
    let reader = BlobReader::from_path(path)?;
    let (tx, rx) = bounded::<Vec<FeatureRow>>(64);
    let progress = Arc::new(ProgressCounter::new(label, 100));

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
    let match_count = match writer_join {
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
    Ok(match_count)
}

pub fn pass2_process(
    path: &Path,
    config: Arc<CompiledConfig>,
    runtime: Arc<RuntimeConfig>,
    node_store: Arc<NodeStoreReader>,
    sink: SinkHandle,
) -> Result<u64> {
    let processor = Arc::new(StandardProcessor {
        config,
        runtime,
        node_store,
    });
    run_pass(path, processor, sink, "Pass 2/2: blocks")
}

pub fn pass_nodes_only(
    path: &Path,
    config: Arc<CompiledConfig>,
    runtime: Arc<RuntimeConfig>,
    sink: SinkHandle,
) -> Result<u64> {
    let processor = Arc::new(NodesOnlyProcessor { config, runtime });
    run_pass(path, processor, sink, "Single pass: blocks")
}

pub fn process_pbf(
    cli: &Cli,
    config: Arc<CompiledConfig>,
    runtime: Arc<RuntimeConfig>,
    sink: SinkHandle,
    needs_nodes: bool,
) -> Result<u64> {
    tracing::info!("Node cache required: {}", needs_nodes);
    if needs_nodes {
        if cli.verbose
            && let Ok(metadata) = std::fs::metadata(&cli.input)
        {
            let size_gb = metadata.len() as f64 / (1024.0 * 1024.0 * 1024.0);
            tracing::info!("Input size: {:.2} GB", size_gb);
        }
        // Resolve auto mode to concrete mode based on input file size
        let (resolved_mode, mode_desc) =
            resolve_node_cache_mode(runtime.node_cache_mode, &cli.input);

        // Create node store based on resolved mode
        let node_store = match resolved_mode {
            NodeCacheMode::Sparse => {
                tracing::info!("Node cache: {} (temp file)", mode_desc);
                NodeStoreWriter::new_sparse()
                    .context("Pipeline: Failed to create sparse node store")?
            }
            NodeCacheMode::Dense => {
                if let Some(ref path) = cli.node_cache {
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
            cli.input,
            pass1_mode
        );
        let (node_store, node_count) = pass1_index_nodes(&cli.input, node_store, use_parallel)?;
        tracing::info!("Indexed {} nodes.", node_count);

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
        let result = pass2_process(&cli.input, config, runtime, node_store, sink)?;

        // Temp file (if any) is cleaned up when node_store is dropped

        Ok(result)
    } else {
        tracing::info!("Single pass: Processing nodes (parallel)...");
        pass_nodes_only(&cli.input, config, runtime, sink)
    }
}
