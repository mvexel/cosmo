use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::{CompiledConfig, NodeCacheMode, RuntimeConfig};
use crate::metadata::MetadataAvailability;
use crate::storage::NodeStoreWriter;

use super::cache::resolve_node_cache_mode;
use super::cli::ConvertArgs;
use super::passes::{
    needs_way_store, pass0_scan_relations, pass1_index_nodes, pass2_process, pass_nodes_only,
};
use super::reporting::user_metadata_warning;
use super::sink_setup::SinkMap;

pub fn process_pbf(
    args: &ConvertArgs,
    verbose: bool,
    config: Arc<CompiledConfig>,
    runtime: Arc<RuntimeConfig>,
    sinks: SinkMap,
    needs_nodes: bool,
) -> Result<HashMap<String, u64>> {
    let user_metadata_usage = config.user_metadata_references();
    let metadata_availability = if user_metadata_usage.user_name || user_metadata_usage.uid {
        Some(Arc::new(MetadataAvailability::default()))
    } else {
        None
    };

    tracing::info!("Node cache required: {}", needs_nodes);
    let result = if needs_nodes {
        if verbose {
            if let Ok(metadata) = std::fs::metadata(&args.input) {
                let size_gb = metadata.len() as f64 / (1024.0 * 1024.0 * 1024.0);
                tracing::info!("Input size: {:.2} GB", size_gb);
            }
        }

        let (resolved_mode, mode_desc) =
            resolve_node_cache_mode(runtime.node_cache_mode, &args.input);

        let node_store = match resolved_mode {
            NodeCacheMode::Sparse => {
                tracing::info!("Node cache: {} (temp file)", mode_desc);
                NodeStoreWriter::new_sparse()
                    .context("Pipeline: Failed to create sparse node store")?
            }
            NodeCacheMode::Dense => {
                if let Some(ref path) = args.node_cache {
                    tracing::info!(
                        "Node cache: {} at {:?} (max {} nodes)",
                        mode_desc,
                        path,
                        runtime.node_cache_max_nodes
                    );
                    NodeStoreWriter::new_dense(path, runtime.node_cache_max_nodes)
                        .context("Pipeline: Failed to create dense node store")?
                } else {
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
            NodeCacheMode::Auto => unreachable!("Auto mode should have been resolved"),
        };

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
        pass2_process(
            &args.input,
            config,
            runtime,
            node_store,
            way_store,
            sinks,
            metadata_availability.clone(),
        )?
    } else {
        tracing::info!("Single pass: Processing nodes (parallel)...");
        pass_nodes_only(
            &args.input,
            config,
            runtime,
            sinks,
            metadata_availability.clone(),
        )?
    };

    if let Some(stats) = metadata_availability.as_ref() {
        if let Some(warning) = user_metadata_warning(user_metadata_usage, stats.snapshot()) {
            tracing::warn!("{}", warning);
        }
    }

    Ok(result)
}
