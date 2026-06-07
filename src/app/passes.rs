use anyhow::{anyhow, Context, Result};
use crossbeam_channel::bounded;
use osmpbf::{BlobDecode, BlobReader, Element, RelMemberType};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use crate::app::cache::NodeCacheStats;
use crate::config::{CompiledConfig, RuntimeConfig};
use crate::metadata::MetadataAvailability;
use crate::pipeline::{BlockProcessor, NodesOnlyProcessor, StandardProcessor, WayStore};
use crate::sinks::FeatureRow;
use crate::storage::{NodeStoreReader, NodeStoreWriter};
use crate::utils::ProgressCounter;

use super::reporting::log_sorted_header;
use super::sink_setup::SinkMap;

pub(crate) fn needs_way_store(config: &CompiledConfig) -> bool {
    config.tables.values().any(|table| table.geometry.relation)
}

/// Pre-scan node IDs before selecting `--node-cache-mode auto`.
///
/// Sparse cache requires monotonically increasing node IDs. Dense cache cost is
/// driven by the highest node ID, not the compressed PBF size, so auto mode
/// needs these real input statistics before it can make a safe choice.
pub(crate) fn prescan_node_cache_stats(path: &Path) -> Result<NodeCacheStats> {
    let mut reader = BlobReader::from_path(path)?;
    let mut stats = NodeCacheStats::new();
    let progress = ProgressCounter::new("Pre-scan: nodes", 1_000_000);

    reader.try_for_each(|blob_result| -> Result<()> {
        let blob = blob_result?;
        match blob.decode() {
            Ok(BlobDecode::OsmData(block)) => {
                let mut node_count = 0u64;
                for element in block.elements() {
                    match element {
                        Element::Node(node) => {
                            stats.observe_node_id(node.id() as u64);
                            node_count += 1;
                        }
                        Element::DenseNode(node) => {
                            stats.observe_node_id(node.id() as u64);
                            node_count += 1;
                        }
                        _ => {}
                    }
                }
                if node_count > 0 {
                    progress.inc(node_count);
                }
                Ok(())
            }
            Ok(_) => Ok(()),
            Err(error) => Err(error.into()),
        }
    })?;

    progress.finish();
    tracing::info!(
        "Pre-scan: found {} nodes; max node ID {}; sorted: {}",
        stats.node_count,
        stats.max_node_id,
        stats.node_ids_sorted
    );
    Ok(stats)
}
/// Pass 0: Scan relations to collect the set of member way IDs that will be
/// needed for relation geometry assembly. This allows pass 1 to collect only
/// the referenced ways instead of all ways, keeping memory bounded for
/// planet-scale inputs.
pub(crate) fn pass0_scan_relations(path: &Path) -> Result<HashSet<i64>> {
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
                                if key == "type" && (value == "multipolygon" || value == "boundary")
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

pub(crate) fn pass1_index_nodes(
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
                    .with_context(|| format!("Pipeline: Failed writing node {id}"))?;
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
                                anyhow!("Pipeline: Failed to send node batch: {err}")
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
                        tx.send(node_batch)
                            .map_err(|err| anyhow!("Pipeline: Failed to send node batch: {err}"))?;
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

    let writer_join = writer.join();

    let (node_store, node_count) = match writer_join {
        Ok(Ok(result)) => result,
        Ok(Err(writer_err)) => {
            return if decode_result.is_err() {
                Err(writer_err.context("writer thread failed (caused channel disconnect)"))
            } else {
                Err(writer_err)
            };
        }
        Err(panic_payload) => {
            let panic_msg = panic_payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic_payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_string());
            return Err(anyhow!(
                "Pipeline: Node writer thread panicked: {panic_msg}"
            ));
        }
    };

    decode_result?;

    let way_store = Arc::try_unwrap(way_store)
        .expect("way_store Arc should have single owner after decode")
        .into_inner()
        .unwrap();
    Ok((node_store, node_count, way_store))
}

pub(crate) fn run_pass<P>(
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
                        .map_err(|e| anyhow!("Sink lock poisoned: {e}"))?;
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
                    .map_err(|err| anyhow!("Pipeline: Failed to send feature batch: {err}"))?;
            }

            Ok(())
        });

    drop(tx);

    let writer_join = writer.join();

    let match_counts = match writer_join {
        Ok(Ok(result)) => result,
        Ok(Err(writer_err)) => {
            return if decode_result.is_err() {
                Err(writer_err
                    .context("Pipeline: Sink writer thread failed (caused channel disconnect)"))
            } else {
                Err(writer_err)
            };
        }
        Err(panic_payload) => {
            let panic_msg = panic_payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic_payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_string());
            return Err(anyhow!(
                "Pipeline: Sink writer thread panicked: {panic_msg}"
            ));
        }
    };

    decode_result?;

    progress.finish();
    Ok(match_counts)
}

pub(crate) fn pass2_process(
    path: &Path,
    config: Arc<CompiledConfig>,
    runtime: Arc<RuntimeConfig>,
    node_store: Arc<NodeStoreReader>,
    way_store: Arc<WayStore>,
    sinks: SinkMap,
    metadata_availability: Option<Arc<MetadataAvailability>>,
) -> Result<HashMap<String, u64>> {
    let processor = Arc::new(StandardProcessor {
        config,
        runtime,
        node_store,
        way_store,
        metadata_availability,
    });
    run_pass(path, processor, sinks, "Pass 2/2: blocks")
}

pub(crate) fn pass_nodes_only(
    path: &Path,
    config: Arc<CompiledConfig>,
    runtime: Arc<RuntimeConfig>,
    sinks: SinkMap,
    metadata_availability: Option<Arc<MetadataAvailability>>,
) -> Result<HashMap<String, u64>> {
    let processor = Arc::new(NodesOnlyProcessor {
        config,
        runtime,
        metadata_availability,
    });
    run_pass(path, processor, sinks, "Single pass: blocks")
}
