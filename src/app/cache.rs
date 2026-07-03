use anyhow::{anyhow, Result};

use crate::config::NodeCacheMode;

const DENSE_BYTES_PER_NODE: u64 = 8;
const SPARSE_BYTES_PER_NODE: u64 = 16;

/// Auto mode stays conservative: memory is only selected when the pre-scan
/// proves the full node table is small enough to keep comfortably in RAM.
pub(crate) const AUTO_MEMORY_MAX_NODES: u64 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NodeCacheStats {
    pub node_count: u64,
    pub max_node_id: u64,
    pub node_ids_sorted: bool,
    last_node_id: Option<u64>,
}

impl NodeCacheStats {
    pub(crate) fn new() -> Self {
        Self {
            node_count: 0,
            max_node_id: 0,
            node_ids_sorted: true,
            last_node_id: None,
        }
    }

    #[cfg(test)]
    fn from_parts(node_count: u64, max_node_id: u64, node_ids_sorted: bool) -> Self {
        Self {
            node_count,
            max_node_id,
            node_ids_sorted,
            last_node_id: None,
        }
    }

    pub(crate) fn observe_node_id(&mut self, id: u64) {
        self.node_count = self.node_count.saturating_add(1);
        self.max_node_id = self.max_node_id.max(id);
        if let Some(last_id) = self.last_node_id {
            if id < last_id {
                self.node_ids_sorted = false;
            }
        }
        self.last_node_id = Some(id);
    }

    fn required_dense_nodes(&self) -> Result<u64> {
        self.max_node_id
            .checked_add(1)
            .ok_or_else(|| anyhow!("node ID {} is too large for dense cache", self.max_node_id))
    }

    fn density(&self) -> f64 {
        if self.node_count == 0 {
            return 0.0;
        }
        self.node_count as f64 / self.required_dense_nodes().unwrap_or(u64::MAX) as f64
    }
}

impl Default for NodeCacheStats {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedNodeCacheMode {
    pub mode: NodeCacheMode,
    pub description: String,
    pub dense_max_nodes: u64,
}

pub(crate) fn resolve_node_cache_mode(
    requested: NodeCacheMode,
    stats: Option<&NodeCacheStats>,
    configured_dense_max_nodes: u64,
) -> Result<ResolvedNodeCacheMode> {
    match requested {
        NodeCacheMode::Auto => resolve_auto_mode(
            stats.ok_or_else(|| anyhow!("node cache auto mode requires node pre-scan stats"))?,
            configured_dense_max_nodes,
        ),
        requested => Ok(ResolvedNodeCacheMode {
            mode: requested,
            description: requested.label().to_string(),
            dense_max_nodes: configured_dense_max_nodes,
        }),
    }
}

fn resolve_auto_mode(
    stats: &NodeCacheStats,
    configured_dense_max_nodes: u64,
) -> Result<ResolvedNodeCacheMode> {
    let dense_max_nodes = stats.required_dense_nodes()?;

    if stats.node_count <= AUTO_MEMORY_MAX_NODES {
        return Ok(ResolvedNodeCacheMode {
            mode: NodeCacheMode::Memory,
            description: format!(
                "memory (auto-selected: {}; below {} node memory limit)",
                describe_stats(stats),
                AUTO_MEMORY_MAX_NODES
            ),
            dense_max_nodes,
        });
    }

    let dense_within_limit = dense_max_nodes <= configured_dense_max_nodes;

    if !stats.node_ids_sorted {
        if dense_within_limit {
            return Ok(ResolvedNodeCacheMode {
                mode: NodeCacheMode::Dense,
                description: format!(
                    "dense (auto-selected: node IDs are not sorted, so sparse cache is unavailable; {})",
                    describe_stats(stats)
                ),
                dense_max_nodes,
            });
        }

        return Err(anyhow!(
            "node cache auto mode cannot select a safe cache: input node IDs are not sorted, sparse cache is unavailable, and max node ID {} exceeds --node-cache-max-nodes {}; use --node-cache-mode memory or increase --node-cache-max-nodes",
            stats.max_node_id,
            configured_dense_max_nodes
        ));
    }

    if !dense_within_limit {
        return Ok(ResolvedNodeCacheMode {
            mode: NodeCacheMode::Sparse,
            description: format!(
                "sparse (auto-selected: max node ID {} exceeds dense limit {}; {})",
                stats.max_node_id,
                configured_dense_max_nodes,
                describe_stats(stats)
            ),
            dense_max_nodes,
        });
    }

    let sparse_bytes = stats.node_count.saturating_mul(SPARSE_BYTES_PER_NODE);
    let dense_bytes = dense_max_nodes.saturating_mul(DENSE_BYTES_PER_NODE);
    if dense_bytes <= sparse_bytes {
        Ok(ResolvedNodeCacheMode {
            mode: NodeCacheMode::Dense,
            description: format!(
                "dense (auto-selected: dense cache {:.1} GiB <= sparse cache {:.1} GiB; {})",
                bytes_to_gib(dense_bytes),
                bytes_to_gib(sparse_bytes),
                describe_stats(stats)
            ),
            dense_max_nodes,
        })
    } else {
        Ok(ResolvedNodeCacheMode {
            mode: NodeCacheMode::Sparse,
            description: format!(
                "sparse (auto-selected: sparse cache {:.1} GiB < dense cache {:.1} GiB; {})",
                bytes_to_gib(sparse_bytes),
                bytes_to_gib(dense_bytes),
                describe_stats(stats)
            ),
            dense_max_nodes,
        })
    }
}

fn describe_stats(stats: &NodeCacheStats) -> String {
    let order = if stats.node_ids_sorted {
        "sorted"
    } else {
        "unsorted"
    };
    format!(
        "{} nodes, max node ID {}, {:.1}% density, {}",
        stats.node_count,
        stats.max_node_id,
        stats.density() * 100.0,
        order
    )
}

fn bytes_to_gib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0 * 1024.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_tracks_count_max_id_and_order() {
        let mut stats = NodeCacheStats::new();
        stats.observe_node_id(10);
        stats.observe_node_id(15);
        stats.observe_node_id(12);

        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.max_node_id, 15);
        assert!(!stats.node_ids_sorted);
    }

    #[test]
    fn auto_selects_memory_for_small_inputs() {
        let stats = NodeCacheStats::from_parts(AUTO_MEMORY_MAX_NODES, 13_000_000_000, false);
        let resolved = resolve_node_cache_mode(NodeCacheMode::Auto, Some(&stats), 16_000_000_000)
            .expect("auto resolves");

        assert_eq!(resolved.mode, NodeCacheMode::Memory);
        assert_eq!(resolved.dense_max_nodes, 13_000_000_001);
    }

    #[test]
    fn auto_selects_sparse_for_sorted_sparse_distribution() {
        let stats = NodeCacheStats::from_parts(2_000_000, 13_000_000_000, true);
        let resolved = resolve_node_cache_mode(NodeCacheMode::Auto, Some(&stats), 16_000_000_000)
            .expect("auto resolves");

        assert_eq!(resolved.mode, NodeCacheMode::Sparse);
    }

    #[test]
    fn auto_selects_dense_for_sorted_dense_distribution() {
        let stats = NodeCacheStats::from_parts(10_000_000, 12_000_000, true);
        let resolved = resolve_node_cache_mode(NodeCacheMode::Auto, Some(&stats), 16_000_000_000)
            .expect("auto resolves");

        assert_eq!(resolved.mode, NodeCacheMode::Dense);
        assert_eq!(resolved.dense_max_nodes, 12_000_001);
    }

    #[test]
    fn auto_selects_dense_for_unsorted_inputs_that_do_not_fit_memory() {
        let stats = NodeCacheStats::from_parts(2_000_000, 12_000_000, false);
        let resolved = resolve_node_cache_mode(NodeCacheMode::Auto, Some(&stats), 16_000_000_000)
            .expect("auto resolves");

        assert_eq!(resolved.mode, NodeCacheMode::Dense);
    }

    #[test]
    fn auto_errors_when_unsorted_input_exceeds_dense_limit() {
        let stats = NodeCacheStats::from_parts(2_000_000, 20_000_000_000, false);
        let err = resolve_node_cache_mode(NodeCacheMode::Auto, Some(&stats), 16_000_000_000)
            .expect_err("auto should fail");

        assert!(err.to_string().contains("cannot select a safe cache"));
    }

    #[test]
    fn manual_modes_do_not_require_stats() {
        let resolved =
            resolve_node_cache_mode(NodeCacheMode::Sparse, None, 42).expect("manual mode");

        assert_eq!(resolved.mode, NodeCacheMode::Sparse);
        assert_eq!(resolved.description, "sparse");
        assert_eq!(resolved.dense_max_nodes, 42);
    }
}
