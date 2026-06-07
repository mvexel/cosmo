use std::path::Path;

use crate::config::NodeCacheMode;

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
                    format!("dense (auto-selected for {size_gb:.1} GB input)"),
                )
            } else {
                (
                    NodeCacheMode::Sparse,
                    format!("sparse (auto-selected for {size_gb:.1} GB input)"),
                )
            }
        }
        requested => (requested, requested.label().to_string()),
    }
}
