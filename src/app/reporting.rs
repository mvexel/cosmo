use osmpbf::HeaderBlock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::config::{CompiledConfig, UserMetadataReferences};
use crate::metadata::MetadataAvailabilitySnapshot;
use crate::sinks::OutputFormat;

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
        "{table_count} table(s), {total_cols} total columns (nodes: {has_node}, ways: {has_way}, relations: {has_rel})"
    )
}

pub fn output_format_label(format: &OutputFormat) -> &'static str {
    format.metadata().canonical_name
}

pub fn log_sorted_header(header: &HeaderBlock, logged: &Arc<AtomicBool>) {
    let mut found = Vec::new();
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

pub fn user_metadata_warning(
    usage: UserMetadataReferences,
    metadata: MetadataAvailabilitySnapshot,
) -> Option<&'static str> {
    if !usage.user_name && !usage.uid {
        return None;
    }

    if metadata.inspected == 0 {
        return None;
    }

    if metadata.with_timestamp == 0
        && metadata.with_user_name == 0
        && metadata.with_nonzero_uid == 0
    {
        return Some(
            "Filter/output references user metadata, but this PBF appears to have no element metadata. Filters on meta:user or meta:uid will match nothing.",
        );
    }

    match (
        usage.user_name && metadata.has_stripped_user_names(),
        usage.uid && metadata.has_stripped_user_ids(),
    ) {
        (true, true) => Some(
            "Filter/output references meta:user and meta:uid, but this PBF appears to have user metadata stripped. Observed timestamps, but user names are empty and uids are 0; filters on meta:user or meta:uid will match nothing.",
        ),
        (true, false) => Some(
            "Filter/output references meta:user, but this PBF appears to have user names stripped. Observed timestamps, but user names are empty; filters on meta:user will match nothing.",
        ),
        (false, true) => Some(
            "Filter/output references meta:uid, but this PBF appears to have user ids stripped. Observed timestamps, but uids are 0; filters on meta:uid will match nothing.",
        ),
        (false, false) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_metadata_warning_mentions_stripped_names_when_user_filter_depends_on_them() {
        let usage = UserMetadataReferences {
            user_name: true,
            uid: false,
        };
        let metadata = MetadataAvailabilitySnapshot {
            inspected: 10,
            with_timestamp: 10,
            with_user_name: 0,
            with_nonzero_uid: 10,
        };

        let warning = user_metadata_warning(usage, metadata).unwrap();

        assert!(warning.contains("meta:user"));
        assert!(warning.contains("user names are empty"));
    }

    #[test]
    fn user_metadata_warning_is_suppressed_without_user_metadata_references() {
        let warning = user_metadata_warning(
            UserMetadataReferences::default(),
            MetadataAvailabilitySnapshot {
                inspected: 10,
                with_timestamp: 10,
                with_user_name: 0,
                with_nonzero_uid: 0,
            },
        );

        assert!(warning.is_none());
    }

    #[test]
    fn user_metadata_warning_is_suppressed_when_requested_user_names_are_present() {
        let warning = user_metadata_warning(
            UserMetadataReferences {
                user_name: true,
                uid: false,
            },
            MetadataAvailabilitySnapshot {
                inspected: 10,
                with_timestamp: 0,
                with_user_name: 10,
                with_nonzero_uid: 10,
            },
        );

        assert!(warning.is_none());
    }
}
