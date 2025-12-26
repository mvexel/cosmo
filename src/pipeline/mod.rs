use anyhow::Result;
use geo::algorithm::centroid::Centroid;
use geo_types::{Geometry, LineString, Point, Polygon};
use osmpbf::{Element, PrimitiveBlock};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::{
    self, ClosedWayMode, FilterExpr, FiltersConfig, RuntimeConfig, WayGeometryMode,
};
use crate::metadata::{
    MetadataFields, build_metadata_from_dense_info, build_metadata_from_info, extract_meta_value,
};
use crate::sinks::{ColumnType, ColumnValue, FeatureRow};
use crate::storage::NodeStoreReader;
use crate::utils::build_tag_map;
use crate::utils::matches_tag;

pub trait BlockProcessor: Send + Sync {
    fn process_block(&self, block: PrimitiveBlock) -> Result<Vec<FeatureRow>>;
}

pub struct StandardProcessor {
    pub filters: Arc<FiltersConfig>,
    pub runtime: Arc<RuntimeConfig>,
    pub node_store: Arc<NodeStoreReader>,
}

impl BlockProcessor for StandardProcessor {
    fn process_block(&self, block: PrimitiveBlock) -> Result<Vec<FeatureRow>> {
        process_block_collect(block, &self.filters, &self.runtime, &self.node_store)
    }
}

pub struct NodesOnlyProcessor {
    pub filters: Arc<FiltersConfig>,
    pub runtime: Arc<RuntimeConfig>,
}

impl BlockProcessor for NodesOnlyProcessor {
    fn process_block(&self, block: PrimitiveBlock) -> Result<Vec<FeatureRow>> {
        process_block_nodes_only_collect(block, &self.filters, &self.runtime)
    }
}

pub fn matches_filter(filter: &FilterExpr, tags: &HashMap<String, String>) -> bool {
    match filter {
        FilterExpr::Simple(map) => map.iter().all(|(k, v)| tags.get(k) == Some(v)),
        FilterExpr::Any { any } => any.iter().any(|expr| matches_filter(expr, tags)),
        FilterExpr::All { all } => all.iter().all(|expr| matches_filter(expr, tags)),
        FilterExpr::Not { not } => !matches_filter(not, tags),
        FilterExpr::Tag(tag_match) => matches_tag(tag_match, tags),
    }
}

pub fn build_feature_row(
    geometry: Geometry<f64>,
    tags: &HashMap<String, String>,
    columns: &[config::ColumnConfig],
    runtime: &RuntimeConfig,
    metadata: Option<MetadataFields>,
    refs: Option<Vec<i64>>,
) -> FeatureRow {
    let mut column_values: HashMap<String, ColumnValue> = HashMap::new();

    for col in columns {
        if col.source == "tags" {
            let json_tags: Map<String, Value> = tags
                .iter()
                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                .collect();
            column_values.insert(
                col.name.clone(),
                ColumnValue::Json(Value::Object(json_tags)),
            );
            continue;
        } else if col.source == "meta" {
            if let Some(meta) = &metadata {
                let json_meta = serde_json::json!({
                    "id": meta.id,
                    "version": meta.version,
                    "timestamp": meta.timestamp,
                    "user": meta.user,
                    "uid": meta.uid,
                    "changeset": meta.changeset,
                    "visible": meta.visible
                });
                column_values.insert(col.name.clone(), ColumnValue::Json(json_meta));
            }
            continue;
        } else if col.source == "refs" {
            if let Some(r) = &refs {
                let json_refs = serde_json::to_value(r).unwrap_or(Value::Null);
                column_values.insert(col.name.clone(), ColumnValue::Json(json_refs));
            }
            continue;
        }

        if col.source.starts_with("tag:") {
            let tag_key = &col.source[4..];
            if let Some(val) = tags.get(tag_key)
                && let Some(value) = parse_column_value(val, &col.col_type)
            {
                column_values.insert(col.name.clone(), value);
            }
        } else if col.source.starts_with("meta:")
            && let Some(meta_val) = extract_meta_value(&col.source[5..], metadata.as_ref())
            && let Some(value) = parse_column_value(&meta_val, &col.col_type)
        {
            column_values.insert(col.name.clone(), value);
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

pub fn parse_column_value(value: &str, col_type: &ColumnType) -> Option<ColumnValue> {
    match col_type {
        ColumnType::Integer => value.parse::<i64>().ok().map(ColumnValue::Integer),
        ColumnType::Float => value.parse::<f64>().ok().map(ColumnValue::Float),
        ColumnType::Json => serde_json::from_str(value).ok().map(ColumnValue::Json),
        ColumnType::String => Some(ColumnValue::String(value.to_string())),
    }
}

pub fn build_way_geometry(
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

pub fn process_block_collect(
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
                            None,
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
                            None,
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
                        let refs: Vec<i64> = way.refs().collect();
                        let coords: Vec<(f64, f64)> = refs
                            .iter()
                            .filter_map(|&id| node_store.get(id as u64))
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
                            Some(refs),
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

pub fn process_block_nodes_only_collect(
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
                            None,
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
                            None,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_way_can_be_linestring() {
        let geometry_cfg = config::GeometryConfig {
            way: config::WaySetting::Enabled(WayGeometryMode::Linestring),
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
            col_type: ColumnType::String,
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
            None,
        );
        assert!(matches!(
            row.columns.get("timestamp"),
            Some(ColumnValue::String(value)) if value == "2024-01-01T00:00:00Z"
        ));
    }
}
