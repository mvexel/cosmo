use anyhow::Result;
use geo::algorithm::centroid::Centroid;
use geo_types::{Geometry, LineString, Point, Polygon};
use osmpbf::{Element, PrimitiveBlock};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::{
    ClosedWayMode, ColumnSource, CompiledColumn, CompiledConfig, RuntimeConfig, WayGeometryMode,
};
use crate::dsl::evaluate_filter;
use crate::expr::{cel_value_to_string, evaluate_cel};
use crate::mapping::evaluate_mapping;
use crate::metadata::{
    MetadataFields, build_metadata_from_dense_info, build_metadata_from_info, extract_meta_value,
};
use crate::sinks::{ColumnValue, FeatureRow};
use crate::storage::NodeStoreReader;
use crate::utils::build_tag_map;

pub trait BlockProcessor: Send + Sync {
    fn process_block(&self, block: PrimitiveBlock) -> Result<Vec<FeatureRow>>;
}

pub struct StandardProcessor {
    pub config: Arc<CompiledConfig>,
    pub runtime: Arc<RuntimeConfig>,
    pub node_store: Arc<NodeStoreReader>,
}

impl BlockProcessor for StandardProcessor {
    fn process_block(&self, block: PrimitiveBlock) -> Result<Vec<FeatureRow>> {
        process_block_collect(block, &self.config, &self.runtime, &self.node_store)
    }
}

pub struct NodesOnlyProcessor {
    pub config: Arc<CompiledConfig>,
    pub runtime: Arc<RuntimeConfig>,
}

impl BlockProcessor for NodesOnlyProcessor {
    fn process_block(&self, block: PrimitiveBlock) -> Result<Vec<FeatureRow>> {
        process_block_nodes_only_collect(block, &self.config, &self.runtime)
    }
}

pub fn build_feature_row(
    geometry: Geometry<f64>,
    tags: &HashMap<String, String>,
    columns: &[CompiledColumn],
    runtime: &RuntimeConfig,
    metadata: Option<MetadataFields>,
    refs: Option<Vec<i64>>,
    config: &CompiledConfig,
) -> FeatureRow {
    let mut column_values: HashMap<String, ColumnValue> = HashMap::new();

    for col in columns {
        let value = match &col.source {
            ColumnSource::Tag(key) => tags.get(key).map(|v| ColumnValue::String(v.clone())),
            ColumnSource::Meta(key) => {
                if let Some(m) = &metadata {
                    extract_meta_value(key, Some(m)).map(ColumnValue::String)
                } else {
                    None
                }
            }
            ColumnSource::AllTags => Some(ColumnValue::Json(
                serde_json::to_value(tags).unwrap_or(Value::Null),
            )),
            ColumnSource::AllMeta => {
                if let Some(m) = &metadata {
                    let json_meta = serde_json::json!({
                        "id": m.id,
                        "version": m.version,
                        "timestamp": m.timestamp,
                        "user": m.user,
                        "uid": m.uid,
                        "changeset": m.changeset,
                        "visible": m.visible
                    });
                    Some(ColumnValue::Json(json_meta))
                } else {
                    None
                }
            }
            ColumnSource::Refs => {
                refs.as_ref().map(|r| {
                    ColumnValue::Json(serde_json::to_value(r).unwrap_or(Value::Null))
                })
            }
            ColumnSource::Mapping(name) => config
                .mappings
                .get(name)
                .and_then(|m| evaluate_mapping(m, tags))
                .map(ColumnValue::String),
            ColumnSource::Cel(program) => {
                match &metadata {
                    Some(m) => {
                        // TODO: Add metadata to CEL context properly if needed beyond tags
                        let mut meta_map = HashMap::new();
                        meta_map.insert("id".to_string(), m.id.to_string());
                        if let Some(v) = &m.version {
                            meta_map.insert("version".to_string(), v.to_string());
                        }
                        if let Some(t) = &m.timestamp {
                            meta_map.insert("timestamp".to_string(), t.clone());
                        }

                        let ctx = crate::expr::CelContext {
                            tags,
                            meta: &meta_map,
                        };
                        match evaluate_cel(program, &ctx) {
                            Ok(v) => cel_value_to_string(&v).map(ColumnValue::String),
                            Err(e) => {
                                tracing::debug!("CEL evaluation failed: {}", e);
                                None
                            }
                        }
                    }
                    None => {
                        let ctx = crate::expr::CelContext {
                            tags,
                            meta: &HashMap::new(),
                        };
                        match evaluate_cel(program, &ctx) {
                            Ok(v) => cel_value_to_string(&v).map(ColumnValue::String),
                            Err(e) => {
                                tracing::debug!("CEL evaluation failed: {}", e);
                                None
                            }
                        }
                    }
                }
            }
        };

        if let Some(val) = value {
            column_values.insert(col.name.clone(), val);
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

pub fn build_way_geometry(
    geometry_cfg: &crate::config::GeometryConfig,
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
    config: &CompiledConfig,
    runtime: &RuntimeConfig,
    node_store: &NodeStoreReader,
) -> Result<Vec<FeatureRow>> {
    let mut rows = Vec::new();
    let table = &config.table;

    for element in block.elements() {
        match element {
            Element::Node(node) => {
                let tag_map = build_tag_map(node.tags());
                if table.geometry.node && evaluate_filter(&table.filter, &tag_map) {
                    let row = build_feature_row(
                        Geometry::Point(Point::new(node.lon(), node.lat())),
                        &tag_map,
                        &table.columns,
                        runtime,
                        Some(build_metadata_from_info(node.id(), &node.info())),
                        None,
                        config,
                    );
                    rows.push(row);
                }
            }
            Element::DenseNode(node) => {
                let tag_map = build_tag_map(node.tags());
                if table.geometry.node && evaluate_filter(&table.filter, &tag_map) {
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
                        config,
                    );
                    rows.push(row);
                }
            }
            Element::Way(way) => {
                let tag_map = build_tag_map(way.tags());
                if table.geometry.way.enabled() && evaluate_filter(&table.filter, &tag_map) {
                    let refs: Vec<i64> = way.refs().collect();
                    let coords: Vec<(f64, f64)> = refs
                        .iter()
                        .filter_map(|&id| node_store.get(id as u64))
                        .collect();

                    if coords.len() >= 2 {
                        let line_string = LineString::from(coords.clone());
                        let geometry = build_way_geometry(&table.geometry, line_string, &coords);
                        let row = build_feature_row(
                            geometry,
                            &tag_map,
                            &table.columns,
                            runtime,
                            Some(build_metadata_from_info(way.id(), &way.info())),
                            Some(refs),
                            config,
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
    config: &CompiledConfig,
    runtime: &RuntimeConfig,
) -> Result<Vec<FeatureRow>> {
    let mut rows = Vec::new();
    let table = &config.table;

    for element in block.elements() {
        match element {
            Element::Node(node) => {
                let tag_map = build_tag_map(node.tags());
                if table.geometry.node && evaluate_filter(&table.filter, &tag_map) {
                    let row = build_feature_row(
                        Geometry::Point(Point::new(node.lon(), node.lat())),
                        &tag_map,
                        &table.columns,
                        runtime,
                        Some(build_metadata_from_info(node.id(), &node.info())),
                        None,
                        config,
                    );
                    rows.push(row);
                }
            }
            Element::DenseNode(node) => {
                let tag_map = build_tag_map(node.tags());
                if table.geometry.node && evaluate_filter(&table.filter, &tag_map) {
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
                        config,
                    );
                    rows.push(row);
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
    use crate::config::CompiledTable;
    use crate::sinks::ColumnType;

    #[test]
    fn closed_way_can_be_linestring() {
        let geometry_cfg = crate::config::GeometryConfig {
            way: crate::config::WaySetting::Enabled(WayGeometryMode::Linestring),
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
        let columns = vec![CompiledColumn {
            name: "timestamp".to_string(),
            source: ColumnSource::Meta("timestamp".to_string()),
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
        let config = CompiledConfig {
            table: CompiledTable {
                name: "test".to_string(),
                filter: crate::dsl::FilterAst::True,
                columns: Vec::new(),
                geometry: crate::config::GeometryConfig::default(),
            },
            mappings: HashMap::new(),
        };
        let row = build_feature_row(
            Geometry::Point(Point::new(0.0, 0.0)),
            &HashMap::new(),
            &columns,
            &RuntimeConfig::default(),
            Some(metadata),
            None,
            &config,
        );
        assert!(matches!(
            row.columns.get("timestamp"),
            Some(ColumnValue::String(value)) if value == "2024-01-01T00:00:00Z"
        ));
    }
}
