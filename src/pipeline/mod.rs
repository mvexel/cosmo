use anyhow::Result;
use geo::algorithm::bounding_rect::BoundingRect;
use geo::algorithm::centroid::Centroid;
use geo::algorithm::intersects::Intersects;
use geo::algorithm::winding_order::Winding;
use geo_types::{Geometry, LineString, MultiPolygon, Point, Polygon, Rect};
use osmpbf::{Element, PrimitiveBlock, RelMemberType};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::config::{
    ClosedWayMode, ColumnSource, CompiledColumn, CompiledConfig, RuntimeConfig, WayGeometryMode,
};
use crate::dsl::{evaluate_filter, evaluate_filter_with_context, FilterContext};
use crate::expr::{cel_value_to_string, evaluate_cel};
use crate::mapping::evaluate_mapping;
use crate::metadata::{
    build_metadata_from_dense_info, build_metadata_from_info, extract_meta_value,
    format_timestamp_millis, ElementType, MetadataAvailability, MetadataFields,
};
use crate::sinks::{ColumnType, ColumnValue, FeatureRow};
use crate::storage::NodeStoreReader;
use crate::utils::build_tag_map;

/// In-memory store mapping way ID → node ref IDs, used for relation assembly.
pub type WayStore = HashMap<i64, Vec<i64>>;

/// Check if a geometry intersects with a bounding box.
/// Returns true if no bbox is set (no filtering), or if the geometry intersects the bbox.
fn geometry_intersects_bbox(geometry: &Geometry<f64>, bbox: Option<[f64; 4]>) -> bool {
    let Some([min_lon, min_lat, max_lon, max_lat]) = bbox else {
        return true; // No bbox filter, pass everything
    };

    // Create a Rect from the bbox (Rect uses (x,y) = (lon,lat) convention)
    let bbox_rect = Rect::new(
        geo_types::Coord {
            x: min_lon,
            y: min_lat,
        },
        geo_types::Coord {
            x: max_lon,
            y: max_lat,
        },
    );

    match geometry {
        Geometry::Point(p) => bbox_rect.intersects(p),
        Geometry::LineString(ls) => bbox_rect.intersects(ls),
        Geometry::Polygon(poly) => bbox_rect.intersects(poly),
        Geometry::MultiPoint(mp) => bbox_rect.intersects(mp),
        Geometry::MultiLineString(mls) => bbox_rect.intersects(mls),
        Geometry::MultiPolygon(mpoly) => bbox_rect.intersects(mpoly),
        Geometry::GeometryCollection(gc) => gc.iter().any(|g| geometry_intersects_bbox(g, bbox)),
        _ => {
            // For other geometry types, use bounding rect as fallback
            if let Some(geom_rect) = geometry.bounding_rect() {
                bbox_rect.intersects(&geom_rect)
            } else {
                true // If we can't determine, include it
            }
        }
    }
}

pub trait BlockProcessor: Send + Sync {
    fn process_block(&self, block: PrimitiveBlock) -> Result<Vec<(String, FeatureRow)>>;
}

pub struct StandardProcessor {
    pub config: Arc<CompiledConfig>,
    pub runtime: Arc<RuntimeConfig>,
    pub node_store: Arc<NodeStoreReader>,
    pub way_store: Arc<WayStore>,
    pub metadata_availability: Option<Arc<MetadataAvailability>>,
}

impl BlockProcessor for StandardProcessor {
    fn process_block(&self, block: PrimitiveBlock) -> Result<Vec<(String, FeatureRow)>> {
        process_block_collect(
            block,
            &self.config,
            &self.runtime,
            &self.node_store,
            &self.way_store,
            self.metadata_availability.as_deref(),
        )
    }
}

pub struct NodesOnlyProcessor {
    pub config: Arc<CompiledConfig>,
    pub runtime: Arc<RuntimeConfig>,
    pub metadata_availability: Option<Arc<MetadataAvailability>>,
}

impl BlockProcessor for NodesOnlyProcessor {
    fn process_block(&self, block: PrimitiveBlock) -> Result<Vec<(String, FeatureRow)>> {
        process_block_nodes_only_collect(
            block,
            &self.config,
            &self.runtime,
            self.metadata_availability.as_deref(),
        )
    }
}

pub fn build_feature_row(
    geometry: Geometry<f64>,
    tags: &HashMap<String, String>,
    columns: &[CompiledColumn],
    runtime: &RuntimeConfig,
    metadata: Option<&MetadataFields>,
    refs: Option<Vec<i64>>,
    config: &CompiledConfig,
) -> FeatureRow {
    let mut column_values: HashMap<String, ColumnValue> = HashMap::new();

    for col in columns {
        let value = match &col.source {
            ColumnSource::Tag(key) => tags.get(key).map(|v| match col.col_type {
                ColumnType::Integer => v
                    .parse::<i64>()
                    .map(ColumnValue::Integer)
                    .unwrap_or_else(|_| ColumnValue::String(v.clone())),
                ColumnType::Float => v
                    .parse::<f64>()
                    .map(ColumnValue::Float)
                    .unwrap_or_else(|_| ColumnValue::String(v.clone())),
                _ => ColumnValue::String(v.clone()),
            }),
            ColumnSource::Meta(key) => {
                if let Some(m) = metadata {
                    extract_meta_value(key, Some(m)).map(|v| match col.col_type {
                        ColumnType::Integer => v
                            .parse::<i64>()
                            .map(ColumnValue::Integer)
                            .unwrap_or_else(|_| ColumnValue::String(v)),
                        ColumnType::Float => v
                            .parse::<f64>()
                            .map(ColumnValue::Float)
                            .unwrap_or_else(|_| ColumnValue::String(v)),
                        _ => ColumnValue::String(v),
                    })
                } else {
                    None
                }
            }
            ColumnSource::AllTags => Some(ColumnValue::Json(
                serde_json::to_value(tags).unwrap_or(Value::Null),
            )),
            ColumnSource::AllMeta => {
                if let Some(m) = metadata {
                    let json_meta = serde_json::json!({
                        "id": m.id,
                        "type": m.element_type.as_str(),
                        "version": m.version,
                        "timestamp": m.timestamp_millis.and_then(format_timestamp_millis),
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
            ColumnSource::Refs => refs
                .as_ref()
                .map(|r| ColumnValue::Json(serde_json::to_value(r).unwrap_or(Value::Null))),
            ColumnSource::Mapping(name) => config
                .mappings
                .get(name)
                .and_then(|m| evaluate_mapping(m, tags))
                .map(ColumnValue::String),
            ColumnSource::ConstString(value) => Some(ColumnValue::String(value.clone())),
            ColumnSource::ConstInteger(value) => Some(ColumnValue::Integer(*value)),
            ColumnSource::ConstFloat(value) => Some(ColumnValue::Float(*value)),
            ColumnSource::ConstJson(value) => Some(ColumnValue::Json(value.clone())),
            ColumnSource::Cel(program) => match metadata {
                Some(m) => {
                    let mut meta_map = HashMap::new();
                    meta_map.insert("id".to_string(), m.id.to_string());
                    meta_map.insert("type".to_string(), m.element_type.as_str().to_string());
                    if let Some(v) = &m.version {
                        meta_map.insert("version".to_string(), v.to_string());
                    }
                    if let Some(t) = m.timestamp_millis.and_then(format_timestamp_millis) {
                        meta_map.insert("timestamp".to_string(), t);
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
            },
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

fn table_filter_matches(
    filter: &crate::dsl::FilterAst,
    tags: &HashMap<String, String>,
    metadata: Option<&MetadataFields>,
) -> bool {
    if filter.uses_metadata() {
        evaluate_filter_with_context(filter, &FilterContext { tags, metadata })
    } else {
        evaluate_filter(filter, tags)
    }
}

fn columns_need_metadata(columns: &[CompiledColumn]) -> bool {
    columns.iter().any(|col| {
        matches!(
            col.source,
            ColumnSource::Meta(_) | ColumnSource::AllMeta | ColumnSource::Cel(_)
        )
    })
}

fn observe_metadata(
    metadata_availability: Option<&MetadataAvailability>,
    metadata: Option<&MetadataFields>,
) {
    if let Some(metadata_availability) = metadata_availability {
        metadata_availability.record(metadata);
    }
}

fn get_or_insert_observed_metadata<'a>(
    metadata: &'a mut Option<MetadataFields>,
    metadata_availability: Option<&MetadataAvailability>,
    build: impl FnOnce() -> MetadataFields,
) -> &'a MetadataFields {
    if metadata.is_none() {
        let value = build();
        observe_metadata(metadata_availability, Some(&value));
        *metadata = Some(value);
    }

    metadata.as_ref().expect("metadata was just inserted")
}

pub fn build_way_geometry(
    geometry_cfg: &crate::config::GeometryConfig,
    mut line_string: LineString<f64>,
    first_coord: (f64, f64),
) -> Option<Geometry<f64>> {
    if line_string.is_closed() {
        if !geometry_cfg.closed_way.enabled() {
            return None;
        }
        return Some(match geometry_cfg.closed_way.mode() {
            ClosedWayMode::Polygon => {
                line_string.make_ccw_winding();
                Geometry::Polygon(Polygon::new(line_string, vec![]))
            }
            ClosedWayMode::Centroid => {
                line_string.make_ccw_winding();
                let polygon = Polygon::new(line_string, vec![]);
                let centroid = polygon
                    .centroid()
                    .unwrap_or_else(|| Point::new(first_coord.0, first_coord.1));
                Geometry::Point(centroid)
            }
            ClosedWayMode::Linestring => Geometry::LineString(line_string),
        });
    }

    if !geometry_cfg.way.enabled() {
        return None;
    }

    Some(match geometry_cfg.way.mode() {
        WayGeometryMode::Linestring => Geometry::LineString(line_string),
        WayGeometryMode::Centroid => {
            let centroid = line_string
                .centroid()
                .unwrap_or_else(|| Point::new(first_coord.0, first_coord.1));
            Geometry::Point(centroid)
        }
    })
}

/// Assemble a multipolygon geometry from an OSM relation.
///
/// Collects outer and inner member ways, resolves their node references to
/// coordinates, joins way segments end-to-end into closed rings, and produces
/// a Polygon or MultiPolygon.
fn build_relation_geometry(
    rel: &osmpbf::Relation<'_>,
    node_store: &NodeStoreReader,
    way_store: &WayStore,
) -> Option<Geometry<f64>> {
    let mut outer_segments: Vec<Vec<(f64, f64)>> = Vec::new();
    let mut inner_segments: Vec<Vec<(f64, f64)>> = Vec::new();

    for member in rel.members() {
        if member.member_type != RelMemberType::Way {
            continue;
        }
        let role = member.role().unwrap_or("");
        let target = match role {
            "outer" | "" => &mut outer_segments, // empty role defaults to outer
            "inner" => &mut inner_segments,
            _ => continue,
        };

        if let Some(node_refs) = way_store.get(&member.member_id) {
            let coords: Vec<(f64, f64)> = node_refs
                .iter()
                .filter_map(|&id| node_store.get(id as u64))
                .collect();
            if coords.len() >= 2 {
                target.push(coords);
            }
        }
    }

    if outer_segments.is_empty() {
        return None;
    }

    let outer_rings = assemble_rings(outer_segments);
    if outer_rings.is_empty() {
        return None;
    }

    let inner_rings = assemble_rings(inner_segments);

    if outer_rings.len() == 1 {
        let mut exterior = LineString::from(outer_rings.into_iter().next().unwrap());
        exterior.make_ccw_winding();
        let holes: Vec<LineString<f64>> = inner_rings
            .into_iter()
            .map(|ring| {
                let mut ls = LineString::from(ring);
                // Inner rings should be CW (opposite of exterior)
                ls.make_cw_winding();
                ls
            })
            .collect();
        Some(Geometry::Polygon(Polygon::new(exterior, holes)))
    } else {
        // Multiple outer rings → MultiPolygon
        // Simple assignment: all inner rings go to the first polygon that contains them.
        // For most real-world data this is correct.
        let polygons: Vec<Polygon<f64>> = outer_rings
            .into_iter()
            .map(|ring| {
                let mut exterior = LineString::from(ring);
                exterior.make_ccw_winding();
                Polygon::new(exterior, vec![])
            })
            .collect();

        let mut result_polygons: Vec<Polygon<f64>> = Vec::with_capacity(polygons.len());
        let mut unassigned_holes: Vec<LineString<f64>> = Vec::new();

        // First pass: create polygons without holes
        for poly in &polygons {
            result_polygons.push(poly.clone());
        }

        // Second pass: assign inner rings to containing polygons
        for ring in inner_rings {
            let mut ls = LineString::from(ring);
            ls.make_cw_winding();

            // Find the polygon that contains the first point of this hole
            let test_point = ls.0.first().map(|c| Point::new(c.x, c.y));
            let mut assigned = false;
            if let Some(pt) = test_point {
                use geo::algorithm::contains::Contains;
                for poly in &mut result_polygons {
                    if poly.contains(&pt) {
                        // Reconstruct with this hole added
                        let mut holes: Vec<LineString<f64>> = poly.interiors().to_vec();
                        holes.push(ls.clone());
                        *poly = Polygon::new(poly.exterior().clone(), holes);
                        assigned = true;
                        break;
                    }
                }
            }
            if !assigned {
                unassigned_holes.push(ls);
            }
        }

        if !unassigned_holes.is_empty() {
            tracing::debug!(
                "Relation {}: {} inner ring(s) could not be assigned to any outer ring",
                rel.id(),
                unassigned_holes.len()
            );
        }

        Some(Geometry::MultiPolygon(MultiPolygon::new(result_polygons)))
    }
}

/// Join coordinate segments end-to-end into closed rings.
///
/// OSM multipolygon rings are often split across multiple ways that share
/// endpoints. This function greedily joins segments by matching the last
/// coordinate of the current chain to the first or last coordinate of an
/// unused segment, building closed rings.
fn assemble_rings(segments: Vec<Vec<(f64, f64)>>) -> Vec<Vec<(f64, f64)>> {
    if segments.is_empty() {
        return vec![];
    }

    let mut unused: Vec<Option<Vec<(f64, f64)>>> = segments.into_iter().map(Some).collect();
    let mut rings: Vec<Vec<(f64, f64)>> = Vec::new();

    loop {
        // Find next unused segment to start a chain
        let start_idx = unused.iter().position(|s| s.is_some());
        let Some(start_idx) = start_idx else {
            break;
        };
        let mut chain = unused[start_idx].take().unwrap();

        // Try to extend the chain by joining segments
        loop {
            let tail = *chain.last().unwrap();
            let head = chain[0];

            // Check if ring is already closed
            if chain.len() >= 4 && (tail.0 - head.0).abs() < 1e-9 && (tail.1 - head.1).abs() < 1e-9
            {
                break;
            }

            // Find a segment whose start or end matches our tail
            let mut found = false;
            for slot in unused.iter_mut() {
                if let Some(seg) = slot {
                    let seg_head = seg[0];
                    let seg_tail = *seg.last().unwrap();

                    if (tail.0 - seg_head.0).abs() < 1e-9 && (tail.1 - seg_head.1).abs() < 1e-9 {
                        // tail matches seg start → append seg (skip first coord, it's the join point)
                        let mut seg = slot.take().unwrap();
                        chain.extend(seg.drain(1..));
                        found = true;
                        break;
                    } else if (tail.0 - seg_tail.0).abs() < 1e-9
                        && (tail.1 - seg_tail.1).abs() < 1e-9
                    {
                        // tail matches seg end → append reversed seg
                        let mut seg = slot.take().unwrap();
                        seg.reverse();
                        chain.extend(seg.drain(1..));
                        found = true;
                        break;
                    }
                }
            }

            if !found {
                // Can't extend further — ring may be incomplete
                break;
            }
        }

        if chain.len() >= 4 {
            // Close ring if not already closed
            let head = chain[0];
            let tail = *chain.last().unwrap();
            if (tail.0 - head.0).abs() >= 1e-9 || (tail.1 - head.1).abs() >= 1e-9 {
                chain.push(head);
            }
            rings.push(chain);
        }
    }

    rings
}

pub fn process_block_collect(
    block: PrimitiveBlock,
    config: &CompiledConfig,
    runtime: &RuntimeConfig,
    node_store: &NodeStoreReader,
    way_store: &WayStore,
    metadata_availability: Option<&MetadataAvailability>,
) -> Result<Vec<(String, FeatureRow)>> {
    let mut rows = Vec::new();

    for element in block.elements() {
        match element {
            Element::Node(node) => {
                let tag_map = build_tag_map(node.tags());
                let mut metadata: Option<MetadataFields> = None;

                // Evaluate ALL table filters
                for (table_name, table) in &config.tables {
                    if !table.geometry.node {
                        continue;
                    }

                    let matches_filter = if table.filter.uses_metadata() {
                        let metadata_ref = get_or_insert_observed_metadata(
                            &mut metadata,
                            metadata_availability,
                            || build_metadata_from_info(node.id(), ElementType::Node, &node.info()),
                        );
                        table_filter_matches(&table.filter, &tag_map, Some(metadata_ref))
                    } else {
                        table_filter_matches(&table.filter, &tag_map, None)
                    };

                    if matches_filter {
                        let geometry = Geometry::Point(Point::new(node.lon(), node.lat()));
                        if geometry_intersects_bbox(&geometry, runtime.bbox) {
                            let row_metadata = if columns_need_metadata(&table.columns) {
                                let metadata_ref = get_or_insert_observed_metadata(
                                    &mut metadata,
                                    metadata_availability,
                                    || {
                                        build_metadata_from_info(
                                            node.id(),
                                            ElementType::Node,
                                            &node.info(),
                                        )
                                    },
                                );
                                Some(metadata_ref)
                            } else {
                                None
                            };
                            let row = build_feature_row(
                                geometry,
                                &tag_map,
                                &table.columns,
                                runtime,
                                row_metadata,
                                None,
                                config,
                            );
                            rows.push((table_name.clone(), row));
                        }
                    }
                }
            }
            Element::DenseNode(node) => {
                let tag_map = build_tag_map(node.tags());
                let mut metadata: Option<MetadataFields> = None;
                let mut metadata_loaded = false;

                for (table_name, table) in &config.tables {
                    if !table.geometry.node {
                        continue;
                    }

                    let matches_filter = if table.filter.uses_metadata() {
                        if !metadata_loaded {
                            metadata = node
                                .info()
                                .map(|info| build_metadata_from_dense_info(node.id(), info));
                            observe_metadata(metadata_availability, metadata.as_ref());
                            metadata_loaded = true;
                        }
                        table_filter_matches(&table.filter, &tag_map, metadata.as_ref())
                    } else {
                        table_filter_matches(&table.filter, &tag_map, None)
                    };

                    if matches_filter {
                        let geometry = Geometry::Point(Point::new(node.lon(), node.lat()));
                        if geometry_intersects_bbox(&geometry, runtime.bbox) {
                            let row_metadata = if columns_need_metadata(&table.columns) {
                                if !metadata_loaded {
                                    metadata = node.info().map(|info| {
                                        build_metadata_from_dense_info(node.id(), info)
                                    });
                                    observe_metadata(metadata_availability, metadata.as_ref());
                                    metadata_loaded = true;
                                }
                                metadata.as_ref()
                            } else {
                                None
                            };
                            let row = build_feature_row(
                                geometry,
                                &tag_map,
                                &table.columns,
                                runtime,
                                row_metadata,
                                None,
                                config,
                            );
                            rows.push((table_name.clone(), row));
                        }
                    }
                }
            }
            Element::Way(way) => {
                let tag_map = build_tag_map(way.tags());
                let mut metadata: Option<MetadataFields> = None;

                for (table_name, table) in &config.tables {
                    if !table.geometry.processes_ways() {
                        continue;
                    }

                    let matches_filter = if table.filter.uses_metadata() {
                        let metadata_ref = get_or_insert_observed_metadata(
                            &mut metadata,
                            metadata_availability,
                            || build_metadata_from_info(way.id(), ElementType::Way, &way.info()),
                        );
                        table_filter_matches(&table.filter, &tag_map, Some(metadata_ref))
                    } else {
                        table_filter_matches(&table.filter, &tag_map, None)
                    };

                    if !matches_filter {
                        continue;
                    }

                    let refs: Vec<i64> = way.refs().collect();
                    let ref_count = refs.len();
                    let coords: Vec<(f64, f64)> = refs
                        .iter()
                        .filter_map(|&id| node_store.get(id as u64))
                        .collect();

                    if coords.len() < ref_count {
                        static MISSING_NODE_WARNINGS: AtomicU64 = AtomicU64::new(0);
                        let count = MISSING_NODE_WARNINGS.fetch_add(1, Ordering::Relaxed);
                        let missing = ref_count - coords.len();
                        if count < 5 {
                            tracing::warn!(
                                "Way {} has {}/{} missing node references",
                                way.id(),
                                missing,
                                ref_count
                            );
                        } else if count == 5 {
                            tracing::warn!(
                                "Way {} has {}/{} missing node references (suppressing further warnings)",
                                way.id(), missing, ref_count
                            );
                        }
                    }
                    if coords.len() >= 2 {
                        let first_coord = coords[0];
                        let line_string = LineString::from(coords);
                        if let Some(geometry) =
                            build_way_geometry(&table.geometry, line_string, first_coord)
                        {
                            if geometry_intersects_bbox(&geometry, runtime.bbox) {
                                let row_metadata = if columns_need_metadata(&table.columns) {
                                    let metadata_ref = get_or_insert_observed_metadata(
                                        &mut metadata,
                                        metadata_availability,
                                        || {
                                            build_metadata_from_info(
                                                way.id(),
                                                ElementType::Way,
                                                &way.info(),
                                            )
                                        },
                                    );
                                    Some(metadata_ref)
                                } else {
                                    None
                                };
                                let row = build_feature_row(
                                    geometry,
                                    &tag_map,
                                    &table.columns,
                                    runtime,
                                    row_metadata,
                                    Some(refs),
                                    config,
                                );
                                rows.push((table_name.clone(), row));
                            }
                        }
                    }
                }
            }
            Element::Relation(rel) => {
                let tag_map = build_tag_map(rel.tags());

                // Process type=multipolygon and type=boundary relations
                let rel_type = tag_map.get("type").map(|s| s.as_str());
                if !matches!(rel_type, Some("multipolygon" | "boundary")) {
                    continue;
                }

                let mut metadata: Option<MetadataFields> = None;
                for (table_name, table) in &config.tables {
                    if !table.geometry.relation {
                        continue;
                    }

                    let matches_filter = if table.filter.uses_metadata() {
                        let metadata_ref = get_or_insert_observed_metadata(
                            &mut metadata,
                            metadata_availability,
                            || {
                                build_metadata_from_info(
                                    rel.id(),
                                    ElementType::Relation,
                                    &rel.info(),
                                )
                            },
                        );
                        table_filter_matches(&table.filter, &tag_map, Some(metadata_ref))
                    } else {
                        table_filter_matches(&table.filter, &tag_map, None)
                    };

                    if !matches_filter {
                        continue;
                    }

                    if let Some(geometry) = build_relation_geometry(&rel, node_store, way_store) {
                        if geometry_intersects_bbox(&geometry, runtime.bbox) {
                            let member_ids: Vec<i64> = rel.members().map(|m| m.member_id).collect();
                            let row_metadata = if columns_need_metadata(&table.columns) {
                                let metadata_ref = get_or_insert_observed_metadata(
                                    &mut metadata,
                                    metadata_availability,
                                    || {
                                        build_metadata_from_info(
                                            rel.id(),
                                            ElementType::Relation,
                                            &rel.info(),
                                        )
                                    },
                                );
                                Some(metadata_ref)
                            } else {
                                None
                            };
                            let row = build_feature_row(
                                geometry,
                                &tag_map,
                                &table.columns,
                                runtime,
                                row_metadata,
                                Some(member_ids),
                                config,
                            );
                            rows.push((table_name.clone(), row));
                        }
                    }
                }
            }
        }
    }

    Ok(rows)
}

pub fn process_block_nodes_only_collect(
    block: PrimitiveBlock,
    config: &CompiledConfig,
    runtime: &RuntimeConfig,
    metadata_availability: Option<&MetadataAvailability>,
) -> Result<Vec<(String, FeatureRow)>> {
    let mut rows = Vec::new();

    for element in block.elements() {
        match element {
            Element::Node(node) => {
                let tag_map = build_tag_map(node.tags());
                let mut metadata: Option<MetadataFields> = None;

                for (table_name, table) in &config.tables {
                    if !table.geometry.node {
                        continue;
                    }

                    let matches_filter = if table.filter.uses_metadata() {
                        let metadata_ref = get_or_insert_observed_metadata(
                            &mut metadata,
                            metadata_availability,
                            || build_metadata_from_info(node.id(), ElementType::Node, &node.info()),
                        );
                        table_filter_matches(&table.filter, &tag_map, Some(metadata_ref))
                    } else {
                        table_filter_matches(&table.filter, &tag_map, None)
                    };

                    if matches_filter {
                        let geometry = Geometry::Point(Point::new(node.lon(), node.lat()));
                        if geometry_intersects_bbox(&geometry, runtime.bbox) {
                            let row_metadata = if columns_need_metadata(&table.columns) {
                                let metadata_ref = get_or_insert_observed_metadata(
                                    &mut metadata,
                                    metadata_availability,
                                    || {
                                        build_metadata_from_info(
                                            node.id(),
                                            ElementType::Node,
                                            &node.info(),
                                        )
                                    },
                                );
                                Some(metadata_ref)
                            } else {
                                None
                            };
                            let row = build_feature_row(
                                geometry,
                                &tag_map,
                                &table.columns,
                                runtime,
                                row_metadata,
                                None,
                                config,
                            );
                            rows.push((table_name.clone(), row));
                        }
                    }
                }
            }
            Element::DenseNode(node) => {
                let tag_map = build_tag_map(node.tags());
                let mut metadata: Option<MetadataFields> = None;
                let mut metadata_loaded = false;

                for (table_name, table) in &config.tables {
                    if !table.geometry.node {
                        continue;
                    }

                    let matches_filter = if table.filter.uses_metadata() {
                        if !metadata_loaded {
                            metadata = node
                                .info()
                                .map(|info| build_metadata_from_dense_info(node.id(), info));
                            observe_metadata(metadata_availability, metadata.as_ref());
                            metadata_loaded = true;
                        }
                        table_filter_matches(&table.filter, &tag_map, metadata.as_ref())
                    } else {
                        table_filter_matches(&table.filter, &tag_map, None)
                    };

                    if matches_filter {
                        let geometry = Geometry::Point(Point::new(node.lon(), node.lat()));
                        if geometry_intersects_bbox(&geometry, runtime.bbox) {
                            let row_metadata = if columns_need_metadata(&table.columns) {
                                if !metadata_loaded {
                                    metadata = node.info().map(|info| {
                                        build_metadata_from_dense_info(node.id(), info)
                                    });
                                    observe_metadata(metadata_availability, metadata.as_ref());
                                    metadata_loaded = true;
                                }
                                metadata.as_ref()
                            } else {
                                None
                            };
                            let row = build_feature_row(
                                geometry,
                                &tag_map,
                                &table.columns,
                                runtime,
                                row_metadata,
                                None,
                                config,
                            );
                            rows.push((table_name.clone(), row));
                        }
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
    use crate::config::CompiledTable;
    use crate::sinks::ColumnType;
    use geo::algorithm::winding_order::Winding;

    #[test]
    fn polygon_exterior_is_ccw_regardless_of_input_winding() {
        let geometry_cfg = crate::config::GeometryConfig {
            way: crate::config::WaySetting::Disabled,
            closed_way: crate::config::ClosedWaySetting::Enabled(ClosedWayMode::Polygon),
            node: false,
            relation: false,
        };

        // CW input: (0,0) -> (0,1) -> (1,1) -> (1,0) -> (0,0)
        let cw_coords: Vec<(f64, f64)> =
            vec![(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)];
        let first = cw_coords[0];
        let cw_ring = LineString::from(cw_coords);
        assert!(cw_ring.is_cw(), "precondition: input should be CW");

        let geometry = build_way_geometry(&geometry_cfg, cw_ring, first).unwrap();
        let Geometry::Polygon(poly) = geometry else {
            panic!("expected polygon");
        };
        assert!(
            poly.exterior().is_ccw(),
            "polygon exterior must be CCW per RFC 7946"
        );

        // CCW input should stay CCW
        let ccw_coords: Vec<(f64, f64)> =
            vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)];
        let first = ccw_coords[0];
        let ccw_ring = LineString::from(ccw_coords);
        assert!(ccw_ring.is_ccw(), "precondition: input should be CCW");

        let geometry = build_way_geometry(&geometry_cfg, ccw_ring, first).unwrap();
        let Geometry::Polygon(poly) = geometry else {
            panic!("expected polygon");
        };
        assert!(poly.exterior().is_ccw(), "polygon exterior must remain CCW");
    }

    #[test]
    fn closed_way_can_be_linestring() {
        let geometry_cfg = crate::config::GeometryConfig {
            way: crate::config::WaySetting::Enabled(WayGeometryMode::Linestring),
            closed_way: crate::config::ClosedWaySetting::Enabled(ClosedWayMode::Linestring),
            node: true,
            relation: false,
        };
        let coords: Vec<(f64, f64)> = vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)];
        let first_coord = coords[0];
        let line_string = LineString::from(coords);
        let geometry = build_way_geometry(&geometry_cfg, line_string, first_coord);
        assert!(matches!(geometry, Some(Geometry::LineString(_))));
    }

    #[test]
    fn closed_way_polygon_with_disabled_open_way_emits_polygon() {
        // Regression for issue #1: way: false must not suppress closed-way polygons.
        let geometry_cfg = crate::config::GeometryConfig {
            way: crate::config::WaySetting::Disabled,
            closed_way: crate::config::ClosedWaySetting::Enabled(ClosedWayMode::Polygon),
            node: false,
            relation: false,
        };
        let coords: Vec<(f64, f64)> = vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)];
        let closed = LineString::from(coords.clone());
        assert!(matches!(
            build_way_geometry(&geometry_cfg, closed, coords[0]),
            Some(Geometry::Polygon(_))
        ));

        // Open way is still dropped because way is disabled.
        let open_coords: Vec<(f64, f64)> = vec![(0.0, 0.0), (1.0, 0.0), (2.0, 1.0)];
        let first_open = open_coords[0];
        let open = LineString::from(open_coords);
        assert!(build_way_geometry(&geometry_cfg, open, first_open).is_none());
    }

    #[test]
    fn disabled_closed_way_drops_closed_ways() {
        let geometry_cfg = crate::config::GeometryConfig {
            way: crate::config::WaySetting::Enabled(WayGeometryMode::Linestring),
            closed_way: crate::config::ClosedWaySetting::Disabled,
            node: false,
            relation: false,
        };
        let coords: Vec<(f64, f64)> = vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)];
        let closed = LineString::from(coords.clone());
        assert!(build_way_geometry(&geometry_cfg, closed, coords[0]).is_none());
    }

    #[test]
    fn open_way_centroid_uses_line_centroid() {
        let geometry_cfg = crate::config::GeometryConfig {
            way: crate::config::WaySetting::Enabled(WayGeometryMode::Centroid),
            closed_way: crate::config::ClosedWaySetting::Disabled,
            node: false,
            relation: false,
        };
        let coords: Vec<(f64, f64)> = vec![(0.0, 0.0), (2.0, 0.0), (2.0, 2.0)];
        let first_coord = coords[0];
        let open = LineString::from(coords);

        let geometry = build_way_geometry(&geometry_cfg, open, first_coord);
        let Some(Geometry::Point(point)) = geometry else {
            panic!("expected open way centroid point");
        };

        assert!((point.x() - 1.5).abs() < f64::EPSILON);
        assert!((point.y() - 0.5).abs() < f64::EPSILON);
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
            element_type: ElementType::Node,
            visible: Some(true),
            version: Some(1),
            changeset: Some(2),
            timestamp_millis: Some(1_704_067_200_000),
            uid: Some(3),
            user: Some("tester".to_string()),
        };
        let mut tables = HashMap::new();
        tables.insert(
            "test".to_string(),
            CompiledTable {
                name: "test".to_string(),
                filter: crate::dsl::FilterAst::True,
                columns: Vec::new(),
                geometry: crate::config::GeometryConfig::default(),
            },
        );
        let config = CompiledConfig {
            tables,
            mappings: HashMap::new(),
        };
        let row = build_feature_row(
            Geometry::Point(Point::new(0.0, 0.0)),
            &HashMap::new(),
            &columns,
            &RuntimeConfig::default(),
            Some(&metadata),
            None,
            &config,
        );
        assert!(matches!(
            row.columns.get("timestamp"),
            Some(ColumnValue::String(value)) if value == "2024-01-01T00:00:00Z"
        ));
    }

    #[test]
    fn meta_column_with_integer_type_produces_typed_value() {
        let columns = vec![CompiledColumn {
            name: "version".to_string(),
            source: ColumnSource::Meta("version".to_string()),
            col_type: ColumnType::Integer,
        }];
        let metadata = MetadataFields {
            id: 1,
            element_type: ElementType::Node,
            visible: Some(true),
            version: Some(7),
            changeset: Some(2),
            timestamp_millis: Some(1_704_067_200_000),
            uid: Some(3),
            user: Some("tester".to_string()),
        };
        let mut tables = HashMap::new();
        tables.insert(
            "test".to_string(),
            CompiledTable {
                name: "test".to_string(),
                filter: crate::dsl::FilterAst::True,
                columns: Vec::new(),
                geometry: crate::config::GeometryConfig::default(),
            },
        );
        let config = CompiledConfig {
            tables,
            mappings: HashMap::new(),
        };
        let row = build_feature_row(
            Geometry::Point(Point::new(0.0, 0.0)),
            &HashMap::new(),
            &columns,
            &RuntimeConfig::default(),
            Some(&metadata),
            None,
            &config,
        );
        assert!(matches!(
            row.columns.get("version"),
            Some(ColumnValue::Integer(7))
        ));
    }

    #[test]
    fn build_way_geometry_returns_none_for_two_point_open_way_when_disabled() {
        let geometry_cfg = crate::config::GeometryConfig {
            way: crate::config::WaySetting::Disabled,
            closed_way: crate::config::ClosedWaySetting::Disabled,
            node: false,
            relation: false,
        };
        let coords: Vec<(f64, f64)> = vec![(0.0, 0.0), (1.0, 1.0)];
        let first_coord = coords[0];
        let line_string = LineString::from(coords);
        assert!(build_way_geometry(&geometry_cfg, line_string, first_coord).is_none());
    }

    #[test]
    fn closed_way_centroid_produces_point() {
        let geometry_cfg = crate::config::GeometryConfig {
            way: crate::config::WaySetting::Disabled,
            closed_way: crate::config::ClosedWaySetting::Enabled(ClosedWayMode::Centroid),
            node: false,
            relation: false,
        };
        let coords: Vec<(f64, f64)> =
            vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)];
        let first_coord = coords[0];
        let line_string = LineString::from(coords);
        let geometry = build_way_geometry(&geometry_cfg, line_string, first_coord);
        let Some(Geometry::Point(point)) = geometry else {
            panic!("expected Point geometry from closed way centroid");
        };
        assert!((point.x() - 0.5).abs() < f64::EPSILON);
        assert!((point.y() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn tag_column_with_integer_type_produces_typed_value() {
        let columns = vec![CompiledColumn {
            name: "lanes".to_string(),
            source: ColumnSource::Tag("lanes".to_string()),
            col_type: ColumnType::Integer,
        }];
        let mut tags = HashMap::new();
        tags.insert("lanes".to_string(), "4".to_string());
        let mut tables = HashMap::new();
        tables.insert(
            "test".to_string(),
            CompiledTable {
                name: "test".to_string(),
                filter: crate::dsl::FilterAst::True,
                columns: Vec::new(),
                geometry: crate::config::GeometryConfig::default(),
            },
        );
        let config = CompiledConfig {
            tables,
            mappings: HashMap::new(),
        };
        let row = build_feature_row(
            Geometry::Point(Point::new(0.0, 0.0)),
            &tags,
            &columns,
            &RuntimeConfig::default(),
            None,
            None,
            &config,
        );
        assert!(matches!(
            row.columns.get("lanes"),
            Some(ColumnValue::Integer(4))
        ));
    }

    #[test]
    fn tag_column_with_float_type_produces_typed_value() {
        let columns = vec![CompiledColumn {
            name: "height".to_string(),
            source: ColumnSource::Tag("height".to_string()),
            col_type: ColumnType::Float,
        }];
        let mut tags = HashMap::new();
        tags.insert("height".to_string(), "12.5".to_string());
        let mut tables = HashMap::new();
        tables.insert(
            "test".to_string(),
            CompiledTable {
                name: "test".to_string(),
                filter: crate::dsl::FilterAst::True,
                columns: Vec::new(),
                geometry: crate::config::GeometryConfig::default(),
            },
        );
        let config = CompiledConfig {
            tables,
            mappings: HashMap::new(),
        };
        let row = build_feature_row(
            Geometry::Point(Point::new(0.0, 0.0)),
            &tags,
            &columns,
            &RuntimeConfig::default(),
            None,
            None,
            &config,
        );
        assert!(matches!(
            row.columns.get("height"),
            Some(ColumnValue::Float(v)) if (*v - 12.5).abs() < f64::EPSILON
        ));
    }

    #[test]
    fn tag_column_with_integer_type_falls_back_to_string_on_parse_failure() {
        let columns = vec![CompiledColumn {
            name: "lanes".to_string(),
            source: ColumnSource::Tag("lanes".to_string()),
            col_type: ColumnType::Integer,
        }];
        let mut tags = HashMap::new();
        tags.insert("lanes".to_string(), "two".to_string());
        let mut tables = HashMap::new();
        tables.insert(
            "test".to_string(),
            CompiledTable {
                name: "test".to_string(),
                filter: crate::dsl::FilterAst::True,
                columns: Vec::new(),
                geometry: crate::config::GeometryConfig::default(),
            },
        );
        let config = CompiledConfig {
            tables,
            mappings: HashMap::new(),
        };
        let row = build_feature_row(
            Geometry::Point(Point::new(0.0, 0.0)),
            &tags,
            &columns,
            &RuntimeConfig::default(),
            None,
            None,
            &config,
        );
        assert!(matches!(
            row.columns.get("lanes"),
            Some(ColumnValue::String(v)) if v == "two"
        ));
    }

    #[test]
    fn tag_column_with_missing_tag_produces_no_value() {
        let columns = vec![CompiledColumn {
            name: "lanes".to_string(),
            source: ColumnSource::Tag("lanes".to_string()),
            col_type: ColumnType::Integer,
        }];
        let tags: HashMap<String, String> = HashMap::new();
        let mut tables = HashMap::new();
        tables.insert(
            "test".to_string(),
            CompiledTable {
                name: "test".to_string(),
                filter: crate::dsl::FilterAst::True,
                columns: Vec::new(),
                geometry: crate::config::GeometryConfig::default(),
            },
        );
        let config = CompiledConfig {
            tables,
            mappings: HashMap::new(),
        };
        let row = build_feature_row(
            Geometry::Point(Point::new(0.0, 0.0)),
            &tags,
            &columns,
            &RuntimeConfig::default(),
            None,
            None,
            &config,
        );
        assert!(!row.columns.contains_key("lanes"));
    }

    #[test]
    fn bbox_none_passes_everything() {
        let geom = Geometry::Point(Point::new(5.0, 5.0));
        assert!(geometry_intersects_bbox(&geom, None));
    }

    #[test]
    fn point_inside_bbox_passes() {
        let geom = Geometry::Point(Point::new(1.0, 1.0));
        assert!(geometry_intersects_bbox(&geom, Some([0.0, 0.0, 2.0, 2.0])));
    }

    #[test]
    fn point_outside_bbox_fails() {
        let geom = Geometry::Point(Point::new(5.0, 5.0));
        assert!(!geometry_intersects_bbox(&geom, Some([0.0, 0.0, 2.0, 2.0])));
    }

    #[test]
    fn linestring_crossing_bbox_passes() {
        let geom = Geometry::LineString(LineString::from(vec![(-1.0, 1.0), (3.0, 1.0)]));
        assert!(geometry_intersects_bbox(&geom, Some([0.0, 0.0, 2.0, 2.0])));
    }

    #[test]
    fn linestring_outside_bbox_fails() {
        let geom = Geometry::LineString(LineString::from(vec![(5.0, 5.0), (6.0, 6.0)]));
        assert!(!geometry_intersects_bbox(&geom, Some([0.0, 0.0, 2.0, 2.0])));
    }

    #[test]
    fn assemble_rings_single_closed_segment() {
        let segments = vec![vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]];
        let rings = assemble_rings(segments);
        assert_eq!(rings.len(), 1);
        assert_eq!(rings[0].first(), rings[0].last());
    }

    #[test]
    fn assemble_rings_two_segments_forward() {
        // Two segments that join end-to-start: A→B, B→C→A
        let seg1 = vec![(0.0, 0.0), (1.0, 0.0)];
        let seg2 = vec![(1.0, 0.0), (1.0, 1.0), (0.0, 0.0)];
        let rings = assemble_rings(vec![seg1, seg2]);
        assert_eq!(rings.len(), 1);
        assert_eq!(rings[0].first(), rings[0].last());
        assert_eq!(rings[0].len(), 4); // (0,0)→(1,0)→(1,1)→(0,0)
    }

    #[test]
    fn assemble_rings_reversed_segment() {
        // Second segment is reversed: A→B, then C→A→B (reversed = B→A→C)
        // Actually: seg1 = (0,0)→(1,0), seg2 = (1,1)→(1,0) — seg2 reversed joins
        let seg1 = vec![(0.0, 0.0), (1.0, 0.0)];
        let seg2 = vec![(1.0, 1.0), (1.0, 0.0)]; // reversed: (1,0)→(1,1)
        let seg3 = vec![(1.0, 1.0), (0.0, 0.0)]; // closes the ring
        let rings = assemble_rings(vec![seg1, seg2, seg3]);
        assert_eq!(rings.len(), 1);
        assert_eq!(rings[0].first(), rings[0].last());
    }

    #[test]
    fn assemble_rings_two_separate_rings() {
        let ring1 = vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)];
        let ring2 = vec![(5.0, 5.0), (6.0, 5.0), (6.0, 6.0), (5.0, 5.0)];
        let rings = assemble_rings(vec![ring1, ring2]);
        assert_eq!(rings.len(), 2);
    }

    #[test]
    fn assemble_rings_empty_input() {
        let rings = assemble_rings(vec![]);
        assert!(rings.is_empty());
    }

    #[test]
    fn assemble_rings_too_short_segment_dropped() {
        // A segment with only 2 points that doesn't join anything → too short for a ring
        let seg = vec![(0.0, 0.0), (1.0, 0.0)];
        let rings = assemble_rings(vec![seg]);
        assert!(rings.is_empty()); // needs >= 4 points for a valid ring
    }
}
