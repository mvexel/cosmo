use anyhow::Result;
use geo_types::Geometry;
use serde_json::{Map, Value};
use std::collections::HashMap;

pub mod geojson;
pub mod geojsonl;
pub mod geoparquet;

pub use self::geojson::GeoJsonSink;
pub use self::geojsonl::GeoJsonlSink;
pub use self::geoparquet::GeoParquetSink;

#[derive(Clone, Debug)]
pub enum ColumnValue {
    String(String),
    Integer(i64),
    Float(f64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnType {
    String,
    Integer,
    Float,
}

#[derive(Clone, Debug)]
pub struct ColumnSpec {
    pub name: String,
    pub col_type: ColumnType,
}

#[derive(Clone, Debug)]
pub struct FeatureRow {
    pub geometry: Geometry<f64>,
    pub columns: HashMap<String, ColumnValue>,
    pub extras: Map<String, Value>,
}

pub trait DataSink: Send {
    fn add_feature(&mut self, row: FeatureRow) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}
