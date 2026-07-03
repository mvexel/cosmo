use anyhow::Result;
use clap::{builder::PossibleValue, ValueEnum};
use geo_types::Geometry;
use serde_json::{Map, Value};
use std::collections::HashMap;

pub mod csv_wkt;
pub mod geojson;
pub mod geojsonl;
pub mod geoparquet;
pub mod maproulette;

pub use self::csv_wkt::CsvWktSink;
pub use self::geojson::GeoJsonSink;
pub use self::geojsonl::GeoJsonlSink;
pub use self::geoparquet::GeoParquetSink;
pub use self::maproulette::MapRouletteSink;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum OutputFormat {
    GeoJson,
    GeoJsonl,
    GeoParquet,
    Csv,
    MapRoulette,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct SinkFormatMetadata {
    pub format: OutputFormat,
    pub canonical_name: &'static str,
    pub aliases: &'static [&'static str],
    pub extensions: &'static [&'static str],
    pub default_extension: &'static str,
}

impl SinkFormatMetadata {
    fn matches_extension(&self, extension: &str) -> bool {
        self.extensions
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(extension))
    }
}

pub const SINK_FORMATS: &[SinkFormatMetadata] = &[
    SinkFormatMetadata {
        format: OutputFormat::GeoJson,
        canonical_name: "geojson",
        aliases: &[],
        extensions: &["geojson"],
        default_extension: "geojson",
    },
    SinkFormatMetadata {
        format: OutputFormat::GeoJsonl,
        canonical_name: "geojsonl",
        aliases: &[],
        extensions: &["geojsonl", "jsonl", "json"],
        default_extension: "geojsonl",
    },
    SinkFormatMetadata {
        format: OutputFormat::GeoParquet,
        canonical_name: "geoparquet",
        aliases: &["parquet"],
        extensions: &["parquet"],
        default_extension: "parquet",
    },
    SinkFormatMetadata {
        format: OutputFormat::Csv,
        canonical_name: "csv",
        aliases: &[],
        extensions: &["csv"],
        default_extension: "csv",
    },
    SinkFormatMetadata {
        format: OutputFormat::MapRoulette,
        canonical_name: "maproulette",
        aliases: &[],
        extensions: &[],
        default_extension: "geojson",
    },
];

static OUTPUT_FORMAT_VARIANTS: [OutputFormat; 5] = [
    OutputFormat::GeoJson,
    OutputFormat::GeoJsonl,
    OutputFormat::GeoParquet,
    OutputFormat::Csv,
    OutputFormat::MapRoulette,
];

impl OutputFormat {
    pub fn metadata(self) -> &'static SinkFormatMetadata {
        SINK_FORMATS
            .iter()
            .find(|metadata| metadata.format == self)
            .expect("output format should be registered")
    }

    pub fn from_extension(extension: &str) -> Option<Self> {
        SINK_FORMATS
            .iter()
            .find(|metadata| metadata.matches_extension(extension))
            .map(|metadata| metadata.format)
    }
}

impl ValueEnum for OutputFormat {
    fn value_variants<'a>() -> &'a [Self] {
        &OUTPUT_FORMAT_VARIANTS
    }

    fn to_possible_value(&self) -> Option<PossibleValue> {
        let metadata = self.metadata();
        let mut value = PossibleValue::new(metadata.canonical_name);
        for alias in metadata.aliases {
            value = value.alias(*alias);
        }
        Some(value)
    }
}

#[derive(Clone, Debug)]
pub enum ColumnValue {
    String(String),
    Integer(i64),
    Float(f64),
    Json(serde_json::Value),
}

pub use crate::config::ColumnType;

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
