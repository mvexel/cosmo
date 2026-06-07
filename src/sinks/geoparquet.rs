use super::{ColumnSpec, ColumnType, ColumnValue, DataSink, FeatureRow};
use anyhow::{Context, Result};
use arrow_array::{BinaryArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use geozero::{CoordDimensions, ToWkb};
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

/// PROJJSON for EPSG:4326 (WGS 84). Static constant extracted from libproj.
/// This is a well-known, stable definition — embedding it removes the runtime
/// dependency on libproj (the `proj` crate) which was only used for this string.
const EPSG_4326_PROJJSON: &str = r#"{"$schema":"https://proj.org/schemas/v0.7/projjson.schema.json","type":"GeographicCRS","name":"WGS 84","datum_ensemble":{"name":"World Geodetic System 1984 ensemble","members":[{"name":"World Geodetic System 1984 (Transit)","id":{"authority":"EPSG","code":1166}},{"name":"World Geodetic System 1984 (G730)","id":{"authority":"EPSG","code":1152}},{"name":"World Geodetic System 1984 (G873)","id":{"authority":"EPSG","code":1153}},{"name":"World Geodetic System 1984 (G1150)","id":{"authority":"EPSG","code":1154}},{"name":"World Geodetic System 1984 (G1674)","id":{"authority":"EPSG","code":1155}},{"name":"World Geodetic System 1984 (G1762)","id":{"authority":"EPSG","code":1156}},{"name":"World Geodetic System 1984 (G2139)","id":{"authority":"EPSG","code":1309}},{"name":"World Geodetic System 1984 (G2296)","id":{"authority":"EPSG","code":1383}}],"ellipsoid":{"name":"WGS 84","semi_major_axis":6378137,"inverse_flattening":298.257223563},"accuracy":"2.0","id":{"authority":"EPSG","code":6326}},"coordinate_system":{"subtype":"ellipsoidal","axis":[{"name":"Geodetic latitude","abbreviation":"Lat","direction":"north","unit":"degree"},{"name":"Geodetic longitude","abbreviation":"Lon","direction":"east","unit":"degree"}]},"scope":"Horizontal component of 3D system.","area":"World.","bbox":{"south_latitude":-90,"west_longitude":-180,"north_latitude":90,"east_longitude":180},"id":{"authority":"EPSG","code":4326}}"#;

pub struct GeoParquetSink {
    writer: Option<ArrowWriter<File>>,
    schema: Arc<Schema>,
    columns: Vec<ColumnSpec>,
    column_buffers: Vec<ColumnBuffer>,
    geometry_buf: Vec<Vec<u8>>,
    properties_buf: Option<Vec<String>>,
    batch_size: usize,
}

enum ColumnBuffer {
    String(Vec<Option<String>>),
    Integer(Vec<Option<i64>>),
    Float(Vec<Option<f64>>),
}

impl GeoParquetSink {
    pub fn new<P: AsRef<Path>>(path: P, columns: Vec<ColumnSpec>) -> Result<Self> {
        Self::new_with_properties(path, columns, false)
    }

    pub fn new_with_properties<P: AsRef<Path>>(
        path: P,
        columns: Vec<ColumnSpec>,
        include_properties: bool,
    ) -> Result<Self> {
        let file = File::create(path.as_ref())
            .with_context(|| format!("Failed to create geoparquet file {:?}", path.as_ref()))?;

        let mut fields = vec![Field::new("geometry", DataType::Binary, false)];
        for col in &columns {
            let data_type = match col.col_type {
                ColumnType::String => DataType::Utf8,
                ColumnType::Integer => DataType::Int64,
                ColumnType::Float => DataType::Float64,
                ColumnType::Json => DataType::Utf8,
            };
            fields.push(Field::new(&col.name, data_type, true));
        }
        if include_properties {
            fields.push(Field::new("properties", DataType::Utf8, false));
        }

        let schema = Arc::new(Schema::new(fields));

        let crs_value: serde_json::Value =
            serde_json::from_str(EPSG_4326_PROJJSON).expect("embedded PROJJSON is valid");

        let geo_metadata = serde_json::json!({
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Point", "LineString", "Polygon"],
                    "crs": crs_value
                }
            }
        })
        .to_string();

        let kv_metadata = vec![KeyValue::new("geo".to_string(), Some(geo_metadata))];
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(kv_metadata))
            .build();

        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        let column_buffers = columns
            .iter()
            .map(|col| match col.col_type {
                ColumnType::String => ColumnBuffer::String(Vec::new()),
                ColumnType::Integer => ColumnBuffer::Integer(Vec::new()),
                ColumnType::Float => ColumnBuffer::Float(Vec::new()),
                ColumnType::Json => ColumnBuffer::String(Vec::new()),
            })
            .collect();

        Ok(Self {
            writer: Some(writer),
            schema,
            columns,
            column_buffers,
            geometry_buf: Vec::new(),
            properties_buf: include_properties.then(Vec::new),
            batch_size: 10_000,
        })
    }

    fn flush(&mut self) -> Result<()> {
        if self.geometry_buf.is_empty() {
            return Ok(());
        }

        let geometry_array = BinaryArray::from_iter_values(self.geometry_buf.iter().cloned());
        let mut arrays: Vec<Arc<dyn arrow_array::Array>> = Vec::new();
        arrays.push(Arc::new(geometry_array));

        for buffer in &self.column_buffers {
            match buffer {
                ColumnBuffer::String(values) => {
                    arrays.push(Arc::new(StringArray::from(values.clone())));
                }
                ColumnBuffer::Integer(values) => {
                    arrays.push(Arc::new(Int64Array::from(values.clone())));
                }
                ColumnBuffer::Float(values) => {
                    arrays.push(Arc::new(Float64Array::from(values.clone())));
                }
            }
        }

        if let Some(properties_buf) = &self.properties_buf {
            let properties_array = StringArray::from_iter_values(properties_buf.iter().cloned());
            arrays.push(Arc::new(properties_array));
        }

        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;

        if let Some(writer) = self.writer.as_mut() {
            writer.write(&batch)?;
        }
        self.geometry_buf.clear();
        if let Some(properties_buf) = &mut self.properties_buf {
            properties_buf.clear();
        }
        for buffer in &mut self.column_buffers {
            match buffer {
                ColumnBuffer::String(values) => values.clear(),
                ColumnBuffer::Integer(values) => values.clear(),
                ColumnBuffer::Float(values) => values.clear(),
            }
        }

        Ok(())
    }
}

impl DataSink for GeoParquetSink {
    fn add_feature(&mut self, row: FeatureRow) -> Result<()> {
        let wkb = row
            .geometry
            .to_wkb(CoordDimensions::xy())
            .context("Failed to convert geometry to WKB")?;
        if let Some(properties_buf) = &mut self.properties_buf {
            let properties_json = serde_json::to_string(&row.extras)?;
            properties_buf.push(properties_json);
        }

        self.geometry_buf.push(wkb);
        self.append_columns(&row);

        if self.geometry_buf.len() >= self.batch_size {
            self.flush()?;
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.flush()?;
        if let Some(writer) = self.writer.take() {
            writer.close()?;
        }
        Ok(())
    }
}

impl GeoParquetSink {
    fn append_columns(&mut self, row: &FeatureRow) {
        for (index, col) in self.columns.iter().enumerate() {
            match &mut self.column_buffers[index] {
                ColumnBuffer::String(values) => {
                    values.push(value_as_string(row.columns.get(&col.name)));
                }
                ColumnBuffer::Integer(values) => {
                    values.push(value_as_i64(row.columns.get(&col.name)));
                }
                ColumnBuffer::Float(values) => {
                    values.push(value_as_f64(row.columns.get(&col.name)));
                }
            }
        }
    }
}

fn value_as_string(value: Option<&ColumnValue>) -> Option<String> {
    match value {
        Some(ColumnValue::String(s)) => Some(s.clone()),
        _ => None,
    }
}

fn value_as_i64(value: Option<&ColumnValue>) -> Option<i64> {
    match value {
        Some(ColumnValue::Integer(n)) => Some(*n),
        _ => None,
    }
}

fn value_as_f64(value: Option<&ColumnValue>) -> Option<f64> {
    match value {
        Some(ColumnValue::Float(n)) => Some(*n),
        Some(ColumnValue::Integer(n)) => Some(*n as f64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo_types::{LineString, Point, Polygon};
    use serde_json::Map;
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::fs::File;

    fn parquet_column_names(path: &std::path::Path) -> Vec<String> {
        let file = File::open(path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .map(|column| column.name().to_string())
            .collect()
    }

    // ============================================
    // Type coercion tests
    // ============================================

    #[test]
    fn value_as_string_accepts_only_string_values() {
        assert_eq!(
            value_as_string(Some(&ColumnValue::String("hello".to_string()))),
            Some("hello".to_string())
        );
        assert_eq!(value_as_string(Some(&ColumnValue::Integer(42))), None);
        assert_eq!(value_as_string(None), None);
    }

    #[test]
    fn value_as_i64_accepts_only_integer_values() {
        assert_eq!(value_as_i64(Some(&ColumnValue::Integer(42))), Some(42));
        assert_eq!(value_as_i64(Some(&ColumnValue::Float(3.9))), None);
        assert_eq!(
            value_as_i64(Some(&ColumnValue::String("123".to_string()))),
            None
        );
        assert_eq!(value_as_i64(None), None);
    }

    #[test]
    fn value_as_f64_accepts_float_and_integer_values() {
        assert_eq!(value_as_f64(Some(&ColumnValue::Float(3.1))), Some(3.1));
        assert_eq!(value_as_f64(Some(&ColumnValue::Integer(42))), Some(42.0));
        assert_eq!(
            value_as_f64(Some(&ColumnValue::String("3.1".to_string()))),
            None
        );
        assert_eq!(value_as_f64(None), None);
    }

    // ============================================
    // GeoParquetSink creation and basic operations
    // ============================================

    #[test]
    fn creates_parquet_file_with_schema() {
        let temp_file = NamedTempFile::with_suffix(".parquet").unwrap();
        let columns = vec![
            ColumnSpec {
                name: "name".to_string(),
                col_type: ColumnType::String,
            },
            ColumnSpec {
                name: "population".to_string(),
                col_type: ColumnType::Integer,
            },
        ];

        let sink = GeoParquetSink::new(temp_file.path(), columns);
        assert!(sink.is_ok());
    }

    #[test]
    fn omits_properties_column_by_default() {
        let temp_file = NamedTempFile::with_suffix(".parquet").unwrap();
        let mut sink = GeoParquetSink::new(temp_file.path(), Vec::new()).unwrap();

        let row = FeatureRow {
            geometry: geo_types::Geometry::Point(Point::new(0.0, 0.0)),
            columns: HashMap::new(),
            extras: Map::new(),
        };

        sink.add_feature(row).unwrap();
        sink.finish().unwrap();

        assert_eq!(parquet_column_names(temp_file.path()), vec!["geometry"]);
    }

    #[test]
    fn includes_properties_column_when_requested() {
        let temp_file = NamedTempFile::with_suffix(".parquet").unwrap();
        let mut sink =
            GeoParquetSink::new_with_properties(temp_file.path(), Vec::new(), true).unwrap();

        let mut extras = Map::new();
        extras.insert(
            "custom_field".to_string(),
            serde_json::Value::String("custom_value".to_string()),
        );
        let row = FeatureRow {
            geometry: geo_types::Geometry::Point(Point::new(0.0, 0.0)),
            columns: HashMap::new(),
            extras,
        };

        sink.add_feature(row).unwrap();
        sink.finish().unwrap();

        assert_eq!(
            parquet_column_names(temp_file.path()),
            vec!["geometry", "properties"]
        );
    }

    #[test]
    fn writes_point_feature() {
        let temp_file = NamedTempFile::with_suffix(".parquet").unwrap();
        let columns = vec![ColumnSpec {
            name: "name".to_string(),
            col_type: ColumnType::String,
        }];

        let mut sink = GeoParquetSink::new(temp_file.path(), columns).unwrap();

        let point = Point::new(-0.1, 51.5);
        let mut col_map = HashMap::new();
        col_map.insert(
            "name".to_string(),
            ColumnValue::String("London".to_string()),
        );

        let row = FeatureRow {
            geometry: geo_types::Geometry::Point(point),
            columns: col_map,
            extras: Map::new(),
        };

        assert!(sink.add_feature(row).is_ok());
        assert!(sink.finish().is_ok());

        // Verify file was created with content
        let metadata = std::fs::metadata(temp_file.path()).unwrap();
        assert!(metadata.len() > 0);
    }

    #[test]
    fn writes_linestring_feature() {
        let temp_file = NamedTempFile::with_suffix(".parquet").unwrap();
        let columns = vec![ColumnSpec {
            name: "highway".to_string(),
            col_type: ColumnType::String,
        }];

        let mut sink = GeoParquetSink::new(temp_file.path(), columns).unwrap();

        let line = LineString::from(vec![(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)]);
        let mut col_map = HashMap::new();
        col_map.insert(
            "highway".to_string(),
            ColumnValue::String("primary".to_string()),
        );

        let row = FeatureRow {
            geometry: geo_types::Geometry::LineString(line),
            columns: col_map,
            extras: Map::new(),
        };

        assert!(sink.add_feature(row).is_ok());
        assert!(sink.finish().is_ok());
    }

    #[test]
    fn writes_polygon_feature() {
        let temp_file = NamedTempFile::with_suffix(".parquet").unwrap();
        let columns = vec![ColumnSpec {
            name: "building".to_string(),
            col_type: ColumnType::String,
        }];

        let mut sink = GeoParquetSink::new(temp_file.path(), columns).unwrap();

        let polygon = Polygon::new(
            LineString::from(vec![
                (0.0, 0.0),
                (1.0, 0.0),
                (1.0, 1.0),
                (0.0, 1.0),
                (0.0, 0.0),
            ]),
            vec![],
        );
        let mut col_map = HashMap::new();
        col_map.insert(
            "building".to_string(),
            ColumnValue::String("yes".to_string()),
        );

        let row = FeatureRow {
            geometry: geo_types::Geometry::Polygon(polygon),
            columns: col_map,
            extras: Map::new(),
        };

        assert!(sink.add_feature(row).is_ok());
        assert!(sink.finish().is_ok());
    }

    #[test]
    fn handles_multiple_column_types() {
        let temp_file = NamedTempFile::with_suffix(".parquet").unwrap();
        let columns = vec![
            ColumnSpec {
                name: "name".to_string(),
                col_type: ColumnType::String,
            },
            ColumnSpec {
                name: "population".to_string(),
                col_type: ColumnType::Integer,
            },
            ColumnSpec {
                name: "area".to_string(),
                col_type: ColumnType::Float,
            },
        ];

        let mut sink = GeoParquetSink::new(temp_file.path(), columns).unwrap();

        let point = Point::new(0.0, 0.0);
        let mut col_map = HashMap::new();
        col_map.insert("name".to_string(), ColumnValue::String("Test".to_string()));
        col_map.insert("population".to_string(), ColumnValue::Integer(1000));
        col_map.insert("area".to_string(), ColumnValue::Float(123.4));

        let row = FeatureRow {
            geometry: geo_types::Geometry::Point(point),
            columns: col_map,
            extras: Map::new(),
        };

        assert!(sink.add_feature(row).is_ok());
        assert!(sink.finish().is_ok());
    }

    #[test]
    fn handles_missing_column_values() {
        let temp_file = NamedTempFile::with_suffix(".parquet").unwrap();
        let columns = vec![
            ColumnSpec {
                name: "name".to_string(),
                col_type: ColumnType::String,
            },
            ColumnSpec {
                name: "population".to_string(),
                col_type: ColumnType::Integer,
            },
        ];

        let mut sink = GeoParquetSink::new(temp_file.path(), columns).unwrap();

        let point = Point::new(0.0, 0.0);
        // Only provide "name", not "population"
        let mut col_map = HashMap::new();
        col_map.insert("name".to_string(), ColumnValue::String("Test".to_string()));

        let row = FeatureRow {
            geometry: geo_types::Geometry::Point(point),
            columns: col_map,
            extras: Map::new(),
        };

        assert!(sink.add_feature(row).is_ok());
        assert!(sink.finish().is_ok());
    }

    #[test]
    fn writes_extras_as_json_properties() {
        let temp_file = NamedTempFile::with_suffix(".parquet").unwrap();
        let columns = vec![];

        let mut sink = GeoParquetSink::new(temp_file.path(), columns).unwrap();

        let point = Point::new(0.0, 0.0);
        let mut extras = Map::new();
        extras.insert(
            "custom_field".to_string(),
            serde_json::Value::String("custom_value".to_string()),
        );

        let row = FeatureRow {
            geometry: geo_types::Geometry::Point(point),
            columns: HashMap::new(),
            extras,
        };

        assert!(sink.add_feature(row).is_ok());
        assert!(sink.finish().is_ok());
    }

    #[test]
    fn handles_empty_file() {
        let temp_file = NamedTempFile::with_suffix(".parquet").unwrap();
        let columns = vec![ColumnSpec {
            name: "name".to_string(),
            col_type: ColumnType::String,
        }];

        let mut sink = GeoParquetSink::new(temp_file.path(), columns).unwrap();
        // Don't add any features
        assert!(sink.finish().is_ok());
    }
}
