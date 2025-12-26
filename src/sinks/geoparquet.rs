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

pub struct GeoParquetSink {
    writer: Option<ArrowWriter<File>>,
    schema: Arc<Schema>,
    columns: Vec<ColumnSpec>,
    column_buffers: Vec<ColumnBuffer>,
    geometry_buf: Vec<Vec<u8>>,
    properties_buf: Vec<String>,
    batch_size: usize,
}

enum ColumnBuffer {
    String(Vec<Option<String>>),
    Integer(Vec<Option<i64>>),
    Float(Vec<Option<f64>>),
}

impl GeoParquetSink {
    pub fn new<P: AsRef<Path>>(path: P, columns: Vec<ColumnSpec>) -> Result<Self> {
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
        fields.push(Field::new("properties", DataType::Utf8, false));

        let schema = Arc::new(Schema::new(fields));

        let geo_metadata = serde_json::json!({
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Point", "LineString", "Polygon"],
                    "crs": "EPSG:4326"
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
            properties_buf: Vec::new(),
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

        let properties_array = StringArray::from_iter_values(self.properties_buf.iter().cloned());
        arrays.push(Arc::new(properties_array));

        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;

        if let Some(writer) = self.writer.as_mut() {
            writer.write(&batch)?;
        }
        self.geometry_buf.clear();
        self.properties_buf.clear();
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
        let properties_json = serde_json::to_string(&row.extras)?;

        self.geometry_buf.push(wkb);
        self.properties_buf.push(properties_json);
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
                    values.push(coerce_string(row.columns.get(&col.name)));
                }
                ColumnBuffer::Integer(values) => {
                    values.push(coerce_i64(row.columns.get(&col.name)));
                }
                ColumnBuffer::Float(values) => {
                    values.push(coerce_f64(row.columns.get(&col.name)));
                }
            }
        }
    }
}

fn coerce_string(value: Option<&ColumnValue>) -> Option<String> {
    match value {
        Some(ColumnValue::String(s)) => Some(s.clone()),
        Some(ColumnValue::Integer(n)) => Some(n.to_string()),
        Some(ColumnValue::Float(n)) => Some(n.to_string()),
        Some(ColumnValue::Json(v)) => Some(v.to_string()),
        None => None,
    }
}

fn coerce_i64(value: Option<&ColumnValue>) -> Option<i64> {
    match value {
        Some(ColumnValue::Integer(n)) => Some(*n),
        Some(ColumnValue::Float(n)) => Some(*n as i64),
        Some(ColumnValue::String(s)) => s.parse::<i64>().ok(),
        Some(ColumnValue::Json(_)) => None,
        None => None,
    }
}

fn coerce_f64(value: Option<&ColumnValue>) -> Option<f64> {
    match value {
        Some(ColumnValue::Float(n)) => Some(*n),
        Some(ColumnValue::Integer(n)) => Some(*n as f64),
        Some(ColumnValue::String(s)) => s.parse::<f64>().ok(),
        Some(ColumnValue::Json(_)) => None,
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo_types::{LineString, Point, Polygon};
    use serde_json::Map;
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

    // ============================================
    // Type coercion tests
    // ============================================

    #[test]
    fn coerce_string_from_string() {
        let value = ColumnValue::String("hello".to_string());
        assert_eq!(coerce_string(Some(&value)), Some("hello".to_string()));
    }

    #[test]
    fn coerce_string_from_integer() {
        let value = ColumnValue::Integer(42);
        assert_eq!(coerce_string(Some(&value)), Some("42".to_string()));
    }

    #[test]
    fn coerce_string_from_float() {
        let value = ColumnValue::Float(3.14);
        assert_eq!(coerce_string(Some(&value)), Some("3.14".to_string()));
    }

    #[test]
    fn coerce_string_from_none() {
        assert_eq!(coerce_string(None), None);
    }

    #[test]
    fn coerce_i64_from_integer() {
        let value = ColumnValue::Integer(42);
        assert_eq!(coerce_i64(Some(&value)), Some(42));
    }

    #[test]
    fn coerce_i64_from_float() {
        let value = ColumnValue::Float(3.9);
        assert_eq!(coerce_i64(Some(&value)), Some(3)); // truncates
    }

    #[test]
    fn coerce_i64_from_valid_string() {
        let value = ColumnValue::String("123".to_string());
        assert_eq!(coerce_i64(Some(&value)), Some(123));
    }

    #[test]
    fn coerce_i64_from_invalid_string() {
        let value = ColumnValue::String("not a number".to_string());
        assert_eq!(coerce_i64(Some(&value)), None);
    }

    #[test]
    fn coerce_i64_from_none() {
        assert_eq!(coerce_i64(None), None);
    }

    #[test]
    fn coerce_f64_from_float() {
        let value = ColumnValue::Float(3.14);
        assert_eq!(coerce_f64(Some(&value)), Some(3.14));
    }

    #[test]
    fn coerce_f64_from_integer() {
        let value = ColumnValue::Integer(42);
        assert_eq!(coerce_f64(Some(&value)), Some(42.0));
    }

    #[test]
    fn coerce_f64_from_valid_string() {
        let value = ColumnValue::String("3.14".to_string());
        assert_eq!(coerce_f64(Some(&value)), Some(3.14));
    }

    #[test]
    fn coerce_f64_from_invalid_string() {
        let value = ColumnValue::String("not a number".to_string());
        assert_eq!(coerce_f64(Some(&value)), None);
    }

    #[test]
    fn coerce_f64_from_none() {
        assert_eq!(coerce_f64(None), None);
    }

    #[test]
    fn coerce_i64_from_negative_float() {
        let value = ColumnValue::Float(-5.7);
        assert_eq!(coerce_i64(Some(&value)), Some(-5));
    }

    #[test]
    fn coerce_i64_from_large_integer() {
        let value = ColumnValue::Integer(i64::MAX);
        assert_eq!(coerce_i64(Some(&value)), Some(i64::MAX));
    }

    #[test]
    fn coerce_string_from_negative_integer() {
        let value = ColumnValue::Integer(-42);
        assert_eq!(coerce_string(Some(&value)), Some("-42".to_string()));
    }

    #[test]
    fn coerce_i64_from_string_with_whitespace() {
        // String parsing doesn't trim whitespace
        let value = ColumnValue::String(" 123 ".to_string());
        assert_eq!(coerce_i64(Some(&value)), None);
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
    fn writes_point_feature() {
        let temp_file = NamedTempFile::with_suffix(".parquet").unwrap();
        let columns = vec![ColumnSpec {
            name: "name".to_string(),
            col_type: ColumnType::String,
        }];

        let mut sink = GeoParquetSink::new(temp_file.path(), columns).unwrap();

        let point = Point::new(-0.1, 51.5);
        let mut col_map = HashMap::new();
        col_map.insert("name".to_string(), ColumnValue::String("London".to_string()));

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
            LineString::from(vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]),
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
        col_map.insert("area".to_string(), ColumnValue::Float(123.45));

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
