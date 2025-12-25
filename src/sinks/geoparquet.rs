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
        None => None,
    }
}

fn coerce_i64(value: Option<&ColumnValue>) -> Option<i64> {
    match value {
        Some(ColumnValue::Integer(n)) => Some(*n),
        Some(ColumnValue::Float(n)) => Some(*n as i64),
        Some(ColumnValue::String(s)) => s.parse::<i64>().ok(),
        None => None,
    }
}

fn coerce_f64(value: Option<&ColumnValue>) -> Option<f64> {
    match value {
        Some(ColumnValue::Float(n)) => Some(*n),
        Some(ColumnValue::Integer(n)) => Some(*n as f64),
        Some(ColumnValue::String(s)) => s.parse::<f64>().ok(),
        None => None,
    }
}
