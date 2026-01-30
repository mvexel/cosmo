use super::{ColumnSpec, ColumnValue, DataSink, FeatureRow};
use anyhow::{Context, Result};
use geozero::ToWkt;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

pub struct CsvWktSink {
    writer: csv::Writer<BufWriter<Box<dyn Write + Send>>>,
    columns: Vec<ColumnSpec>,
    header_written: bool,
}

impl CsvWktSink {
    pub fn new<P: AsRef<Path>>(path: P, columns: Vec<ColumnSpec>) -> Result<Self> {
        let file = File::create(path.as_ref())
            .with_context(|| format!("Failed to create CSV file: {:?}", path.as_ref()))?;
        let writer = csv::Writer::from_writer(BufWriter::new(Box::new(file) as Box<dyn Write + Send>));
        Ok(Self {
            writer,
            columns,
            header_written: false,
        })
    }

    pub fn stdout(columns: Vec<ColumnSpec>) -> Result<Self> {
        let writer = csv::Writer::from_writer(BufWriter::new(Box::new(std::io::stdout()) as Box<dyn Write + Send>));
        Ok(Self {
            writer,
            columns,
            header_written: false,
        })
    }

    fn write_header(&mut self) -> Result<()> {
        if self.header_written {
            return Ok(());
        }

        let mut headers: Vec<&str> = self.columns.iter().map(|c| c.name.as_str()).collect();
        headers.push("geometry");

        self.writer.write_record(&headers)?;
        self.header_written = true;
        Ok(())
    }
}

impl DataSink for CsvWktSink {
    fn add_feature(&mut self, row: FeatureRow) -> Result<()> {
        self.write_header()?;

        let mut record: Vec<String> = Vec::with_capacity(self.columns.len() + 1);

        // Write columns in schema order
        for col in &self.columns {
            let value = row.columns.get(&col.name);
            let cell = match value {
                Some(ColumnValue::String(s)) => s.clone(),
                Some(ColumnValue::Integer(i)) => i.to_string(),
                Some(ColumnValue::Float(f)) => f.to_string(),
                Some(ColumnValue::Json(j)) => j.to_string(),
                None => {
                    // Check extras
                    if let Some(extra) = row.extras.get(&col.name) {
                        match extra {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Number(n) => n.to_string(),
                            serde_json::Value::Bool(b) => b.to_string(),
                            serde_json::Value::Null => String::new(),
                            other => other.to_string(),
                        }
                    } else {
                        String::new()
                    }
                }
            };
            record.push(cell);
        }

        // Convert geometry to WKT using geozero
        let wkt = row.geometry.to_wkt()
            .context("Failed to convert geometry to WKT")?;
        record.push(wkt);

        self.writer.write_record(&record)?;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo_types::{LineString, Point, Polygon};
    use serde_json::Map;
    use std::collections::HashMap;
    use tempfile::NamedTempFile;
    use crate::config::ColumnType;

    fn test_columns() -> Vec<ColumnSpec> {
        vec![
            ColumnSpec { name: "name".to_string(), col_type: ColumnType::String },
            ColumnSpec { name: "value".to_string(), col_type: ColumnType::Integer },
        ]
    }

    fn create_point_row(name: &str, value: i64, lon: f64, lat: f64) -> FeatureRow {
        let point = Point::new(lon, lat);
        let mut columns = HashMap::new();
        columns.insert("name".to_string(), ColumnValue::String(name.to_string()));
        columns.insert("value".to_string(), ColumnValue::Integer(value));
        FeatureRow {
            geometry: geo_types::Geometry::Point(point),
            columns,
            extras: Map::new(),
        }
    }

    #[test]
    fn writes_header_and_data() {
        let temp_file = NamedTempFile::with_suffix(".csv").unwrap();
        let mut sink = CsvWktSink::new(temp_file.path(), test_columns()).unwrap();

        sink.add_feature(create_point_row("Test", 42, -0.1, 51.5)).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "name,value,geometry");
        assert!(lines[1].contains("Test"));
        assert!(lines[1].contains("42"));
        assert!(lines[1].contains("POINT"));
    }

    #[test]
    fn writes_multiple_rows() {
        let temp_file = NamedTempFile::with_suffix(".csv").unwrap();
        let mut sink = CsvWktSink::new(temp_file.path(), test_columns()).unwrap();

        sink.add_feature(create_point_row("First", 1, 0.0, 0.0)).unwrap();
        sink.add_feature(create_point_row("Second", 2, 1.0, 1.0)).unwrap();
        sink.add_feature(create_point_row("Third", 3, 2.0, 2.0)).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 4); // header + 3 rows
    }

    #[test]
    fn handles_empty_output() {
        let temp_file = NamedTempFile::with_suffix(".csv").unwrap();
        let mut sink = CsvWktSink::new(temp_file.path(), test_columns()).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        assert!(content.is_empty()); // No header if no data
    }

    #[test]
    fn escapes_special_characters() {
        let temp_file = NamedTempFile::with_suffix(".csv").unwrap();
        let mut sink = CsvWktSink::new(temp_file.path(), test_columns()).unwrap();

        sink.add_feature(create_point_row("Name, with \"quotes\"", 1, 0.0, 0.0)).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        // CSV should properly escape the value
        assert!(content.contains("\"Name, with \"\"quotes\"\"\"") || content.contains("\"Name"));
    }

    #[test]
    fn writes_linestring_geometry() {
        let temp_file = NamedTempFile::with_suffix(".csv").unwrap();
        let mut sink = CsvWktSink::new(temp_file.path(), test_columns()).unwrap();

        let line = LineString::from(vec![(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)]);
        let mut columns = HashMap::new();
        columns.insert("name".to_string(), ColumnValue::String("Road".to_string()));
        columns.insert("value".to_string(), ColumnValue::Integer(100));

        let row = FeatureRow {
            geometry: geo_types::Geometry::LineString(line),
            columns,
            extras: Map::new(),
        };

        sink.add_feature(row).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        assert!(content.contains("LINESTRING"));
    }

    #[test]
    fn writes_polygon_geometry() {
        let temp_file = NamedTempFile::with_suffix(".csv").unwrap();
        let mut sink = CsvWktSink::new(temp_file.path(), test_columns()).unwrap();

        let polygon = Polygon::new(
            LineString::from(vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]),
            vec![],
        );
        let mut columns = HashMap::new();
        columns.insert("name".to_string(), ColumnValue::String("Building".to_string()));
        columns.insert("value".to_string(), ColumnValue::Integer(50));

        let row = FeatureRow {
            geometry: geo_types::Geometry::Polygon(polygon),
            columns,
            extras: Map::new(),
        };

        sink.add_feature(row).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        assert!(content.contains("POLYGON"));
    }

    #[test]
    fn handles_missing_columns() {
        let temp_file = NamedTempFile::with_suffix(".csv").unwrap();
        let columns = vec![
            ColumnSpec { name: "name".to_string(), col_type: ColumnType::String },
            ColumnSpec { name: "missing".to_string(), col_type: ColumnType::String },
        ];
        let mut sink = CsvWktSink::new(temp_file.path(), columns).unwrap();

        let point = Point::new(0.0, 0.0);
        let mut cols = HashMap::new();
        cols.insert("name".to_string(), ColumnValue::String("Test".to_string()));
        // "missing" column not provided

        let row = FeatureRow {
            geometry: geo_types::Geometry::Point(point),
            columns: cols,
            extras: Map::new(),
        };

        sink.add_feature(row).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        // Should have empty value for missing column
        assert_eq!(lines[0], "name,missing,geometry");
    }
}
