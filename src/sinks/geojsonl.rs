use super::{ColumnValue, DataSink, FeatureRow};
use anyhow::Result;
use geojson::{Feature, GeoJson};
use serde_json::Value;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

pub struct GeoJsonlSink {
    writer: BufWriter<Box<dyn Write + Send>>,
}

impl GeoJsonlSink {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            writer: BufWriter::new(Box::new(file)),
        })
    }

    pub fn stdout() -> Result<Self> {
        Ok(Self {
            writer: BufWriter::new(Box::new(std::io::stdout())),
        })
    }
}

impl DataSink for GeoJsonlSink {
    fn add_feature(&mut self, row: FeatureRow) -> Result<()> {
        let mut properties = row.extras;
        for (name, value) in row.columns {
            if properties.contains_key(&name) {
                continue;
            }
            let json_value = match value {
                ColumnValue::String(val) => Value::String(val),
                ColumnValue::Integer(val) => Value::from(val),
                ColumnValue::Float(val) => Value::from(val),
            };
            properties.insert(name, json_value);
        }

        let geometry = geojson::Geometry::from(&row.geometry);
        let feature = Feature {
            bbox: None,
            geometry: Some(geometry),
            id: None,
            properties: Some(properties),
            foreign_members: None,
        };

        let geojson = GeoJson::Feature(feature);
        serde_json::to_writer(&mut self.writer, &geojson)?;
        writeln!(self.writer)?;
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

    fn create_point_row(name: &str, lon: f64, lat: f64) -> FeatureRow {
        let point = Point::new(lon, lat);
        let mut columns = HashMap::new();
        columns.insert("name".to_string(), ColumnValue::String(name.to_string()));
        FeatureRow {
            geometry: geo_types::Geometry::Point(point),
            columns,
            extras: Map::new(),
        }
    }

    #[test]
    fn writes_one_feature_per_line() {
        let temp_file = NamedTempFile::with_suffix(".geojsonl").unwrap();
        let mut sink = GeoJsonlSink::new(temp_file.path()).unwrap();

        sink.add_feature(create_point_row("First", 0.0, 0.0)).unwrap();
        sink.add_feature(create_point_row("Second", 1.0, 1.0)).unwrap();
        sink.add_feature(create_point_row("Third", 2.0, 2.0)).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 3);

        // Each line should be valid JSON
        for line in lines {
            let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(parsed["type"], "Feature");
        }
    }

    #[test]
    fn each_line_is_complete_feature() {
        let temp_file = NamedTempFile::with_suffix(".geojsonl").unwrap();
        let mut sink = GeoJsonlSink::new(temp_file.path()).unwrap();

        sink.add_feature(create_point_row("Test", -0.1, 51.5)).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        let line = content.lines().next().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(line).unwrap();

        assert_eq!(parsed["type"], "Feature");
        assert_eq!(parsed["geometry"]["type"], "Point");
        assert_eq!(parsed["properties"]["name"], "Test");
    }

    #[test]
    fn writes_empty_file_when_no_features() {
        let temp_file = NamedTempFile::with_suffix(".geojsonl").unwrap();
        let mut sink = GeoJsonlSink::new(temp_file.path()).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        assert!(content.is_empty());
    }

    #[test]
    fn includes_all_column_types() {
        let temp_file = NamedTempFile::with_suffix(".geojsonl").unwrap();
        let mut sink = GeoJsonlSink::new(temp_file.path()).unwrap();

        let point = Point::new(0.0, 0.0);
        let mut columns = HashMap::new();
        columns.insert("name".to_string(), ColumnValue::String("Test".to_string()));
        columns.insert("count".to_string(), ColumnValue::Integer(42));
        columns.insert("value".to_string(), ColumnValue::Float(3.14));

        let row = FeatureRow {
            geometry: geo_types::Geometry::Point(point),
            columns,
            extras: Map::new(),
        };

        sink.add_feature(row).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

        let props = &parsed["properties"];
        assert_eq!(props["name"], "Test");
        assert_eq!(props["count"], 42);
        assert!((props["value"].as_f64().unwrap() - 3.14).abs() < 1e-10);
    }

    #[test]
    fn extras_take_precedence_over_columns() {
        let temp_file = NamedTempFile::with_suffix(".geojsonl").unwrap();
        let mut sink = GeoJsonlSink::new(temp_file.path()).unwrap();

        let point = Point::new(0.0, 0.0);
        let mut columns = HashMap::new();
        columns.insert("name".to_string(), ColumnValue::String("FromColumn".to_string()));

        let mut extras = Map::new();
        extras.insert("name".to_string(), serde_json::Value::String("FromExtras".to_string()));

        let row = FeatureRow {
            geometry: geo_types::Geometry::Point(point),
            columns,
            extras,
        };

        sink.add_feature(row).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

        assert_eq!(parsed["properties"]["name"], "FromExtras");
    }

    #[test]
    fn writes_linestring_geometry() {
        let temp_file = NamedTempFile::with_suffix(".geojsonl").unwrap();
        let mut sink = GeoJsonlSink::new(temp_file.path()).unwrap();

        let line = LineString::from(vec![(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)]);
        let row = FeatureRow {
            geometry: geo_types::Geometry::LineString(line),
            columns: HashMap::new(),
            extras: Map::new(),
        };

        sink.add_feature(row).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

        assert_eq!(parsed["geometry"]["type"], "LineString");
        let coords = parsed["geometry"]["coordinates"].as_array().unwrap();
        assert_eq!(coords.len(), 3);
    }

    #[test]
    fn writes_polygon_geometry() {
        let temp_file = NamedTempFile::with_suffix(".geojsonl").unwrap();
        let mut sink = GeoJsonlSink::new(temp_file.path()).unwrap();

        let polygon = Polygon::new(
            LineString::from(vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]),
            vec![],
        );
        let row = FeatureRow {
            geometry: geo_types::Geometry::Polygon(polygon),
            columns: HashMap::new(),
            extras: Map::new(),
        };

        sink.add_feature(row).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

        assert_eq!(parsed["geometry"]["type"], "Polygon");
    }

    #[test]
    fn lines_end_with_newline() {
        let temp_file = NamedTempFile::with_suffix(".geojsonl").unwrap();
        let mut sink = GeoJsonlSink::new(temp_file.path()).unwrap();

        sink.add_feature(create_point_row("Test", 0.0, 0.0)).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        assert!(content.ends_with('\n'));
    }

    #[test]
    fn no_trailing_comma_between_features() {
        let temp_file = NamedTempFile::with_suffix(".geojsonl").unwrap();
        let mut sink = GeoJsonlSink::new(temp_file.path()).unwrap();

        sink.add_feature(create_point_row("First", 0.0, 0.0)).unwrap();
        sink.add_feature(create_point_row("Second", 1.0, 1.0)).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();

        // GeoJSONL should NOT have commas between lines (unlike GeoJSON array)
        assert!(!content.contains("},\n{"));

        // Each line should end with just } and newline
        for line in content.lines() {
            assert!(line.ends_with('}'));
        }
    }
}
