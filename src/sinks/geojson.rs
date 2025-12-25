use super::{ColumnValue, DataSink, FeatureRow};
use anyhow::Result;
use geojson::{Feature, GeoJson};
use serde_json::Value;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

pub struct GeoJsonSink {
    writer: BufWriter<File>,
    first_feature: bool,
}

impl GeoJsonSink {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        // Write the header of the FeatureCollection
        writeln!(writer, "{{")?;
        writeln!(writer, "  \"type\": \"FeatureCollection\",")?;
        writeln!(writer, "  \"features\": [")?;

        Ok(Self {
            writer,
            first_feature: true,
        })
    }
}

impl DataSink for GeoJsonSink {
    fn add_feature(&mut self, row: FeatureRow) -> Result<()> {
        if !self.first_feature {
            writeln!(self.writer, ",")?;
        }
        self.first_feature = false;

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

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        // Close the array and object
        writeln!(self.writer)?;
        writeln!(self.writer, "  ]")?;
        writeln!(self.writer, "}}")?;
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
    fn creates_valid_geojson_structure() {
        let temp_file = NamedTempFile::with_suffix(".geojson").unwrap();
        let mut sink = GeoJsonSink::new(temp_file.path()).unwrap();

        sink.add_feature(create_point_row("Test", 0.0, 0.0)).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();

        assert_eq!(parsed["type"], "FeatureCollection");
        assert!(parsed["features"].is_array());
        assert_eq!(parsed["features"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn writes_multiple_features_with_commas() {
        let temp_file = NamedTempFile::with_suffix(".geojson").unwrap();
        let mut sink = GeoJsonSink::new(temp_file.path()).unwrap();

        sink.add_feature(create_point_row("First", 0.0, 0.0)).unwrap();
        sink.add_feature(create_point_row("Second", 1.0, 1.0)).unwrap();
        sink.add_feature(create_point_row("Third", 2.0, 2.0)).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();

        assert_eq!(parsed["features"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn writes_empty_feature_collection() {
        let temp_file = NamedTempFile::with_suffix(".geojson").unwrap();
        let mut sink = GeoJsonSink::new(temp_file.path()).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();

        assert_eq!(parsed["type"], "FeatureCollection");
        assert!(parsed["features"].as_array().unwrap().is_empty());
    }

    #[test]
    fn includes_geometry_in_feature() {
        let temp_file = NamedTempFile::with_suffix(".geojson").unwrap();
        let mut sink = GeoJsonSink::new(temp_file.path()).unwrap();

        sink.add_feature(create_point_row("Test", -0.1, 51.5)).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();

        let feature = &parsed["features"][0];
        assert_eq!(feature["type"], "Feature");
        assert_eq!(feature["geometry"]["type"], "Point");

        let coords = &feature["geometry"]["coordinates"];
        assert!((coords[0].as_f64().unwrap() - (-0.1)).abs() < 1e-10);
        assert!((coords[1].as_f64().unwrap() - 51.5).abs() < 1e-10);
    }

    #[test]
    fn includes_properties_from_columns() {
        let temp_file = NamedTempFile::with_suffix(".geojson").unwrap();
        let mut sink = GeoJsonSink::new(temp_file.path()).unwrap();

        let point = Point::new(0.0, 0.0);
        let mut columns = HashMap::new();
        columns.insert("name".to_string(), ColumnValue::String("Test".to_string()));
        columns.insert("population".to_string(), ColumnValue::Integer(1000));
        columns.insert("area".to_string(), ColumnValue::Float(123.45));

        let row = FeatureRow {
            geometry: geo_types::Geometry::Point(point),
            columns,
            extras: Map::new(),
        };

        sink.add_feature(row).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();

        let props = &parsed["features"][0]["properties"];
        assert_eq!(props["name"], "Test");
        assert_eq!(props["population"], 1000);
        assert!((props["area"].as_f64().unwrap() - 123.45).abs() < 1e-10);
    }

    #[test]
    fn extras_take_precedence_over_columns() {
        let temp_file = NamedTempFile::with_suffix(".geojson").unwrap();
        let mut sink = GeoJsonSink::new(temp_file.path()).unwrap();

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
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();

        // extras should take precedence
        assert_eq!(parsed["features"][0]["properties"]["name"], "FromExtras");
    }

    #[test]
    fn writes_linestring_geometry() {
        let temp_file = NamedTempFile::with_suffix(".geojson").unwrap();
        let mut sink = GeoJsonSink::new(temp_file.path()).unwrap();

        let line = LineString::from(vec![(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)]);
        let row = FeatureRow {
            geometry: geo_types::Geometry::LineString(line),
            columns: HashMap::new(),
            extras: Map::new(),
        };

        sink.add_feature(row).unwrap();
        sink.finish().unwrap();

        let content = std::fs::read_to_string(temp_file.path()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();

        assert_eq!(parsed["features"][0]["geometry"]["type"], "LineString");
    }

    #[test]
    fn writes_polygon_geometry() {
        let temp_file = NamedTempFile::with_suffix(".geojson").unwrap();
        let mut sink = GeoJsonSink::new(temp_file.path()).unwrap();

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
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();

        assert_eq!(parsed["features"][0]["geometry"]["type"], "Polygon");
    }
}
