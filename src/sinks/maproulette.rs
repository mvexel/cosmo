use super::{ColumnValue, DataSink, FeatureRow};
use anyhow::Result;
use geojson::{Feature, FeatureCollection, GeoJson};
use serde_json::Value;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// RFC 7464 record separator (0x1E).
const RS: u8 = 0x1E;

/// Sink that writes MapRoulette-compatible line-by-line GeoJSON.
///
/// Each line is an RFC 7464 record: a `\x1E` byte followed by a
/// `FeatureCollection` containing a single `Feature`.
/// See <https://learn.maproulette.org/en-US/documentation/line-by-line-geojson/>.
pub struct MapRouletteSink {
    writer: BufWriter<Box<dyn Write + Send>>,
}

impl MapRouletteSink {
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

impl DataSink for MapRouletteSink {
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
                ColumnValue::Json(val) => val,
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

        let fc = FeatureCollection {
            bbox: None,
            features: vec![feature],
            foreign_members: None,
        };

        self.writer.write_all(&[RS])?;
        let geojson = GeoJson::FeatureCollection(fc);
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
    use geo_types::{Geometry, Point};
    use serde_json::Map;
    use std::collections::HashMap;
    use std::io::Read;
    use tempfile::NamedTempFile;

    fn point_row(name: &str, lon: f64, lat: f64) -> FeatureRow {
        let mut columns = HashMap::new();
        columns.insert("name".to_string(), ColumnValue::String(name.to_string()));
        FeatureRow {
            geometry: Geometry::Point(Point::new(lon, lat)),
            columns,
            extras: Map::new(),
        }
    }

    #[test]
    fn each_line_starts_with_rs() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let mut sink = MapRouletteSink::new(&path).unwrap();

        sink.add_feature(point_row("Café", 2.35, 48.86)).unwrap();
        sink.add_feature(point_row("Bar", 2.36, 48.87)).unwrap();
        sink.finish().unwrap();

        let mut buf = Vec::new();
        std::fs::File::open(&path)
            .unwrap()
            .read_to_end(&mut buf)
            .unwrap();

        let lines: Vec<&[u8]> = buf.split(|&b| b == b'\n').collect();
        // Two features + trailing empty after final newline
        assert_eq!(lines.len(), 3);
        assert!(lines[2].is_empty()); // trailing newline

        for line in &lines[..2] {
            assert_eq!(line[0], RS, "line must start with RS (0x1E)");
        }
    }

    #[test]
    fn each_line_is_feature_collection() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let mut sink = MapRouletteSink::new(&path).unwrap();

        sink.add_feature(point_row("Museum", 13.4, 52.5)).unwrap();
        sink.finish().unwrap();

        let raw = std::fs::read(&path).unwrap();
        // Skip the RS byte, parse the rest of the line
        let json_bytes = &raw[1..raw.len() - 1]; // skip RS and trailing newline
        let parsed: serde_json::Value = serde_json::from_slice(json_bytes).unwrap();

        assert_eq!(parsed["type"], "FeatureCollection");
        assert_eq!(parsed["features"].as_array().unwrap().len(), 1);
        assert_eq!(parsed["features"][0]["type"], "Feature");
        assert_eq!(parsed["features"][0]["properties"]["name"], "Museum");
    }

    #[test]
    fn properties_include_columns() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let mut sink = MapRouletteSink::new(&path).unwrap();

        let mut columns = HashMap::new();
        columns.insert("osm_id".to_string(), ColumnValue::Integer(12345));
        columns.insert("name".to_string(), ColumnValue::String("Shop".into()));
        let row = FeatureRow {
            geometry: Geometry::Point(Point::new(0.0, 0.0)),
            columns,
            extras: Map::new(),
        };
        sink.add_feature(row).unwrap();
        sink.finish().unwrap();

        let raw = std::fs::read(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&raw[1..raw.len() - 1]).unwrap();
        let props = &parsed["features"][0]["properties"];
        assert_eq!(props["osm_id"], 12345);
        assert_eq!(props["name"], "Shop");
    }

    #[test]
    fn empty_output_is_empty_file() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let mut sink = MapRouletteSink::new(&path).unwrap();
        sink.finish().unwrap();

        let contents = std::fs::read_to_string(&path).unwrap();
        assert!(contents.is_empty());
    }
}
