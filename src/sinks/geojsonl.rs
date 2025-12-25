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
