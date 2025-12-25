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
