use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::config::{CompiledConfig, CompiledTable};
use crate::sinks::{
    ColumnSpec, CsvWktSink, DataSink, GeoJsonSink, GeoJsonlSink, GeoParquetSink, MapRouletteSink,
    OutputFormat,
};

use super::reporting::output_format_label;

pub type SinkMap = HashMap<String, Arc<Mutex<Box<dyn DataSink>>>>;

pub fn init_sink(
    format: &OutputFormat,
    output: &Path,
    config: &CompiledConfig,
    include_properties: bool,
) -> Result<Box<dyn DataSink>> {
    match format {
        OutputFormat::GeoJson => {
            if output == Path::new("-") {
                anyhow::bail!(
                    "CLI: GeoJSON output to stdout is not supported; use geojsonl instead"
                );
            }
            tracing::info!("Sink: {} -> {:?}", output_format_label(format), output);
            Ok(Box::new(GeoJsonSink::new(output)?))
        }
        OutputFormat::GeoJsonl => {
            if output == Path::new("-") {
                tracing::info!("Sink: {} -> stdout", output_format_label(format));
                Ok(Box::new(GeoJsonlSink::stdout()?))
            } else {
                tracing::info!("Sink: {} -> {:?}", output_format_label(format), output);
                Ok(Box::new(GeoJsonlSink::new(output)?))
            }
        }
        OutputFormat::GeoParquet => {
            if output == Path::new("-") {
                anyhow::bail!("CLI: Parquet output to stdout is not supported");
            }
            let columns = collect_columns(config)?;
            tracing::info!(
                "Sink: {} -> {:?} ({} columns)",
                output_format_label(format),
                output,
                columns.len()
            );
            let sink = if include_properties {
                GeoParquetSink::new_with_properties(output, columns, true)?
            } else {
                GeoParquetSink::new(output, columns)?
            };
            Ok(Box::new(sink))
        }
        OutputFormat::Csv => {
            let columns = collect_columns(config)?;
            if output == Path::new("-") {
                tracing::info!(
                    "Sink: {} -> stdout ({} columns)",
                    output_format_label(format),
                    columns.len()
                );
                Ok(Box::new(CsvWktSink::stdout(columns)?))
            } else {
                tracing::info!(
                    "Sink: {} -> {:?} ({} columns)",
                    output_format_label(format),
                    output,
                    columns.len()
                );
                Ok(Box::new(CsvWktSink::new(output, columns)?))
            }
        }
        OutputFormat::MapRoulette => {
            if output == Path::new("-") {
                tracing::info!("Sink: {} -> stdout", output_format_label(format));
                Ok(Box::new(MapRouletteSink::stdout()?))
            } else {
                tracing::info!("Sink: {} -> {:?}", output_format_label(format), output);
                Ok(Box::new(MapRouletteSink::new(output)?))
            }
        }
    }
}

pub fn init_sinks(
    format: &OutputFormat,
    output_dir: &Path,
    config: &CompiledConfig,
    include_properties: bool,
) -> Result<SinkMap> {
    if output_dir == Path::new("-") {
        anyhow::bail!("Multi-table output to stdout is not supported; specify a directory path");
    }

    if !output_dir.exists() {
        std::fs::create_dir_all(output_dir)
            .with_context(|| format!("Failed to create output directory: {output_dir:?}"))?;
        tracing::info!("Created output directory: {:?}", output_dir);
    } else if !output_dir.is_dir() {
        anyhow::bail!("Output path exists but is not a directory: {output_dir:?}");
    }

    let mut sinks = HashMap::new();
    let extension = format.metadata().default_extension;

    for (table_name, table_config) in &config.tables {
        let file_path = output_dir.join(format!("{table_name}.{extension}"));

        let sink: Box<dyn DataSink> = match format {
            OutputFormat::GeoJson => Box::new(GeoJsonSink::new(&file_path)?),
            OutputFormat::GeoJsonl => Box::new(GeoJsonlSink::new(&file_path)?),
            OutputFormat::GeoParquet => {
                let columns = collect_columns_for_table(table_config)?;
                if include_properties {
                    Box::new(GeoParquetSink::new_with_properties(
                        &file_path, columns, true,
                    )?)
                } else {
                    Box::new(GeoParquetSink::new(&file_path, columns)?)
                }
            }
            OutputFormat::Csv => {
                let columns = collect_columns_for_table(table_config)?;
                Box::new(CsvWktSink::new(&file_path, columns)?)
            }
            OutputFormat::MapRoulette => Box::new(MapRouletteSink::new(&file_path)?),
        };

        tracing::info!(
            "Sink: {} -> {:?} ({} columns)",
            table_name,
            file_path,
            table_config.columns.len()
        );

        sinks.insert(table_name.clone(), Arc::new(Mutex::new(sink)));
    }

    Ok(sinks)
}

pub fn collect_columns(config: &CompiledConfig) -> Result<Vec<ColumnSpec>> {
    let table = config
        .tables
        .values()
        .next()
        .ok_or_else(|| anyhow!("No tables defined in config"))?;
    collect_columns_for_table(table)
}

fn collect_columns_for_table(table: &CompiledTable) -> Result<Vec<ColumnSpec>> {
    let mut columns: Vec<ColumnSpec> = table
        .columns
        .iter()
        .map(|col| ColumnSpec {
            name: col.name.clone(),
            col_type: col.col_type,
        })
        .collect();

    columns.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(columns)
}

pub fn needs_node_store_compiled(config: &CompiledConfig) -> bool {
    config
        .tables
        .values()
        .any(|table| table.geometry.processes_ways() || table.geometry.relation)
}
