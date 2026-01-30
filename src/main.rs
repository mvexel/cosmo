mod app;
mod config;
mod dsl;
mod expr;
mod mapping;
mod metadata;
mod pipeline;
mod sinks;
mod storage;
mod utils;

use anyhow::{Context, Result};
use clap::Parser;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use app::{
    Cli, OutputFormat, init_sink, init_sinks, needs_node_store_compiled, process_pbf,
    summarize_filters_compiled,
};
use config::{FiltersConfig, RuntimeConfig};

// anyhow::Result allows us to use ? operator in main to emit errors
fn main() -> Result<()> {
    let cli = Cli::parse();

    /*
    ******************
    Initialize logging
    ******************
    */
    let level = if cli.verbose {
        tracing::Level::INFO
    } else {
        tracing::Level::WARN
    };

    // tracing_subscriber is the engine that listens to log events and outputs them
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(level.into())
                .from_env_lossy(),
        )
        .with_writer(std::io::stderr)
        .init();

    /*
    *********************
    Initialize thread pool
    *********************
    */
    if let Some(jobs) = cli.jobs {
        rayon::ThreadPoolBuilder::new()
            .num_threads(jobs)
            .build_global()
            .context("CLI: Failed to initialize thread pool")?;
    }

    /*
    ******************
    Load filter config
    ******************
    */
    let filters = FiltersConfig::load(&cli.filters)?;
    let compiled = Arc::new(
        filters
            .compile()
            .context("CLI: Failed to compile filter config")?,
    );

    // feedback to user about the filters
    let summary = summarize_filters_compiled(&compiled);
    tracing::info!("Config: {}", summary);

    // Log each table's details
    for (table_name, table) in &compiled.tables {
        tracing::info!(
            "  - Table '{}': {} columns (nodes: {}, ways: {}, relations: {})",
            table_name,
            table.columns.len(),
            table.geometry.node,
            table.geometry.way.enabled(),
            table.geometry.relation
        );
    }

    /*
    *************************
    Initialize runtime config
    *************************
    */
    let runtime_defaults = RuntimeConfig::default();
    let runtime = Arc::new(RuntimeConfig {
        node_cache_mode: cli
            .node_cache_mode
            .unwrap_or(runtime_defaults.node_cache_mode),
        node_cache_max_nodes: cli
            .node_cache_max_nodes
            .unwrap_or(runtime_defaults.node_cache_max_nodes),
        all_tags: cli.all_tags,
        bbox: cli.bbox,
    });

    // Log bbox filter if active
    if let Some([min_lon, min_lat, max_lon, max_lat]) = runtime.bbox {
        tracing::info!(
            "Bbox filter: [{}, {}, {}, {}]",
            min_lon,
            min_lat,
            max_lon,
            max_lat
        );
    }

    // Detect format from extension if not provided
    let format = cli
        .format
        .or_else(|| {
            let ext = cli.output.extension()?.to_str()?;
            match ext.to_lowercase().as_str() {
                "geojson" => Some(OutputFormat::GeoJson),
                "geojsonl" | "jsonl" | "json" => Some(OutputFormat::GeoJsonl),
                "parquet" => Some(OutputFormat::GeoParquet),
                _ => None,
            }
        })
        .context("CLI: Could not detect output format from extension; use --format")?;

    /*
    ************************
    Initialize sink(s)
    ************************
    */
    let sinks = if compiled.tables.len() == 1 {
        // Single table mode: support stdout and file output
        let (table_name, _) = compiled.tables.iter().next().unwrap();
        let sink = init_sink(&format, &cli.output, &compiled)?;
        let mut map = HashMap::new();
        map.insert(table_name.clone(), Arc::new(Mutex::new(sink)));
        map
    } else {
        // Multi-table mode: require directory
        init_sinks(&format, &cli.output, &compiled)?
    };

    /*
    ************************
    Main processing pipeline
    ************************
    */
    let needs_nodes = needs_node_store_compiled(&compiled);
    let start = std::time::Instant::now();
    let match_counts = process_pbf(&cli, compiled, runtime, sinks.clone(), needs_nodes)?;

    /*
    ********************
    Clean up and metrics
    ********************
    */
    for sink in sinks.values() {
        let mut s = sink.lock().unwrap();
        s.finish().context("Pipeline: Failed to finalize sink")?;
    }

    let elapsed = start.elapsed();
    let total_features: u64 = match_counts.values().sum();
    tracing::info!(
        "Done! Processed {} total features in {:.2}s ({} features/s)",
        total_features,
        elapsed.as_secs_f64(),
        (total_features as f64 / elapsed.as_secs_f64()) as u64
    );
    for (table_name, count) in &match_counts {
        tracing::info!("  - Table '{}': {} features", table_name, count);
    }

    Ok(())
}
