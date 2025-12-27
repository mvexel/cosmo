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
use std::sync::Arc;

use app::{Cli, OutputFormat, init_sink, needs_node_store_compiled, process_pbf, summarize_filters_compiled};
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
    if let Some(threads) = cli.threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
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
    let (table_name, col_count, has_node, has_way, has_rel) =
        summarize_filters_compiled(&compiled);
    tracing::info!(
        "Table: '{}' with {} columns (nodes: {}, ways: {}, relations: {})",
        table_name,
        col_count,
        has_node,
        has_way,
        has_rel
    );

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
    });

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
    Initialize selected sink
    ************************
    */
    let sink = init_sink(&format, &cli.output, &compiled)?;
    let sink_handle = Arc::new(std::sync::Mutex::new(sink));

    /*
    ************************
    Main processing pipeline
    ************************
    */
    let needs_nodes = needs_node_store_compiled(&compiled);
    let start = std::time::Instant::now();
    let match_count = process_pbf(&cli, compiled, runtime, sink_handle.clone(), needs_nodes)?;

    /*
    ********************
    Clean up and metrics
    ********************
    */
    {
        let mut sink = sink_handle.lock().unwrap();
        sink.finish().context("Pipeline: Failed to finalize sink")?;
    }

    let elapsed = start.elapsed();
    tracing::info!(
        "Done! Written {} features in {:.2}s ({} features/s)",
        match_count,
        elapsed.as_secs_f64(),
        (match_count as f64 / elapsed.as_secs_f64()) as u64
    );

    Ok(())
}
