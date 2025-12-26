mod app;
mod config;
mod metadata;
mod pipeline;
mod sinks;
mod storage;
mod utils;

use anyhow::{Context, Result};
use clap::Parser;
use std::sync::Arc;

use app::{Cli, OutputFormat, init_sink, needs_node_store, process_pbf, summarize_filters};
use config::{FiltersConfig, RuntimeConfig};

fn main() -> Result<()> {
    let cli = Cli::parse();

    let level = if cli.verbose {
        tracing::Level::INFO
    } else {
        tracing::Level::WARN
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(level.into())
                .from_env_lossy(),
        )
        .with_writer(std::io::stderr)
        .init();

    if let Some(threads) = cli.threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()
            .context("CLI: Failed to initialize thread pool")?;
    }

    let filters = Arc::new(FiltersConfig::load(&cli.filters)?);

    let (table_count, col_count, has_node, has_way, has_rel) = summarize_filters(&filters);
    tracing::info!(
        "Filters: {} tables, {} columns (nodes: {}, ways: {}, relations: {})",
        table_count,
        col_count,
        has_node,
        has_way,
        has_rel
    );

    let runtime = Arc::new(RuntimeConfig {
        node_cache_mode: cli.node_cache_mode.unwrap_or(config::NodeCacheMode::Auto),
        node_cache_max_nodes: cli.node_cache_max_nodes.unwrap_or(1_000_000_000),
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

    let sink = init_sink(&format, &cli.output, &filters)?;
    let sink_handle = Arc::new(std::sync::Mutex::new(sink));

    let needs_nodes = needs_node_store(&filters);
    let start = std::time::Instant::now();
    let match_count = process_pbf(&cli, filters, runtime, sink_handle.clone(), needs_nodes)?;

    // Finalize sink
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
