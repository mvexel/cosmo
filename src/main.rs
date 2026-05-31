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
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use app::{
    init_sink, init_sinks, needs_node_store_compiled, process_pbf, summarize_filters_compiled, Cli,
    Commands, ConvertArgs, OutputFormat, ValidateArgs,
};
use config::{FiltersConfig, RuntimeConfig};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
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

    match cli.command {
        Commands::Validate(args) => run_validate(args),
        Commands::Convert(args) => run_convert(args, cli.verbose),
    }
}

fn run_validate(args: ValidateArgs) -> Result<()> {
    if !args.filters.exists() {
        anyhow::bail!(
            "Filter configuration file not found: {:?}\n\
             Hint: Check the path or set COSMO_FILTERS environment variable.",
            args.filters
        );
    }

    /*
    ******************
    Load filter config
    ******************
    */
    let filters = FiltersConfig::load(&args.filters).with_context(|| {
        format!(
            "Failed to load filter configuration from {:?}",
            args.filters
        )
    })?;
    let compiled = Arc::new(
        filters
            .compile()
            .context("CLI: Failed to compile filter config")?,
    );

    // feedback to user about the filters
    let summary = summarize_filters_compiled(&compiled);
    tracing::info!("Config: {}", summary);

    // Log each table's details (verbose only)
    for (table_name, table) in &compiled.tables {
        tracing::info!(
            "  - Table '{}': {} columns (nodes: {}, ways: {}, relations: {})",
            table_name,
            table.columns.len(),
            table.geometry.node,
            table.geometry.processes_ways(),
            table.geometry.relation
        );
    }

    // Always print success message to stdout
    println!("✓ Configuration is valid");

    Ok(())
}

fn run_convert(args: ConvertArgs, verbose: bool) -> Result<()> {
    /*
    *********************
    Initialize thread pool
    *********************
    */
    if let Some(jobs) = args.jobs {
        rayon::ThreadPoolBuilder::new()
            .num_threads(jobs)
            .build_global()
            .context("CLI: Failed to initialize thread pool")?;
    }

    /*
    ******************
    Validate input files
    ******************
    */
    if !args.input.exists() {
        anyhow::bail!(
            "Input PBF file not found: {:?}\n\
             Hint: Check the path or set COSMO_INPUT environment variable.",
            args.input
        );
    }
    if !args.filters.exists() {
        anyhow::bail!(
            "Filter configuration file not found: {:?}\n\
             Hint: Check the path or set COSMO_FILTERS environment variable.",
            args.filters
        );
    }

    /*
    ******************
    Load filter config
    ******************
    */
    let filters = FiltersConfig::load(&args.filters).with_context(|| {
        format!(
            "Failed to load filter configuration from {:?}",
            args.filters
        )
    })?;
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
            table.geometry.processes_ways(),
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
        node_cache_mode: args
            .node_cache_mode
            .unwrap_or(runtime_defaults.node_cache_mode),
        node_cache_max_nodes: args
            .node_cache_max_nodes
            .unwrap_or(runtime_defaults.node_cache_max_nodes),
        all_tags: args.all_tags,
        bbox: args.bbox,
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
    let format = args
        .format
        .or_else(|| {
            let ext = args.output.extension()?.to_str()?;
            match ext.to_lowercase().as_str() {
                "geojson" => Some(OutputFormat::GeoJson),
                "geojsonl" | "jsonl" | "json" => Some(OutputFormat::GeoJsonl),
                "parquet" => Some(OutputFormat::GeoParquet),
                "csv" => Some(OutputFormat::Csv),
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
        let sink = init_sink(&format, &args.output, &compiled)?;
        let mut map = HashMap::new();
        map.insert(table_name.clone(), Arc::new(Mutex::new(sink)));
        map
    } else {
        // Multi-table mode: require directory
        init_sinks(&format, &args.output, &compiled)?
    };

    /*
    ************************
    Main processing pipeline
    ************************
    */
    let needs_nodes = needs_node_store_compiled(&compiled);
    let start = std::time::Instant::now();
    let match_counts = process_pbf(
        &args,
        verbose,
        compiled,
        runtime,
        sinks.clone(),
        needs_nodes,
    )?;

    /*
    ********************
    Clean up and metrics
    ********************
    */
    for sink in sinks.values() {
        let mut s = sink
            .lock()
            .map_err(|e| anyhow::anyhow!("Sink lock poisoned: {e}"))?;
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
