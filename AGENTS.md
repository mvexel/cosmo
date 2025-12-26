# AGENTS.md - Cosmo Project Overview

> A high-performance Rust CLI tool for converting OSM PBF files to GeoJSON, GeoJSONL, and GeoParquet formats with declarative filtering and geometry transformation.

## Project Summary

**Cosmo** is a command-line tool that processes OpenStreetMap PBF files and exports filtered, transformed geospatial data to multiple output formats. It uses a YAML-based configuration for declarative filtering and column mapping, supports parallel processing with Rayon, and implements multiple node caching strategies optimized for different input sizes.

## Directory Structure

```
cosmo/
├── src/
│   ├── main.rs              # CLI entrypoint, PBF processing pipeline (1249 lines)
│   ├── config/
│   │   └── mod.rs           # YAML configuration parsing, filter/geometry/column types
│   ├── sinks/
│   │   ├── mod.rs           # DataSink trait and shared types
│   │   ├── geojson.rs       # GeoJSON FeatureCollection output
│   │   ├── geojsonl.rs      # Newline-delimited GeoJSON (streaming to stdout supported)
│   │   └── geoparquet.rs    # GeoParquet with Arrow/Parquet integration
│   └── storage/
│       └── mod.rs           # Node coordinate cache (sparse, dense, memory modes)
├── tests/
│   ├── pbf_integration.rs   # Integration tests using fixture PBF
│   └── pass_through_integration.rs
├── examples/                # Example filter YAML files
├── fixture/                 # Test PBF file (library_square.osm.pbf)
├── docs/
│   └── sinks.md             # Documentation for implementing new sinks
├── Cargo.toml               # Dependencies and project metadata
└── README.md                # User-facing documentation
```

## Core Architecture

### Processing Pipeline

1. **Configuration Loading**: Parse YAML filters into `FiltersConfig` struct
2. **Sink Initialization**: Create output writer based on format
3. **Node Indexing (Pass 1)**: For way/relation processing, cache node coordinates
4. **Feature Extraction (Pass 2)**: Parallel block processing with filter matching
5. **Output**: Stream `FeatureRow` to sink, flush and finalize

### Key Abstractions

| Abstraction | Location | Purpose |
|-------------|----------|---------|
| `DataSink` trait | `src/sinks/mod.rs` | Pluggable output format interface |
| `NodeStoreWriter/Reader` | `src/storage/mod.rs` | Node coordinate caching strategies |
| `FiltersConfig` | `src/config/mod.rs` | Declarative filter configuration |
| `FeatureRow` | `src/sinks/mod.rs` | Intermediate representation for output |
| `FilterExpr` | `src/config/mod.rs` | Recursive filter expression tree |

### Node Cache Modes

- **Auto**: Selects mode based on input file size (default)
- **Sparse**: Sorted array with binary search - efficient for extracts (<5GB)
- **Dense**: Memory-mapped file indexed by node ID - best for planet/continent (≥5GB)
- **Memory**: In-memory HashMap - no disk usage but high RAM

## Dependencies

Key dependencies (see `Cargo.toml`):

| Crate | Purpose |
|-------|---------|
| `osmpbf` | PBF parsing (using async-blob-reader fork) |
| `rayon` | Parallel iteration for block processing |
| `crossbeam-channel` | Bounded channels for producer/consumer |
| `arrow-array`, `parquet` | GeoParquet output with Arrow integration |
| `geo`, `geo-types`, `geozero` | Geometry types and WKB conversion |
| `clap` | CLI parsing with env var support |
| `serde`, `config` | YAML configuration parsing |
| `memmap2` | Memory-mapped file I/O for dense cache |

## Configuration Reference

Filter YAML structure:

```yaml
tables:
  <table_name>:
    filter:
      # FilterExpr: tag/value matching, any/all/not logic
    geometry:
      node: true/false
      way: linestring/polygon/centroid/false
      closed_way: polygon/centroid/linestring
      relation: true/false
    columns:
      - name: "<output_column>"
        source: "tag:<key>" | "meta:<field>" | "tags" | "meta" | "refs"
        type: "string" | "integer" | "float" | "json"
```

## Coding Conventions

### Error Handling
- Use `anyhow::Result` for propagation with `.context()` for error chains
- Avoid `unwrap()` in production code; use `?` operator

### Parallelism
- Rayon for parallel iteration (`par_bridge()`)
- Crossbeam channels for producer/consumer patterns
- `Arc<Mutex<>>` for shared mutable state (sink handle)

### Testing
- Unit tests inline in each module (`#[cfg(test)] mod tests`)
- Integration tests in `tests/` directory
- Use `tempfile::NamedTempFile` for test file I/O

## Adding Features

### New Output Format (Sink)

1. Create `src/sinks/<format>.rs`
2. Implement `DataSink` trait:
   ```rust
   fn add_feature(&mut self, row: FeatureRow) -> Result<()>;
   fn finish(&mut self) -> Result<()>;
   ```
3. Add to `src/sinks/mod.rs` (module + re-export)
4. Add variant to `OutputFormat` enum in `main.rs`
5. Add constructor case in `init_sink()` function

### New Node Cache Mode

1. Add variant to `NodeCacheMode` enum in `src/config/mod.rs`
2. Implement `NodeStoreWriterImpl` and `NodeStoreReaderImpl` variants
3. Add constructor in `NodeStoreWriter`
4. Update `resolve_node_cache_mode()` if auto-selection needed
5. Add case in `process_pbf()` for mode initialization

## Testing

```bash
# Run all tests
cargo test

# Run with verbose output
cargo test -- --nocapture

# Run specific test
cargo test extracts_tree_nodes

# Run integration tests only
cargo test --test pbf_integration
```

## CLI Usage

```bash
cosmo --input <file.osm.pbf> \
      --output <output> \
      --format <geojson|geojsonl|parquet> \
      --filters <filters.yaml> \
      [--node-cache-mode <auto|sparse|dense|memory>] \
      [--all-tags] \
      [--verbose]
```

Environment variables: `COSMO_INPUT`, `COSMO_OUTPUT`, `COSMO_FORMAT`, etc.

## Performance Notes

- GeoJSONL with `--output -` streams to stdout, enabling `cosmo | tippecanoe` pipelines
- Sparse mode requires sorted PBF input (`osmium sort`)
- Dense mode creates sparse files (~128GB virtual, actual size depends on nodes)
- Batch size of 10,000 features for Parquet row groups
