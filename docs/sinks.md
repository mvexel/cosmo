# Adding Sinks

This project uses a column-first sink contract so all formats share the same data model.

## Data Model

Sinks receive a `FeatureRow`:

- `geometry`: `geo_types::Geometry<f64>`
- `columns`: typed values from the filters YAML
- `extras`: a JSON map for any fields not mapped to columns (e.g., `tags`)

Types are defined in `src/sinks/mod.rs`:

- `ColumnValue` (`String`, `Integer`, `Float`)
- `FeatureRow`
- `DataSink` trait

## DataSink trait

Implement:

```rust
fn add_feature(&mut self, row: FeatureRow) -> Result<()>;
fn finish(&mut self) -> Result<()>;
```

## Behavior

- The core builds `FeatureRow` from tags/columns and optional `meta:*` sources.
- Sinks choose how to materialize `columns` and `extras`.
  - GeoJSON merges both into `properties`.
  - Parquet writes explicit columns and stores `extras` in a JSON `properties` column.

## Adding a new sink

1) Create a new file in `src/sinks/` (e.g., `flatgeobuf.rs`).
2) Implement `DataSink` for your type.
3) Register the module + re-export in `src/sinks/mod.rs`.
4) Update `OutputFormat` in `src/main.rs` and add a constructor in `init_sink`.

Keep output deterministic and avoid side effects in `add_feature` beyond writing.
