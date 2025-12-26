# Cosmo Rust Improvement Plan

> Expert Rust engineering review identifying bad practices, architectural debt, and improvement opportunities.

## Executive Summary

The cosmo codebase is functional and demonstrates solid understanding of Rust basics, but has accumulated several architectural issues that will impact maintainability, testability, and performance as the project grows. The most critical issues are:

1. **Monolithic main.rs** (1249 lines) - violates single responsibility
2. **Unsafe memory-mapping without safety documentation**
3. **Duplicate code across processing paths**
4. **Missing structured logging**
5. **Tight coupling between components**

---

## Priority 1: Critical Issues

### 1.1 Monolithic `main.rs` (~1249 lines)

**Problem**: The entire processing pipeline lives in `main.rs`, mixing CLI parsing, business logic, data processing, and coordination. This violates separation of concerns and makes testing difficult.

**Current Structure**:
```
main.rs contains:
├── CLI argument parsing (Cli struct)
├── Progress reporting (ProgressCounter)  
├── Pipeline orchestration (process_pbf, pass1_index_nodes, pass2_process, pass_nodes_only)
├── Feature row building (build_feature_row, build_metadata_*)
├── Filter matching logic (matches_filter, matches_tag, glob_match)
├── Geometry construction (build_way_geometry)
├── Block processing (process_block_collect, process_block_nodes_only_collect)
└── Unit tests
```

**Recommended Refactoring**:

```
src/
├── main.rs                  # CLI only (~50 lines)
├── lib.rs                   # Re-exports for library use
├── pipeline/
│   ├── mod.rs               # Pipeline orchestration
│   ├── indexer.rs           # Pass 1: Node indexing
│   ├── processor.rs         # Pass 2: Block processing
│   └── progress.rs          # Progress reporting utilities
├── filter/
│   ├── mod.rs               # Filter matching engine
│   └── glob.rs              # Glob pattern matching
├── geometry/
│   └── mod.rs               # Geometry building from way refs
├── feature/
│   ├── mod.rs               # FeatureRow building
│   └── metadata.rs          # OSM metadata extraction
├── config/                  # (existing)
├── sinks/                   # (existing)
└── storage/                 # (existing)
```

**Benefits**:
- Each module is independently testable
- `cosmo` can be used as a library (`use cosmo::pipeline::Pipeline`)
- Clearer ownership boundaries
- Easier to parallelize development

---

### 1.2 Unsafe Memory Mapping Without `SAFETY` Comments

**Problem**: The storage module uses `unsafe` for memory-mapping without documenting safety invariants.

**Location**: `src/storage/mod.rs` lines 106, 134-136, 285-287

**Current Code**:
```rust
let mmap = unsafe { MmapMut::map_mut(&file).context("Failed to map node store file")? };
```

**Recommended Fix**:
```rust
// SAFETY: The file handle is exclusively owned by this struct.
// The mmap remains valid as long as the file exists (guaranteed by NamedTempFile).
// No other process accesses this file. The mmap is only accessed in single-threaded
// context during write phase, then converted to read-only before sharing.
let mmap = unsafe { 
    MmapMut::map_mut(&file).context("Failed to map node store file")? 
};
```

**Additionally**: Consider wrapping all mmap operations in a `MmapHandle` abstraction that enforces invariants at the type level.

---

### 1.3 Duplicate Block Processing Functions

**Problem**: `process_block_collect` and `process_block_nodes_only_collect` share ~80% of their code with minor variations.

**Location**: Lines 476-564 and 1076-1130 of `main.rs`

**Current Duplication**:
```rust
// Both functions iterate blocks, match filters, build feature rows
// Only difference: one looks up node coords, one skips ways
```

**Recommended Fix**: Extract a generic block processor with configuration:

```rust
struct BlockProcessor<'a> {
    filters: &'a FiltersConfig,
    runtime: &'a RuntimeConfig,
    node_store: Option<&'a NodeStoreReader>,
}

impl BlockProcessor<'_> {
    fn process(&self, block: PrimitiveBlock) -> Result<Vec<FeatureRow>> {
        let mut rows = Vec::new();
        for element in block.elements() {
            self.process_element(element, &mut rows)?;
        }
        Ok(rows)
    }
    
    fn process_element(&self, element: Element, rows: &mut Vec<FeatureRow>) {
        match element {
            Element::Node(node) | Element::DenseNode(node) => {
                self.process_node(node, rows);
            }
            Element::Way(way) if self.node_store.is_some() => {
                self.process_way(way, rows);
            }
            _ => {}
        }
    }
}
```

---

## Priority 2: Architectural Improvements

### 2.1 Introduce Structured Logging

**Problem**: The codebase uses ad-hoc `eprintln!` and a custom `vprintln!` macro. This doesn't scale for debugging production issues.

**Current Code**:
```rust
macro_rules! vprintln {
    ($verbose:expr, $($arg:tt)*) => {
        if $verbose {
            // TODO: Consider a structured logging crate (log/tracing) if verbosity grows.
            eprintln!($($arg)*);
        }
    };
}
```

**Recommended Fix**: Adopt `tracing` crate with spans for pipeline phases:

```rust
use tracing::{debug, info, instrument, span, Level};

#[instrument(skip(node_store, filters))]
fn pass1_index_nodes(path: &Path, node_store: NodeStoreWriter) -> Result<(NodeStoreWriter, u64)> {
    let span = span!(Level::INFO, "pass1", path = %path.display());
    let _enter = span.enter();
    
    info!(mode = %if use_parallel { "parallel" } else { "sequential" });
    // ...
}
```

**Benefits**:
- Structured JSON logging for production
- Span-based timing automatically
- Filter by component: `RUST_LOG=cosmo::pipeline=debug`
- Async-aware tracing for future async work

---

### 2.2 Type Safety for Column Types

**Problem**: Column types are represented as strings (`"string"`, `"integer"`) and parsed at runtime.

**Location**: `src/config/mod.rs` line 180, `main.rs` line 466-474

**Current Code**:
```rust
pub struct ColumnConfig {
    pub name: String,
    pub source: String,
    #[serde(rename = "type")]
    pub col_type: String,  // Runtime string parsing
}

fn parse_column_type(value: &str) -> Result<ColumnType> {
    match value.to_ascii_lowercase().as_str() {
        "string" => Ok(ColumnType::String),
        // ...
    }
}
```

**Recommended Fix**: Use `ColumnType` enum directly with serde:

```rust
#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    String,
    Integer,
    Float,
    Json,
}

pub struct ColumnConfig {
    pub name: String,
    pub source: String,
    #[serde(rename = "type")]
    pub col_type: ColumnType,  // Parsed at deserialization
}
```

**Benefits**:
- Errors surface at config load time, not processing time
- Eliminates `parse_column_type()` function entirely
- Type-safe throughout the codebase

---

### 2.3 Source Parsing with Typed Enum

**Problem**: Column `source` field is a string parsed with `starts_with()` checks scattered throughout `build_feature_row`.

**Location**: `main.rs` lines 898-939

**Current Code**:
```rust
if col.source == "tags" { ... }
else if col.source == "meta" { ... }
else if col.source == "refs" { ... }
else if col.source.starts_with("tag:") { 
    let tag_key = &col.source[4..];
    // ...
}
```

**Recommended Fix**: Parse at config load time:

```rust
#[derive(Debug, Clone)]
pub enum ColumnSource {
    AllTags,           // "tags"
    AllMeta,           // "meta"
    Refs,              // "refs"
    Tag(String),       // "tag:name"
    Meta(MetaField),   // "meta:id", "meta:timestamp", etc.
}

#[derive(Debug, Clone)]
pub enum MetaField {
    Id, Version, Visible, Changeset, Timestamp, Uid, User
}

impl FromStr for ColumnSource {
    // Parse once, use everywhere
}
```

---

### 2.4 Builder Pattern for FeatureRow

**Problem**: `build_feature_row` has 6 parameters and growing complexity.

**Location**: `main.rs` lines 887-956

**Current Signature**:
```rust
fn build_feature_row(
    geometry: Geometry<f64>,
    tags: &HashMap<String, String>,
    columns: &[config::ColumnConfig],
    runtime: &RuntimeConfig,
    metadata: Option<MetadataFields>,
    refs: Option<Vec<i64>>,
) -> FeatureRow
```

**Recommended Fix**: Builder pattern:

```rust
impl FeatureRow {
    pub fn builder(geometry: Geometry<f64>) -> FeatureRowBuilder {
        FeatureRowBuilder::new(geometry)
    }
}

struct FeatureRowBuilder {
    geometry: Geometry<f64>,
    tags: Option<HashMap<String, String>>,
    columns: Vec<(String, ColumnValue)>,
    metadata: Option<MetadataFields>,
    refs: Option<Vec<i64>>,
}

impl FeatureRowBuilder {
    pub fn with_tags(mut self, tags: HashMap<String, String>) -> Self { ... }
    pub fn with_metadata(mut self, meta: MetadataFields) -> Self { ... }
    pub fn with_refs(mut self, refs: Vec<i64>) -> Self { ... }
    pub fn add_column(mut self, config: &ColumnConfig) -> Self { ... }
    pub fn build(self) -> FeatureRow { ... }
}
```

---

## Priority 3: Code Quality Improvements

### 3.1 Edition 2024 (Resolved)

> [!NOTE]
> Rust 2024 is stable and is being used for this project to leverage modern features like let-chains.

**Problem**: `Cargo.toml` specifies `edition = "2024"` which doesn't exist yet.

**Location**: `Cargo.toml` line 4

**Fix**: Utilized Rust 2024 (now stable) for modern features.

---

### 3.2 Consistent Error Context Messages

**Problem**: Some error messages include context, others don't. Inconsistent capitalization.

**Examples**:
```rust
.context("Failed to create temporary sparse node cache file")  // Good
.context("Failed to map node store file")?                     // Missing "sparse/dense"
Err(anyhow!("node ids are out of order..."))                   // lowercase
```

**Recommended Style Guide**:
- Always start with capital letter
- Include the component name (sparse/dense cache, parquet sink, etc.)
- Include relevant IDs/paths in message

---

### 3.3 Progress Counter is Not Thread-Safe

**Problem**: `ProgressCounter` uses interior mutability but isn't `Send`/`Sync`. It's wrapped in `Arc<Mutex<>>` in some places but not others.

**Location**: `main.rs` lines 138-173

**Current Code**:
```rust
struct ProgressCounter {
    label: &'static str,
    interval: u64,
    count: u64,  // Not atomic
}
```

**Recommended Fix**: Use atomics for thread-safe counting:

```rust
use std::sync::atomic::{AtomicU64, Ordering};

pub struct ProgressCounter {
    label: &'static str,
    interval: u64,
    count: AtomicU64,
}

impl ProgressCounter {
    pub fn inc(&self, delta: u64) {
        let old = self.count.fetch_add(delta, Ordering::Relaxed);
        let new = old + delta;
        if (new / self.interval) > (old / self.interval) {
            self.print(new);
        }
    }
}
```

---

### 3.4 Clone Overhead in Hot Path

**Problem**: Unnecessary clones in the hot processing loop.

**Location**: `main.rs` line 543

**Current Code**:
```rust
let line_string = LineString::from(coords.clone());  // Clones Vec
let geometry = build_way_geometry(&table.geometry, line_string, &coords);  // Uses original
```

**Recommended Fix**: Build LineString directly from iterator:

```rust
let coords: Vec<_> = refs.iter()
    .filter_map(|&id| node_store.get(id as u64))
    .collect();
    
if coords.len() < 2 { continue; }

let line_string = LineString::from_iter(coords.iter().copied());
let geometry = build_way_geometry(&table.geometry, line_string, &coords);
```

---

### 3.5 Use `let-else` Instead of `if let` with `continue`

**Problem**: Inconsistent use of Rust 1.65+ `let-else` pattern.

**Examples to Update**:
```rust
// Current
if coords.len() < 2 {
    continue;
}

// Better (already correct)
let Some(tag_val) = tags.get(&tag_match.tag) else {
    return false;
};

// Could be improved:
// if let Some(val) = tags.get(tag_key)... -> let Some(val) = ... else { continue };
```

---

## Priority 4: Performance Optimizations

### 4.1 Pre-allocate FeatureRow Vector

**Problem**: `process_block_collect` starts with empty Vec and grows dynamically.

**Location**: `main.rs` line 482

**Current Code**:
```rust
let mut rows = Vec::new();
```

**Recommended Fix**:
```rust
// Estimate based on typical block density
let mut rows = Vec::with_capacity(block.elements().size_hint().0 / 10);
```

---

### 4.2 Avoid Repeated Tag Map Building

**Problem**: `build_tag_map()` allocates a new HashMap for every element.

**Location**: `main.rs` lines 486, 505, 527, etc.

**Recommended Fix**: Reuse HashMap with clear:
```rust
// In processing loop, reuse allocation
let mut tag_map = HashMap::with_capacity(32);
for element in block.elements() {
    tag_map.clear();
    tag_map.extend(element.tags().map(|(k, v)| (k.to_string(), v.to_string())));
    // ...
}
```

---

### 4.3 Use `SmallVec` for Way Refs

**Problem**: Most ways have <256 nodes but allocate full Vec.

**Location**: `main.rs` line 533

**Recommendation**: Use `smallvec::SmallVec<[i64; 256]>` to stack-allocate common cases.

---

## Priority 5: Testing Improvements

### 5.1 Add Benchmarks

**Problem**: No performance benchmarks exist.

**Recommended Addition**:
```rust
// benches/processing.rs
use criterion::{criterion_group, criterion_main, Criterion};

fn bench_filter_matching(c: &mut Criterion) {
    let tags = create_test_tags();
    let filter = create_complex_filter();
    
    c.bench_function("filter_matching", |b| {
        b.iter(|| matches_filter(&filter, &tags))
    });
}
```

---

### 5.2 Property-Based Testing

**Problem**: Unit tests only cover specific cases.

**Recommended Addition for `glob_match`**:
```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn glob_wildcard_matches_all(value in "\\PC*") {
        prop_assert!(glob_match("*", &value));
    }
    
    #[test]
    fn glob_identity(s in "[a-z]+") {
        prop_assert!(glob_match(&s, &s));
    }
}
```

---

### 5.3 Test Coverage for Error Paths

**Problem**: Many error paths are untested.

**Missing Tests**:
- Config file not found
- Invalid YAML syntax
- Conflicting column types across tables
- Node ID overflow in dense cache
- Malformed PBF blocks

---

## Implementation Roadmap

### Phase 1: Quick Wins (Completed)
- [x] Confirm Rust 2024 (stable)
- [x] Add SAFETY comments to unsafe blocks
- [x] Use ColumnType enum with serde
- [x] Consistent error messages

### Phase 2: Refactoring (Completed)
- [x] Split main.rs into modules
- [x] Extract BlockProcessor abstraction
- [x] Add tracing for structured logging
- [x] Atomic ProgressCounter

### Phase 3: Performance (3-5 days)
- [ ] Pre-allocate vectors
- [ ] Reuse tag HashMap
- [ ] Consider SmallVec for way refs
- [ ] Add benchmarks

### Phase 4: Testing (3-5 days)
- [ ] Add property-based tests
- [ ] Improve error path coverage
- [ ] Add integration tests for edge cases

---

## Conclusion

The cosmo codebase is functional and demonstrates good understanding of Rust idioms in many places. The primary technical debt is the monolithic `main.rs` which should be the first priority to address. The unsafe code needs safety documentation. After those are addressed, the performance optimizations and testing improvements will provide substantial value.

Total estimated effort: **2-3 weeks** for full implementation.
