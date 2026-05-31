# Architecture Improvements — Work Log

## Branches

| Branch | Base | Purpose |
|--------|------|---------|
| `architecture-improvements` | `0.2.0` | Cleanup, correctness, tests, relation support, boundary relations |
| `planet-scale-relations` | `architecture-improvements` | 3-pass architecture for large inputs |

## Decision Log

| # | Decision | Rationale | ADR? |
|---|----------|-----------|------|
| D1 | Produce typed `ColumnValue::Integer`/`Float` upstream | Variants matched in sinks but never constructed. Avoids re-parse at sink time. | No |
| D2 | Eliminate `coords.clone()` on way hot path | Only `coords[0]` was used. Changed to `first_coord: (f64, f64)`. | No |
| D3 | Embed PROJJSON as static string, drop `proj` dep | `proj` pulled libproj for a single constant. | No |
| D4 | Enforce CCW winding order on polygon exteriors | Required by GeoJSON RFC 7946 and GeoParquet/OGC. | No |
| D5 | Warn on missing nodes, rate-limited | Silent data loss is worse than noisy warnings. | No |
| D6 | In-memory way store with pass0 filtering | Extract-scale: collect all relation-referenced ways. Planet-scale: pass0 pre-scan limits to ~20M ways (~2 GB). | **ADR-001** |
| D7 | Multipolygon + boundary relations | Both use identical ring assembly. Route relations excluded (need MultiLineString). | **ADR-001** |
| D8 | Fix `relation` serde default to `false` | Caught by integration test. Existing configs must not suddenly process relations. | No |
| D9 | 3-pass when relations enabled | Pass 0 scans relations for way IDs. Pass 1 collects only those ways. No extra cost when relations disabled. | **ADR-001** |

## Commit Log

### `architecture-improvements`

| Commit | Description |
|--------|-------------|
| `113e7dc` | Add mise.toml pinning Rust 1.88.0 |
| `7c7c9f4` | Eliminate `coords.clone()` on way hot path |
| `3302806` | Remove `proj` crate, embed EPSG:4326 PROJJSON |
| `d29dd5f` | Produce typed `ColumnValue::Integer/Float` from pipeline |
| `81e5b73` | Enforce CCW winding order on polygon exteriors |
| `c795332` | Warn on missing node references in ways |
| `cec3f62` | 11 pipeline unit tests |
| `9413dfe` | Multipolygon relation support |
| `593084c` | Relation integration tests + fix default |
| `96a008e` | Worklog + ADR-001 |
| `bce14e9` | Boundary relation support |

### `planet-scale-relations`

| Commit | Description |
|--------|-------------|
| `81f1e87` | 3-pass architecture for planet-scale relations |
