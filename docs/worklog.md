# Architecture Improvements — Work Log

Branch: `architecture-improvements`
Base: `0.2.0` (`83fa1b0`)

## Decision Log

| # | Decision | Rationale | ADR? |
|---|----------|-----------|------|
| D1 | Produce typed `ColumnValue::Integer`/`Float` in pipeline when column type and tag value agree | Variants were matched in all 4 sinks but never constructed in production. Fix produces typed values upstream, exercising existing arms and avoiding re-parse at sink time. | No |
| D2 | Eliminate `coords.clone()` on way hot path | `build_way_geometry` received full `&[(f64,f64)]` but only used `coords[0]` for centroid fallback. Changed to `first_coord: (f64, f64)`. | No |
| D3 | Embed PROJJSON as static string, drop `proj` dep | `proj` pulled libproj (C dep) for a single constant. EPSG:4326 PROJJSON is stable and well-known. | No |
| D4 | Enforce CCW winding order on polygon exteriors | GeoJSON RFC 7946 and GeoParquet/OGC require it. OSM data has no winding guarantee. | No — spec compliance |
| D5 | Warn on missing nodes, rate-limited | Silent data loss is worse than noisy warnings. Rate-limit to avoid flooding on incomplete extracts. | No |
| D6 | In-memory `HashMap` way store for relation support | Extract-scale only. Planet-scale would need disk-backed store — out of scope for now. | **ADR-001** |
| D7 | Multipolygon relations only for initial relation support | Route/boundary relations need different geometry types. Multipolygon is the highest-value relation type. | **ADR-001** |
| D8 | Fix `relation` serde default from `default_true` to `#[serde(default)]` | Caught by integration test. Without fix, existing configs would suddenly process relations on upgrade. Must be opt-in. | No — bug fix |

## Work Log

| Step | Description | Commit | Status |
|------|-------------|--------|--------|
| 0 | Add mise.toml for toolchain management | `113e7dc` | Done |
| 1 | Produce typed ColumnValue in pipeline | `d29dd5f` | Done |
| 2 | Eliminate `coords.clone()` on hot path | `7c7c9f4` | Done |
| 3 | Replace `proj` with static PROJJSON | `3302806` | Done |
| 4 | Enforce polygon winding order | `81e5b73` | Done |
| 5 | Warn on missing nodes in ways | `c795332` | Done |
| 6 | Strengthen pipeline unit tests | `cec3f62` | Done |
| 7-9 | Implement multipolygon relation support | `9413dfe` | Done |
| 10 | Add relation integration tests + fix default | `593084c` | Done |
