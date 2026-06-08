# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project follows pre-1.0 semantic versioning.

## [0.2.0] - 2026-05-31

### Added

- **MapRoulette output format** (`--format maproulette`): RFC 7464 line-by-line GeoJSON with record separator prefix and FeatureCollection wrapping, ready for direct upload to MapRoulette challenges.
- **Multipolygon and boundary relation support** (`relation: true` in geometry config): assembles outer/inner way members into polygon/multipolygon geometries for `type=multipolygon` and `type=boundary` relations.
- **3-pass architecture** for planet-scale relation processing: pass 0 scans relation member way IDs, pass 1 collects only those ways, pass 2 processes all elements. Skipped when no table enables relations.
- **Constant column source** (`source: const:<value>`): emits typed literal values without compiling/evaluating CEL per feature.
- **Bounding box output filter** (`--bbox min_lon,min_lat,max_lon,max_lat`): emits only features whose resolved geometry intersects the bbox, without clipping geometry or replacing `osmium extract` for topology-preserving PBF cuts.
- Added example filter for MapRoulette challenges (`examples/maproulette/business-missing-phone.yml`).
- Added example filter for building extraction with relations (`examples/buildings.yaml`).
- Added `closed_way: false` support to exclude closed ways independently from open ways.
- Added regression coverage for `way: false` combined with `closed_way: polygon`, ensuring closed-way polygons are still emitted.
- Added validation coverage for rejected geometry forms:
  - `way: polygon` is rejected because open ways cannot form valid polygons.
  - `way: true` is rejected because geometry modes must be explicit.
- Added coverage for the documented `filter: {}` match-all shorthand.
- Added coverage that open-way `centroid` uses the line centroid instead of treating the open line as a polygon.
- Added 11 pipeline unit tests for `build_way_geometry`, `build_feature_row`, and bbox filtering.
- Added relation integration tests.

### Changed

- Simplified filter configuration loading to parse the single YAML file directly with `serde_yaml` instead of routing through the multi-source `config` crate.
- Standardized the public filter interface on the string DSL, while keeping `filter: {}` as the documented match-all shorthand.
- Kept the raw-config to compiled-config phase split so filters, mappings, and CEL expressions are parsed once before PBF processing.
- Replaced the `winnow`-based tokenizer with a small purpose-built Rust lexer for the existing filter DSL.
- Updated open-way geometry semantics so `way` only accepts valid open-way outputs: `linestring`, `centroid`, or `false`.

  The `centroid` mode now means a line centroid for open ways and an area centroid for closed ways.
- Replaced `auto` node cache selection's fixed 5 GiB file-size cutoff with a node pre-scan that chooses from real node count, max node ID, and node ID sortedness.
- GeoParquet now omits the always-empty `properties` column unless extra properties are requested with `--all-tags`.
- Updated documentation to distinguish open-way handling from closed-way handling and to remove open-way polygon examples.
- Updated the declared Rust minimum version to 1.88.0 to match the current dependency graph.
- Eliminated `coords.clone()` allocation on the way-processing hot path.
- Produce typed `ColumnValue::Integer`/`Float` from the pipeline instead of re-parsing at sink time.
- Enforce CCW winding order on polygon exterior rings per GeoJSON RFC 7946.
- Warn (rate-limited) on missing node references in ways instead of silent data loss.

### Removed

- Removed the undocumented structured YAML filter forms:
  - `filter: { tag: ... }`
  - `filter: { any: ... }`
  - `filter: { all: ... }`
  - `filter: { not: ... }`
  - arbitrary simple tag maps such as `filter: { highway: primary }`
- Removed `way: polygon` support for open ways.
- Removed undocumented boolean `true` shorthand for `way` and `closed_way`.
- Removed direct dependencies on `config`, `winnow`, and `proj` (PROJJSON embedded as a static string).

### Fixed

- Fixed node-store detection and way processing to consider `closed_way` independently from `way`, so disabling open ways no longer suppresses closed-way output.
- Fixed open-way centroid calculation to use the `LineString` itself rather than an invalid polygon shell.
- Fixed docs that previously implied open ways could safely be emitted as polygons.
- Fixed `relation` geometry config defaulting to `true` — now defaults to `false` so existing configs are unaffected.
- Fixed dense node cache SIGBUS on large inputs when the temporary mmap cache landed on tmpfs-backed `/tmp`: dense temp caches now default to `/var/tmp` unless `TMPDIR` is set, and dense cache creation fails fast when the target filesystem lacks enough free space.

### Breaking Changes

- Configs using structured YAML filters must migrate to the filter DSL.
  - Before: `filter: { tag: "highway" }`
  - After: `filter: "highway"`
  - Before: `filter: { tag: "natural", value: "tree" }`
  - After: `filter: "natural=tree"`
- Configs using `way: polygon` must choose a valid open-way behavior.
  - Use `way: linestring` for linear features.
  - Use `way: centroid` for point output.
  - Use `way: false` with `closed_way: polygon` for area-only extracts.
- Configs using `way: true` or `closed_way: true` must use explicit modes.
  - Use `way: linestring` instead of `way: true`.
  - Use `closed_way: polygon` instead of `closed_way: true`.
- The declared minimum Rust version is now 1.88.0.
