# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project follows pre-1.0 semantic versioning.

## [0.2.0] - 2026-05-30

### Added

- Added `closed_way: false` support to exclude closed ways independently from open ways.
- Added regression coverage for `way: false` combined with `closed_way: polygon`, ensuring closed-way polygons are still emitted.
- Added validation coverage for rejected geometry forms:
  - `way: polygon` is rejected because open ways cannot form valid polygons.
  - `way: true` is rejected because geometry modes must be explicit.
- Added coverage for the documented `filter: {}` match-all shorthand.
- Added coverage that open-way `centroid` uses the line centroid instead of treating the open line as a polygon.

### Changed

- Simplified filter configuration loading to parse the single YAML file directly with `serde_yaml` instead of routing through the multi-source `config` crate.
- Standardized the public filter interface on the string DSL, while keeping `filter: {}` as the documented match-all shorthand.
- Kept the raw-config to compiled-config phase split so filters, mappings, and CEL expressions are parsed once before PBF processing.
- Replaced the `winnow`-based tokenizer with a small purpose-built Rust lexer for the existing filter DSL.
- Updated open-way geometry semantics so `way` only accepts valid open-way outputs: `linestring`, `centroid`, or `false`.
  The `centroid` mode now means a line centroid for open ways and an area centroid for closed ways.
- Updated documentation to distinguish open-way handling from closed-way handling and to remove open-way polygon examples.
- Updated the declared Rust minimum version to 1.88.0 to match the current dependency graph.

### Removed

- Removed the undocumented structured YAML filter forms:
  - `filter: { tag: ... }`
  - `filter: { any: ... }`
  - `filter: { all: ... }`
  - `filter: { not: ... }`
  - arbitrary simple tag maps such as `filter: { highway: primary }`
- Removed `way: polygon` support for open ways.
- Removed undocumented boolean `true` shorthand for `way` and `closed_way`.
- Removed direct dependencies on `config` and `winnow`.

### Fixed

- Fixed node-store detection and way processing to consider `closed_way` independently from `way`, so disabling open ways no longer suppresses closed-way output.
- Fixed open-way centroid calculation to use the `LineString` itself rather than an invalid polygon shell.
- Fixed docs that previously implied open ways could safely be emitted as polygons.

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
