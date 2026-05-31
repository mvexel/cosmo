# ADR-001: Relation Support — Scope and Storage Strategy

**Status:** Accepted
**Date:** 2026-05-31
**Deciders:** TBD

## Context

Cosmo currently processes OSM nodes and ways but ignores relations entirely (`Element::Relation(_) => { // TODO }`). This means:

- Administrative boundaries (countries, states, cities) are not extracted
- Large buildings modeled as multipolygon relations are missing
- Lakes, forests, and other natural features split across multiple ways are lost
- Route relations (bus routes, hiking trails) are unavailable

The fixture PBF (Library Square, Salt Lake City) contains 26 relations and 431 ways, confirming that even small extracts contain relations.

## Decision

### 1. Multipolygon and boundary relations

We implement `type=multipolygon` and `type=boundary` relations. Both use identical geometry: outer/inner way members assembled into polygon rings. The `type=boundary` case was initially excluded under the assumption it needed different geometry handling — it does not.

**Not in scope:**
- `type=route` relations (would produce `MultiLineString` — different geometry pipeline)
- `type=restriction`, `type=public_transport`, and other non-geometric relation types

**Rationale:** Multipolygon and boundary are the two relation types that produce polygon geometry compatible with the existing sink pipeline. Route relations require `MultiLineString` support in all sinks, which is a separate concern.

### 2. 3-pass architecture for planet-scale support

To avoid storing all ~900M ways from a planet file in memory, we use a 3-pass approach when any table has `relation: true`:

| Pass | Purpose | What it reads | What it produces |
|------|---------|---------------|------------------|
| 0 | Scan relations for member way IDs | Relations only | `HashSet<i64>` of needed way IDs |
| 1 | Index nodes + collect filtered ways | Nodes + filtered ways | `NodeStore` + `WayStore` (only relation-referenced ways) |
| 2 | Process all elements | Everything | Output features |

When no table has `relation: true`, pass 0 is skipped and pass 1 does not collect ways — the pipeline remains 2-pass (or 1-pass for nodes-only).

**Memory analysis for planet-scale:**

| Component | Size | Notes |
|-----------|------|-------|
| Way ID set (pass 0 output) | ~160 MB | ~20M i64 entries in HashSet |
| Filtered way store | ~2 GB | ~20M ways × ~100 bytes each |
| Node store | Unchanged | Dense/sparse modes already handle planet scale |

**Trade-off vs alternatives:**
- In-memory HashMap of ALL ways: ~100 GB — infeasible
- Disk-backed way store: 2 passes but complex variable-length storage
- 3-pass with filtered collection: simple, ~2 GB RAM, one extra sequential read (~2-3 min on planet)

### 3. Ring assembly from multi-way segments

OSM multipolygon outer/inner rings are frequently split across multiple ways that share endpoints. We join way segments end-to-end to form complete rings before creating polygons.

Algorithm:
1. Group member ways by role (`outer` / `inner`)
2. For each group, resolve way refs to coordinate sequences via node store
3. Join segments: match last coord of one segment to first coord of the next (or reversed)
4. Validate ring closure (first == last coord)
5. Create `Polygon` with assembled outer ring(s) as exterior, inner ring(s) as holes
6. If multiple outer rings → `MultiPolygon`

## Consequences

- **Opt-in:** `relation` defaults to `false` in `GeometryConfig`, so this is opt-in. Existing configs are unaffected.
- **Incomplete extracts:** Relations referencing ways outside the extract will produce incomplete or missing geometries. Handled same as missing nodes in ways: warn and skip.
- **Planet-scale cost:** Extra sequential PBF read for pass 0 (~2-3 minutes). Acceptable given the alternative is OOM.
- **Large file warning:** When input exceeds 5 GB and relations are enabled, emit a warning about memory usage.
