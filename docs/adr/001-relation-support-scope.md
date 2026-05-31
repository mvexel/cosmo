# ADR-001: Relation Support — Scope and Storage Strategy

**Status:** Proposed
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

### 1. Multipolygon relations only (initial scope)

We will implement `type=multipolygon` relations only. This covers the highest-value use case: polygon features whose geometry is assembled from member ways with `role=outer` and `role=inner`.

**Not in scope:**
- `type=route` relations (would produce `MultiLineString` — different geometry pipeline)
- `type=boundary` relations without multipolygon geometry
- `type=restriction`, `type=public_transport`, and other non-geometric relation types

**Rationale:** Multipolygon is the most common relation type in OSM and the only one that produces polygon geometry compatible with the existing sink pipeline. Adding other types requires `MultiLineString` support in all sinks, which is a separate concern.

### 2. In-memory way store (extract-scale)

During pass 1 (node indexing), we will also collect way member references into an in-memory `HashMap<i64, Vec<i64>>` mapping `way_id → [node_ref_ids]`. This store is consulted during pass 2 when assembling relation geometries.

**Trade-offs:**

| Approach | Memory (planet) | Memory (city extract) | Complexity |
|----------|----------------|----------------------|------------|
| In-memory HashMap | ~70 GB (infeasible) | ~10 MB | Low |
| Disk-backed (like node store) | ~10 GB on disk | ~1 MB on disk | High — variable-length entries |
| Third PBF pass | 0 extra | 0 extra | Medium — slower |

**Rationale:** Cosmo's primary use case is regional extracts, not planet-scale processing. An in-memory way store is simple, fast, and adequate for extracts up to country-scale (~1-2 GB RAM for a country like Germany). Planet-scale support can be added later with a disk-backed way store following the existing `NodeStore` pattern.

### 3. Ring assembly from multi-way segments

OSM multipolygon outer/inner rings are frequently split across multiple ways that share endpoints. We must join way segments end-to-end to form complete rings before creating polygons.

Algorithm:
1. Group member ways by role (`outer` / `inner`)
2. For each group, resolve way refs to coordinate sequences via node store
3. Join segments: match last coord of one segment to first coord of the next (or reversed)
4. Validate ring closure (first == last coord)
5. Create `Polygon` with assembled outer ring(s) as exterior, inner ring(s) as holes
6. If multiple outer rings → `MultiPolygon`

## Consequences

- **Extract-only limitation:** Running Cosmo with `relation: true` on a planet PBF will OOM. We should document this and emit a warning when the input exceeds a size threshold (e.g., >5 GB) and relations are enabled. A future ADR can address disk-backed way storage.
- **Incomplete extracts:** Relations referencing ways outside the extract will produce incomplete or missing geometries. We handle this the same way as missing nodes in ways: warn and skip.
- **Config default:** `relation` defaults to `false` in `GeometryConfig` (already the case), so this is opt-in. Existing configs are unaffected.
