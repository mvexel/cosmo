# Cosmo vs DuckDB: POI Extraction Comparison

This document compares the effort required to extract 30 categorized POI tables from an OSM PBF file using Cosmo versus DuckDB Spatial.

## The Task

Extract named points of interest from OSM data into 30 separate tables/layers:
- Restaurants, cafes, bars, fast food, grocery stores, retail, lodging, transport, healthcare, education, etc.
- Include both node geometries and building centroids (for POIs mapped as buildings)
- Compute fallback English names (use `name:en` if available, otherwise `name`)
- Include metadata: OSM ID, version, timestamp
- Output all tags as JSON

## Approach 1: Cosmo (Declarative YAML)

**File:** `examples/pois_multiple_tables.yaml`

```yaml
tables:
  restaurants:
    filter: 'name & (amenity=restaurant|food_court|diner|bbq)'
    geometry:
      node: true
      way: centroid
      closed_way: centroid
    columns:
      - name: name
        source: tag:name
      - name: name_en
        source: 'expr:has(tags.name_en) ? tags.name_en : tags.name'
      - name: tags
        source: tags
        type: json
      - name: osm_id
        source: meta:id
        type: integer
      - name: version
        source: meta:version
        type: integer
      - name: timestamp
        source: meta:timestamp

  cafes_bakeries:
    filter: 'name & (amenity=cafe|coffee_shop|tea)'
    # ... same structure repeated for 28 more categories
```

**Command:**
```bash
cosmo --input extract.osm.pbf \
      --output pois.parquet \
      --format parquet \
      --filters examples/pois_multiple_tables.yaml
```

**What happens:**
- Single command execution
- Automatic two-pass processing (nodes cached, ways/relations resolved)
- Parallel block processing with Rayon
- Centroid calculation automatic for buildings
- CEL expressions evaluated during extraction
- 30 separate tables in output Parquet file

**Lines of configuration:** ~745 lines (but highly repetitive, could be templated)

**Complexity:** Low - declarative, version-controlled, reusable

---

## Approach 2: DuckDB with ST_ReadOSM (Fast but Manual)

`ST_ReadOSM()` gives raw nodes/ways/relations without constructed geometries. You must manually join to build geometries.

### Step 1: Load Raw OSM Data

```sql
-- Load nodes (has geometry automatically)
CREATE TABLE nodes AS
SELECT * FROM ST_ReadOSM('extract.osm.pbf', 'nodes');

-- Load ways (no geometry - just refs)
CREATE TABLE ways AS
SELECT * FROM ST_ReadOSM('extract.osm.pbf', 'ways');

-- Load way refs (node references for each way)
CREATE TABLE way_refs AS
SELECT * FROM ST_ReadOSM('extract.osm.pbf', 'way_refs');
```

### Step 2: Construct Way Centroids (Memory Intensive!)

```sql
-- Join way refs to nodes to get coordinates, then compute centroids
CREATE TABLE way_centroids AS
SELECT
    wr.way_id,
    ST_Centroid(ST_MakeLine(
        ARRAY_AGG(ST_Point(n.lon, n.lat) ORDER BY wr.ref_index)
    )) as geometry
FROM way_refs wr
JOIN nodes n ON wr.node_id = n.id
GROUP BY wr.way_id;

-- This will likely OOM on large extracts!
-- You'd need to batch by geographic region or way_id ranges
```

### Step 3: Create One CTE Per Category (30 times!)

```sql
WITH
-- Category 1: Restaurants
restaurants_nodes AS (
    SELECT
        'node' as osm_type,
        id as osm_id,
        tags->>'name' as name,
        COALESCE(tags->>'name:en', tags->>'name') as name_en,
        tags,
        version,
        timestamp,
        geometry
    FROM nodes
    WHERE tags->>'name' IS NOT NULL
        AND (tags->>'amenity' IN ('restaurant', 'food_court', 'diner', 'bbq'))
),
restaurants_ways AS (
    SELECT
        'way' as osm_type,
        w.id as osm_id,
        w.tags->>'name' as name,
        COALESCE(w.tags->>'name:en', w.tags->>'name') as name_en,
        w.tags,
        w.version,
        w.timestamp,
        wc.geometry
    FROM ways w
    JOIN way_centroids wc ON w.id = wc.way_id
    WHERE w.tags->>'name' IS NOT NULL
        AND (w.tags->>'amenity' IN ('restaurant', 'food_court', 'diner', 'bbq'))
),
restaurants AS (
    SELECT * FROM restaurants_nodes
    UNION ALL
    SELECT * FROM restaurants_ways
),

-- Category 2: Cafes (repeat entire pattern)
cafes_nodes AS (
    SELECT
        'node' as osm_type,
        id as osm_id,
        tags->>'name' as name,
        COALESCE(tags->>'name:en', tags->>'name') as name_en,
        tags,
        version,
        timestamp,
        geometry
    FROM nodes
    WHERE tags->>'name' IS NOT NULL
        AND (tags->>'amenity' IN ('cafe', 'coffee_shop', 'tea'))
),
cafes_ways AS (
    SELECT
        'way' as osm_type,
        w.id as osm_id,
        w.tags->>'name' as name,
        COALESCE(w.tags->>'name:en', w.tags->>'name') as name_en,
        w.tags,
        w.version,
        w.timestamp,
        wc.geometry
    FROM ways w
    JOIN way_centroids wc ON w.id = wc.way_id
    WHERE w.tags->>'name' IS NOT NULL
        AND (w.tags->>'amenity' IN ('cafe', 'coffee_shop', 'tea'))
),
cafes AS (
    SELECT * FROM cafes_nodes
    UNION ALL
    SELECT * FROM cafes_ways
),

-- ... repeat 28 more times for each category ...

-- Category 30: Industrial
industrial_nodes AS (
    SELECT
        'node' as osm_type,
        id as osm_id,
        tags->>'name' as name,
        COALESCE(tags->>'name:en', tags->>'name') as name_en,
        tags,
        version,
        timestamp,
        geometry
    FROM nodes
    WHERE tags->>'name' IS NOT NULL
        AND (tags->>'name' IS NOT NULL
            AND (tags->>'man_made' IN ('works', 'plant', 'power_wind', 'chimney', 'crane')
                OR tags->>'power' IN ('plant', 'substation', 'generator')
                OR tags->>'industrial' IS NOT NULL
                OR tags->>'landuse' = 'industrial'))
),
industrial_ways AS (
    SELECT
        'way' as osm_type,
        w.id as osm_id,
        w.tags->>'name' as name,
        COALESCE(w.tags->>'name:en', w.tags->>'name') as name_en,
        w.tags,
        w.version,
        w.timestamp,
        wc.geometry
    FROM ways w
    JOIN way_centroids wc ON w.id = wc.way_id
    WHERE w.tags->>'name' IS NOT NULL
        AND (tags->>'man_made' IN ('works', 'plant', 'power_wind', 'chimney', 'crane')
            OR tags->>'power' IN ('plant', 'substation', 'generator')
            OR tags->>'industrial' IS NOT NULL
            OR tags->>'landuse' = 'industrial')
),
industrial AS (
    SELECT * FROM industrial_nodes
    UNION ALL
    SELECT * FROM industrial_ways
)

-- Export each category
SELECT * FROM restaurants;
-- Repeat 29 more times, or use COPY TO for each table
```

### Step 4: Export Each Table

```bash
# You'd need a script to export each table
for category in restaurants cafes bars ...; do
    duckdb database.db -c "COPY ${category} TO '${category}.parquet' (FORMAT PARQUET)"
done
```

**Estimated lines of SQL:** ~1,500-2,000 lines (highly repetitive)

**Key problems:**
- Way centroid join is **memory intensive** - likely to OOM on city/country extracts
- Would need to batch by region or implement incremental processing
- Extremely repetitive SQL (could be templated with scripting)
- No built-in parallelism for multi-table extraction
- Tag filtering is verbose (`tags->>'key'` for every check)
- Relations not handled at all in this example

---

## Approach 3: DuckDB with ST_Read/GDAL (Automatic Geometry but Slow)

`ST_Read()` uses GDAL's OSM driver, which constructs geometries automatically but is "very slow for large files" and "not appropriate for interactive use" beyond small extracts.

### Step 1: Load OSM Data (Single-Threaded, Slow)

```sql
-- GDAL gives you 5 predefined layers
CREATE TABLE osm_points AS
SELECT * FROM ST_Read('extract.osm.pbf', layer='points');

CREATE TABLE osm_multipolygons AS
SELECT * FROM ST_Read('extract.osm.pbf', layer='multipolygons');

-- This will be VERY SLOW for files > a few MB
-- GDAL documentation warns against using this for large files
```

### Step 2: Extract Categories (Still Need 30 Queries)

```sql
-- At least geometry is already built, but filtering still verbose
CREATE TABLE restaurants AS
SELECT
    other_tags->>'name' as name,
    COALESCE(other_tags->>'name:en', other_tags->>'name') as name_en,
    other_tags as tags,
    osm_id,
    osm_version as version,
    osm_timestamp as timestamp,
    geometry
FROM osm_points
WHERE other_tags->>'name' IS NOT NULL
    AND other_tags->>'amenity' IN ('restaurant', 'food_court', 'diner', 'bbq')
UNION ALL
SELECT
    other_tags->>'name' as name,
    COALESCE(other_tags->>'name:en', other_tags->>'name') as name_en,
    other_tags as tags,
    osm_id,
    osm_version as version,
    osm_timestamp as timestamp,
    ST_Centroid(geometry) as geometry
FROM osm_multipolygons
WHERE other_tags->>'name' IS NOT NULL
    AND other_tags->>'amenity' IN ('restaurant', 'food_court', 'diner', 'bbq');

-- Repeat 29 more times...
```

**Estimated lines of SQL:** ~800-1,000 lines

**Key problems:**
- **Performance:** GDAL OSM driver is single-threaded and very slow for files >10MB
- GDAL docs explicitly say it's "not appropriate" for large files
- Still repetitive filtering for 30 categories
- No built-in way to process multiple categories in parallel
- Layer organization doesn't match desired output structure

---

## Comparison Table

| Aspect | Cosmo (YAML) | DuckDB ST_ReadOSM | DuckDB ST_Read (GDAL) |
|--------|--------------|-------------------|----------------------|
| **Configuration Lines** | ~745 (declarative YAML) | ~1,500-2,000 (SQL) | ~800-1,000 (SQL) |
| **Geometry Construction** | ✅ Automatic | ❌ Manual joins required | ✅ Automatic (GDAL) |
| **Memory Efficiency** | ✅ Optimized node cache | ❌ Way joins = OOM risk | ⚠️ GDAL internal caching |
| **Performance (large files)** | ✅ Fast (parallel Rust) | ⚠️ Fast IF you solve joins | ❌ Very slow (GDAL warning) |
| **Suitable File Size** | ✅ Small to planet | ⚠️ Up to region (memory limited) | ❌ Small extracts only (<10MB) |
| **Computed Columns (CEL)** | ✅ Native support | ❌ Translate to SQL CASE | ❌ Translate to SQL CASE |
| **Parallelism** | ✅ Built-in (Rayon) | ⚠️ DuckDB query parallelism | ❌ Single-threaded (GDAL) |
| **Repetition** | ⚠️ Repetitive but templateable | ❌ Very repetitive SQL | ❌ Very repetitive SQL |
| **Single Command** | ✅ Yes | ❌ Multi-step scripting | ❌ Multi-step scripting |
| **Version Control** | ✅ Clean YAML diffs | ⚠️ SQL in repo | ⚠️ SQL in repo |
| **CI/CD Friendly** | ✅ Excellent | ⚠️ Requires scripting | ⚠️ Requires scripting |
| **SQL Knowledge Required** | ✅ None | ❌ Advanced (joins, CTEs) | ⚠️ Intermediate |
| **Relations Support** | ✅ Yes | ❌ Not in example (complex) | ✅ Yes (GDAL) |

---

## Real-World Effort Estimate

### Cosmo
- **Write YAML config:** 2-3 hours (mostly copy-paste of table definitions)
- **Test and iterate:** 30 minutes
- **Total:** ~3 hours, results in clean, reusable config

### DuckDB ST_ReadOSM
- **Research how to join nodes to ways:** 1-2 hours
- **Write way centroid join query:** 1-2 hours
- **Debug OOM errors, implement batching:** 4-8 hours (varies by file size)
- **Write 30 category extraction queries:** 3-4 hours
- **Write export script:** 1 hour
- **Test and debug:** 2-3 hours
- **Total:** ~12-20 hours, results in complex SQL that may OOM on large files

### DuckDB ST_Read (GDAL)
- **Discover GDAL is too slow:** 30 minutes of painful waiting
- **Switch to ST_ReadOSM approach:** See above
- **Total:** Not viable for files >10MB

---

## Maintenance Considerations

### Adding a New POI Category

**Cosmo:**
```yaml
# Add 25 lines to YAML
new_category:
  filter: 'name & (amenity=new_type)'
  geometry:
    node: true
    way: centroid
  columns: [...]
```

**DuckDB:**
```sql
-- Add ~50-100 lines of SQL
-- Update export script
-- Re-test entire pipeline
```

### Changing Column Schema (e.g., add `name_fr`)

**Cosmo:**
```yaml
# Add 3 lines per table (or use YAML anchors/templates)
- name: name_fr
  source: 'expr:has(tags.name_fr) ? tags.name_fr : tags.name'
  type: string
```

**DuckDB:**
```sql
-- Modify 30 separate SELECT statements
-- Add COALESCE for 30 categories
```

---

## Conclusion

For this specific use case (30 categorized POI tables with computed columns and building centroids):

**Cosmo wins on:**
- Simplicity (no SQL joins required)
- Performance (optimized node caching, parallel processing)
- Memory efficiency (handles large files without OOM)
- Maintainability (declarative config, clean version control)
- Single-command execution

**DuckDB wins on:**
- Flexibility (arbitrary SQL transformations)
- Ecosystem integration (join with other datasets)
- No new tool required (if you already use DuckDB)

**The pragmatic answer:**
- **Use Cosmo** if your workflow is: "Extract these specific OSM features to Parquet/GeoJSON repeatedly"
- **Use DuckDB** if you need: "Complex spatial analysis combining OSM with census data and custom aggregations"

The YAML approach isn't just "easier" - it solves the fundamental geometry construction and memory management problems that make the DuckDB approach painful at scale.
