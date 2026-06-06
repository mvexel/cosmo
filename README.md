```
     ______   ______   ______   ___ __ __   ______      
    /_____/\ /_____/\ /_____/\ /__//_//_/\ /_____/\     
    \:::__\/ \:::_ \ \\::::_\/_\::\| \| \ \\:::_ \ \    
     \:\ \  __\:\ \ \ \\:\/___/\\:.      \ \\:\ \ \ \   
      \:\ \/_/\\:\ \ \ \\_::._\:\\:.\-/\  \ \\:\ \ \ \  
       \:\_\ \ \\:\_\ \ \ /____\:\\. \  \  \ \\:\_\ \ \ 
        \_____\/ \_____\/ \_____\/ \__\/ \__\/ \_____\/ 
```

Your favorite drink, now as a command line tool! This one won't give you a buzz, but it will convert your OSM PBF files into a bunch of other formats with really good filtering, which is arguably more useful.

Why not use...

- **osmium**: Equally fast. Filtering is very basic. Only OSM output formats.
- **osm2pgsql**: Arguably better filtering with Lua scripting, but output limited to PostgreSQL
- **gdal**: swiss army knife of geospatial data manipulation, but not great at working with the OSM file format at scale.

There are many tools that fill the niche "I have OSM data but I want it in another format and transformed". Compare them and use what best fits your needs.

## Quick Start

### Installation

```bash
cargo install --path .
```

### Basic Usage

```bash
cosmo convert \
  --input city.osm.pbf \
  --filters config.yaml \
  --output output.geojson
```

### Example Filter Configuration

Extract named restaurants and cafes as points:

```yaml
table:
  name: pois
  filter: 'name & (amenity=restaurant|cafe)'
  geometry:
    node: true
    way: centroid      # Convert open ways to points
    closed_way: centroid
  columns:
    - name: name
      source: tag:name
    - name: cuisine
      source: tag:cuisine
    - name: osm_id
      source: meta:id
      type: integer
```

**See [examples/](examples/) for more filter configurations and the [Filter YAML Guide](docs/filter_yaml_guide.md) for a step by step guide and reference documentation.**

## Command Line Interface

### Convert Command

The primary command for converting OSM PBF files:

```bash
cosmo convert [OPTIONS]
```

#### Required Options

| Option | Short | Description |
|--------|-------|-------------|
| `--input <PATH>` | `-i` | Input OSM PBF file |
| `--filters <PATH>` | `-f` | Filter configuration (YAML) |
| `--output <PATH>` | `-o` | Output file (`.geojson`, `.geojsonl`, `.parquet`) or `-` for stdout (GeoJSONL only) |

#### Common Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--format <FORMAT>` | | Override format detection: `geojson`, `geojsonl`, `parquet` | Auto-detect from extension |
| `--bbox <BBOX>` | | Filter by bounding box: `min_lon,min_lat,max_lon,max_lat` | None |
| `--all-tags` | | Include all OSM tags as JSON in addition to configured columns | false |
| `--verbose` | `-v` | Enable detailed logging | false |
| `--jobs <N>` | `-j` | Parallel processing threads | All CPUs |

#### Node Cache Options

OSM ways reference nodes by ID, so Cosmo needs a way to look up node coordinates when building way geometries. The node cache is built during the first pass through the PBF file and used during the second pass.

| Option | Description | Default |
|--------|-------------|---------|
| `--node-cache-mode <MODE>` | Cache strategy: `auto`, `sparse`, `dense`, `memory` | `auto` |
| `--node-cache <PATH>` | Custom cache file path (dense mode only) | Temp file in `$TMPDIR` if set, otherwise `/var/tmp` |
| `--node-cache-max-nodes <N>` | Maximum node ID to allocate address space for (dense mode) | 16,000,000,000 |

**Cache Modes:**
- **auto** - Automatically selects `sparse` for files <5GB or `dense` for files ≥5GB
- **sparse** - Sorted array with binary search (O(log n)). Low memory, best for regional extracts with sparse node ID coverage. **Requires sorted input** - PBF files from OSM/GeoFabrik are sorted; use `osmium sort` if unsure
- **dense** - Direct array indexing by node ID (O(1)). Memory-mapped file using OS sparse file support. Best for large files with dense node coverage (planet/continent). Creates a sparse file with ~128GB virtual size and checks the target filesystem has enough free space before mapping it; use `--node-cache` to place it on a large disk
- **memory** - In-memory HashMap (O(1)). No disk I/O but high RAM usage (~24+ bytes per node). For planet processing you'd need 384GB+ RAM

**Why these options?** OSM node IDs are globally assigned, so even a small city extract will have IDs scattered across the entire range (10+ billion). For small extracts, sparse mode with binary search is actually faster than dense. For large extracts, dense mode's O(1) lookup wins and uses less disk space. Memory mode is fastest but only practical for smaller extracts or machines with lots of RAM

#### Example: Extract POIs from a Region

```bash
cosmo convert \
  --input utah-latest.osm.pbf \
  --filters examples/pois_simple.yaml \
  --output pois.geojsonl \
  --bbox -112.1,40.5,-111.8,40.8 \
  --node-cache-mode sparse \
  --verbose
```

### Validate Command

Validate a filter configuration without processing data:

```bash
cosmo validate <config.yaml> [--verbose]
```

### Environment Variables

All options support environment variables with the `COSMO_` prefix:

```bash
export COSMO_INPUT=city.osm.pbf
export COSMO_FILTERS=config.yaml
export COSMO_OUTPUT=output.geojsonl
cosmo convert
```

## Filter Configuration

Filters are defined in YAML and consist of:

1. **Filter DSL** - Which features to extract
2. **Geometry configuration** - How to handle nodes/ways/relations
3. **Columns** - Which tags/metadata to include

### Filter DSL Quick Reference

| Syntax | Description | Example |
|--------|-------------|---------|
| `key` | Tag exists | `highway` |
| `key=value` | Exact match | `highway=primary` |
| `key=v1\|v2` | Match any value | `amenity=cafe\|restaurant` |
| `expr1 & expr2` | Logical AND | `highway & name` |
| `expr1 \| expr2` | Logical OR | `amenity \| shop` |
| `key=*pattern*` | Glob matching | `highway=*_link` |
| `key>=value` | Numeric comparison | `lanes>=2` |
| `meta:version >= 2`, `meta:timestamp >= "2024-01-01"` | OSM metadata comparison | Matches by version, timestamp, user, uid, changeset, visibility, id, or element type |

**See the [Filter YAML Guide](docs/filter_yaml_guide.md) for complete syntax and examples**

### Column Sources

Extract data from OSM features:

| Source | Description | Example |
|--------|-------------|---------|
| `tag:key` | OSM tag value | `source: tag:name` |
| `meta:field` | Metadata field (id, type, version, timestamp, uid, user, changeset, visible) | `source: meta:id` |
| `tags` | All tags as JSON | `source: tags` (requires `type: json`) |
| `meta` | All metadata as JSON | `source: meta` (requires `type: json`) |
| `refs` | Way node IDs as JSON array | `source: refs` (requires `type: json`) |
| `mapping:name` | Apply categorization mapping | `source: mapping:poi_class` |
| `const:value` | Literal value typed by the column's `type` | `source: const:buildings-v1` |
| `expr:CEL` | Compute with CEL expression | `source: 'expr:has(tags.name_en) ? tags.name_en : tags.name'` |

**Column Types:** `string` (default), `integer`, `float`, `json`

**See the [Filter YAML Guide](docs/filter_yaml_guide.md#column-sources) for details and advanced features**

## Output Formats

### GeoJSON
Complete FeatureCollection in a single file. Best for small extracts and web mapping.

### GeoJSONL (Newline-Delimited GeoJSON)
One Feature per line. Supports streaming to stdout (`--output -`) for piping to other tools:
```bash
cosmo convert -i data.osm.pbf -f config.yaml -o - | tippecanoe
```

### GeoParquet
Columnar format with spatial metadata. Efficient for large datasets and analytics.

**Note:** Output is not spatially sorted. For best performance, sort with DuckDB's Hilbert function. See [CloudNativeGeo best practices](https://cloudnativegeo.org/blog/2025/01/using-duckdbs-hilbert-function-with-geoparquet/).

### MapRoulette

MapRoulette is a platform for creating and managing OSM mapping tasks. Cosmo can output [MapRoulette-compatible](https://learn.maproulette.org/en-US/documentation/line-by-line-geojson/) GeoJSONL files that can be uploaded to MapRoulette as tasks when you create a new challenge.

## Documentation

- **[Filter YAML Guide](docs/filter_yaml_guide.md)** - Complete reference for filter syntax, mappings, CEL expressions, and examples
- **[Sink Development Guide](docs/sinks.md)** - How to implement new output formats
- **[Examples Directory](examples/)** - Sample filter configurations for common use cases

## License

ISC
