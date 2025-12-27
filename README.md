```
#     ______   ______   ______   ___ __ __   ______      
#    /_____/\ /_____/\ /_____/\ /__//_//_/\ /_____/\     
#    \:::__\/ \:::_ \ \\::::_\/_\::\| \| \ \\:::_ \ \    
#     \:\ \  __\:\ \ \ \\:\/___/\\:.      \ \\:\ \ \ \   
#      \:\ \/_/\\:\ \ \ \\_::._\:\\:.\-/\  \ \\:\ \ \ \  
#       \:\_\ \ \\:\_\ \ \ /____\:\\. \  \  \ \\:\_\ \ \ 
#        \_____\/ \_____\/ \_____\/ \__\/ \__\/ \_____\/ 
#                                                        
```

Your favorite drink, now as a command line tool! This one won't give you a buzz, but it will convert your OSM PBF files into a bunch of other formats with optional filtering, which is arguably more useful.

## Usage

Basic usage: 

```bash
cosmo --input <input.osm.pbf> \
  --output <output> \
  --format <geojson|geojsonl|parquet> \
  --filters <filters.yaml> \
  [--all-tags] \
  [--verbose]
```

### Options

- `--format`: Output format. `parquet` (or `geoparquet`) creates a GeoParquet file. `geojson` creates a standard FeatureCollection. `geojsonl` creates newline-delimited GeoJSON.
- `--all-tags`: Include all original OSM tags in the output 'tags' property (JSON object), in addition to any explicit columns.
- `--verbose`: Enable detailed logging.


## Configuration Reference

The configuration file (default: `filters.yaml`) defines how OSM data is processed. It consists of a single "table" definition.

### Filter DSL

Filters determine which OSM features are included. Cosmo uses a powerful filter DSL:

| Syntax | Description | Example |
| :--- | :--- | :--- |
| `key` | Matches if the tag exists | `highway` |
| `key=value` | Exact match | `highway=primary` |
| `key=v1\|v2` | Matches any of the pipe-separated values | `amenity=cafe\|restaurant` |
| `expr1 & expr2` | Logical AND | `highway=primary & name` |
| `expr1 \| expr2` | Logical OR | `amenity \| shop` |
| `(expr)` | Grouping | `(amenity \| shop) & name` |

For a full list of operators and syntax, see the [Filter YAML Guide](docs/filter_yaml_guide.md).

### Geometry Configuration

Controls how geometries are constructed for the table:

- `way`: `linestring` (default), `polygon`, `centroid`, or `false` (disable).
- `closed_way`: `polygon` (default), `centroid`, `linestring`. Applied to ways that start and end at the same node.
- `node`: `true`/`false`.
- `relation`: `true`/`false`. **Note:** Relation geometries are currently not supported and will be ignored.

### Columns & Metadata

Map OSM tags or metadata to output columns.

- `type`: `string`, `integer`, `float`, or `json`.
- `source: "tag:<key>"`: (or just `"<key>"`) Extracts the value of the OSM tag `<key>`.
- `source: "tags"`: Extracts all OSM tags as a JSON object.
- `source: "meta"`: Extracts all metadata fields as a JSON object.
- `source: "refs"`: Extracts way node references as a JSON array (ways only).
- `source: "mapping:<name>"`: Categorizes features using a named mapping.
- `source: "expr:<cel>"`: Computes a value using a [CEL expression](docs/filter_yaml_guide.md#cel-expressions).
- `source: "meta:<field>"`: Extracts OSM metadata. Supported fields:
    - `id`: OSM ID (integer).
    - `version`: Version number (integer).
    - `timestamp`: Modification timestamp (string, ISO 8601).
    - `uid`: User ID (integer).
    - `user`: User name (string).

For more details on sources and advanced logic, see the [Filter YAML Guide](docs/filter_yaml_guide.md).

### Example Configuration

```yaml
mappings:
  poi_class:
    rules:
      - match: 'amenity=restaurant|food_court'
        value: restaurant
      - match: 'amenity=cafe|coffee_shop'
        value: cafe
    default: misc

table:
  name: pois
  filter: 'name & (amenity=restaurant|food_court|cafe|coffee_shop)'
  geometry:
    node: true
    way: centroid
  columns:
    - name: name
      source: tag:name
      type: string
    - name: class
      source: mapping:poi_class
      type: string
    - name: osm_id
      source: meta:id
      type: integer
    - name: name_en
      source: "expr:has(tags.name_en) ? tags.name_en : tags.name"
      type: string
```

### Pass-through Configuration

To export all features with their tags and metadata as JSON blobs (useful for raw data conversion):

```yaml
table:
  name: all_features
  filter: "" # Or {} - matches everything
  columns:
    - name: tags
      source: tags
      type: json
    - name: meta
      source: meta
      type: json
    - name: refs
      source: refs
      type: json
```

Runtime flags for tweaking the node cache method:

```bash
cosmo --input <input.osm.pbf> \
  --output <output> \
  --format <geojson|geoparquet> \
  --filters <filters.yaml> \
  --node-cache-mode <auto|sparse|dense|memory> \
  --node-cache-max-nodes <count>
```

## Node Cache

The node cache stores node coordinates for resolving way geometries. It is rebuilt on every run. By default, a temporary cache file is created and deleted after the run. You can override the path with `--node-cache`, but it is still rebuilt each time to avoid mixing node coordinates across extracts.

### Cache Modes

- **auto** (default): Automatically selects `sparse` or `dense` based on input file size.
- **sparse**: Sorted array (disk-backed) with binary search. Low RAM for extracts. Uses sequential indexing to preserve sort order; requires sorted input (use `osmium sort` if needed).
- **dense**: Memory-mapped file indexed by node ID. Best for planet/continent. Uses parallel indexing for maximum speed.
- **memory**: In-memory HashMap. No disk usage, but high RAM consumption. If you have a lot of RAM, you may be able to process the planet like this? (would be cool. I only have 16GB. Let me know.)

### Auto-Selection

By default (`--node-cache-mode auto`), the mode is selected based on input file size. For PBF files smaller than 5GB, `sparse` is selected. For larger files, `dense` is selected. You can override this with `--node-cache-mode`.

The output tells you what was selected:
```
Node cache: sparse (auto-selected for 1.2 GB input)
```

### Why This Matters

OSM node IDs are globally assigned (~13 billion max). Even a small city extract references IDs scattered across this range:

| Mode | Storage | Lookup |
|------|---------|--------|
| **dense** | 8 bytes × max_node_id (~98 GB for planet) | O(1) direct indexing |
| **sparse** | 16 bytes × actual_nodes (file-backed) | O(log n) binary search |

For a US extract (1.49B nodes, 11% density), sparse uses ~22 GB vs dense's ~98 GB sparse file.

### Dense Mode and Sparse Files

When using `dense` mode, the cache file is created as a **sparse file** with a virtual size of ~128 GiB (for 16B max nodes). On most modern file systems (APFS, Ext4, NTFS, XFS), this file only consumes disk space for nodes actually present. For planet files, it will grow to ~90 GB.

**Warning:** Some file systems (FAT32) or network mounts (SMB/NFS) may not support sparse files and will attempt to allocate the full size immediately. Use `--node-cache-mode sparse` or `memory` in these cases.

## Environment Variables

All CLI flags can be provided via environment variables using the `COSMO_` prefix:

- `COSMO_INPUT`
- `COSMO_OUTPUT`
- `COSMO_FORMAT`
- `COSMO_FILTERS`
- `COSMO_NODE_CACHE`
- `COSMO_NODE_CACHE_MODE`
- `COSMO_NODE_CACHE_MAX_NODES`
- `COSMO_ALL_TAGS`
- `COSMO_VERBOSE`

## Notes on built-in Sinks

GeoParquet output includes a `geometry` column (WKB) plus explicit columns from the filters YAML. A `properties` JSON column is also included for any extra fields (tags/metadata) not mapped to explicit columns.

GeoJSONL output writes one `Feature` per line and supports streaming to stdout with `--output -` so you can do `cosmo --output - | tippecanoe` for example. Parquet and GeoJSON outputs do not support stdout. Input must be a file path (stdin is not supported).

## Developing Sinks

See `docs/sinks.md` for the sink interface and implementation notes.

## FAQ

### Why not use osmosis/osmium/imposm/osm2pgsql?

These tools are all great, but serve different purposes. osm2pgsql offers really good filtering with its Flex mode and Lua scripting, but it can only write to PostgreSQL. Imposm is similar. Osmosis and Osmium are focused on converting between OSM formats and have limited filtering and output options. What I was missing is a tool that can read an OSM PBF file, apply complex filters including tag manipulation, and write to non-OSM formats. Cosmo aims to fill that gap.

### Why only GeoJSON, GeoJSONL, and GeoParquet outputs?

Just pragmatic reasons. I need these formats for my work and wanted to build a tool that does them well. If you need other formats, please open an issue or contribute a sink!

### Why not use the `gdal` crate?

I could have, but GDAL is a pretty big dependency with a complex build process. I wanted to keep Cosmo lightweight and easy to build/install. Implementing sinks directly allows for more control and fewer dependencies.

## License

Cosmo is licensed under the Apache License 2.0. See the LICENSE file for details.
