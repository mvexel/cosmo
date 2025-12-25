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

Your favorite drink, now as a command line tool! This one won't give you a buzz, but it will convert your OSM PBF files in a bunch of other formats with optional filtering, which is arguably more useful.

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

The configuration file (default: `filters.yaml`) defines how OSM data is processed. It consists of one or more "tables".

### Filter Syntax

Filters determine which OSM features are included.

- **Simple key-value:** `key: value` (implicit AND).
- **Complex logic:** `any` (OR), `all` (AND), `not` (NOT).
- **Tag matching:** 
    - `tag`: The tag key to check.
    - `value`: Exact match for the value.
    - `values`: List of values. Supports globbing (e.g., `*_link` matches `motorway_link`).

### Geometry Configuration

Controls how geometries are constructed for each table:

- `way`: `linestring` (default), `polygon`, `centroid`, or `false` (disable).
- `closed_way`: `polygon` (default), `centroid`, `linestring`. Applied to ways that start and end at the same node.
- `node`: `true`/`false`.
- `relation`: `true`/`false`. **Note:** Relation geometries are currently not supported and will be ignored.

### Columns & Metadata

Map OSM tags or metadata to output columns.

- `source: "tag:<key>"`: Extracts the value of the OSM tag `<key>`.
- `source: "meta:<field>"`: Extracts OSM metadata. Supported fields:
    - `id`: OSM ID (integer).
    - `version`: Version number (integer).
    - `visible`: Visible flag (boolean).
    - `changeset`: Changeset ID (integer).
    - `timestamp`: Modification timestamp (string, ISO 8601).
    - `uid`: User ID (integer).
    - `user`: User name (string).

### Example Configuration

```yaml
tables:
  restaurants:
    filter:
      any:
        - tag: "amenity"
          value: "restaurant"
    geometry:
      way: "linestring"
      closed_way: "centroid"
      node: true
      relation: false
    columns:
      - name: "name"
        source: "tag:name"
        type: "string"
      - name: "cuisine"
        source: "tag:cuisine"
        type: "string"
      - name: "phone"
        source: "tag:phone"
        type: "string"
      - name: "website"
        source: "tag:website"
        type: "string"
      - name: "opening_hours"
        source: "tag:opening_hours"
        type: "string"
      - name: "osm_id"
        source: "meta:id"
        type: "integer"
```

Runtime flags for tweaking the node cache method / sparse file size:

```bash
cosmo --input <input.osm.pbf> \
  --output <output> \
  --format <geojson|geoparquet> \
  --filters <filters.yaml> \
  --node-cache-mode <mmap|memory> \
  --node-cache-max-nodes <count>
```

## Node Cache

The node cache is rebuilt on every run. By default, a temporary cache file is created and deleted after the run. You can override the path with `--node-cache`, but it is still rebuilt each time to avoid mixing node coordinates across extracts.

**Defaults:**
- Mode: `mmap` (memory-mapped file). Can be changed to `memory` (RAM only) via `--node-cache-mode`.
- Max Nodes: 16_000_000_000. Controlled by `--node-cache-max-nodes`. You should generally leave this alone. The size is determined by the highest node ID, not the number of nodes in your extract.

### Sparse File Support

When using `mmap` mode, the cache file is created as a **sparse file** with a virtual size of ~90 GiB (for 16B nodes). On most modern file systems (APFS, Ext4, NTFS, XFS), this file will only consume disk space for the nodes actually present in your input PBF. So for the entire planet, the file **will** grow to 90ish GB.
**Warning:** Some file systems (like FAT32) or network mounts (SMB/NFS) may not support sparse files and will attempt to allocate the full 90 GiB immediately. If you run out of disk space, use `--node-cache-mode memory` or a lower `--node-cache-max-nodes` if your extract has low node IDs.

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
