# Your Guide to `cosmo`

`cosmo` is a conversion tool for OpenStreetMap data. In this guide, you will learn how to use it by example.

Happy learning and please send me your feedback and questions,
Martijn

## Why `cosmo`?

Fair question. There are a lot of tools out there that can read OSM data and write it into some other format. None of them did what I want in an efficient way, so I decided to take the opportunity to fill that gap and learn something new in the process.

Before you proceed, you should appreciate what cosmo can and cannot do, so you can choose the right tool for the job. I designed cosmo to do one thing ande do it well: **converting and curating OSM data to GIS-friendly format in a reproducible, analysis data pipeline friendly way**.

For anything else, you're likely better off with osm2pgsql, tilemaker, QGIS, osmium or one of the many other tools that can import and filter OSM data.

In the rest of this guide, I will walk you through a typical cosmo usage scenario. You will find a complete reference guide below the guide.

## Using Cosmo, step by step.

### Download and build

Cosmo is currently not on crates.io, so you will need to build it yourself or get a release from the codeberg page.

If you want to build yourself, you will need the rust compiler. This is beyond the scope of this guide, but if you have the latest stable rust, it will compile.

```bash
cargo build --release
```

then copy the binary to a directory in your path, for example `~/.local/bin`:

```bash
cp target/release/cosmo ~/.local/bin
```

Then try `cosmo --help` to confirm it runs.

## Let's build a toilet dataset!

cosmo is not a straight up conversion tool. You will have to define a **filter declaration** to use it. A filter declaration is a yaml file that tells cosmo what to do with the data. Let's work through this.

### Toilets v1

Let's say we want to extract all toilets from an OSM file. In OSM, toilets [can be](https://wiki.openstreetmap.org/wiki/Tag:amenity%3Dtoilets#How_to_map) either a node with `amenity=toilet` or a closed way with `building=toilet` or `amenity=toilet` tags. Toilets can have a variety of useful attributes, like `toilets:paper_supplied=yes/no`, `toilets:handwashing=yes/no` and `toilets:menstrual_products=yes/limited/no`. You can find a full list of (100+) attributes that mappers have used in combination with `amenity=toilets` on [TagInfo](https://taginfo.openstreetmap.org/tags/amenity=toilets#combinations).

This is a bit overwhelming, so let's start simple and extract just the features `amenity=toilets`:

```yaml
table:
  name: toilets
  filter: "amenity=toilets"
  columns: []
```

This is pretty much the minimally viable declaration. It contains one table with one filter, and one column.

- **table**: A `table` is the top level element, and defines one data table. You can have more than one `table` in a declaration. Each `table` becomes a separate output file. A `table` has a name. This becomes the file name.
- **filter**: A `table` has exactly one `filter`. The `filter` defines which OSM features will be in the output. A filter can be as simple as `amenity=toilets`, which would match any OSM feature that has exactly this tag key and value.
- **columns**: This is the declaration of the properties, or fields, of the output object. It is always a list, or [sequence](https://yaml.org/spec/1.2.2/#sequence) in YAML lingo. In our simple example, it is an empty sequence! That means that we will not get any properties in the output, just the geometry.

Let's make sure this is a valid filter declaration:

```bash
cosmo validate examples/guide/toilets_v1.yaml
```

This will output `cosmo validate examples/guide/toilets_v1.yaml
✓ Configuration is valid`: the filter declaration is good to go! (If you want more detail, you can add `--verbose`)

now we run this on a Salt Lake City OSM PBF file I just extracted using [OSM Slice](https://slice.openstreetmap.us/slice/?uuid=693aafc7-a660-4de9-8c88-3652b2937f82#10.39/40.6535/-111.9335):

```bash
cosmo convert -i saltlakecity.osm.pbf -f examples/guide/toilets_v1.yaml -o toilets_v1.geojson
```

This will take just a fraction of a second to run on your computer. And look! We have toilets!

<img src="https://images.osm.lol/SCR-20260205-miz.png" aria-label="map with toilet features" width=800>
[GeoJSON.io](https://geojson.io/#data=data:text/x-url,https%3A%2F%2Fimages.osm.lol%2Ftoilets_v1.geojson)

### Toilets v2: buildings to nodes

Scroll and clicking around on this map, you will notice that the features indeed have no properties. You will also notice that there both points (nodes) and polygons (closed ways). This is because the simple filter will match **any**[1] OSM feature with the `amenity=toilets` tag.

If you are like me, you probably prefer to have cosmo just output nodes, even if the tag was used on a building. cosmo has you covered! You can add a `geometry` section in the table declaration to tell cosmo exactly what to do with individual geometry types. If you omit the `geometry` section, it will include nodes as points, ways as linestrings, closed ways as polygons, and omit relations.

If you want to include closed ways as points, you can declare the geometry section like this:

```
table:
  name: toilets
  filter: amenity=toilets
  geometry:
    closed_way: centroid
  columns: []
```

Now we have an output file with just points.

<img src="https://images.osm.lol/SCR-20260206-g4k.png" aria-label="map of toilets as closed ways and nodes" witdh=800 />
[GeoJSON.io](https://geojson.io/#data=data:text/x-url,https%3A%2F%2Fimages.osm.lol%2Ftoilets_v2.geojson)

### Toilets v3: Introducing Properties

We still have no properties on our output features. OSM potentially has many, as we have seen, so let's try adding a few as columns:

```
table:
  name: toilets
  filter: amenity=toilets
  geometry:
    closed_way: centroid
  columns:
    - name: name
      source: tag:name
    - name: paper_supplied
      source: tag:toilets:paper_supplied
    - name: handwashing
      source: tag:toilets:handwashing
    - name: wheelchair_accessible
      source: tag:wheelchair
    - name: fee
      source: tag:fee
    - name: opening_hours
      source: tag:opening_hours
```

What we did here is add a handful of `column` declarations. Each `column` has a `name` and a `source`. In this example, the `source` specifies which OSM tag will populate the column (using the `tag:` prefix). But sources can also be metadata fields (`meta:`), expressions (`expr:`), mappings (`mapping:`), or aggregate data like `tags`, `meta`, or `refs`. See the [Column Sources](#column-sources) section below for all the options.

[GeoJSON.io](https://geojson.io/#data=data:text/x-url,https%3A%2F%2Fimages.osm.lol%2Ftoilets_v3.geojson)

Let's do one more example where we introduce...mappings!

### Toilets v4: Categorizing with Mappings

Looking at our toilet data, we might want to categorize toilets based on their accessibility and cost. Instead of writing complex conditional logic, we can use **mappings** to classify features into categories.

Mappings use a "first match wins" approach: they check each rule in order and return the value for the first rule that matches.

```yaml
mappings:
  toilet_category:
    rules:
      - match: 'toilets:wheelchair=yes & fee=no'
        value: accessible_free
      - match: 'toilets:wheelchair=yes'
        value: accessible
      - match: 'fee=no'
        value: free
      - match: 'fee=yes'
        value: paid
    default: unknown

table:
  name: toilets
  filter: amenity=toilets
  geometry:
    closed_way: centroid
  columns:
    - name: name
      source: tag:name
    - name: category
      source: mapping:toilet_category
    - name: wheelchair
      source: tag:toilets:wheelchair
    - name: fee
      source: tag:fee
    - name: paper_supplied
      source: tag:toilets:paper_supplied
```

Now when you run this:

```bash
cosmo convert -i saltlakecity.osm.pbf -f examples/guide/toilets_v4.yaml -o toilets_v4.geojson
```

Each toilet feature will have a `category` property with values like `accessible_free`, `accessible`, `free`, `paid`, or `unknown`. 

[GeoJSON.io](https://geojson.io/#data=data:text/x-url,https%3A%2F%2Fimages.osm.lol%2Ftoilets_v4.geojson)


For more details on mappings including advanced patterns, see the [Mappings](#mappings) section below.

---

# Filter YAML Guide

This guide provides a comprehensive reference for writing filter configurations in Cosmo. Filters determine which OSM features are included in the output and how they are transformed.

## Table of Contents

- [Filter DSL Syntax](#filter-dsl-syntax)
- [Column Sources](#column-sources)
- [Mappings](#mappings)
- [CEL Expressions](#cel-expressions)
- [Geometry Configuration](#geometry-configuration)
- [Complete Examples](#complete-examples)
- [Best Practices](#best-practices)
- [Additional Resources](#additional-resources)
- [Troubleshooting](#troubleshooting)
- [Known Limitations](#known-limitations)

## Filter DSL Syntax

The filter DSL is a string-based expression language for matching OSM tags and metadata, configured via YAML.

### Basic Tag Operations

| Syntax           | Description                         | Example                                           |
| ---------------- | ----------------------------------- | ------------------------------------------------- |
| `key`            | Tag exists (any value)              | `name` matches any feature with a `name` tag      |
| `!key`           | Tag does not exist                  | `!name` matches features without a `name` tag     |
| `key=value`      | Exact value match                   | `highway=primary` matches only `highway=primary`  |
| `key=v1\|v2\|v3` | Match any of multiple values        | `amenity=cafe\|restaurant\|bar`                   |
| `key=*`          | Tag exists with any non-empty value | `shop=*` matches any shop                         |

**Note:** To exclude a specific tag value, use negation with parentheses: `!(highway=residential)` instead of `highway!=residential`. The `!=` operator is reserved for numeric comparisons only (see below).

### Glob Patterns

Glob patterns allow wildcard matching within values:

| Pattern         | Description      | Example                                                      |
| --------------- | ---------------- | ------------------------------------------------------------ |
| `key=*_link`    | Suffix wildcard  | `highway=*_link` matches `motorway_link`, `trunk_link`, etc. |
| `key=primary_*` | Prefix wildcard  | `name=primary_*` matches `primary_school`, `primary_care`    |
| `key=*middle*`  | Contains pattern | `name=*hospital*` matches any name containing "hospital"     |

Note: The standalone `*` (as in `key=*`) means "any value", while wildcards within strings (like `*_link`) are glob patterns.

### Numeric Comparisons

Compare tag values numerically:

| Operator     | Description           | Example            |
| ------------ | --------------------- | ------------------ |
| `key<value`  | Less than             | `lanes<3`          |
| `key<=value` | Less than or equal    | `maxspeed<=50`     |
| `key>value`  | Greater than          | `population>10000` |
| `key>=value` | Greater than or equal | `height>=100`      |

**Note:** Numeric comparisons automatically extract leading numeric values from strings. For example, `maxspeed>=50` will match tags like `maxspeed=50`, `maxspeed=60`, and even `maxspeed=50 mph` or `maxspeed=60 km/h` (the numeric part is extracted automatically). Non-numeric values or tags without leading numbers are treated as non-matching.

### Logical Operators

Combine expressions using boolean logic:

| Operator         | Description                   | Example                    |
| ---------------- | ----------------------------- | -------------------------- |
| `expr1 & expr2`  | Logical AND (both must match) | `highway=primary & name`   |
| `expr1 \| expr2` | Logical OR (either can match) | `amenity \| shop`          |
| `!expr`          | Logical NOT (negation)        | `!(highway=residential)`   |
| `(expr)`         | Grouping                      | `(amenity \| shop) & name` |

### Operator Precedence

Operators are evaluated in this order (highest to lowest):

1. `!` (NOT / negation)
2. `&` (AND)
3. `|` (OR)

Use parentheses `()` to override precedence and make complex expressions clear.

### Complex Filter Examples

```yaml
# Named restaurants or cafes
filter: 'name & (amenity=restaurant|cafe)'

# Major roads with 2+ lanes
filter: 'highway=motorway|trunk|primary & lanes>=2'

# Any shop except convenience stores
filter: 'shop=* & !shop=convenience'

# Highway links (motorway_link, trunk_link, etc.)
filter: 'highway=*_link'

# Buildings that are not residential
filter: 'building & !building=residential'

# Complex nested logic
filter: '(highway=primary | highway=secondary) & (lanes>=2 | name)'

# Features with international name but no English name
filter: 'name:en & !name:en'
```

### Empty Filters

An empty filter matches all features:

```yaml
filter: ""
# or
filter: {}
```

**Note:** Empty filters match ALL features including untagged nodes (way vertices). This creates very large output files where 70-80% of features typically have empty tags. For most use cases, filter by specific tags instead.

## Column Sources

Columns define what data is extracted from OSM features and how it's formatted in the output. The `source` field specifies where the data comes from.

### Tag Sources

Extract values from OSM tags:

```yaml
columns:
  # Explicit tag: prefix (type defaults to string)
  - name: name
    source: tag:name

  # Implicit (no prefix defaults to tag, type defaults to string)
  - name: highway_type
    source: highway

  # Tags that might be numeric (must specify type)
  - name: lanes
    source: tag:lanes
    type: integer

  - name: maxspeed
    source: tag:maxspeed
    type: float
```

### Metadata Sources

Extract OSM metadata fields:

```yaml
columns:
  # OSM element ID
  - name: osm_id
    source: meta:id
    type: integer

  # Element visibility flag
  - name: visible
    source: meta:visible

  # Version number
  - name: version
    source: meta:version
    type: integer

  # Changeset ID
  - name: changeset
    source: meta:changeset
    type: integer

  # Last edit timestamp (ISO 8601 format)
  - name: timestamp
    source: meta:timestamp

  # User ID who made the last edit
  - name: user_id
    source: meta:uid
    type: integer

  # Username who made the last edit
  - name: user_name
    source: meta:user
```

### JSON Aggregate Sources

Export multiple fields as JSON objects or arrays:

```yaml
columns:
  # All tags as JSON object
  - name: tags
    source: tags
    type: json

  # All metadata as JSON object
  - name: meta
    source: meta
    type: json

  # Way node references as JSON array (ways only)
  - name: refs
    source: refs
    type: json
```

Example output:

```json
{
  "tags": { "name": "Main Street", "highway": "primary", "lanes": "2" },
  "meta": { "id": 12345, "version": 3, "timestamp": "2024-01-15T10:30:00Z" },
  "refs": [101, 102, 103, 104]
}
```

### Mapping Sources

Use named mappings for categorization (see [Mappings](#mappings)):

```yaml
columns:
  - name: class
    source: mapping:poi_class
    type: string
```

### Expression Sources

Compute values using CEL expressions (see [CEL Expressions](#cel-expressions)):

```yaml
columns:
  # Localized name with fallback
  - name: name_en
    source: "expr:has(tags.name_en) ? tags.name_en : tags.name"

  # Conditional logic
  - name: road_importance
    source: 'expr:tags.highway == "motorway" ? "high" : "normal"'
```

### Column Types

Specify how values are stored in the output. The `type` field is **optional** and defaults to `string` if not specified.

| Type      | Description           | Example Use Cases                      |
| --------- | --------------------- | -------------------------------------- |
| `string`  | Text values (default) | Names, descriptions, classification    |
| `integer` | Whole numbers         | IDs, version numbers, counts           |
| `float`   | Decimal numbers       | Measurements, coordinates              |
| `json`    | JSON objects/arrays   | All tags, metadata, complex structures |

**Note**: You only need to specify `type` when it's NOT a string. For most tag sources, you can omit the `type` field entirely.

## Mappings

Mappings provide first-match-wins categorization of features based on filter rules. They're useful for classifying features into higher-level categories.

### Basic Mapping Structure

```yaml
mappings:
  mapping_name:
    rules:
      - match: "filter_expression"
        value: category_name
      - match: "another_filter"
        value: another_category
    default: default_value # Optional, null if omitted
```

### How Mappings Work

1. Rules are evaluated in order from top to bottom
2. The first matching rule's `value` is returned
3. If no rules match, the `default` value is used (or `null`)
4. The `match` field uses the same filter DSL syntax as table filters

### Mapping Example

```yaml
mappings:
  poi_class:
    rules:
      - match: "amenity=restaurant|food_court|diner"
        value: restaurant
      - match: "amenity=cafe|coffee_shop"
        value: cafe
      - match: "amenity=bar|pub|biergarten"
        value: bar
      - match: "shop=supermarket|convenience|grocery"
        value: grocery
      - match: "shop=*"
        value: retail
      - match: "amenity=bank|atm"
        value: finance
      - match: "amenity | shop"
        value: misc
    default: null

table:
  name: pois
  filter: "name & (amenity | shop)"
  columns:
    - name: name
      source: tag:name
      type: string
    - name: class
      source: mapping:poi_class
      type: string
```

In this example:

- A cafe gets classified as `cafe`
- A supermarket gets classified as `grocery`
- A clothing shop matches `shop=*` and gets `retail`
- A feature with `amenity=library` (not in any rule) gets `misc` from the last catch-all
- A feature with neither amenity nor shop would get `null` but wouldn't match the table filter anyway

### Multiple Mappings

You can define multiple mappings for different classification schemes:

```yaml
mappings:
  road_class:
    rules:
      - match: "highway=motorway|trunk"
        value: major
      - match: "highway=primary|secondary"
        value: arterial
      - match: "highway=tertiary|residential"
        value: local
    default: other

  road_surface:
    rules:
      - match: "surface=asphalt|paved|concrete"
        value: paved
      - match: "surface=gravel|unpaved|dirt"
        value: unpaved
      - match: "!surface"
        value: unknown
    default: other

table:
  name: roads
  filter: "highway"
  columns:
    - name: name
      source: tag:name
      type: string
    - name: class
      source: mapping:road_class
      type: string
    - name: surface_type
      source: mapping:road_surface
      type: string
```

## CEL Expressions

[CEL (Common Expression Language)](https://github.com/google/cel-spec) is a powerful expression language for computing derived values. Use CEL when you need conditional logic, string manipulation, or calculations.

### Basic CEL Syntax

```yaml
columns:
  - name: computed_value
    source: "expr:CEL_EXPRESSION_HERE"
    type: string
```

### Available Variables

CEL expressions have access to two variables:

- `tags`: A map (dictionary) of all OSM tags (keys and values are strings)
- `meta`: A map of metadata fields with the following keys:
  - `id` - OSM element ID (string)
  - `version` - Version number (string, may be absent)
  - `timestamp` - Last edit timestamp in ISO 8601 format (string, may be absent)

**Note:** Other metadata fields like `uid`, `user`, `changeset`, and `visible` are available via `meta:` column sources but are not currently included in the CEL `meta` variable.

### Common CEL Patterns

#### Conditional with Fallback

```yaml
# Use localized name if available, otherwise use default name
- name: name_en
  source: "expr:has(tags.name_en) ? tags.name_en : tags.name"
  type: string

# Use alternative tag names
- name: street_name
  source: 'expr:has(tags.name) ? tags.name : (has(tags.alt_name) ? tags.alt_name : "")'
  type: string
```

#### Tag Existence Checks

```yaml
# Check if a tag exists
- name: has_name
  source: "expr:has(tags.name)"
  type: string

# Check for multiple tags
- name: is_named_building
  source: "expr:has(tags.building) && has(tags.name)"
  type: string
```

#### Value Comparisons

```yaml
# String comparison
- name: is_primary_road
  source: 'expr:tags.highway == "primary"'
  type: string

# Numeric comparison (note: tags are strings, so convert first)
- name: is_wide_road
  source: "expr:has(tags.lanes) && int(tags.lanes) >= 4"
  type: string

# Multiple conditions
- name: is_major_highway
  source: 'expr:tags.highway in ["motorway", "trunk", "primary"]'
  type: string
```

#### String Manipulation

```yaml
# Concatenate strings
- name: full_ref
  source: 'expr:has(tags.ref) ? "Route " + tags.ref : "No ref"'
```

**Note:** Cosmo uses the `cel-rust` crate (v0.11) which has limited standard library support. String methods like `.upperAscii()`, `.toLowerCase()`, etc. may not be available. For string manipulation beyond concatenation, consider using mappings or post-processing the output data.

#### Ternary Expressions

```yaml
# Simple ternary
- name: road_type
  source: 'expr:tags.highway == "motorway" ? "highway" : "road"'
  type: string

# Nested ternary
- name: importance
  source: 'expr:tags.highway == "motorway" ? "high" : (tags.highway == "primary" ? "medium" : "low")'
  type: string

# With tag existence check
- name: display_name
  source: 'expr:has(tags.name) ? tags.name : (has(tags.ref) ? tags.ref : "Unnamed")'
  type: string
```

#### Accessing Metadata

```yaml
# Use metadata in expressions
- name: is_recent
  source: 'expr:has(meta.timestamp) && timestamp(meta.timestamp) > timestamp("2024-01-01T00:00:00Z")'
  type: string

# Combine tags and metadata
- name: info
  source: 'expr:tags.name + " (v" + string(meta.version) + ")"'
  type: string
```

#### Map Operations

```yaml
# Check if key exists in tags
- name: has_multilingual
  source: 'expr:"name:en" in tags || "name:es" in tags || "name:fr" in tags'
  type: string

# Count number of name translations (approximation)
- name: is_international
  source: 'expr:("name:en" in tags ? 1 : 0) + ("name:es" in tags ? 1 : 0) >= 2'
  type: string
```

### CEL Type Conversions

When working with tag values (which are always strings), you may need type conversions:

| Function    | Description        | Example                |
| ----------- | ------------------ | ---------------------- |
| `int(x)`    | Convert to integer | `int(tags.lanes)`      |
| `double(x)` | Convert to float   | `double(tags.height)`  |
| `string(x)` | Convert to string  | `string(meta.version)` |

### CEL Limitations

- CEL expressions can only access `tags` and `meta` variables
- Complex looping or iteration is not supported
- Keep expressions relatively simple for maintainability
- For very complex logic, consider using mappings instead

## Geometry Configuration

Control how OSM elements are converted to geometries. Each element type (nodes, ways, relations) can be handled differently.

### Geometry Configuration Structure

```yaml
table:
  geometry:
    node: true|false
    way: linestring|centroid|false
    closed_way: polygon|linestring|centroid|false
    relation: true|false
```

### Node Geometry

```yaml
geometry:
  node: true   # Include nodes as Point geometries
  # or
  node: false  # Exclude nodes
```

Use `node: true` for point features like POIs, trees, or street furniture.

### Way Geometry

Open ways (where first and last nodes are different):

```yaml
geometry:
  way: linestring  # Default: linear features (roads, rivers, walls)
  # or
  way: centroid    # Convert open ways to point centroids
  # or
  way: false       # Exclude open ways
```

`way` controls **open ways only**. It is independent of `closed_way`: setting
`way: false` excludes open ways but leaves closed ways governed by `closed_way`.

For open ways, `way: centroid` computes the centroid of the line geometry. It
does not turn the open way into an area.

### Closed Way Geometry

Closed ways (where first and last nodes are the same):

```yaml
geometry:
  closed_way: polygon     # Default: area features (buildings, parks)
  # or
  closed_way: linestring  # Keep as linear (e.g., circular roads)
  # or
  closed_way: centroid    # Convert to point (e.g., building centroids)
  # or
  closed_way: false       # Exclude closed ways
```

To emit closed-way polygons while dropping open ways, set `way: false` and
`closed_way: polygon`. To exclude all ways (e.g. a node-only extract), set both
`way: false` and `closed_way: false`.

### Relation Geometry

```yaml
geometry:
  relation: true   # Include relations (multipolygons, routes, etc.)
  # or
  relation: false  # Exclude relations
```

**Note:** Relation geometry support is currently limited in Cosmo. Relations are often ignored even when set to `true`.

### Common Geometry Configurations

#### Point Features (POIs)

```yaml
geometry:
  node: true # Points like shops, amenities
  way: centroid # Buildings as centroids
  closed_way: centroid # Closed ways as centroids
  relation: false
```

#### Linear Features (Roads, Rivers)

```yaml
geometry:
  node: false
  way: linestring
  closed_way: linestring # Treat roundabouts as lines
  relation: false
```

#### Area Features (Buildings, Landuse)

```yaml
geometry:
  node: false
  way: false          # Open ways cannot form valid polygons
  closed_way: polygon # Closed ways become polygons
  relation: true      # Multipolygon relations
```

#### Mixed (Transportation Network)

```yaml
geometry:
  node: true # Bus stops, traffic signals
  way: linestring # Roads
  closed_way: linestring # Roundabouts
  relation: false
```

## Complete Examples

### Example 1: Point of Interest (POI) Extraction

A comprehensive POI extraction with classification:

```yaml
mappings:
  poi_class:
    rules:
      - match: "amenity=restaurant|food_court|diner|bbq"
        value: restaurant
      - match: "amenity=cafe|coffee_shop|tea"
        value: cafe
      - match: "amenity=bar|pub|biergarten"
        value: bar
      - match: "shop=supermarket|convenience|grocery"
        value: grocery
      - match: "shop=*"
        value: retail
      - match: "amenity=bank|atm|bureau_de_change"
        value: finance
      - match: "tourism=hotel|guest_house|hostel|motel"
        value: lodging
      - match: "amenity=hospital|clinic|doctors|pharmacy"
        value: healthcare
      - match: "amenity | shop | tourism"
        value: misc
    default: null

table:
  name: pois
  filter: "name & (amenity | shop | leisure | tourism)"
  geometry:
    node: true
    way: centroid
    closed_way: centroid
    relation: false
  columns:
    - name: name
      source: tag:name
    - name: name_en
      source: "expr:has(tags.name_en) ? tags.name_en : tags.name"
    - name: class
      source: mapping:poi_class
    - name: tags
      source: tags
      type: json
    - name: osm_id
      source: meta:id
      type: integer
```

### Example 2: Road Network

Extract roads with classification and attributes:

```yaml
mappings:
  road_class:
    rules:
      - match: "highway=motorway|trunk"
        value: major
      - match: "highway=primary|secondary"
        value: arterial
      - match: "highway=tertiary|residential|unclassified"
        value: local
      - match: "highway=service|track"
        value: service
    default: other

table:
  name: roads
  filter: "highway=motorway|trunk|primary|secondary|tertiary|residential|unclassified|service|*_link"
  geometry:
    node: false
    way: linestring
    closed_way: linestring
    relation: false
  columns:
    - name: name
      source: tag:name
    - name: ref
      source: tag:ref
    - name: highway_type
      source: tag:highway
    - name: road_class
      source: mapping:road_class
    - name: lanes
      source: tag:lanes
      type: integer
    - name: maxspeed
      source: tag:maxspeed
    - name: oneway
      source: tag:oneway
    - name: surface
      source: tag:surface
```

### Example 3: Building Footprints

Extract buildings with metadata:

```yaml
table:
  name: buildings
  filter: "building"
  geometry:
    node: false
    way: false
    closed_way: polygon
    relation: true
  columns:
    - name: building_type
      source: tag:building
    - name: name
      source: tag:name
    - name: addr_housenumber
      source: tag:addr:housenumber
    - name: addr_street
      source: tag:addr:street
    - name: height
      source: tag:height
      type: float
    - name: levels
      source: tag:building:levels
      type: integer
    - name: osm_id
      source: meta:id
      type: integer
    - name: version
      source: meta:version
      type: integer
```

### Example 4: Complex Filtering

Advanced filter with multiple conditions:

```yaml
table:
  name: complex_pois
  # Named features that are either:
  # - Restaurants with outdoor seating, OR
  # - Any tourism feature, OR
  # - Shops with wheelchair access
  filter: "name & ((amenity=restaurant & outdoor_seating=yes) | tourism | (shop & wheelchair=yes))"
  geometry:
    node: true
    way: centroid
    closed_way: centroid
    relation: false
  columns:
    - name: name
      source: tag:name
    - name: category
      source: "expr:has(tags.amenity) ? tags.amenity : (has(tags.tourism) ? tags.tourism : tags.shop)"
    - name: accessibility
      source: 'expr:has(tags.wheelchair) ? tags.wheelchair : "unknown"'
    - name: outdoor_seating
      source: tag:outdoor_seating
```

## Best Practices

### Filter Design

1. **Start simple, add complexity**: Begin with basic tag existence checks, then add value matching and logic
2. **Test incrementally**: Test each part of complex filters separately before combining
3. **Use parentheses**: Make complex logic explicit with grouping, even when not strictly required
4. **Consider performance**: Very broad filters (like empty `""`) process all features, which is slower

### Column Configuration

1. **Only extract what you need**: Extra columns increase output size and processing time
2. **Use appropriate types**: Store IDs as integers, measurements as floats, text as strings
3. **Leverage mappings**: Use mappings for categorization instead of complex CEL expressions
4. **Document CEL expressions**: Complex expressions benefit from YAML comments explaining the logic

### Geometry Handling

1. **Match geometry to use case**: Points for POIs, lines for roads, polygons for areas
2. **Consider closed ways carefully**: Decide whether circular features should be polygons or linestrings
3. **Use centroids for mixed features**: When extracting both buildings (ways) and POIs (nodes), use centroids for buildings

### Mapping Strategy

1. **Order matters**: Put most specific rules first, general rules last
2. **Use catch-all carefully**: A final catch-all rule (like `amenity | shop`) ensures everything matches something
3. **Set meaningful defaults**: Use `null` when no category applies, or a default like `other` or `misc`
4. **Keep mappings focused**: Create separate mappings for different classification schemes rather than one giant mapping

## Additional Resources

- [CEL Language Specification](https://github.com/google/cel-spec)
- [OpenStreetMap Tag Documentation](https://wiki.openstreetmap.org/wiki/Map_Features)
- [OSM Tag Info (usage statistics)](https://taginfo.openstreetmap.org/)

## Troubleshooting

### No features in output

- Check that your filter matches features in your input data
- Verify geometry settings allow the feature types you want (nodes/ways/relations)
- Test with an empty filter `""` to see if data flows through

### Unexpected filtering results

- Test filter components separately
- Watch for ambiguous `|` operators (use parentheses to clarify)
- Check that glob patterns use `*` correctly

### CEL expression errors

- Verify variable names: `tags.key` not `tag.key`
- Use `has(tags.key)` before accessing optional tags
- Remember tags are strings: convert with `int()` or `double()` for numeric operations

### Mapping not categorizing correctly

- Check rule order: first match wins
- Test each match expression separately as a filter
- Verify the mapping is referenced correctly in the column: `source: mapping:name`

## Known Limitations

### Tag Value Inequality (`key!=value`) Not Supported

The `!=` operator only works for numeric comparisons, not for tag value matching.

```yaml
# ❌ This does NOT work
filter: 'highway!=residential'

# use instead
filter: '!(highway=residential)'
```

### Limited Metadata in CEL Context

Only `id`, `version`, and `timestamp` are available in the CEL `meta` variable. Other metadata fields (`uid`, `user`, `changeset`, `visible`) are not included. Use `meta:` column sources instead of CEL expressions for metadata fields if possible.

### Limited String Functions

Cosmo uses `cel-rust` v0.11, which has limited standard library support compared to the full CEL specification. String methods like `.upperAscii()`, `.toLowerCase()`, `.contains()`, `.startsWith()`, etc. may not be available. I have not tested this much.

### Relations Not Fully Supported

Relation geometry processing is currently limited. Even when `relation: true` is set, relations are often skipped. Relation geometry construction (multipolygons, routes, boundaries) is not yet implemented.
