# Filter YAML Guide

This guide provides a comprehensive reference for writing filter configurations in Cosmo. Filters determine which OSM features are included in the output and how they are transformed.

## Table of Contents

- [Filter DSL Syntax](#filter-dsl-syntax)
- [Column Sources](#column-sources)
- [Mappings](#mappings)
- [CEL Expressions](#cel-expressions)
- [Geometry Configuration](#geometry-configuration)
- [Complete Examples](#complete-examples)

## Filter DSL Syntax

The filter DSL is a string-based expression language for matching OSM tags and metadata, configured via YAML.

### Basic Tag Operations

| Syntax | Description | Example |
|--------|-------------|---------|
| `key` | Tag exists (any value) | `name` matches any feature with a `name` tag |
| `!key` | Tag does not exist | `!name` matches features without a `name` tag |
| `key=value` | Exact value match | `highway=primary` matches only `highway=primary` |
| `key!=value` | Value does not equal | `highway!=residential` excludes residential roads |
| `key=v1\|v2\|v3` | Match any of multiple values | `amenity=cafe\|restaurant\|bar` |
| `key=*` | Tag exists with any non-empty value | `shop=*` matches any shop |

### Glob Patterns

Glob patterns allow wildcard matching within values:

| Pattern | Description | Example |
|---------|-------------|---------|
| `key=*_link` | Suffix wildcard | `highway=*_link` matches `motorway_link`, `trunk_link`, etc. |
| `key=primary_*` | Prefix wildcard | `name=primary_*` matches `primary_school`, `primary_care` |
| `key=*middle*` | Contains pattern | `name=*hospital*` matches any name containing "hospital" |

Note: The standalone `*` (as in `key=*`) means "any value", while wildcards within strings (like `*_link`) are glob patterns.

### Numeric Comparisons

Compare tag values numerically:

| Operator | Description | Example |
|----------|-------------|---------|
| `key<value` | Less than | `lanes<3` |
| `key<=value` | Less than or equal | `maxspeed<=50` |
| `key>value` | Greater than | `population>10000` |
| `key>=value` | Greater than or equal | `height>=100` |

### Logical Operators

Combine expressions using boolean logic:

| Operator | Description | Example |
|----------|-------------|---------|
| `expr1 & expr2` | Logical AND (both must match) | `highway=primary & name` |
| `expr1 \| expr2` | Logical OR (either can match) | `amenity \| shop` |
| `!expr` | Logical NOT (negation) | `!(highway=residential)` |
| `(expr)` | Grouping | `(amenity \| shop) & name` |

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

This is useful for pass-through configurations where you want to export everything.

## Column Sources

Columns define what data is extracted from OSM features and how it's formatted in the output. The `source` field specifies where the data comes from.

### Tag Sources

Extract values from OSM tags:

```yaml
columns:
  # Explicit tag: prefix
  - name: name
    source: tag:name
    type: string

  # Implicit (no prefix defaults to tag)
  - name: highway_type
    source: highway
    type: string

  # Tags that might be numeric
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

  # Version number
  - name: version
    source: meta:version
    type: integer

  # Last edit timestamp (ISO 8601 format)
  - name: timestamp
    source: meta:timestamp
    type: string

  # User ID who made the last edit
  - name: user_id
    source: meta:uid
    type: integer

  # Username who made the last edit
  - name: user_name
    source: meta:user
    type: string
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
  "tags": {"name": "Main Street", "highway": "primary", "lanes": "2"},
  "meta": {"id": 12345, "version": 3, "timestamp": "2024-01-15T10:30:00Z"},
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
    source: 'expr:has(tags.name_en) ? tags.name_en : tags.name'
    type: string

  # Conditional logic
  - name: road_importance
    source: 'expr:tags.highway == "motorway" ? "high" : "normal"'
    type: string
```

### Column Types

Specify how values are stored in the output:

| Type | Description | Example Use Cases |
|------|-------------|-------------------|
| `string` | Text values | Names, descriptions, classification |
| `integer` | Whole numbers | IDs, version numbers, counts |
| `float` | Decimal numbers | Measurements, coordinates |
| `json` | JSON objects/arrays | All tags, metadata, complex structures |

## Mappings

Mappings provide first-match-wins categorization of features based on filter rules. They're useful for classifying features into higher-level categories.

### Basic Mapping Structure

```yaml
mappings:
  mapping_name:
    rules:
      - match: 'filter_expression'
        value: category_name
      - match: 'another_filter'
        value: another_category
    default: default_value  # Optional, null if omitted
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
      - match: 'amenity=restaurant|food_court|diner'
        value: restaurant
      - match: 'amenity=cafe|coffee_shop'
        value: cafe
      - match: 'amenity=bar|pub|biergarten'
        value: bar
      - match: 'shop=supermarket|convenience|grocery'
        value: grocery
      - match: 'shop=*'
        value: retail
      - match: 'amenity=bank|atm'
        value: finance
      - match: 'amenity | shop'
        value: misc
    default: null

table:
  name: pois
  filter: 'name & (amenity | shop)'
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
      - match: 'highway=motorway|trunk'
        value: major
      - match: 'highway=primary|secondary'
        value: arterial
      - match: 'highway=tertiary|residential'
        value: local
    default: other

  road_surface:
    rules:
      - match: 'surface=asphalt|paved|concrete'
        value: paved
      - match: 'surface=gravel|unpaved|dirt'
        value: unpaved
      - match: '!surface'
        value: unknown
    default: other

table:
  name: roads
  filter: 'highway'
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
    source: 'expr:CEL_EXPRESSION_HERE'
    type: string
```

### Available Variables

CEL expressions have access to two variables:

- `tags`: A map (dictionary) of all OSM tags
- `meta`: A map of metadata fields (id, version, timestamp, uid, user)

### Common CEL Patterns

#### Conditional with Fallback

```yaml
# Use localized name if available, otherwise use default name
- name: name_en
  source: 'expr:has(tags.name_en) ? tags.name_en : tags.name'
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
  source: 'expr:has(tags.name)'
  type: string

# Check for multiple tags
- name: is_named_building
  source: 'expr:has(tags.building) && has(tags.name)'
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
  source: 'expr:has(tags.lanes) && int(tags.lanes) >= 4'
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
  type: string

# Uppercase conversion (using cel-rust available functions)
- name: highway_upper
  source: 'expr:has(tags.highway) ? tags.highway.upperAscii() : ""'
  type: string
```

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

| Function | Description | Example |
|----------|-------------|---------|
| `int(x)` | Convert to integer | `int(tags.lanes)` |
| `double(x)` | Convert to float | `double(tags.height)` |
| `string(x)` | Convert to string | `string(meta.version)` |

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
    way: linestring|polygon|centroid|false
    closed_way: polygon|linestring|centroid
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
  way: polygon     # Treat as polygon (rare for open ways)
  # or
  way: centroid    # Convert to point at the centroid
  # or
  way: false       # Exclude open ways
```

### Closed Way Geometry

Closed ways (where first and last nodes are the same):

```yaml
geometry:
  closed_way: polygon     # Default: area features (buildings, parks)
  # or
  closed_way: linestring  # Keep as linear (e.g., circular roads)
  # or
  closed_way: centroid    # Convert to point (e.g., building centroids)
```

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
  node: true           # Points like shops, amenities
  way: centroid        # Buildings as centroids
  closed_way: centroid # Closed ways as centroids
  relation: false
```

#### Linear Features (Roads, Rivers)

```yaml
geometry:
  node: false
  way: linestring
  closed_way: linestring  # Treat roundabouts as lines
  relation: false
```

#### Area Features (Buildings, Landuse)

```yaml
geometry:
  node: false
  way: polygon
  closed_way: polygon
  relation: true  # Multipolygon relations
```

#### Mixed (Transportation Network)

```yaml
geometry:
  node: true              # Bus stops, traffic signals
  way: linestring         # Roads
  closed_way: linestring  # Roundabouts
  relation: false
```

## Complete Examples

### Example 1: Point of Interest (POI) Extraction

A comprehensive POI extraction with classification:

```yaml
mappings:
  poi_class:
    rules:
      - match: 'amenity=restaurant|food_court|diner|bbq'
        value: restaurant
      - match: 'amenity=cafe|coffee_shop|tea'
        value: cafe
      - match: 'amenity=bar|pub|biergarten'
        value: bar
      - match: 'shop=supermarket|convenience|grocery'
        value: grocery
      - match: 'shop=*'
        value: retail
      - match: 'amenity=bank|atm|bureau_de_change'
        value: finance
      - match: 'tourism=hotel|guest_house|hostel|motel'
        value: lodging
      - match: 'amenity=hospital|clinic|doctors|pharmacy'
        value: healthcare
      - match: 'amenity | shop | tourism'
        value: misc
    default: null

table:
  name: pois
  filter: 'name & (amenity | shop | leisure | tourism)'
  geometry:
    node: true
    way: centroid
    closed_way: centroid
    relation: false
  columns:
    - name: name
      source: tag:name
      type: string
    - name: name_en
      source: 'expr:has(tags.name_en) ? tags.name_en : tags.name'
      type: string
    - name: class
      source: mapping:poi_class
      type: string
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
      - match: 'highway=motorway|trunk'
        value: major
      - match: 'highway=primary|secondary'
        value: arterial
      - match: 'highway=tertiary|residential|unclassified'
        value: local
      - match: 'highway=service|track'
        value: service
    default: other

table:
  name: roads
  filter: 'highway=motorway|trunk|primary|secondary|tertiary|residential|unclassified|service|*_link'
  geometry:
    node: false
    way: linestring
    closed_way: linestring
    relation: false
  columns:
    - name: name
      source: tag:name
      type: string
    - name: ref
      source: tag:ref
      type: string
    - name: highway_type
      source: tag:highway
      type: string
    - name: road_class
      source: mapping:road_class
      type: string
    - name: lanes
      source: tag:lanes
      type: integer
    - name: maxspeed
      source: tag:maxspeed
      type: string
    - name: oneway
      source: tag:oneway
      type: string
    - name: surface
      source: tag:surface
      type: string
```

### Example 3: Building Footprints

Extract buildings with metadata:

```yaml
table:
  name: buildings
  filter: 'building'
  geometry:
    node: false
    way: polygon
    closed_way: polygon
    relation: true
  columns:
    - name: building_type
      source: tag:building
      type: string
    - name: name
      source: tag:name
      type: string
    - name: addr_housenumber
      source: tag:addr:housenumber
      type: string
    - name: addr_street
      source: tag:addr:street
      type: string
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

### Example 4: Pass-Through (Export Everything)

Export all OSM data with minimal filtering:

```yaml
table:
  name: all_features
  filter: ""  # Empty filter matches everything
  geometry:
    node: true
    way: linestring
    closed_way: polygon
    relation: true
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

### Example 5: Complex Filtering

Advanced filter with multiple conditions:

```yaml
table:
  name: complex_pois
  # Named features that are either:
  # - Restaurants with outdoor seating, OR
  # - Any tourism feature, OR
  # - Shops with wheelchair access
  filter: 'name & ((amenity=restaurant & outdoor_seating=yes) | tourism | (shop & wheelchair=yes))'
  geometry:
    node: true
    way: centroid
    closed_way: centroid
    relation: false
  columns:
    - name: name
      source: tag:name
      type: string
    - name: category
      source: 'expr:has(tags.amenity) ? tags.amenity : (has(tags.tourism) ? tags.tourism : tags.shop)'
      type: string
    - name: accessibility
      source: 'expr:has(tags.wheelchair) ? tags.wheelchair : "unknown"'
      type: string
    - name: outdoor_seating
      source: tag:outdoor_seating
      type: string
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
