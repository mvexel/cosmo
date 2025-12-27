use serde_json::Value;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Command;

// =============================================================================
// Test Helpers
// =============================================================================

fn fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixture")
        .join("library_square.osm.pbf")
}

fn write_temp_filters(contents: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    path.push(format!("cosmo_filters_{pid}_{nanos}.yaml"));
    std::fs::write(&path, contents).expect("write filters yaml");
    path
}

fn run_cosmo(filters_yaml: &str) -> Vec<String> {
    let filters_path = write_temp_filters(filters_yaml);
    let exe = env!("CARGO_BIN_EXE_cosmo");

    let output = Command::new(exe)
        .arg("--input")
        .arg(fixture_path())
        .arg("--output")
        .arg("-")
        .arg("--format")
        .arg("geojsonl")
        .arg("--filters")
        .arg(&filters_path)
        .arg("--node-cache-mode")
        .arg("memory")
        .output()
        .expect("run cosmo");

    let _ = std::fs::remove_file(&filters_path);

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("cosmo failed: {}", stderr);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect()
}

fn run_cosmo_to_file(filters_yaml: &str, format: &str, output_path: &Path) {
    let filters_path = write_temp_filters(filters_yaml);
    let exe = env!("CARGO_BIN_EXE_cosmo");

    let output = Command::new(exe)
        .arg("--input")
        .arg(fixture_path())
        .arg("--output")
        .arg(output_path)
        .arg("--format")
        .arg(format)
        .arg("--filters")
        .arg(&filters_path)
        .arg("--node-cache-mode")
        .arg("memory")
        .output()
        .expect("run cosmo");

    let _ = std::fs::remove_file(&filters_path);

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("cosmo failed: {}", stderr);
    }
}

fn run_cosmo_expect_failure(filters_yaml: &str, args: &[&str]) -> String {
    let filters_path = write_temp_filters(filters_yaml);
    let exe = env!("CARGO_BIN_EXE_cosmo");

    let output = Command::new(exe)
        .arg("--input")
        .arg(fixture_path())
        .arg("--output")
        .arg("-")
        .arg("--format")
        .arg("geojsonl")
        .arg("--filters")
        .arg(&filters_path)
        .args(args)
        .output()
        .expect("run cosmo");

    let _ = std::fs::remove_file(&filters_path);

    assert!(!output.status.success(), "expected failure");
    String::from_utf8_lossy(&output.stderr).to_string()
}

#[test]
fn extracts_tree_nodes() {
    let filters = r#"
table:
  name: trees
  filter:
    tag: "natural"
    value: "tree"
  geometry:
    node: true
    way: false
    relation: false
  columns:
    - name: "id"
      source: "meta:id"
      type: "integer"
"#;
    let lines = run_cosmo(filters);
    assert_eq!(lines.len(), 261);
}

#[test]
fn extracts_lanes_tagged_ways() {
    let filters = r#"
table:
  name: lanes
  filter:
    tag: "lanes"
  geometry:
    node: false
    way: "linestring"
    relation: false
  columns:
    - name: "lanes"
      source: "tag:lanes"
      type: "string"
"#;
    let lines = run_cosmo(filters);
    assert_eq!(lines.len(), 49);
}

#[test]
fn writes_parquet_output() {
    let filters = r#"
table:
  name: lanes
  filter:
    tag: "lanes"
  geometry:
    node: false
    way: "linestring"
    relation: false
  columns:
    - name: "lanes"
      source: "tag:lanes"
      type: "string"
"#;
    let mut output_path = std::env::temp_dir();
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    output_path.push(format!("cosmo_test_{pid}_{nanos}.parquet"));

    run_cosmo_to_file(filters, "parquet", &output_path);

    let metadata = std::fs::metadata(&output_path).expect("parquet exists");
    assert!(metadata.len() > 0, "parquet output should not be empty");
    let _ = std::fs::remove_file(&output_path);
}

#[test]
fn mmap_cache_errors_on_small_size() {
    let filters = r#"
table:
  name: lanes
  filter:
    tag: "lanes"
  geometry:
    node: false
    way: "linestring"
    relation: false
  columns:
    - name: "lanes"
      source: "tag:lanes"
      type: "string"
"#;
    let stderr = run_cosmo_expect_failure(
        filters,
        &[
            "--node-cache-mode",
            "mmap",
            "--node-cache-max-nodes",
            "1000",
        ],
    );
    assert!(
        stderr.contains("exceeds node_cache_max_nodes"),
        "unexpected stderr: {stderr}"
    );
}

// =============================================================================
// Feature Parsing Helpers
// =============================================================================

fn parse_features(lines: &[String]) -> Vec<Value> {
    lines
        .iter()
        .map(|line| serde_json::from_str(line).expect("valid GeoJSON feature"))
        .collect()
}

fn geometry_type(feature: &Value) -> &str {
    feature["geometry"]["type"]
        .as_str()
        .expect("geometry.type should be a string")
}

fn get_property<'a>(feature: &'a Value, key: &str) -> Option<&'a Value> {
    feature["properties"].get(key)
}

fn get_osm_id(feature: &Value) -> Option<&str> {
    get_property(feature, "osm_id").and_then(|v| v.as_str())
}

// =============================================================================
// Geometry Mode Tests
// =============================================================================

#[test]
fn closed_way_centroid_outputs_only_points() {
    // This test verifies that when closed_way: centroid is set,
    // buildings (closed ways) are converted to Point geometries.
    let filters = r#"
table:
  name: buildings
  filter: 'building'
  geometry:
    node: false
    way: centroid
    closed_way: centroid
    relation: false
  columns:
    - name: osm_id
      source: meta:id
      type: string
    - name: name
      source: tag:name
      type: string
"#;
    let lines = run_cosmo(filters);
    let features = parse_features(&lines);

    assert!(!features.is_empty(), "should extract some buildings");

    // ALL features should be Points (from centroid conversion)
    let non_point_features: Vec<_> = features
        .iter()
        .filter(|f| geometry_type(f) != "Point")
        .collect();

    assert!(
        non_point_features.is_empty(),
        "closed_way: centroid should produce only Points, but found {} non-Point features: {:?}",
        non_point_features.len(),
        non_point_features
            .iter()
            .map(|f| format!(
                "osm_id={} type={}",
                get_osm_id(f).unwrap_or("?"),
                geometry_type(f)
            ))
            .collect::<Vec<_>>()
    );
}

#[test]
fn closed_way_polygon_outputs_polygons() {
    // This test verifies that when closed_way: polygon is set (default),
    // buildings are output as Polygon geometries.
    let filters = r#"
table:
  name: buildings
  filter: 'building'
  geometry:
    node: false
    way: linestring
    closed_way: polygon
    relation: false
  columns:
    - name: osm_id
      source: meta:id
      type: string
"#;
    let lines = run_cosmo(filters);
    let features = parse_features(&lines);

    assert!(!features.is_empty(), "should extract some buildings");

    // ALL features should be Polygons
    let non_polygon_features: Vec<_> = features
        .iter()
        .filter(|f| geometry_type(f) != "Polygon")
        .collect();

    assert!(
        non_polygon_features.is_empty(),
        "closed_way: polygon should produce only Polygons, but found {} non-Polygon features",
        non_polygon_features.len()
    );
}

#[test]
fn way_linestring_outputs_linestrings() {
    // Open ways (like roads) with way: linestring should output LineStrings
    let filters = r#"
table:
  name: roads
  filter: 'highway'
  geometry:
    node: false
    way: linestring
    closed_way: linestring
    relation: false
  columns:
    - name: osm_id
      source: meta:id
      type: string
"#;
    let lines = run_cosmo(filters);
    let features = parse_features(&lines);

    assert!(!features.is_empty(), "should extract some roads");

    // Should have LineStrings (open ways remain LineStrings)
    let linestring_count = features
        .iter()
        .filter(|f| geometry_type(f) == "LineString")
        .count();

    assert!(
        linestring_count > 0,
        "way: linestring should produce some LineString features"
    );
}

// =============================================================================
// Single-Table Tests
// =============================================================================

#[test]
fn single_table_no_duplicates() {
    // A single table should never produce duplicate osm_ids for the same geometry type
    let filters = r#"
table:
  name: restaurants
  filter: 'amenity=restaurant'
  geometry:
    node: true
    way: centroid
    closed_way: centroid
    relation: false
  columns:
    - name: osm_id
      source: meta:id
      type: string
"#;
    let lines = run_cosmo(filters);
    let features = parse_features(&lines);

    let osm_ids: Vec<&str> = features.iter().filter_map(get_osm_id).collect();
    let unique_ids: HashSet<&str> = osm_ids.iter().copied().collect();

    assert_eq!(
        osm_ids.len(),
        unique_ids.len(),
        "single table should not produce duplicate osm_ids"
    );
}

// =============================================================================
// Metadata Column Tests
// =============================================================================

#[test]
fn metadata_columns_are_populated() {
    let filters = r#"
table:
  name: pois
  filter: 'amenity=restaurant'
  geometry:
    node: true
    way: centroid
    closed_way: centroid
    relation: false
  columns:
    - name: osm_id
      source: meta:id
      type: string
    - name: version
      source: meta:version
      type: integer
    - name: timestamp
      source: meta:timestamp
      type: string
"#;
    let lines = run_cosmo(filters);
    let features = parse_features(&lines);

    assert!(!features.is_empty(), "should extract some restaurants");

    // Check that metadata columns are populated
    for feature in &features {
        let osm_id = get_property(feature, "osm_id");
        let version = get_property(feature, "version");
        let timestamp = get_property(feature, "timestamp");

        assert!(
            osm_id.is_some() && !osm_id.unwrap().is_null(),
            "osm_id should be populated"
        );
        assert!(
            version.is_some() && !version.unwrap().is_null(),
            "version should be populated for feature with osm_id {:?}",
            osm_id
        );
        assert!(
            timestamp.is_some() && !timestamp.unwrap().is_null(),
            "timestamp should be populated for feature with osm_id {:?}",
            osm_id
        );
    }
}

#[test]
fn metadata_id_matches_osm_id_column() {
    let filters = r#"
table:
  name: pois
  filter: 'amenity'
  geometry:
    node: true
    way: false
    relation: false
  columns:
    - name: osm_id
      source: meta:id
      type: string
"#;
    let lines = run_cosmo(filters);
    let features = parse_features(&lines);

    assert!(!features.is_empty(), "should extract some amenities");

    // All osm_ids should be valid integers (as strings)
    for feature in &features {
        let osm_id = get_property(feature, "osm_id")
            .and_then(|v| v.as_str())
            .expect("osm_id should be a string");

        assert!(
            osm_id.parse::<i64>().is_ok(),
            "osm_id '{}' should be a valid integer",
            osm_id
        );
    }
}

// =============================================================================
// DSL Filter Tests
// =============================================================================

#[test]
fn dsl_filter_with_or_operator() {
    let filters = r#"
table:
  name: food
  filter: 'amenity=restaurant | amenity=cafe'
  geometry:
    node: true
    way: false
    relation: false
  columns:
    - name: osm_id
      source: meta:id
      type: string
    - name: amenity
      source: tag:amenity
      type: string
"#;
    let lines = run_cosmo(filters);
    let features = parse_features(&lines);

    assert!(!features.is_empty(), "should extract restaurants or cafes");

    // All features should have amenity=restaurant or amenity=cafe
    for feature in &features {
        let amenity = get_property(feature, "amenity")
            .and_then(|v| v.as_str())
            .expect("amenity should be present");

        assert!(
            amenity == "restaurant" || amenity == "cafe",
            "amenity should be 'restaurant' or 'cafe', got '{}'",
            amenity
        );
    }
}

#[test]
fn dsl_filter_with_and_operator() {
    let filters = r#"
table:
  name: named_restaurants
  filter: 'amenity=restaurant & name'
  geometry:
    node: true
    way: centroid
    closed_way: centroid
    relation: false
  columns:
    - name: osm_id
      source: meta:id
      type: string
    - name: name
      source: tag:name
      type: string
"#;
    let lines = run_cosmo(filters);
    let features = parse_features(&lines);

    // All features should have a name
    for feature in &features {
        let name = get_property(feature, "name");
        assert!(
            name.is_some() && !name.unwrap().is_null(),
            "all features should have a name (filter requires it)"
        );
    }
}

#[test]
fn dsl_filter_with_pipe_separated_values() {
    // Note: shops in the fixture are mostly closed ways (buildings), not nodes
    let filters = r#"
table:
  name: shops
  filter: 'shop=bakery|books|florist'
  geometry:
    node: true
    way: centroid
    closed_way: centroid
    relation: false
  columns:
    - name: osm_id
      source: meta:id
      type: string
    - name: shop
      source: tag:shop
      type: string
"#;
    let lines = run_cosmo(filters);
    let features = parse_features(&lines);

    assert!(!features.is_empty(), "should find bakery, books, or florist shops");

    // All features should have shop in the allowed values
    for feature in &features {
        let shop = get_property(feature, "shop")
            .and_then(|v| v.as_str())
            .expect("shop should be present");

        assert!(
            shop == "bakery" || shop == "books" || shop == "florist",
            "shop should be 'bakery', 'books', or 'florist', got '{}'",
            shop
        );
    }
}

// =============================================================================
// Regression Tests - Bugs Found in Production
// =============================================================================

#[test]
fn missing_closed_way_defaults_to_polygon() {
    // REGRESSION TEST: When closed_way is not specified, it defaults to polygon.
    // This test verifies the DEFAULT behavior so we know what to expect.
    let filters = r#"
table:
  name: buildings
  filter: 'building'
  geometry:
    node: false
    way: centroid
    # closed_way NOT specified - defaults to polygon!
    relation: false
  columns:
    - name: osm_id
      source: meta:id
      type: string
"#;
    let lines = run_cosmo(filters);
    let features = parse_features(&lines);

    assert!(!features.is_empty(), "should extract some buildings");

    // Without closed_way specified, closed ways become Polygons (the default)
    let polygon_count = features
        .iter()
        .filter(|f| geometry_type(f) == "Polygon")
        .count();

    assert!(
        polygon_count > 0,
        "closed_way defaults to polygon, so we should see Polygon features"
    );
}

#[test]
fn pois_annotated_yaml_integration() {
    // Integration test using the actual pois_annotated.yaml config file.
    // This is the config that originally exposed the bugs.
    let filters_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("pois_annotated.yaml");

    let exe = env!("CARGO_BIN_EXE_cosmo");

    let output = Command::new(exe)
        .arg("--input")
        .arg(fixture_path())
        .arg("--output")
        .arg("-")
        .arg("--format")
        .arg("geojsonl")
        .arg("--filters")
        .arg(&filters_path)
        .arg("--node-cache-mode")
        .arg("memory")
        .output()
        .expect("run cosmo");

    assert!(output.status.success(), "cosmo should succeed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<String> = stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.to_string())
        .collect();

    let features = parse_features(&lines);
    assert!(!features.is_empty(), "should extract POIs");

    // Count geometry types
    let point_count = features
        .iter()
        .filter(|f| geometry_type(f) == "Point")
        .count();
    let polygon_count = features
        .iter()
        .filter(|f| geometry_type(f) == "Polygon")
        .count();

    // Document current behavior - we expect both Points and Polygons
    // because pois_localized doesn't have closed_way: centroid
    println!(
        "pois_annotated.yaml output: {} Points, {} Polygons, {} total",
        point_count,
        polygon_count,
        features.len()
    );

    // This is a documentation test - if the YAML is fixed, this will need updating
    // Currently: pois has closed_way: centroid, pois_localized defaults to polygon
    // So we expect both geometry types
}

// =============================================================================
// Mapping Tests
// =============================================================================

#[test]
fn all_tags_flag_includes_tags_object() {
    // Test that --all-tags flag adds a "tags" object to properties
    let filters = r#"
table:
  name: amenities
  filter: 'amenity'
  geometry:
    node: true
    way: false
    relation: false
  columns:
    - name: osm_id
      source: meta:id
      type: string
"#;
    let filters_path = write_temp_filters(filters);
    let exe = env!("CARGO_BIN_EXE_cosmo");

    let output = Command::new(exe)
        .arg("--input")
        .arg(fixture_path())
        .arg("--output")
        .arg("-")
        .arg("--format")
        .arg("geojsonl")
        .arg("--filters")
        .arg(&filters_path)
        .arg("--node-cache-mode")
        .arg("memory")
        .arg("--all-tags")
        .output()
        .expect("run cosmo");

    let _ = std::fs::remove_file(&filters_path);

    assert!(output.status.success(), "cosmo should succeed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<String> = stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.to_string())
        .collect();

    let features = parse_features(&lines);
    assert!(!features.is_empty(), "should extract some amenities");

    // With --all-tags, every feature should have a "tags" object in properties
    for feature in &features {
        let tags = get_property(feature, "tags");
        assert!(
            tags.is_some() && tags.unwrap().is_object(),
            "with --all-tags, properties should contain a 'tags' object"
        );

        // The tags object should contain the amenity key
        let tags_obj = tags.unwrap().as_object().unwrap();
        assert!(
            tags_obj.contains_key("amenity"),
            "tags object should contain 'amenity' key"
        );
    }
}

#[test]
fn geojson_output_is_valid_feature_collection() {
    let filters = r#"
table:
  name: trees
  filter:
    tag: "natural"
    value: "tree"
  geometry:
    node: true
    way: false
    relation: false
  columns:
    - name: "id"
      source: "meta:id"
      type: "integer"
"#;
    let mut output_path = std::env::temp_dir();
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    output_path.push(format!("cosmo_test_{pid}_{nanos}.geojson"));

    run_cosmo_to_file(filters, "geojson", &output_path);

    let content = std::fs::read_to_string(&output_path).expect("read geojson");
    let _ = std::fs::remove_file(&output_path);

    let geojson: Value = serde_json::from_str(&content).expect("valid JSON");

    // Verify it's a FeatureCollection
    assert_eq!(
        geojson["type"].as_str(),
        Some("FeatureCollection"),
        "should be a FeatureCollection"
    );

    // Verify it has features array
    assert!(
        geojson["features"].is_array(),
        "should have features array"
    );

    let features = geojson["features"].as_array().unwrap();
    assert!(!features.is_empty(), "should have features");

    // Verify each feature has required GeoJSON properties
    for feature in features {
        assert_eq!(
            feature["type"].as_str(),
            Some("Feature"),
            "each element should be a Feature"
        );
        assert!(feature["geometry"].is_object(), "should have geometry");
        assert!(feature["properties"].is_object(), "should have properties");
    }
}

#[test]
fn empty_filter_matches_nothing() {
    // An impossible filter should produce no output
    let filters = r#"
table:
  name: impossible
  filter: 'nonexistent_tag_xyz123=impossible_value_abc789'
  geometry:
    node: true
    way: false
    relation: false
  columns:
    - name: osm_id
      source: meta:id
      type: string
"#;
    let lines = run_cosmo(filters);
    assert!(lines.is_empty(), "impossible filter should match nothing");
}

#[test]
fn tag_exists_filter_works() {
    // Filter for "just tag exists" (no value constraint)
    let filters = r#"
table:
  name: named
  filter: 'name'
  geometry:
    node: true
    way: false
    relation: false
  columns:
    - name: osm_id
      source: meta:id
      type: string
    - name: name
      source: tag:name
      type: string
"#;
    let lines = run_cosmo(filters);
    let features = parse_features(&lines);

    assert!(!features.is_empty(), "should find nodes with names");

    // All features should have a name
    for feature in &features {
        let name = get_property(feature, "name");
        assert!(
            name.is_some() && !name.unwrap().is_null(),
            "all features should have a name (filter requires tag exists)"
        );
    }
}

#[test]
fn nodes_only_mode_skips_ways() {
    // When way: false, we should only get nodes even if ways match the filter
    let filters = r#"
table:
  name: amenities
  filter: 'amenity'
  geometry:
    node: true
    way: false
    relation: false
  columns:
    - name: osm_id
      source: meta:id
      type: string
"#;
    let lines = run_cosmo(filters);
    let features = parse_features(&lines);

    assert!(!features.is_empty(), "should find amenity nodes");

    // All features should be Points (from nodes only)
    for feature in &features {
        assert_eq!(
            geometry_type(feature),
            "Point",
            "with way: false, all features should be Points"
        );
    }
}

// =============================================================================
// Mapping Tests
// =============================================================================

#[test]
fn mapping_column_is_populated() {
    let filters = r#"
mappings:
  poi_type:
    rules:
      - match: 'amenity=restaurant'
        value: dining
      - match: 'amenity=cafe'
        value: coffee
      - match: 'shop'
        value: retail
    default: other

table:
  name: pois
  filter: 'amenity=restaurant | amenity=cafe | shop'
  geometry:
    node: true
    way: false
    relation: false
  columns:
    - name: osm_id
      source: meta:id
      type: string
    - name: category
      source: mapping:poi_type
      type: string
"#;
    let lines = run_cosmo(filters);
    let features = parse_features(&lines);

    assert!(!features.is_empty(), "should extract some POIs");

    // All features should have a category from the mapping
    let valid_categories = ["dining", "coffee", "retail", "other"];
    for feature in &features {
        let category = get_property(feature, "category")
            .and_then(|v| v.as_str())
            .expect("category should be present");

        assert!(
            valid_categories.contains(&category),
            "category should be one of {:?}, got '{}'",
            valid_categories,
            category
        );
    }
}
