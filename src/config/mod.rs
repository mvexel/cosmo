use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

#[cfg(test)]
use tempfile::NamedTempFile;

#[derive(Debug, Deserialize, Serialize)]
pub struct FiltersConfig {
    pub tables: HashMap<String, TableConfig>,
}

impl FiltersConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let settings = ::config::Config::builder()
            .add_source(::config::File::from(path))
            .build()?;
        Ok(settings.try_deserialize()?)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RuntimeConfig {
    pub node_cache_mode: NodeCacheMode,
    pub node_cache_max_nodes: u64,
    pub all_tags: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            node_cache_mode: NodeCacheMode::Auto,
            // OSM has ~10.3B nodes as of 2025; use generous headroom to skip prepass scan
            node_cache_max_nodes: 16_000_000_000,
            all_tags: false,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NodeCacheMode {
    /// Automatically select based on input file size (default)
    Auto,
    /// Sorted array - memory-efficient for extracts (<5GB)
    Sparse,
    /// Direct ID indexing - best for planet/continent (≥5GB)
    Dense,
    /// In-memory HashMap (no disk usage)
    Memory,
}

impl FromStr for NodeCacheMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "auto" => Ok(NodeCacheMode::Auto),
            "sparse" => Ok(NodeCacheMode::Sparse),
            "dense" | "mmap" => Ok(NodeCacheMode::Dense), // mmap kept for backwards compatibility
            "memory" => Ok(NodeCacheMode::Memory),
            _ => Err(format!("invalid node_cache_mode: {value}")),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TableConfig {
    #[serde(default)]
    pub filter: FilterExpr,
    pub columns: Vec<ColumnConfig>,
    #[serde(default)]
    pub geometry: GeometryConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GeometryConfig {
    #[serde(default)]
    pub way: WaySetting,
    #[serde(default)]
    pub closed_way: ClosedWayMode,
    #[serde(default = "default_true")]
    pub node: bool,
    #[serde(default = "default_true")]
    pub relation: bool,
}

impl Default for GeometryConfig {
    fn default() -> Self {
        Self {
            way: WaySetting::default(),
            closed_way: ClosedWayMode::Polygon,
            node: true,
            relation: true,
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default)]
#[serde(rename_all = "lowercase")]
pub enum ClosedWayMode {
    #[default]
    Polygon,
    Centroid,
    Linestring,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum WayGeometryMode {
    Linestring,
    Polygon,
    Centroid,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(untagged)]
pub enum WaySetting {
    Enabled(WayGeometryMode),
    Disabled(bool),
}

impl Default for WaySetting {
    fn default() -> Self {
        WaySetting::Enabled(WayGeometryMode::Linestring)
    }
}

impl WaySetting {
    pub fn enabled(&self) -> bool {
        match self {
            WaySetting::Enabled(_) => true,
            WaySetting::Disabled(value) => *value,
        }
    }

    pub fn mode(&self) -> WayGeometryMode {
        match self {
            WaySetting::Enabled(mode) => *mode,
            WaySetting::Disabled(_) => WayGeometryMode::Linestring,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum FilterExpr {
    Tag(TagMatch),
    Any { any: Vec<FilterExpr> },
    All { all: Vec<FilterExpr> },
    Not { not: Box<FilterExpr> },
    Simple(HashMap<String, String>),
}

impl Default for FilterExpr {
    fn default() -> Self {
        FilterExpr::Simple(HashMap::new())
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TagMatch {
    pub tag: String,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub values: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ColumnConfig {
    pub name: String,
    pub source: String,
    #[serde(rename = "type")]
    pub col_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // ============================================
    // NodeCacheMode FromStr tests
    // ============================================

    #[test]
    fn node_cache_mode_parses_auto() {
        assert!(matches!(
            NodeCacheMode::from_str("auto"),
            Ok(NodeCacheMode::Auto)
        ));
        assert!(matches!(
            NodeCacheMode::from_str("AUTO"),
            Ok(NodeCacheMode::Auto)
        ));
    }

    #[test]
    fn node_cache_mode_parses_sparse() {
        assert!(matches!(
            NodeCacheMode::from_str("sparse"),
            Ok(NodeCacheMode::Sparse)
        ));
        assert!(matches!(
            NodeCacheMode::from_str("SPARSE"),
            Ok(NodeCacheMode::Sparse)
        ));
    }

    #[test]
    fn node_cache_mode_parses_dense() {
        assert!(matches!(
            NodeCacheMode::from_str("dense"),
            Ok(NodeCacheMode::Dense)
        ));
        assert!(matches!(
            NodeCacheMode::from_str("DENSE"),
            Ok(NodeCacheMode::Dense)
        ));
        // Backwards compatibility: mmap maps to dense
        assert!(matches!(
            NodeCacheMode::from_str("mmap"),
            Ok(NodeCacheMode::Dense)
        ));
    }

    #[test]
    fn node_cache_mode_parses_memory() {
        assert!(matches!(
            NodeCacheMode::from_str("memory"),
            Ok(NodeCacheMode::Memory)
        ));
        assert!(matches!(
            NodeCacheMode::from_str("MEMORY"),
            Ok(NodeCacheMode::Memory)
        ));
    }

    #[test]
    fn node_cache_mode_rejects_invalid() {
        let result = NodeCacheMode::from_str("invalid");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid"));
    }

    // ============================================
    // RuntimeConfig default tests
    // ============================================

    #[test]
    fn runtime_config_defaults() {
        let config = RuntimeConfig::default();
        assert!(matches!(config.node_cache_mode, NodeCacheMode::Auto));
        assert_eq!(config.node_cache_max_nodes, 16_000_000_000);
        assert!(!config.all_tags);
    }

    // ============================================
    // WaySetting tests
    // ============================================

    #[test]
    fn way_setting_enabled_returns_true_for_mode() {
        let setting = WaySetting::Enabled(WayGeometryMode::Linestring);
        assert!(setting.enabled());
    }

    #[test]
    fn way_setting_enabled_returns_value_for_disabled() {
        assert!(!WaySetting::Disabled(false).enabled());
        assert!(WaySetting::Disabled(true).enabled());
    }

    #[test]
    fn way_setting_mode_returns_correct_mode() {
        assert!(matches!(
            WaySetting::Enabled(WayGeometryMode::Polygon).mode(),
            WayGeometryMode::Polygon
        ));
        assert!(matches!(
            WaySetting::Enabled(WayGeometryMode::Centroid).mode(),
            WayGeometryMode::Centroid
        ));
    }

    #[test]
    fn way_setting_mode_returns_linestring_for_disabled() {
        let setting = WaySetting::Disabled(false);
        assert!(matches!(setting.mode(), WayGeometryMode::Linestring));
    }

    #[test]
    fn way_setting_default_is_linestring() {
        let setting = WaySetting::default();
        assert!(setting.enabled());
        assert!(matches!(setting.mode(), WayGeometryMode::Linestring));
    }

    // ============================================
    // GeometryConfig default tests
    // ============================================

    #[test]
    fn geometry_config_defaults() {
        let config = GeometryConfig::default();
        assert!(config.way.enabled());
        assert!(matches!(config.closed_way, ClosedWayMode::Polygon));
        assert!(config.node);
        assert!(config.relation);
    }

    // ============================================
    // FilterExpr default tests
    // ============================================

    #[test]
    fn filter_expr_default_is_empty_simple() {
        let filter = FilterExpr::default();
        if let FilterExpr::Simple(map) = filter {
            assert!(map.is_empty());
        } else {
            panic!("expected FilterExpr::Simple");
        }
    }

    // ============================================
    // FiltersConfig YAML loading tests
    // ============================================

    fn write_temp_yaml(contents: &str) -> NamedTempFile {
        let mut file = NamedTempFile::with_suffix(".yaml").unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn loads_simple_filter_config() {
        let yaml = r#"
tables:
  roads:
    filter:
      tag: "highway"
    columns:
      - name: "name"
        source: "tag:name"
        type: "string"
"#;
        let file = write_temp_yaml(yaml);
        let config = FiltersConfig::load(file.path()).unwrap();

        assert!(config.tables.contains_key("roads"));
        let roads = &config.tables["roads"];
        assert_eq!(roads.columns.len(), 1);
        assert_eq!(roads.columns[0].name, "name");
        assert_eq!(roads.columns[0].source, "tag:name");
        assert_eq!(roads.columns[0].col_type, "string");
    }

    #[test]
    fn loads_config_with_tag_value_filter() {
        let yaml = r#"
tables:
  trees:
    filter:
      tag: "natural"
      value: "tree"
    columns:
      - name: "id"
        source: "meta:id"
        type: "integer"
"#;
        let file = write_temp_yaml(yaml);
        let config = FiltersConfig::load(file.path()).unwrap();

        let trees = &config.tables["trees"];
        if let FilterExpr::Tag(tag_match) = &trees.filter {
            assert_eq!(tag_match.tag, "natural");
            assert_eq!(tag_match.value, Some("tree".to_string()));
        } else {
            panic!("expected FilterExpr::Tag");
        }
    }

    #[test]
    fn loads_config_with_tag_values_filter() {
        let yaml = r#"
tables:
  amenities:
    filter:
      tag: "amenity"
      values:
        - "cafe"
        - "restaurant"
        - "bar"
    columns:
      - name: "type"
        source: "tag:amenity"
        type: "string"
"#;
        let file = write_temp_yaml(yaml);
        let config = FiltersConfig::load(file.path()).unwrap();

        let amenities = &config.tables["amenities"];
        if let FilterExpr::Tag(tag_match) = &amenities.filter {
            assert_eq!(tag_match.tag, "amenity");
            assert_eq!(tag_match.values, vec!["cafe", "restaurant", "bar"]);
        } else {
            panic!("expected FilterExpr::Tag");
        }
    }

    #[test]
    fn loads_config_with_any_filter() {
        let yaml = r#"
tables:
  pois:
    filter:
      any:
        - tag: "amenity"
        - tag: "shop"
    columns:
      - name: "name"
        source: "tag:name"
        type: "string"
"#;
        let file = write_temp_yaml(yaml);
        let config = FiltersConfig::load(file.path()).unwrap();

        let pois = &config.tables["pois"];
        if let FilterExpr::Any { any } = &pois.filter {
            assert_eq!(any.len(), 2);
        } else {
            panic!("expected FilterExpr::Any");
        }
    }

    #[test]
    fn loads_config_with_all_filter() {
        let yaml = r#"
tables:
  named_roads:
    filter:
      all:
        - tag: "highway"
        - tag: "name"
    columns:
      - name: "name"
        source: "tag:name"
        type: "string"
"#;
        let file = write_temp_yaml(yaml);
        let config = FiltersConfig::load(file.path()).unwrap();

        let named_roads = &config.tables["named_roads"];
        if let FilterExpr::All { all } = &named_roads.filter {
            assert_eq!(all.len(), 2);
        } else {
            panic!("expected FilterExpr::All");
        }
    }

    #[test]
    fn loads_config_with_not_filter() {
        let yaml = r#"
tables:
  non_residential:
    filter:
      not:
        tag: "building"
        value: "residential"
    columns:
      - name: "type"
        source: "tag:building"
        type: "string"
"#;
        let file = write_temp_yaml(yaml);
        let config = FiltersConfig::load(file.path()).unwrap();

        let non_residential = &config.tables["non_residential"];
        if let FilterExpr::Not { not } = &non_residential.filter {
            if let FilterExpr::Tag(tag_match) = not.as_ref() {
                assert_eq!(tag_match.tag, "building");
                assert_eq!(tag_match.value, Some("residential".to_string()));
            } else {
                panic!("expected inner FilterExpr::Tag");
            }
        } else {
            panic!("expected FilterExpr::Not");
        }
    }

    #[test]
    fn loads_config_with_geometry_settings() {
        let yaml = r#"
tables:
  roads:
    filter:
      tag: "highway"
    geometry:
      node: false
      way: "linestring"
      closed_way: "linestring"
      relation: false
    columns:
      - name: "name"
        source: "tag:name"
        type: "string"
"#;
        let file = write_temp_yaml(yaml);
        let config = FiltersConfig::load(file.path()).unwrap();

        let roads = &config.tables["roads"];
        assert!(!roads.geometry.node);
        assert!(!roads.geometry.relation);
        assert!(roads.geometry.way.enabled());
        assert!(matches!(
            roads.geometry.closed_way,
            ClosedWayMode::Linestring
        ));
    }

    #[test]
    fn loads_config_with_disabled_way() {
        let yaml = r#"
tables:
  points:
    filter:
      tag: "amenity"
    geometry:
      node: true
      way: false
      relation: false
    columns:
      - name: "type"
        source: "tag:amenity"
        type: "string"
"#;
        let file = write_temp_yaml(yaml);
        let config = FiltersConfig::load(file.path()).unwrap();

        let points = &config.tables["points"];
        assert!(points.geometry.node);
        assert!(!points.geometry.way.enabled());
        assert!(!points.geometry.relation);
    }

    #[test]
    fn loads_config_with_multiple_columns() {
        let yaml = r#"
tables:
  roads:
    filter:
      tag: "highway"
    columns:
      - name: "id"
        source: "meta:id"
        type: "integer"
      - name: "name"
        source: "tag:name"
        type: "string"
      - name: "lanes"
        source: "tag:lanes"
        type: "integer"
      - name: "maxspeed"
        source: "tag:maxspeed"
        type: "float"
"#;
        let file = write_temp_yaml(yaml);
        let config = FiltersConfig::load(file.path()).unwrap();

        let roads = &config.tables["roads"];
        assert_eq!(roads.columns.len(), 4);
        assert_eq!(roads.columns[0].col_type, "integer");
        assert_eq!(roads.columns[1].col_type, "string");
        assert_eq!(roads.columns[2].col_type, "integer");
        assert_eq!(roads.columns[3].col_type, "float");
    }

    #[test]
    fn loads_config_with_multiple_tables() {
        let yaml = r#"
tables:
  roads:
    filter:
      tag: "highway"
    columns:
      - name: "name"
        source: "tag:name"
        type: "string"
  buildings:
    filter:
      tag: "building"
    columns:
      - name: "type"
        source: "tag:building"
        type: "string"
"#;
        let file = write_temp_yaml(yaml);
        let config = FiltersConfig::load(file.path()).unwrap();

        assert!(config.tables.contains_key("roads"));
        assert!(config.tables.contains_key("buildings"));
        assert_eq!(config.tables.len(), 2);
    }

    #[test]
    fn fails_on_invalid_yaml() {
        let yaml = r#"
tables:
  invalid yaml here
    - this is wrong
"#;
        let file = write_temp_yaml(yaml);
        let result = FiltersConfig::load(file.path());
        assert!(result.is_err());
    }

    #[test]
    fn fails_on_missing_required_fields() {
        let yaml = r#"
tables:
  roads:
    filter:
      tag: "highway"
"#;
        let file = write_temp_yaml(yaml);
        let result = FiltersConfig::load(file.path());
        // Should fail because columns is required
        assert!(result.is_err());
    }

    #[test]
    fn loads_config_with_simple_filter() {
        let yaml = r#"
tables:
  roads:
    filter:
      highway: "primary"
    columns:
      - name: "name"
        source: "tag:name"
        type: "string"
"#;
        let file = write_temp_yaml(yaml);
        let config = FiltersConfig::load(file.path()).unwrap();

        let roads = &config.tables["roads"];
        if let FilterExpr::Simple(map) = &roads.filter {
            assert_eq!(map.get("highway"), Some(&"primary".to_string()));
        } else {
            panic!("expected FilterExpr::Simple");
        }
    }

    // ============================================
    // ClosedWayMode default tests
    // ============================================

    #[test]
    fn closed_way_mode_default_is_polygon() {
        assert!(matches!(ClosedWayMode::default(), ClosedWayMode::Polygon));
    }
}
