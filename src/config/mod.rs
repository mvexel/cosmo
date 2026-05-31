use crate::dsl::{parse_filter, FilterAst};
use crate::expr::{compile_cel, CelProgram};
use crate::mapping::{Mapping, MappingConfig};
use anyhow::Context;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::str::FromStr;

#[cfg(test)]
use tempfile::NamedTempFile;

#[derive(Debug, Deserialize, Serialize)]
pub struct FiltersConfig {
    /// Single table configuration (backwards compatible)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<TableConfig>,
    /// Multiple table configurations
    #[serde(default)]
    pub tables: HashMap<String, TableConfig>,
    /// Named mappings for derived columns
    #[serde(default)]
    pub mappings: HashMap<String, MappingConfig>,
}

impl FiltersConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let file = File::open(path)
            .with_context(|| format!("failed to open filter configuration {}", path.display()))?;
        serde_yaml::from_reader(file).with_context(|| {
            format!(
                "failed to parse filter configuration {} as YAML",
                path.display()
            )
        })
    }

    /// Normalize config to always use multi-table structure
    fn normalize_tables(&self) -> anyhow::Result<HashMap<String, TableConfig>> {
        let mut tables = HashMap::new();

        // Add tables from tables: syntax (table name is the key)
        for (table_name, mut table_config) in self.tables.clone() {
            // Set the name field if not already set
            if table_config.name.is_none() {
                table_config.name = Some(table_name.clone());
            }
            tables.insert(table_name, table_config);
        }

        // If single table: syntax provided, add it
        if let Some(ref single_table) = self.table {
            let table_name = single_table
                .name
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Single table must have a name field"))?;

            if tables.contains_key(table_name) {
                anyhow::bail!(
                    "Duplicate table name '{}' in both 'table:' and 'tables:' syntax",
                    table_name
                );
            }
            tables.insert(table_name.clone(), single_table.clone());
        }

        if tables.is_empty() {
            anyhow::bail!("Config must define at least one table (use 'table:' or 'tables:')");
        }

        Ok(tables)
    }

    /// Compile the config, parsing all DSL strings and CEL expressions.
    pub fn compile(&self) -> anyhow::Result<CompiledConfig> {
        let mut mappings = HashMap::new();

        // Compile mappings first (they may be referenced by columns)
        for (name, config) in &self.mappings {
            let mapping = Mapping::compile(name.clone(), config)?;
            mappings.insert(name.clone(), mapping);
        }

        // Normalize to multi-table structure
        let raw_tables = self.normalize_tables()?;

        // Compile all tables
        let mut compiled_tables = HashMap::new();
        for (table_name, table_config) in raw_tables {
            let filter = match &table_config.filter {
                FilterInput::Dsl(s) => parse_filter(s).map_err(|e| {
                    anyhow::anyhow!("Filter error in table '{}': {}", table_name, e)
                })?,
                FilterInput::EmptyMap(map) if map.is_empty() => FilterAst::True,
                FilterInput::EmptyMap(_) => {
                    anyhow::bail!(
                        "Filter error in table '{}': filter must be a string DSL expression or an empty map",
                        table_name
                    );
                }
            };

            let mut columns = Vec::new();
            for col in &table_config.columns {
                let source = parse_column_source(&col.source, &mappings)?;
                columns.push(CompiledColumn {
                    name: col.name.clone(),
                    source,
                    col_type: col.col_type,
                });
            }

            let compiled_table = CompiledTable {
                name: table_name.clone(),
                filter,
                columns,
                geometry: table_config.geometry.clone(),
            };

            compiled_tables.insert(table_name, compiled_table);
        }

        Ok(CompiledConfig {
            tables: compiled_tables,
            mappings,
        })
    }
}

pub const DENSE_THRESHOLD_BYTES: u64 = 5 * 1024 * 1024 * 1024; // 5 GB
pub const DEFAULT_MAX_NODES: u64 = 16_000_000_000;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RuntimeConfig {
    pub node_cache_mode: NodeCacheMode,
    pub node_cache_max_nodes: u64,
    pub all_tags: bool,
    /// Optional bounding box filter: [min_lon, min_lat, max_lon, max_lat]
    pub bbox: Option<[f64; 4]>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            node_cache_mode: NodeCacheMode::Auto,
            // OSM has ~10.3B nodes as of 2025; use generous headroom to skip prepass scan
            node_cache_max_nodes: DEFAULT_MAX_NODES,
            all_tags: false,
            bbox: None,
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

impl NodeCacheMode {
    pub fn label(&self) -> &'static str {
        match self {
            NodeCacheMode::Auto => "auto",
            NodeCacheMode::Sparse => "sparse",
            NodeCacheMode::Dense => "dense",
            NodeCacheMode::Memory => "memory",
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TableConfig {
    /// Table name (used in output file naming and logging)
    /// Optional when using tables: syntax (key becomes the name)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default)]
    pub filter: FilterInput,
    pub columns: Vec<ColumnConfig>,
    #[serde(default)]
    pub geometry: GeometryConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum FilterInput {
    /// DSL string: "highway=primary & lanes>=2"
    Dsl(String),
    /// Documented match-all shorthand: `filter: {}`.
    EmptyMap(HashMap<String, serde_yaml::Value>),
}

impl Default for FilterInput {
    fn default() -> Self {
        FilterInput::EmptyMap(HashMap::new())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct GeometryConfig {
    #[serde(default)]
    pub way: WaySetting,
    #[serde(default)]
    pub closed_way: ClosedWaySetting,
    #[serde(default = "default_true")]
    pub node: bool,
    #[serde(default)]
    pub relation: bool,
}

impl GeometryConfig {
    /// True when this table emits any way geometry, i.e. open ways (`way`) or
    /// closed ways (`closed_way`) are enabled. Used to gate way processing and
    /// node indexing independently of the open-way setting.
    pub fn processes_ways(&self) -> bool {
        self.way.enabled() || self.closed_way.enabled()
    }
}

impl Default for GeometryConfig {
    fn default() -> Self {
        Self {
            way: WaySetting::default(),
            closed_way: ClosedWaySetting::default(),
            node: true,
            relation: false,
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

#[derive(Debug, Clone, Copy)]
pub enum ClosedWaySetting {
    Enabled(ClosedWayMode),
    Disabled,
}

impl Default for ClosedWaySetting {
    fn default() -> Self {
        ClosedWaySetting::Enabled(ClosedWayMode::Polygon)
    }
}

impl ClosedWaySetting {
    pub fn enabled(&self) -> bool {
        matches!(self, ClosedWaySetting::Enabled(_))
    }

    pub fn mode(&self) -> ClosedWayMode {
        match self {
            ClosedWaySetting::Enabled(mode) => *mode,
            ClosedWaySetting::Disabled => ClosedWayMode::Polygon,
        }
    }
}

impl<'de> Deserialize<'de> for ClosedWaySetting {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl de::Visitor<'_> for Visitor {
            type Value = ClosedWaySetting;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("one of polygon, centroid, linestring, or false")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value {
                    Err(E::custom("true is not valid for closed_way; use polygon, centroid, linestring, or false"))
                } else {
                    Ok(ClosedWaySetting::Disabled)
                }
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value {
                    "polygon" => Ok(ClosedWaySetting::Enabled(ClosedWayMode::Polygon)),
                    "centroid" => Ok(ClosedWaySetting::Enabled(ClosedWayMode::Centroid)),
                    "linestring" => Ok(ClosedWaySetting::Enabled(ClosedWayMode::Linestring)),
                    _ => Err(E::unknown_variant(
                        value,
                        &["polygon", "centroid", "linestring"],
                    )),
                }
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

impl Serialize for ClosedWaySetting {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ClosedWaySetting::Enabled(mode) => mode.serialize(serializer),
            ClosedWaySetting::Disabled => serializer.serialize_bool(false),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum WayGeometryMode {
    Linestring,
    Centroid,
}

#[derive(Debug, Clone, Copy)]
pub enum WaySetting {
    Enabled(WayGeometryMode),
    Disabled,
}

impl Default for WaySetting {
    fn default() -> Self {
        WaySetting::Enabled(WayGeometryMode::Linestring)
    }
}

impl WaySetting {
    pub fn enabled(&self) -> bool {
        matches!(self, WaySetting::Enabled(_))
    }

    pub fn mode(&self) -> WayGeometryMode {
        match self {
            WaySetting::Enabled(mode) => *mode,
            WaySetting::Disabled => WayGeometryMode::Linestring,
        }
    }
}

impl<'de> Deserialize<'de> for WaySetting {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl de::Visitor<'_> for Visitor {
            type Value = WaySetting;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("one of linestring, centroid, or false")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value {
                    Err(E::custom(
                        "true is not valid for way; use linestring, centroid, or false",
                    ))
                } else {
                    Ok(WaySetting::Disabled)
                }
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value {
                    "linestring" => Ok(WaySetting::Enabled(WayGeometryMode::Linestring)),
                    "centroid" => Ok(WaySetting::Enabled(WayGeometryMode::Centroid)),
                    _ => Err(E::unknown_variant(value, &["linestring", "centroid"])),
                }
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

impl Serialize for WaySetting {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            WaySetting::Enabled(mode) => mode.serialize(serializer),
            WaySetting::Disabled => serializer.serialize_bool(false),
        }
    }
}

// ----------------------------------------------------------------------------
// Compiled Config Types
// ----------------------------------------------------------------------------

pub struct CompiledConfig {
    pub tables: HashMap<String, CompiledTable>,
    pub mappings: HashMap<String, Mapping>,
}

pub struct CompiledTable {
    #[allow(dead_code)]
    pub name: String,
    pub filter: FilterAst,
    pub columns: Vec<CompiledColumn>,
    pub geometry: GeometryConfig,
}

pub struct CompiledColumn {
    pub name: String,
    pub source: ColumnSource,
    pub col_type: ColumnType,
}

#[derive(Debug, Clone)]
pub enum ColumnSource {
    Tag(String),
    Meta(String),
    AllTags,
    AllMeta,
    Refs,
    Mapping(String),
    Cel(CelProgram),
}

// ----------------------------------------------------------------------------
// Compilation Helpers
// ----------------------------------------------------------------------------

/// Parse a column source string.
fn parse_column_source(
    source: &str,
    mappings: &HashMap<String, Mapping>,
) -> anyhow::Result<ColumnSource> {
    if source == "tags" {
        return Ok(ColumnSource::AllTags);
    }
    if source == "meta" {
        return Ok(ColumnSource::AllMeta);
    }
    if source == "refs" {
        return Ok(ColumnSource::Refs);
    }
    if let Some(tag) = source.strip_prefix("tag:") {
        return Ok(ColumnSource::Tag(tag.to_string()));
    }
    if let Some(field) = source.strip_prefix("meta:") {
        return Ok(ColumnSource::Meta(field.to_string()));
    }
    if let Some(name) = source.strip_prefix("mapping:") {
        if !mappings.contains_key(name) {
            return Err(anyhow::anyhow!("Unknown mapping: {}", name));
        }
        return Ok(ColumnSource::Mapping(name.to_string()));
    }
    if let Some(expr) = source.strip_prefix("expr:") {
        let program = compile_cel(expr)?;
        return Ok(ColumnSource::Cel(program));
    }

    // Default: treat as tag
    Ok(ColumnSource::Tag(source.to_string()))
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    #[default]
    String,
    Integer,
    Float,
    Json,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ColumnConfig {
    pub name: String,
    pub source: String,
    #[serde(rename = "type", default)]
    pub col_type: ColumnType,
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
        assert!(config.bbox.is_none());
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
    fn way_setting_disabled_returns_false() {
        assert!(!WaySetting::Disabled.enabled());
    }

    #[test]
    fn way_setting_mode_returns_correct_mode() {
        assert!(matches!(
            WaySetting::Enabled(WayGeometryMode::Linestring).mode(),
            WayGeometryMode::Linestring
        ));
        assert!(matches!(
            WaySetting::Enabled(WayGeometryMode::Centroid).mode(),
            WayGeometryMode::Centroid
        ));
    }

    #[test]
    fn way_setting_mode_returns_linestring_for_disabled() {
        let setting = WaySetting::Disabled;
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
        assert!(config.closed_way.enabled());
        assert!(matches!(config.closed_way.mode(), ClosedWayMode::Polygon));
        assert!(config.node);
        assert!(!config.relation);
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
table:
  name: roads
  filter: "highway"
  columns:
    - name: "name"
      source: "tag:name"
      type: "string"
"#;
        let file = write_temp_yaml(yaml);
        let config = FiltersConfig::load(file.path()).unwrap();

        let roads = config.table.as_ref().unwrap();
        assert_eq!(roads.name.as_deref(), Some("roads"));
        assert_eq!(roads.columns.len(), 1);
        assert_eq!(roads.columns[0].name, "name");
        assert_eq!(roads.columns[0].source, "tag:name");
        assert_eq!(roads.columns[0].col_type, ColumnType::String);
    }

    #[test]
    fn loads_config_with_geometry_settings() {
        let yaml = r#"
table:
  name: roads
  filter: "highway"
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

        let roads = config.table.as_ref().unwrap();
        assert!(!roads.geometry.node);
        assert!(!roads.geometry.relation);
        assert!(roads.geometry.way.enabled());
        assert!(matches!(
            roads.geometry.closed_way.mode(),
            ClosedWayMode::Linestring
        ));
    }

    #[test]
    fn loads_config_with_disabled_way() {
        let yaml = r#"
table:
  name: points
  filter: "amenity"
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

        let points = config.table.as_ref().unwrap();
        assert!(points.geometry.node);
        assert!(!points.geometry.way.enabled());
        assert!(!points.geometry.relation);
    }

    #[test]
    fn loads_config_with_multiple_columns() {
        let yaml = r#"
table:
  name: roads
  filter: "highway"
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

        let roads = config.table.as_ref().unwrap();
        assert_eq!(roads.columns.len(), 4);
        assert_eq!(roads.columns[0].col_type, ColumnType::Integer);
        assert_eq!(roads.columns[1].col_type, ColumnType::String);
        assert_eq!(roads.columns[2].col_type, ColumnType::Integer);
        assert_eq!(roads.columns[3].col_type, ColumnType::Float);
    }

    #[test]
    fn fails_on_invalid_yaml() {
        let yaml = r#"
table:
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
table:
  name: roads
  filter: "highway"
"#;
        let file = write_temp_yaml(yaml);
        let result = FiltersConfig::load(file.path());
        // Should fail because columns is required
        assert!(result.is_err());
    }

    #[test]
    fn empty_filter_map_compiles_as_match_all() {
        let yaml = r#"
table:
  name: everything
  filter: {}
  columns: []
"#;
        let file = write_temp_yaml(yaml);
        let config = FiltersConfig::load(file.path()).unwrap();

        let compiled = config.compile().unwrap();
        let table = compiled.tables.get("everything").unwrap();
        assert!(matches!(table.filter, FilterAst::True));
    }

    #[test]
    fn structured_filter_yaml_is_rejected() {
        let yaml = r#"
table:
  name: roads
  filter:
    tag: "highway"
  columns: []
"#;
        let file = write_temp_yaml(yaml);
        let config = FiltersConfig::load(file.path()).unwrap();

        let err = match config.compile() {
            Ok(_) => panic!("expected structured filter to be rejected"),
            Err(err) => err.to_string(),
        };
        assert!(
            err.contains("filter must be a string DSL expression or an empty map"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn way_polygon_is_rejected_for_open_ways() {
        let yaml = r#"
table:
  name: invalid
  filter: "highway"
  geometry:
    way: polygon
  columns: []
"#;
        let file = write_temp_yaml(yaml);
        let err = format!("{:#}", FiltersConfig::load(file.path()).unwrap_err());
        assert!(
            err.contains("unknown variant `polygon`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn geometry_true_values_are_rejected() {
        let yaml = r#"
table:
  name: invalid
  filter: "highway"
  geometry:
    way: true
  columns: []
"#;
        let file = write_temp_yaml(yaml);
        let err = format!("{:#}", FiltersConfig::load(file.path()).unwrap_err());
        assert!(
            err.contains("true is not valid for way"),
            "unexpected error: {err}"
        );
    }

    // ClosedWayMode default tests
    // ============================================

    #[test]
    fn closed_way_mode_default_is_polygon() {
        assert!(matches!(ClosedWayMode::default(), ClosedWayMode::Polygon));
    }

    #[test]
    fn closed_way_can_be_disabled() {
        let yaml = r#"
table:
  name: open_only
  filter: "highway"
  geometry:
    way: linestring
    closed_way: false
  columns: []
"#;
        let file = write_temp_yaml(yaml);
        let config = FiltersConfig::load(file.path()).unwrap();
        let table = config.table.as_ref().unwrap();
        assert!(!table.geometry.closed_way.enabled());
        assert!(table.geometry.processes_ways());
    }

    #[test]
    fn closed_way_polygon_with_disabled_open_way_still_processes() {
        let yaml = r#"
table:
  name: polygons
  filter: "landuse"
  geometry:
    way: false
    closed_way: polygon
  columns: []
"#;
        let file = write_temp_yaml(yaml);
        let config = FiltersConfig::load(file.path()).unwrap();
        let table = config.table.as_ref().unwrap();
        assert!(!table.geometry.way.enabled());
        assert!(table.geometry.closed_way.enabled());
        assert!(table.geometry.processes_ways());
    }
}
