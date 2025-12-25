use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

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
            node_cache_mode: NodeCacheMode::Mmap,
            // OSM has ~10.3B nodes as of 2024; use generous headroom to skip prepass scan
            node_cache_max_nodes: 11_000_000_000,
            all_tags: false,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum NodeCacheMode {
    Mmap,
    Memory,
}

impl FromStr for NodeCacheMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "mmap" => Ok(NodeCacheMode::Mmap),
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

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum ClosedWayMode {
    Polygon,
    Centroid,
    Linestring,
}

impl Default for ClosedWayMode {
    fn default() -> Self {
        ClosedWayMode::Polygon
    }
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
