use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

use crate::config::NodeCacheMode;
use crate::sinks::OutputFormat;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Enable verbose output
    #[arg(short, long, env = "COSMO_VERBOSE", global = true)]
    pub verbose: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Convert OSM PBF data to other formats
    Convert(ConvertArgs),
    /// Validate filter configuration
    Validate(ValidateArgs),
}

#[derive(Args)]
pub struct ValidateArgs {
    /// Filter configuration file (YAML)
    #[arg(env = "COSMO_FILTERS")]
    pub filters: PathBuf,
}

#[derive(Args)]
pub struct ConvertArgs {
    /// Input PBF file
    #[arg(short, long, env = "COSMO_INPUT")]
    pub input: PathBuf,

    /// Output file or directory (.geojson, .geojsonl, .parquet, .csv)
    #[arg(short, long, env = "COSMO_OUTPUT")]
    pub output: PathBuf,

    /// Filter configuration file (YAML)
    #[arg(short, long, env = "COSMO_FILTERS")]
    pub filters: PathBuf,

    /// Force specific node cache mode
    #[arg(long, value_enum, env = "COSMO_NODE_CACHE_MODE")]
    pub node_cache_mode: Option<NodeCacheMode>,

    /// Dense node cache file path
    #[arg(long, env = "COSMO_NODE_CACHE")]
    pub node_cache: Option<PathBuf>,

    /// Maximum nodes for dense cache (default: 16B)
    #[arg(long, env = "COSMO_NODE_CACHE_MAX_NODES")]
    pub node_cache_max_nodes: Option<u64>,

    /// Number of parallel jobs (default: all cores)
    #[arg(short, long, env = "COSMO_JOBS")]
    pub jobs: Option<usize>,

    /// Output format (auto-detected if omitted)
    #[arg(long, value_enum, env = "COSMO_FORMAT")]
    pub format: Option<OutputFormat>,

    /// Include all tags in a 'tags' JSON column
    #[arg(long, env = "COSMO_ALL_TAGS")]
    pub all_tags: bool,

    /// Bounding box filter: min_lon,min_lat,max_lon,max_lat
    #[arg(long, value_parser = parse_bbox, allow_hyphen_values = true, env = "COSMO_BBOX")]
    pub bbox: Option<[f64; 4]>,
}

/// Parse a bounding box from a comma-separated string: min_lon,min_lat,max_lon,max_lat
fn parse_bbox(s: &str) -> Result<[f64; 4], String> {
    let parts: Vec<&str> = s.split(',').collect();
    if parts.len() != 4 {
        return Err(format!(
            "bbox must have 4 values (min_lon,min_lat,max_lon,max_lat), got {}",
            parts.len()
        ));
    }
    let coords: Result<Vec<f64>, _> = parts.iter().map(|p| p.trim().parse::<f64>()).collect();
    let coords = coords.map_err(|e| format!("invalid bbox coordinate: {e}"))?;

    let [min_lon, min_lat, max_lon, max_lat] = [coords[0], coords[1], coords[2], coords[3]];

    if min_lon > max_lon {
        return Err(format!(
            "bbox min_lon ({min_lon}) must be <= max_lon ({max_lon})"
        ));
    }
    if min_lat > max_lat {
        return Err(format!(
            "bbox min_lat ({min_lat}) must be <= max_lat ({max_lat})"
        ));
    }
    if min_lat < -90.0 || max_lat > 90.0 {
        return Err("bbox latitude must be between -90 and 90".to_string());
    }
    if min_lon < -180.0 || max_lon > 180.0 {
        return Err("bbox longitude must be between -180 and 180".to_string());
    }

    Ok([min_lon, min_lat, max_lon, max_lat])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bbox_valid_utah() {
        let result = parse_bbox("-114.0556,36.9963,-109.0394,42.00323");
        assert!(result.is_ok());
        let bbox = result.unwrap();
        assert!((bbox[0] - -114.0556).abs() < 1e-6);
        assert!((bbox[1] - 36.9963).abs() < 1e-6);
        assert!((bbox[2] - -109.0394).abs() < 1e-6);
        assert!((bbox[3] - 42.00323).abs() < 1e-6);
    }

    #[test]
    fn parse_bbox_with_spaces() {
        let result = parse_bbox("-114.05, 36.99, -109.03, 42.00");
        assert!(result.is_ok());
    }

    #[test]
    fn parse_bbox_wrong_count() {
        let result = parse_bbox("-114.05,36.99,-109.03");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("4 values"));
    }

    #[test]
    fn parse_bbox_invalid_number() {
        let result = parse_bbox("-114.05,foo,-109.03,42.00");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid"));
    }

    #[test]
    fn parse_bbox_min_greater_than_max_lon() {
        let result = parse_bbox("-109.0,36.99,-114.0,42.00");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("min_lon"));
    }

    #[test]
    fn parse_bbox_min_greater_than_max_lat() {
        let result = parse_bbox("-114.0,42.00,-109.0,36.99");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("min_lat"));
    }

    #[test]
    fn parse_bbox_lat_out_of_range() {
        let result = parse_bbox("-114.0,-91.0,-109.0,42.0");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("latitude"));
    }

    #[test]
    fn parse_bbox_lon_out_of_range() {
        let result = parse_bbox("-181.0,36.99,-109.0,42.0");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("longitude"));
    }
}
