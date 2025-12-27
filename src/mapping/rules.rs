//! Mapping rule definitions and evaluation.

use crate::dsl::{FilterAst, evaluate_filter, parse_filter};
use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;

/// A mapping definition with ordered rules.
#[derive(Debug, Clone)]
pub struct Mapping {
    pub rules: Vec<CompiledRule>,
    pub default: Option<String>,
}

/// A compiled mapping rule.
#[derive(Debug, Clone)]
pub struct CompiledRule {
    pub filter: FilterAst,
    pub value: String,
}

/// Raw mapping rule from YAML (before compilation).
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct MappingRule {
    /// The filter expression (DSL string)
    #[serde(rename = "match")]
    pub match_expr: String,
    /// The value to return if this rule matches
    pub value: String,
}

/// Raw mapping from YAML (before compilation).
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct MappingConfig {
    pub rules: Vec<MappingRule>,
    #[serde(default)]
    pub default: Option<String>,
}

impl Mapping {
    /// Compile a mapping from config.
    pub fn compile(name: String, config: &MappingConfig) -> Result<Self> {
        let mut rules = Vec::with_capacity(config.rules.len());

        for (i, rule) in config.rules.iter().enumerate() {
            let filter = parse_filter(&rule.match_expr).map_err(|e| {
                anyhow::anyhow!("Error parsing rule {} in mapping '{}': {}", i + 1, name, e)
            })?;

            rules.push(CompiledRule {
                filter,
                value: rule.value.clone(),
            });
        }

        Ok(Mapping {
            rules,
            default: config.default.clone(),
        })
    }
}

/// Evaluate a mapping against tags, returning the first matching value.
pub fn evaluate_mapping(mapping: &Mapping, tags: &HashMap<String, String>) -> Option<String> {
    for rule in &mapping.rules {
        if evaluate_filter(&rule.filter, tags) {
            return Some(rule.value.clone());
        }
    }

    mapping.default.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn make_mapping() -> Mapping {
        let config = MappingConfig {
            rules: vec![
                MappingRule {
                    match_expr: "amenity=restaurant|food_court|diner".into(),
                    value: "restaurant".into(),
                },
                MappingRule {
                    match_expr: "amenity=cafe|coffee_shop".into(),
                    value: "cafe".into(),
                },
                MappingRule {
                    match_expr: "shop=*".into(),
                    value: "retail".into(),
                },
                MappingRule {
                    match_expr: "amenity | shop | leisure | tourism".into(),
                    value: "misc".into(),
                },
            ],
            default: None,
        };

        Mapping::compile("poi_class".into(), &config).unwrap()
    }

    #[test]
    fn test_first_match_wins() {
        let mapping = make_mapping();

        assert_eq!(
            evaluate_mapping(&mapping, &tags(&[("amenity", "restaurant")])),
            Some("restaurant".into())
        );

        assert_eq!(
            evaluate_mapping(&mapping, &tags(&[("amenity", "cafe")])),
            Some("cafe".into())
        );
    }

    #[test]
    fn test_wildcard_match() {
        let mapping = make_mapping();

        assert_eq!(
            evaluate_mapping(&mapping, &tags(&[("shop", "supermarket")])),
            Some("retail".into())
        );
    }

    #[test]
    fn test_fallback() {
        let mapping = make_mapping();

        assert_eq!(
            evaluate_mapping(&mapping, &tags(&[("tourism", "hotel")])),
            Some("misc".into())
        );
    }

    #[test]
    fn test_no_match() {
        let mapping = make_mapping();

        assert_eq!(
            evaluate_mapping(&mapping, &tags(&[("highway", "primary")])),
            None
        );
    }
}
