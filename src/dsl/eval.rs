//! Evaluator for the filter DSL AST.

use super::ast::{CompareOp, FilterAst, MetadataCompareValue, TagValue};
use crate::metadata::{
    metadata_integer_value, metadata_value_exists, metadata_value_string, MetadataField,
    MetadataFields,
};
use std::collections::HashMap;

pub struct FilterContext<'a> {
    pub tags: &'a HashMap<String, String>,
    pub metadata: Option<&'a MetadataFields>,
}

/// Evaluate a filter AST against a set of tags.
pub fn evaluate_filter(ast: &FilterAst, tags: &HashMap<String, String>) -> bool {
    evaluate_filter_with_context(
        ast,
        &FilterContext {
            tags,
            metadata: None,
        },
    )
}

pub fn evaluate_filter_with_context(ast: &FilterAst, ctx: &FilterContext<'_>) -> bool {
    match ast {
        FilterAst::True => true,

        FilterAst::TagExists { key, negated } => {
            let exists = ctx.tags.contains_key(key);
            if *negated {
                !exists
            } else {
                exists
            }
        }

        FilterAst::TagMatch { key, values } => match ctx.tags.get(key) {
            None => false,
            Some(actual) => values.iter().any(|v| match_value(v, actual)),
        },

        FilterAst::NumericCompare { key, op, value } => match ctx.tags.get(key) {
            None => false,
            Some(actual) => match parse_numeric(actual) {
                None => false,
                Some(actual_num) => compare(*op, actual_num, *value),
            },
        },

        FilterAst::MetadataExists { field, negated } => {
            let exists = metadata_value_exists(*field, ctx.metadata);
            if *negated {
                !exists
            } else {
                exists
            }
        }

        FilterAst::MetadataMatch { field, values } => {
            match metadata_value_string(*field, ctx.metadata) {
                None => false,
                Some(actual) => values.iter().any(|v| match_value(v, &actual)),
            }
        }

        FilterAst::MetadataCompare { field, op, value } => match value {
            MetadataCompareValue::Integer(expected) => metadata_integer_value(*field, ctx.metadata)
                .is_some_and(|actual| compare_i64(*op, actual, *expected)),
            MetadataCompareValue::TimestampMillis(expected) => {
                metadata_timestamp_millis(*field, ctx.metadata)
                    .is_some_and(|actual| compare_i64(*op, actual, *expected))
            }
        },

        FilterAst::And(exprs) => exprs.iter().all(|e| evaluate_filter_with_context(e, ctx)),

        FilterAst::Or(exprs) => exprs.iter().any(|e| evaluate_filter_with_context(e, ctx)),

        FilterAst::Not(inner) => !evaluate_filter_with_context(inner, ctx),
    }
}

fn metadata_timestamp_millis(
    field: MetadataField,
    metadata: Option<&MetadataFields>,
) -> Option<i64> {
    if field != MetadataField::Timestamp {
        return None;
    }

    metadata.and_then(|m| m.timestamp_millis)
}

/// Match a TagValue against an actual value.
fn match_value(pattern: &TagValue, actual: &str) -> bool {
    match pattern {
        TagValue::Any => true,
        TagValue::Exact(expected) => actual == expected,
        TagValue::Glob(pattern) => glob_match(pattern, actual),
    }
}

/// Simple glob matching (supports * at start, end, or both).
fn glob_match(pattern: &str, actual: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let starts_star = pattern.starts_with('*');
    let ends_star = pattern.ends_with('*');

    match (starts_star, ends_star) {
        (true, true) => {
            // *foo* - contains
            let inner = &pattern[1..pattern.len() - 1];
            actual.contains(inner)
        }
        (true, false) => {
            // *foo - ends with
            let suffix = &pattern[1..];
            actual.ends_with(suffix)
        }
        (false, true) => {
            // foo* - starts with
            let prefix = &pattern[..pattern.len() - 1];
            actual.starts_with(prefix)
        }
        (false, false) => {
            // No wildcards - exact match
            actual == pattern
        }
    }
}

/// Parse a numeric value from a string.
/// Handles common OSM patterns like "50 mph", "30", "5.5".
fn parse_numeric(s: &str) -> Option<f64> {
    // Try direct parse first
    if let Ok(n) = s.parse::<f64>() {
        return Some(n);
    }

    // Try to extract leading number (e.g., "50 mph" -> 50)
    let numeric_part: String = s
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
        .collect();

    numeric_part.parse::<f64>().ok()
}

/// Apply a comparison operator.
fn compare(op: CompareOp, left: f64, right: f64) -> bool {
    match op {
        CompareOp::Eq => (left - right).abs() < f64::EPSILON,
        CompareOp::Ne => (left - right).abs() >= f64::EPSILON,
        CompareOp::Lt => left < right,
        CompareOp::Le => left <= right,
        CompareOp::Gt => left > right,
        CompareOp::Ge => left >= right,
    }
}

fn compare_i64(op: CompareOp, left: i64, right: i64) -> bool {
    match op {
        CompareOp::Eq => left == right,
        CompareOp::Ne => left != right,
        CompareOp::Lt => left < right,
        CompareOp::Le => left <= right,
        CompareOp::Gt => left > right,
        CompareOp::Ge => left >= right,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{parse_filter, FilterContext};
    use crate::metadata::{ElementType, MetadataFields};

    fn tags(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn metadata() -> MetadataFields {
        MetadataFields {
            id: 42,
            element_type: ElementType::Node,
            visible: Some(true),
            version: Some(3),
            changeset: Some(99),
            timestamp_millis: Some(1_704_067_200_000),
            uid: Some(7),
            user: Some("Jane Doe".to_string()),
        }
    }

    #[test]
    fn test_existence() {
        let ast = parse_filter("name").unwrap();
        assert!(evaluate_filter(&ast, &tags(&[("name", "Foo")])));
        assert!(!evaluate_filter(&ast, &tags(&[("highway", "primary")])));
    }

    #[test]
    fn test_negated_existence() {
        let ast = parse_filter("!name").unwrap();
        assert!(!evaluate_filter(&ast, &tags(&[("name", "Foo")])));
        assert!(evaluate_filter(&ast, &tags(&[("highway", "primary")])));
    }

    #[test]
    fn test_exact_match() {
        let ast = parse_filter("highway=primary").unwrap();
        assert!(evaluate_filter(&ast, &tags(&[("highway", "primary")])));
        assert!(!evaluate_filter(&ast, &tags(&[("highway", "secondary")])));
    }

    #[test]
    fn test_multiple_values() {
        let ast = parse_filter("highway=primary|secondary").unwrap();
        assert!(evaluate_filter(&ast, &tags(&[("highway", "primary")])));
        assert!(evaluate_filter(&ast, &tags(&[("highway", "secondary")])));
        assert!(!evaluate_filter(&ast, &tags(&[("highway", "tertiary")])));
    }

    #[test]
    fn test_numeric_comparison() {
        let ast = parse_filter("lanes>=2").unwrap();
        assert!(evaluate_filter(&ast, &tags(&[("lanes", "2")])));
        assert!(evaluate_filter(&ast, &tags(&[("lanes", "4")])));
        assert!(!evaluate_filter(&ast, &tags(&[("lanes", "1")])));
    }

    #[test]
    fn test_numeric_with_units() {
        let ast = parse_filter("maxspeed>=50").unwrap();
        assert!(evaluate_filter(&ast, &tags(&[("maxspeed", "50 mph")])));
        assert!(evaluate_filter(&ast, &tags(&[("maxspeed", "60")])));
        assert!(!evaluate_filter(&ast, &tags(&[("maxspeed", "30")])));
    }

    #[test]
    fn test_and() {
        let ast = parse_filter("highway=primary & lanes>=2").unwrap();
        assert!(evaluate_filter(
            &ast,
            &tags(&[("highway", "primary"), ("lanes", "3")])
        ));
        assert!(!evaluate_filter(
            &ast,
            &tags(&[("highway", "primary"), ("lanes", "1")])
        ));
        assert!(!evaluate_filter(
            &ast,
            &tags(&[("highway", "secondary"), ("lanes", "3")])
        ));
    }

    #[test]
    fn test_or() {
        let ast = parse_filter("highway=primary | highway=secondary").unwrap();
        assert!(evaluate_filter(&ast, &tags(&[("highway", "primary")])));
        assert!(evaluate_filter(&ast, &tags(&[("highway", "secondary")])));
        assert!(!evaluate_filter(&ast, &tags(&[("highway", "tertiary")])));
    }

    #[test]
    fn test_glob() {
        let ast = parse_filter("highway=*_link").unwrap();
        assert!(evaluate_filter(
            &ast,
            &tags(&[("highway", "motorway_link")])
        ));
        assert!(evaluate_filter(&ast, &tags(&[("highway", "trunk_link")])));
        assert!(!evaluate_filter(&ast, &tags(&[("highway", "motorway")])));
    }

    #[test]
    fn test_metadata_user_match() {
        let ast = parse_filter(r#"meta:user="Jane Doe""#).unwrap();
        let tag_map = tags(&[]);
        let meta = metadata();
        assert!(evaluate_filter_with_context(
            &ast,
            &FilterContext {
                tags: &tag_map,
                metadata: Some(&meta),
            }
        ));
    }

    #[test]
    fn test_metadata_type_visible_and_version_match() {
        let ast = parse_filter("meta:type=node & meta:visible=true & meta:version>=2").unwrap();
        let tag_map = tags(&[]);
        let meta = metadata();
        assert!(evaluate_filter_with_context(
            &ast,
            &FilterContext {
                tags: &tag_map,
                metadata: Some(&meta),
            }
        ));
    }

    #[test]
    fn test_metadata_timestamp_match() {
        let ast = parse_filter(r#"meta:timestamp>="2024-01-01""#).unwrap();
        let tag_map = tags(&[]);
        let meta = metadata();
        assert!(evaluate_filter_with_context(
            &ast,
            &FilterContext {
                tags: &tag_map,
                metadata: Some(&meta),
            }
        ));
    }

    #[test]
    fn test_metadata_missing_and_negated_string_match() {
        let missing = parse_filter("meta:user").unwrap();
        let not_user = parse_filter(r#"!(meta:user="Jane Doe")"#).unwrap();
        let tag_map = tags(&[]);
        let meta = metadata();
        assert!(!evaluate_filter_with_context(
            &missing,
            &FilterContext {
                tags: &tag_map,
                metadata: None,
            }
        ));
        assert!(!evaluate_filter_with_context(
            &not_user,
            &FilterContext {
                tags: &tag_map,
                metadata: Some(&meta),
            }
        ));
    }
}
