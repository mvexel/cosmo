//! Evaluator for the filter DSL AST.

use super::ast::{CompareOp, FilterAst, TagValue};
use std::collections::HashMap;

/// Evaluate a filter AST against a set of tags.
pub fn evaluate_filter(ast: &FilterAst, tags: &HashMap<String, String>) -> bool {
    match ast {
        FilterAst::True => true,

        FilterAst::TagExists { key, negated } => {
            let exists = tags.contains_key(key);
            if *negated { !exists } else { exists }
        }

        FilterAst::TagMatch { key, values } => match tags.get(key) {
            None => false,
            Some(actual) => values.iter().any(|v| match_value(v, actual)),
        },

        FilterAst::NumericCompare { key, op, value } => {
            match tags.get(key) {
                None => false,
                Some(actual) => {
                    // Try to parse the tag value as a number
                    match parse_numeric(actual) {
                        None => false,
                        Some(actual_num) => compare(*op, actual_num, *value),
                    }
                }
            }
        }

        FilterAst::And(exprs) => exprs.iter().all(|e| evaluate_filter(e, tags)),

        FilterAst::Or(exprs) => exprs.iter().any(|e| evaluate_filter(e, tags)),

        FilterAst::Not(inner) => !evaluate_filter(inner, tags),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::parse_filter;

    fn tags(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
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
}
