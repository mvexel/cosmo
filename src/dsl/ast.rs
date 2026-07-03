//! AST types for the filter DSL.

use crate::metadata::MetadataField;
use std::fmt;

/// Root filter expression.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterAst {
    /// Tag existence check: `name` or `!name`
    TagExists { key: String, negated: bool },

    /// Tag value match: `highway=primary` or `highway=primary|secondary`
    TagMatch { key: String, values: Vec<TagValue> },

    /// Numeric comparison: `lanes>=2`, `maxspeed<50`
    NumericCompare {
        key: String,
        op: CompareOp,
        value: f64,
    },

    /// Metadata existence check: `meta:user` or `!meta:user`
    MetadataExists { field: MetadataField, negated: bool },

    /// Metadata string/boolean value match: `meta:type=node|way`
    MetadataMatch {
        field: MetadataField,
        values: Vec<TagValue>,
    },

    /// Typed metadata comparison: `meta:version>=2`, `meta:timestamp>="2024-01-01"`
    MetadataCompare {
        field: MetadataField,
        op: CompareOp,
        value: MetadataCompareValue,
    },

    /// Boolean AND: `expr1 & expr2`
    And(Vec<FilterAst>),

    /// Boolean OR: `expr1 | expr2`
    Or(Vec<FilterAst>),

    /// Boolean NOT: `!expr`
    Not(Box<FilterAst>),

    /// Always true (empty filter)
    True,
}

/// A value to match against a tag.
#[derive(Debug, Clone, PartialEq)]
pub enum TagValue {
    /// Exact string match
    Exact(String),
    /// Wildcard (any value)
    Any,
    /// Glob pattern (e.g., `*_link`)
    Glob(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataCompareValue {
    Integer(i64),
    TimestampMillis(i64),
}

/// Numeric comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq, // =
    Ne, // !=
    Lt, // <
    Le, // <=
    Gt, // >
    Ge, // >=
}

impl fmt::Display for CompareOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompareOp::Eq => write!(f, "="),
            CompareOp::Ne => write!(f, "!="),
            CompareOp::Lt => write!(f, "<"),
            CompareOp::Le => write!(f, "<="),
            CompareOp::Gt => write!(f, ">"),
            CompareOp::Ge => write!(f, ">="),
        }
    }
}

impl FilterAst {
    /// Simplify the AST by flattening nested And/Or.
    pub fn simplify(self) -> Self {
        match self {
            FilterAst::And(exprs) => {
                let mut flat = Vec::new();
                for expr in exprs {
                    let simplified = expr.simplify();
                    match simplified {
                        FilterAst::And(inner) => flat.extend(inner),
                        FilterAst::True => {} // skip
                        other => flat.push(other),
                    }
                }
                match flat.len() {
                    0 => FilterAst::True,
                    1 => flat.pop().unwrap(),
                    _ => FilterAst::And(flat),
                }
            }
            FilterAst::Or(exprs) => {
                let mut flat = Vec::new();
                for expr in exprs {
                    let simplified = expr.simplify();
                    match simplified {
                        FilterAst::Or(inner) => flat.extend(inner),
                        other => flat.push(other),
                    }
                }
                match flat.len() {
                    0 => FilterAst::True,
                    1 => flat.pop().unwrap(),
                    _ => FilterAst::Or(flat),
                }
            }
            FilterAst::Not(inner) => FilterAst::Not(Box::new(inner.simplify())),
            other => other,
        }
    }

    pub fn uses_metadata(&self) -> bool {
        match self {
            Self::MetadataExists { .. }
            | Self::MetadataMatch { .. }
            | Self::MetadataCompare { .. } => true,
            Self::And(exprs) | Self::Or(exprs) => exprs.iter().any(Self::uses_metadata),
            Self::Not(inner) => inner.uses_metadata(),
            _ => false,
        }
    }

    pub fn uses_metadata_field(&self, target: MetadataField) -> bool {
        match self {
            Self::MetadataExists { field, .. }
            | Self::MetadataMatch { field, .. }
            | Self::MetadataCompare { field, .. } => *field == target,
            Self::And(exprs) | Self::Or(exprs) => {
                exprs.iter().any(|expr| expr.uses_metadata_field(target))
            }
            Self::Not(inner) => inner.uses_metadata_field(target),
            _ => false,
        }
    }
}
