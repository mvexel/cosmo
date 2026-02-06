//! Mapping rules for derived columns.
//!
//! Mappings provide first-match-wins rule evaluation to classify
//! OSM elements into categories.

mod rules;

pub use rules::{evaluate_mapping, Mapping, MappingConfig};
