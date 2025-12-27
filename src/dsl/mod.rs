//! Simple filter DSL for OSM tag matching.
//!
//! Syntax:
//!   tag                     - tag exists
//!   !tag                    - tag doesn't exist
//!   tag=value               - exact match
//!   tag=val1|val2|val3      - match any value
//!   tag=*                   - any value (same as existence)
//!   tag>=n, tag>n, etc.     - numeric comparison
//!   expr1 & expr2           - AND
//!   expr1 | expr2           - OR (note: lower precedence than &)
//!   !expr                   - NOT
//!   (expr)                  - grouping

mod ast;
mod lexer;
mod parser;
mod eval;

pub use ast::*;
pub use parser::parse_filter;
pub use eval::evaluate_filter;
