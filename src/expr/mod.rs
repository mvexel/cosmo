//! Expression evaluation using CEL (Common Expression Language).

mod cel;

pub use cel::{cel_value_to_string, compile_cel, evaluate_cel, CelContext, CelProgram};
