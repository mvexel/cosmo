//! Expression evaluation using CEL (Common Expression Language).

mod cel;

pub use cel::{CelContext, CelProgram, cel_value_to_string, compile_cel, evaluate_cel};
