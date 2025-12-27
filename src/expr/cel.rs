//! CEL expression compilation and evaluation.

use anyhow::Result;
use cel::{Context, Program, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// A compiled CEL program ready for evaluation.
#[derive(Clone)]
pub struct CelProgram {
    program: Arc<Program>,
    source: String,
}

impl std::fmt::Debug for CelProgram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CelProgram")
            .field("source", &self.source)
            .finish()
    }
}

/// Context for CEL evaluation (tags and metadata).
pub struct CelContext<'a> {
    pub tags: &'a HashMap<String, String>,
    pub meta: &'a HashMap<String, String>,
}

/// Compile a CEL expression string into a program.
pub fn compile_cel(source: &str) -> Result<CelProgram> {
    let program =
        Program::compile(source).map_err(|e| anyhow::anyhow!("CEL compile error: {}", e))?;

    Ok(CelProgram {
        program: Arc::new(program),
        source: source.to_string(),
    })
}

/// Evaluate a compiled CEL program with the given context.
pub fn evaluate_cel(program: &CelProgram, ctx: &CelContext) -> Result<Value> {
    let mut cel_ctx = Context::default();

    // Add tags as a map variable
    cel_ctx
        .add_variable("tags", ctx.tags.clone())
        .map_err(|e| anyhow::anyhow!("CEL context error: {}", e))?;

    // Add meta as a map variable
    cel_ctx
        .add_variable("meta", ctx.meta.clone())
        .map_err(|e| anyhow::anyhow!("CEL context error: {}", e))?;

    // CEL natively supports 'key in tags' and 'tags[key]'
    // or tags.get('key', 'default') if macros are enabled (not yet in this version)
    // For now, we use native map access.

    program
        .program
        .execute(&cel_ctx)
        .map_err(|e| anyhow::anyhow!("CEL execution error: {}", e))
}

/// Convert a CEL Value to a string for output.
pub fn cel_value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.to_string()),
        Value::Int(i) => Some(i.to_string()),
        Value::UInt(u) => Some(u.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null => None,
        _ => Some(format!("{:?}", value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ctx<'a>(
        tags: &'a HashMap<String, String>,
        meta: &'a HashMap<String, String>,
    ) -> CelContext<'a> {
        CelContext { tags, meta }
    }

    #[test]
    fn test_simple_tag_access() {
        let program = compile_cel("tags.name").unwrap();

        let tags: HashMap<String, String> = [("name".into(), "Foo".into())].into();
        let meta = HashMap::new();
        let ctx = make_ctx(&tags, &meta);

        let result = evaluate_cel(&program, &ctx).unwrap();
        assert_eq!(cel_value_to_string(&result), Some("Foo".into()));
    }

    #[test]
    fn test_tag_comparison() {
        let program = compile_cel("tags.highway == 'primary'").unwrap();

        let tags: HashMap<String, String> = [("highway".into(), "primary".into())].into();
        let meta = HashMap::new();
        let ctx = make_ctx(&tags, &meta);

        let result = evaluate_cel(&program, &ctx).unwrap();
        assert!(matches!(result, Value::Bool(true)));
    }

    #[test]
    fn test_has_tag_native() {
        let program = compile_cel("'name' in tags").unwrap();

        let tags: HashMap<String, String> = [("name".into(), "Foo".into())].into();
        let meta = HashMap::new();
        let ctx = make_ctx(&tags, &meta);

        let result = evaluate_cel(&program, &ctx).unwrap();
        assert!(matches!(result, Value::Bool(true)));
    }

    #[test]
    fn test_get_tag_alternative() {
        let program = compile_cel("has(tags.name) ? tags.name : 'default'").unwrap();

        let tags: HashMap<String, String> = [("name".into(), "Foo".into())].into();
        let meta = HashMap::new();
        let ctx = make_ctx(&tags, &meta);

        let result = evaluate_cel(&program, &ctx).unwrap();
        assert_eq!(cel_value_to_string(&result), Some("Foo".into()));
    }
}
