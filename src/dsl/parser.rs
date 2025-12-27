//! Parser for the filter DSL.
//!
//! Grammar (in rough EBNF):
//!
//! filter     = or_expr
//! or_expr    = and_expr ("|" and_expr)*
//! and_expr   = unary_expr ("&" unary_expr)*
//! unary_expr = "!" unary_expr | primary
//! primary    = "(" filter ")" | tag_expr
//! tag_expr   = IDENT (compare_op value_list)?
//! compare_op = "=" | "!=" | "<" | "<=" | ">" | ">="
//! value_list = value ("|" value)*
//! value      = IDENT | NUMBER | "*"

use super::ast::{CompareOp, FilterAst, TagValue};
use super::lexer::{Token, tokenize};

/// Parser state.
struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> Token {
        let tok = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        self.pos += 1;
        tok
    }

    fn expect(&mut self, expected: Token) -> Result<(), String> {
        let tok = self.advance();
        if tok == expected {
            Ok(())
        } else {
            Err(format!("Expected {:?}, got {:?}", expected, tok))
        }
    }

    /// Parse the top-level filter expression.
    fn parse_filter(&mut self) -> Result<FilterAst, String> {
        self.parse_or_expr()
    }

    /// Parse OR expression: and_expr ("|" and_expr)*
    fn parse_or_expr(&mut self) -> Result<FilterAst, String> {
        let mut left = self.parse_and_expr()?;

        while matches!(self.peek(), Token::Or) {
            // Check if this is boolean OR or value alternative
            // Boolean OR is at expression level, value | is within a tag match
            // We're at expression level here, so it's boolean OR
            self.advance(); // consume |
            let right = self.parse_and_expr()?;
            left = FilterAst::Or(vec![left, right]);
        }

        Ok(left.simplify())
    }

    /// Parse AND expression: unary_expr ("&" unary_expr)*
    fn parse_and_expr(&mut self) -> Result<FilterAst, String> {
        let mut left = self.parse_unary_expr()?;

        while matches!(self.peek(), Token::And) {
            self.advance(); // consume &
            let right = self.parse_unary_expr()?;
            left = FilterAst::And(vec![left, right]);
        }

        Ok(left.simplify())
    }

    /// Parse unary expression: "!" unary_expr | primary
    fn parse_unary_expr(&mut self) -> Result<FilterAst, String> {
        if matches!(self.peek(), Token::Not) {
            self.advance(); // consume !

            // Check if next is an identifier (negated existence) or expression
            if let Token::Ident(key) = self.peek().clone() {
                // Could be !tag (negated existence) or !(expr)
                // Peek ahead to see if it's followed by an operator
                let next_pos = self.pos + 1;
                let next_tok = self.tokens.get(next_pos);

                match next_tok {
                    Some(Token::Eq) | Some(Token::Ne) | Some(Token::Lt) | Some(Token::Le)
                    | Some(Token::Gt) | Some(Token::Ge) => {
                        // It's a tag expression, wrap in NOT
                        let inner = self.parse_primary()?;
                        return Ok(FilterAst::Not(Box::new(inner)));
                    }
                    Some(Token::And) | Some(Token::Or) | Some(Token::RParen) | Some(Token::Eof)
                    | None => {
                        // Simple negated existence: !tag
                        self.advance(); // consume ident
                        return Ok(FilterAst::TagExists { key, negated: true });
                    }
                    _ => {
                        // Wrap whatever comes next
                        let inner = self.parse_primary()?;
                        return Ok(FilterAst::Not(Box::new(inner)));
                    }
                }
            }

            let inner = self.parse_unary_expr()?;
            Ok(FilterAst::Not(Box::new(inner)))
        } else {
            self.parse_primary()
        }
    }

    /// Parse primary expression: "(" filter ")" | tag_expr
    fn parse_primary(&mut self) -> Result<FilterAst, String> {
        match self.peek().clone() {
            Token::LParen => {
                self.advance(); // consume (
                let inner = self.parse_filter()?;
                self.expect(Token::RParen)?;
                Ok(inner)
            }
            Token::Ident(_) => self.parse_tag_expr(),
            Token::Eof => Ok(FilterAst::True),
            other => Err(format!("Unexpected token: {:?}", other)),
        }
    }

    /// Parse tag expression: IDENT (compare_op value_list)?
    fn parse_tag_expr(&mut self) -> Result<FilterAst, String> {
        let key = match self.advance() {
            Token::Ident(k) => k,
            other => return Err(format!("Expected identifier, got {:?}", other)),
        };

        // Check for comparison operator
        let op = match self.peek() {
            Token::Eq => Some(CompareOp::Eq),
            Token::Ne => Some(CompareOp::Ne),
            Token::Lt => Some(CompareOp::Lt),
            Token::Le => Some(CompareOp::Le),
            Token::Gt => Some(CompareOp::Gt),
            Token::Ge => Some(CompareOp::Ge),
            _ => None,
        };

        match op {
            None => {
                // Simple existence check
                Ok(FilterAst::TagExists {
                    key,
                    negated: false,
                })
            }
            Some(CompareOp::Eq) => {
                self.advance(); // consume =
                let values = self.parse_value_list()?;
                Ok(FilterAst::TagMatch { key, values })
            }
            Some(op) => {
                self.advance(); // consume operator
                // For non-equality comparisons, expect a number
                match self.advance() {
                    Token::Number(n) => Ok(FilterAst::NumericCompare { key, op, value: n }),
                    other => Err(format!("Expected number after {:?}, got {:?}", op, other)),
                }
            }
        }
    }

    /// Parse value list: value ("|" value)*
    fn parse_value_list(&mut self) -> Result<Vec<TagValue>, String> {
        let mut values = vec![self.parse_value()?];

        while matches!(self.peek(), Token::Or) {
            // Ambiguity: "highway=primary | lanes=2"
            // The "|" could be ORing the whole expression or just the value.
            // Lookahead: if the next token is an identifier and the token after THAT is an operator,
            // then it's a new expression, so stop parsing the value list.
            if let Some(Token::Ident(_)) = self.tokens.get(self.pos + 1) {
                let next_next = self.tokens.get(self.pos + 2);
                if matches!(
                    next_next,
                    Some(Token::Eq)
                        | Some(Token::Ne)
                        | Some(Token::Lt)
                        | Some(Token::Le)
                        | Some(Token::Gt)
                        | Some(Token::Ge)
                ) {
                    break; // Stop at boolean OR
                }
            }

            self.advance(); // consume |
            values.push(self.parse_value()?);
        }

        Ok(values)
    }

    /// Parse a single value: IDENT | NUMBER | "*"
    fn parse_value(&mut self) -> Result<TagValue, String> {
        match self.advance() {
            Token::Star => Ok(TagValue::Any),
            Token::Ident(s) => {
                // Check for glob pattern
                if s.contains('*') {
                    Ok(TagValue::Glob(s))
                } else {
                    Ok(TagValue::Exact(s))
                }
            }
            Token::Number(n) => Ok(TagValue::Exact(n.to_string())),
            other => Err(format!("Expected value, got {:?}", other)),
        }
    }
}

/// Parse a filter DSL string into an AST.
pub fn parse_filter(input: &str) -> Result<FilterAst, String> {
    let input = input.trim();
    if input.is_empty() {
        return Ok(FilterAst::True);
    }

    let tokens = tokenize(input)?;
    let mut parser = Parser::new(tokens);
    let ast = parser.parse_filter()?;

    // Ensure we consumed all tokens
    if !matches!(parser.peek(), Token::Eof) {
        return Err(format!(
            "Unexpected token after expression: {:?}",
            parser.peek()
        ));
    }

    Ok(ast)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_existence() {
        let ast = parse_filter("name").unwrap();
        assert_eq!(
            ast,
            FilterAst::TagExists {
                key: "name".into(),
                negated: false
            }
        );
    }

    #[test]
    fn test_negated_existence() {
        let ast = parse_filter("!name").unwrap();
        assert_eq!(
            ast,
            FilterAst::TagExists {
                key: "name".into(),
                negated: true
            }
        );
    }

    #[test]
    fn test_exact_match() {
        let ast = parse_filter("highway=primary").unwrap();
        assert_eq!(
            ast,
            FilterAst::TagMatch {
                key: "highway".into(),
                values: vec![TagValue::Exact("primary".into())],
            }
        );
    }

    #[test]
    fn test_multiple_values() {
        let ast = parse_filter("highway=primary|secondary|tertiary").unwrap();
        assert_eq!(
            ast,
            FilterAst::TagMatch {
                key: "highway".into(),
                values: vec![
                    TagValue::Exact("primary".into()),
                    TagValue::Exact("secondary".into()),
                    TagValue::Exact("tertiary".into()),
                ],
            }
        );
    }

    #[test]
    fn test_wildcard() {
        let ast = parse_filter("shop=*").unwrap();
        assert_eq!(
            ast,
            FilterAst::TagMatch {
                key: "shop".into(),
                values: vec![TagValue::Any],
            }
        );
    }

    #[test]
    fn test_numeric_comparison() {
        let ast = parse_filter("lanes>=2").unwrap();
        assert_eq!(
            ast,
            FilterAst::NumericCompare {
                key: "lanes".into(),
                op: CompareOp::Ge,
                value: 2.0,
            }
        );
    }

    #[test]
    fn test_and_expression() {
        let ast = parse_filter("highway=primary & lanes>=2").unwrap();
        assert!(matches!(ast, FilterAst::And(_)));
    }

    #[test]
    fn test_or_expression() {
        let ast = parse_filter("highway=primary | highway=secondary").unwrap();
        assert!(matches!(ast, FilterAst::Or(_)));
    }

    #[test]
    fn test_complex_expression() {
        let ast = parse_filter("(highway=primary | highway=secondary) & lanes>=2").unwrap();
        assert!(matches!(ast, FilterAst::And(_)));
    }

    #[test]
    fn test_empty_filter() {
        let ast = parse_filter("").unwrap();
        assert_eq!(ast, FilterAst::True);
    }

    #[test]
    fn test_mixed_value_alternatives_and_boolean_or() {
        let ast = parse_filter("highway=primary|secondary | lanes>=2").unwrap();
        // Should parse as: (highway IN [primary, secondary]) OR (lanes >= 2)
        assert!(matches!(ast, FilterAst::Or(_)));
        if let FilterAst::Or(exprs) = ast {
            assert_eq!(exprs.len(), 2);
            assert!(matches!(exprs[0], FilterAst::TagMatch { .. }));
            assert!(matches!(exprs[1], FilterAst::NumericCompare { .. }));

            // Verify the TagMatch has two values
            if let FilterAst::TagMatch { values, .. } = &exprs[0] {
                assert_eq!(values.len(), 2);
            }
        }
    }
}
