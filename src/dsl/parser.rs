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

use super::ast::{CompareOp, FilterAst, MetadataCompareValue, TagValue};
use super::lexer::{tokenize, Token};
use crate::metadata::{MetadataField, MetadataFieldKind};
use time::format_description::well_known::Rfc3339;
use time::{Date, Month, OffsetDateTime, Time};

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
            Err(format!("Expected {expected:?}, got {tok:?}"))
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
                        // Simple negated existence: !tag or !meta:field
                        self.advance(); // consume ident
                        if let Some(field_name) = key.strip_prefix("meta:") {
                            let field = field_name.parse::<MetadataField>()?;
                            return Ok(FilterAst::MetadataExists {
                                field,
                                negated: true,
                            });
                        }
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
            other => Err(format!("Unexpected token: {other:?}")),
        }
    }

    /// Parse tag or metadata expression: IDENT (compare_op value_list)?
    fn parse_tag_expr(&mut self) -> Result<FilterAst, String> {
        let key = match self.advance() {
            Token::Ident(k) => k,
            other => return Err(format!("Expected identifier, got {other:?}")),
        };

        let op = self.peek_compare_op();

        if let Some(field_name) = key.strip_prefix("meta:") {
            let field = field_name.parse::<MetadataField>()?;
            return self.parse_metadata_expr(field, op);
        }

        match op {
            None => Ok(FilterAst::TagExists {
                key,
                negated: false,
            }),
            Some(CompareOp::Eq) => {
                self.advance(); // consume =
                let values = self.parse_value_list()?;
                Ok(FilterAst::TagMatch { key, values })
            }
            Some(op) => {
                self.advance(); // consume operator
                match self.advance() {
                    Token::Number(n) => Ok(FilterAst::NumericCompare { key, op, value: n }),
                    other => Err(format!("Expected number after {op:?}, got {other:?}")),
                }
            }
        }
    }

    fn peek_compare_op(&self) -> Option<CompareOp> {
        match self.peek() {
            Token::Eq => Some(CompareOp::Eq),
            Token::Ne => Some(CompareOp::Ne),
            Token::Lt => Some(CompareOp::Lt),
            Token::Le => Some(CompareOp::Le),
            Token::Gt => Some(CompareOp::Gt),
            Token::Ge => Some(CompareOp::Ge),
            _ => None,
        }
    }

    fn parse_metadata_expr(
        &mut self,
        field: MetadataField,
        op: Option<CompareOp>,
    ) -> Result<FilterAst, String> {
        match op {
            None => Ok(FilterAst::MetadataExists {
                field,
                negated: false,
            }),
            Some(CompareOp::Eq) if field == MetadataField::Timestamp => {
                self.advance(); // consume =
                match self.advance() {
                    Token::Star => Ok(FilterAst::MetadataExists {
                        field,
                        negated: false,
                    }),
                    Token::String(value) => Ok(FilterAst::MetadataCompare {
                        field,
                        op: CompareOp::Eq,
                        value: MetadataCompareValue::TimestampMillis(parse_timestamp_millis(
                            &value,
                        )?),
                    }),
                    other => Err(format!(
                        "Expected quoted timestamp after meta:timestamp=, got {other:?}"
                    )),
                }
            }
            Some(CompareOp::Eq) => {
                self.advance(); // consume =
                let values = self.parse_value_list()?;
                Ok(FilterAst::MetadataMatch { field, values })
            }
            Some(op) => {
                self.advance(); // consume operator
                match field.kind() {
                    MetadataFieldKind::Integer => match self.advance() {
                        Token::Number(n) => Ok(FilterAst::MetadataCompare {
                            field,
                            op,
                            value: MetadataCompareValue::Integer(parse_integer_literal(n)?),
                        }),
                        other => Err(format!(
                            "Expected integer after metadata comparison operator {op}, got {other:?}"
                        )),
                    },
                    MetadataFieldKind::Timestamp => match self.advance() {
                        Token::String(value) => Ok(FilterAst::MetadataCompare {
                            field,
                            op,
                            value: MetadataCompareValue::TimestampMillis(parse_timestamp_millis(
                                &value,
                            )?),
                        }),
                        other => Err(format!(
                            "Expected quoted timestamp after meta:timestamp{op}, got {other:?}"
                        )),
                    },
                    MetadataFieldKind::String | MetadataFieldKind::Boolean => Err(format!(
                        "Operator {op} is not supported for this metadata field; use !(meta:<field>=\"value\") around an equality expression"
                    )),
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

    /// Parse a single value: IDENT | NUMBER | STRING | "*"
    fn parse_value(&mut self) -> Result<TagValue, String> {
        match self.advance() {
            Token::Star => Ok(TagValue::Any),
            Token::Ident(s) => {
                if s.contains('*') {
                    Ok(TagValue::Glob(s))
                } else {
                    Ok(TagValue::Exact(s))
                }
            }
            Token::Number(n) => Ok(TagValue::Exact(n.to_string())),
            Token::String(s) => Ok(TagValue::Exact(s)),
            other => Err(format!("Expected value, got {other:?}")),
        }
    }
}

fn parse_integer_literal(value: f64) -> Result<i64, String> {
    if !value.is_finite() || value.fract() != 0.0 {
        return Err(format!(
            "Expected whole integer metadata value, got {value}"
        ));
    }
    if value < i64::MIN as f64 || value > i64::MAX as f64 {
        return Err(format!("Integer metadata value out of range: {value}"));
    }
    Ok(value as i64)
}

fn parse_timestamp_millis(value: &str) -> Result<i64, String> {
    if let Ok(dt) = OffsetDateTime::parse(value, &Rfc3339) {
        return timestamp_to_millis(dt);
    }

    parse_date_only_timestamp(value)
}

fn parse_date_only_timestamp(value: &str) -> Result<i64, String> {
    let mut parts = value.split('-');
    let year = parts
        .next()
        .ok_or_else(|| timestamp_error(value))?
        .parse::<i32>()
        .map_err(|_| timestamp_error(value))?;
    let month = parts
        .next()
        .ok_or_else(|| timestamp_error(value))?
        .parse::<u8>()
        .map_err(|_| timestamp_error(value))?;
    let day = parts
        .next()
        .ok_or_else(|| timestamp_error(value))?
        .parse::<u8>()
        .map_err(|_| timestamp_error(value))?;
    if parts.next().is_some() {
        return Err(timestamp_error(value));
    }

    let month = Month::try_from(month).map_err(|_| timestamp_error(value))?;
    let date = Date::from_calendar_date(year, month, day).map_err(|_| timestamp_error(value))?;
    timestamp_to_millis(date.with_time(Time::MIDNIGHT).assume_utc())
}

fn timestamp_to_millis(dt: OffsetDateTime) -> Result<i64, String> {
    i64::try_from(dt.unix_timestamp_nanos() / 1_000_000)
        .map_err(|_| "Timestamp is outside supported range".to_string())
}

fn timestamp_error(value: &str) -> String {
    format!("Invalid timestamp literal '{value}'; expected quoted RFC3339 date-time or YYYY-MM-DD")
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
    use crate::metadata::MetadataField;

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

    #[test]
    fn test_metadata_user_match() {
        let ast = parse_filter(r#"meta:user="Jane Doe""#).unwrap();
        assert_eq!(
            ast,
            FilterAst::MetadataMatch {
                field: MetadataField::User,
                values: vec![TagValue::Exact("Jane Doe".into())],
            }
        );
    }

    #[test]
    fn test_metadata_version_compare() {
        let ast = parse_filter("meta:version>=2").unwrap();
        assert_eq!(
            ast,
            FilterAst::MetadataCompare {
                field: MetadataField::Version,
                op: CompareOp::Ge,
                value: MetadataCompareValue::Integer(2),
            }
        );
    }

    #[test]
    fn test_metadata_timestamp_date_compare() {
        let ast = parse_filter(r#"meta:timestamp>="2024-01-01""#).unwrap();
        assert_eq!(
            ast,
            FilterAst::MetadataCompare {
                field: MetadataField::Timestamp,
                op: CompareOp::Ge,
                value: MetadataCompareValue::TimestampMillis(1_704_067_200_000),
            }
        );
    }

    #[test]
    fn test_metadata_type_alternatives() {
        let ast = parse_filter("meta:type=node|way").unwrap();
        assert_eq!(
            ast,
            FilterAst::MetadataMatch {
                field: MetadataField::Type,
                values: vec![
                    TagValue::Exact("node".into()),
                    TagValue::Exact("way".into())
                ],
            }
        );
    }

    #[test]
    fn test_metadata_unknown_field_rejected() {
        let err = parse_filter("meta:unknown=1").unwrap_err();
        assert!(err.contains("supported fields"));
    }
}
