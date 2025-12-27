//! Lexer/tokenizer for the filter DSL.

use winnow::ascii::space0;
use winnow::combinator::alt;
use winnow::prelude::*;
use winnow::token::take_while;

/// Token types for the DSL.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Identifiers and values
    Ident(String), // tag key or value
    Number(f64),   // numeric literal

    // Operators
    Eq, // =
    Ne, // !=
    Lt, // <
    Le, // <=
    Gt, // >
    Ge, // >=

    // Boolean operators
    And, // &
    Or,  // |
    Not, // !

    // Punctuation
    LParen, // (
    RParen, // )
    Star,   // *

    // End of input
    Eof,
}

// Manually define PResult for resilience against winnow version changes
type PResult<T> = Result<T, winnow::error::ErrMode<winnow::error::ContextError>>;

/// Lex an identifier (tag key or value).
/// Allowed: alphanumeric, underscore, colon, dash, and wildcard *
fn lex_ident(input: &mut &str) -> PResult<Token> {
    let first = take_while(1.., |c: char| {
        c.is_alphabetic() || c == '_' || c == ':' || c == '*' || c == '-'
    })
    .parse_next(input)?;
    let rest = take_while(0.., |c: char| {
        c.is_alphanumeric() || c == '_' || c == ':' || c == '-' || c == '*'
    })
    .parse_next(input)?;

    let s = format!("{}{}", first, rest);
    if s == "*" {
        Ok(Token::Star)
    } else {
        Ok(Token::Ident(s))
    }
}

/// Lex a number (integer or float).
fn lex_number(input: &mut &str) -> PResult<Token> {
    let neg = winnow::combinator::opt('-').parse_next(input)?;
    let num_str = take_while(1.., |c: char| c.is_ascii_digit() || c == '.').parse_next(input)?;
    let full = if neg.is_some() {
        format!("-{}", num_str)
    } else {
        num_str.to_string()
    };
    let n: f64 = full
        .parse()
        .map_err(|_| winnow::error::ErrMode::Backtrack(winnow::error::ContextError::default()))?;
    Ok(Token::Number(n))
}

/// Lex a single token.
fn lex_token(input: &mut &str) -> PResult<Token> {
    space0.parse_next(input)?;

    if input.is_empty() {
        return Ok(Token::Eof);
    }

    alt((
        // Multi-char operators first
        "!=".value(Token::Ne),
        "<=".value(Token::Le),
        ">=".value(Token::Ge),
        // Single-char operators
        "=".value(Token::Eq),
        "<".value(Token::Lt),
        ">".value(Token::Gt),
        "&".value(Token::And),
        "|".value(Token::Or),
        "!".value(Token::Not),
        "(".value(Token::LParen),
        ")".value(Token::RParen),
        // Number (before ident to catch negative numbers)
        lex_number,
        // Identifier (includes star if part of string)
        lex_ident,
    ))
    .parse_next(input)
}

/// Tokenize the entire input.
pub fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut remaining = input;
    let mut tokens = Vec::new();

    loop {
        match lex_token(&mut remaining) {
            Ok(Token::Eof) => break,
            Ok(tok) => tokens.push(tok),
            Err(e) => return Err(format!("Lexer error at '{}': {:?}", remaining, e)),
        }
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_tokens() {
        let tokens = tokenize("highway=primary").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Ident("highway".into()),
                Token::Eq,
                Token::Ident("primary".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_numeric_comparison() {
        let tokens = tokenize("lanes>=2").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Ident("lanes".into()),
                Token::Ge,
                Token::Number(2.0),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_complex_expression() {
        let tokens = tokenize("highway=primary & lanes>=2").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Ident("highway".into()),
                Token::Eq,
                Token::Ident("primary".into()),
                Token::And,
                Token::Ident("lanes".into()),
                Token::Ge,
                Token::Number(2.0),
                Token::Eof,
            ]
        );
    }
}
