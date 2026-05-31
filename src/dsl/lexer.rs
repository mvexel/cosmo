//! Lexer/tokenizer for the filter DSL.

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

/// Tokenize the entire input.
pub fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut lexer = Lexer::new(input);
    let mut tokens = Vec::new();

    while let Some(token) = lexer.next_token()? {
        tokens.push(token);
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

struct Lexer<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn next_token(&mut self) -> Result<Option<Token>, String> {
        self.skip_whitespace();

        let Some(ch) = self.peek_char() else {
            return Ok(None);
        };

        if let Some(token) = self.operator_token(ch) {
            return Ok(Some(token));
        }

        if ch.is_ascii_digit() || self.starts_negative_number() {
            return self.lex_number().map(Some);
        }

        if is_ident_start(ch) {
            return Ok(Some(self.lex_ident()));
        }

        Err(format!(
            "Lexer error at '{}': unexpected character '{}'",
            self.remaining(),
            ch
        ))
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek_char() {
            if ch.is_whitespace() {
                self.pos += ch.len_utf8();
            } else {
                break;
            }
        }
    }

    fn operator_token(&mut self, ch: char) -> Option<Token> {
        let token = match ch {
            '!' if self.remaining().starts_with("!=") => {
                self.pos += 2;
                Token::Ne
            }
            '<' if self.remaining().starts_with("<=") => {
                self.pos += 2;
                Token::Le
            }
            '>' if self.remaining().starts_with(">=") => {
                self.pos += 2;
                Token::Ge
            }
            '=' => {
                self.pos += 1;
                Token::Eq
            }
            '<' => {
                self.pos += 1;
                Token::Lt
            }
            '>' => {
                self.pos += 1;
                Token::Gt
            }
            '&' => {
                self.pos += 1;
                Token::And
            }
            '|' => {
                self.pos += 1;
                Token::Or
            }
            '!' => {
                self.pos += 1;
                Token::Not
            }
            '(' => {
                self.pos += 1;
                Token::LParen
            }
            ')' => {
                self.pos += 1;
                Token::RParen
            }
            _ => return None,
        };

        Some(token)
    }

    fn lex_number(&mut self) -> Result<Token, String> {
        let start = self.pos;

        if self.peek_char() == Some('-') {
            self.pos += 1;
        }

        let mut has_digit = false;
        let mut has_dot = false;
        while let Some(ch) = self.peek_char() {
            if ch.is_ascii_digit() {
                has_digit = true;
                self.pos += 1;
            } else if ch == '.' && !has_dot {
                has_dot = true;
                self.pos += 1;
            } else {
                break;
            }
        }

        if !has_digit {
            return Err(format!(
                "Lexer error at '{}': expected number",
                &self.input[start..]
            ));
        }

        let value = &self.input[start..self.pos];
        value
            .parse::<f64>()
            .map(Token::Number)
            .map_err(|err| format!("Lexer error at '{value}': invalid number: {err}"))
    }

    fn lex_ident(&mut self) -> Token {
        let start = self.pos;

        while let Some(ch) = self.peek_char() {
            if is_ident_continue(ch) {
                self.pos += ch.len_utf8();
            } else {
                break;
            }
        }

        let ident = &self.input[start..self.pos];
        if ident == "*" {
            Token::Star
        } else {
            Token::Ident(ident.to_string())
        }
    }

    fn starts_negative_number(&self) -> bool {
        let mut chars = self.remaining().chars();
        matches!(chars.next(), Some('-')) && matches!(chars.next(), Some(ch) if ch.is_ascii_digit())
    }

    fn peek_char(&self) -> Option<char> {
        self.remaining().chars().next()
    }

    fn remaining(&self) -> &'a str {
        &self.input[self.pos..]
    }
}

fn is_ident_start(ch: char) -> bool {
    ch.is_alphabetic() || ch == '_' || ch == ':' || ch == '*' || ch == '-'
}

fn is_ident_continue(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_' || ch == ':' || ch == '-' || ch == '*'
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
