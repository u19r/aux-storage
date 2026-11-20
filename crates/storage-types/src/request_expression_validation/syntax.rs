//! One lexer and one grammar per expression kind. Grammar failures carry the
//! AWS `Syntax error; token: ..., near: ...` text. Runs before every other
//! expression rule; builds no AST.

use super::{ExpressionKind, validation_message};

#[derive(Clone, Copy, PartialEq, Eq)]
enum TokenKind {
    Ident,
    Name,
    Value,
    Number,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Dot,
    Plus,
    Minus,
    Invalid,
    Eof,
}

#[derive(Clone, Copy)]
struct Token {
    kind: TokenKind,
    start: usize,
    end: usize,
}

fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// On-demand tokens of `source`; after the last real token it yields `Eof`
/// forever. `Eof` starts directly after the last real token and ends at the
/// end of `source`, so trailing whitespace is part of the `near` text of an
/// `<EOF>` error (AWS probe P4/P5).
#[derive(Clone, Copy)]
struct Lexer<'a> {
    source: &'a str,
    at: usize,
    last_end: usize,
}

impl<'a> Lexer<'a> {
    fn new(source: &'a str) -> Self {
        Self {
            source,
            at: 0,
            last_end: 0,
        }
    }

    fn next_token(&mut self) -> Token {
        let source = self.source;
        let bytes = source.as_bytes();
        let mut at = self.at;
        while at < bytes.len() && bytes[at].is_ascii_whitespace() {
            at += 1;
        }
        if at >= bytes.len() {
            self.at = bytes.len();
            return Token {
                kind: TokenKind::Eof,
                start: self.last_end,
                end: bytes.len(),
            };
        }
        let byte = bytes[at];
        let start = at;
        let kind = match byte {
            b'#' | b':' => {
                let mut end = at + 1;
                while end < bytes.len() && is_word_byte(bytes[end]) {
                    end += 1;
                }
                if end == at + 1 {
                    at += 1;
                    TokenKind::Invalid
                } else {
                    at = end;
                    if byte == b'#' {
                        TokenKind::Name
                    } else {
                        TokenKind::Value
                    }
                }
            }
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                at += 1;
                while at < bytes.len() && is_word_byte(bytes[at]) {
                    at += 1;
                }
                TokenKind::Ident
            }
            b'0'..=b'9' => {
                while at < bytes.len() && bytes[at].is_ascii_digit() {
                    at += 1;
                }
                TokenKind::Number
            }
            b'<' => {
                at += 1;
                match bytes.get(at) {
                    Some(b'>') => {
                        at += 1;
                        TokenKind::Ne
                    }
                    Some(b'=') => {
                        at += 1;
                        TokenKind::Le
                    }
                    _ => TokenKind::Lt,
                }
            }
            b'>' => {
                at += 1;
                if bytes.get(at) == Some(&b'=') {
                    at += 1;
                    TokenKind::Ge
                } else {
                    TokenKind::Gt
                }
            }
            _ => {
                at += 1;
                match byte {
                    b'=' => TokenKind::Eq,
                    b'(' => TokenKind::LParen,
                    b')' => TokenKind::RParen,
                    b'[' => TokenKind::LBracket,
                    b']' => TokenKind::RBracket,
                    b',' => TokenKind::Comma,
                    b'.' => TokenKind::Dot,
                    b'+' => TokenKind::Plus,
                    b'-' => TokenKind::Minus,
                    _ => {
                        // One whole UTF-8 character.
                        while at < bytes.len() && !source.is_char_boundary(at) {
                            at += 1;
                        }
                        TokenKind::Invalid
                    }
                }
            }
        };
        self.at = at;
        self.last_end = at;
        Token {
            kind,
            start,
            end: at,
        }
    }
}

/// The offending token with what the `near` text needs: the previous token's
/// start and the following token's end. Compared by token position.
#[derive(Clone, Copy)]
struct SyntaxError {
    position: usize,
    token: Token,
    previous_start: Option<usize>,
    next_end: usize,
}

type SyntaxResult = Result<(), SyntaxError>;

/// Current token, one token of lookahead and the previous token's start.
/// `Copy` so that backtracking is a plain assignment.
#[derive(Clone, Copy)]
struct Grammar<'a> {
    source: &'a str,
    lexer: Lexer<'a>,
    current: Token,
    next: Token,
    previous_start: Option<usize>,
    position: usize,
}

impl<'a> Grammar<'a> {
    fn new(source: &'a str) -> Self {
        let mut lexer = Lexer::new(source);
        let current = lexer.next_token();
        let next = if current.kind == TokenKind::Eof {
            current
        } else {
            lexer.next_token()
        };
        Self {
            source,
            lexer,
            current,
            next,
            previous_start: None,
            position: 0,
        }
    }

    fn advance(&mut self) {
        self.previous_start = Some(self.current.start);
        self.position += 1;
        self.current = self.next;
        if self.current.kind != TokenKind::Eof {
            self.next = self.lexer.next_token();
        }
    }

    fn error(&self) -> SyntaxError {
        SyntaxError {
            position: self.position,
            token: self.current,
            previous_start: self.previous_start,
            next_end: self.next.end,
        }
    }

    fn kind(&self) -> TokenKind {
        self.current.kind
    }

    fn keyword(&self, word: &str) -> bool {
        self.kind() == TokenKind::Ident
            && self.source.as_bytes()[self.current.start..self.current.end]
                .eq_ignore_ascii_case(word.as_bytes())
    }

    fn eat(&mut self, kind: TokenKind) -> SyntaxResult {
        if self.kind() == kind {
            self.advance();
            Ok(())
        } else {
            Err(self.error())
        }
    }

    fn eat_keyword(&mut self, word: &str) -> SyntaxResult {
        if self.keyword(word) {
            self.advance();
            Ok(())
        } else {
            Err(self.error())
        }
    }

    fn end(&mut self) -> SyntaxResult {
        self.eat(TokenKind::Eof)
    }

    /// `(Ident | Name) ( '.' (Ident | Name) | '[' Number ']' )*`
    fn path(&mut self) -> SyntaxResult {
        if !matches!(self.kind(), TokenKind::Ident | TokenKind::Name) {
            return Err(self.error());
        }
        self.advance();
        loop {
            match self.kind() {
                TokenKind::Dot => {
                    self.advance();
                    if !matches!(self.kind(), TokenKind::Ident | TokenKind::Name) {
                        return Err(self.error());
                    }
                    self.advance();
                }
                TokenKind::LBracket => {
                    self.advance();
                    self.eat(TokenKind::Number)?;
                    self.eat(TokenKind::RBracket)?;
                }
                _ => return Ok(()),
            }
        }
    }

    /// `Ident '(' operand (',' operand)* ')'`; the caller saw `Ident '('`.
    fn call(&mut self, operand: fn(&mut Self) -> SyntaxResult) -> SyntaxResult {
        self.advance();
        self.advance();
        operand(self)?;
        while self.kind() == TokenKind::Comma {
            self.advance();
            operand(self)?;
        }
        self.eat(TokenKind::RParen)
    }

    fn at_call(&self) -> bool {
        self.kind() == TokenKind::Ident && self.next.kind == TokenKind::LParen
    }

    // ---- Condition, Filter, KeyCondition ----

    fn condition(&mut self) -> SyntaxResult {
        self.or_condition()?;
        self.end()
    }

    fn or_condition(&mut self) -> SyntaxResult {
        self.and_condition()?;
        while self.keyword("OR") {
            self.advance();
            self.and_condition()?;
        }
        Ok(())
    }

    fn and_condition(&mut self) -> SyntaxResult {
        self.not_condition()?;
        while self.keyword("AND") {
            self.advance();
            self.not_condition()?;
        }
        Ok(())
    }

    fn not_condition(&mut self) -> SyntaxResult {
        if self.keyword("NOT") {
            self.advance();
            return self.not_condition();
        }
        self.primary_condition()
    }

    /// `'(' condition ')'` or a comparison whose left operand may itself be
    /// parenthesized (`((#a)) = :v` is valid syntax). Both are tried; the
    /// failure that reached the later token is reported (first one on a tie).
    fn primary_condition(&mut self) -> SyntaxResult {
        if self.kind() == TokenKind::LParen {
            let start = *self;
            self.advance();
            let grouped = self
                .or_condition()
                .and_then(|()| self.eat(TokenKind::RParen));
            let Err(grouped_at) = grouped else {
                // `(size(a)) = :v`: the group was a parenthesized operand.
                if self.at_comparison_tail() {
                    let after_group = *self;
                    *self = start;
                    if self.comparison().is_err() {
                        *self = after_group;
                    }
                }
                return Ok(());
            };
            *self = start;
            let compared = self.comparison();
            let Err(compared_at) = compared else {
                return Ok(());
            };
            return Err(if compared_at.position > grouped_at.position {
                compared_at
            } else {
                grouped_at
            });
        }
        if self.at_call() {
            self.call(Self::condition_operand)?;
            if self.at_comparison_tail() {
                return self.comparison_tail();
            }
            return Ok(());
        }
        self.comparison()
    }

    fn condition_operand(&mut self) -> SyntaxResult {
        match self.kind() {
            TokenKind::Value => {
                self.advance();
                Ok(())
            }
            TokenKind::LParen => {
                self.advance();
                self.condition_operand()?;
                self.eat(TokenKind::RParen)
            }
            TokenKind::Ident if self.at_call() => self.call(Self::condition_operand),
            _ => self.path(),
        }
    }

    fn at_comparison_tail(&self) -> bool {
        matches!(
            self.kind(),
            TokenKind::Eq
                | TokenKind::Ne
                | TokenKind::Lt
                | TokenKind::Le
                | TokenKind::Gt
                | TokenKind::Ge
        ) || self.keyword("BETWEEN")
            || self.keyword("IN")
    }

    fn comparison(&mut self) -> SyntaxResult {
        self.condition_operand()?;
        self.comparison_tail()
    }

    fn comparison_tail(&mut self) -> SyntaxResult {
        match self.kind() {
            TokenKind::Eq
            | TokenKind::Ne
            | TokenKind::Lt
            | TokenKind::Le
            | TokenKind::Gt
            | TokenKind::Ge => {
                self.advance();
                self.condition_operand()
            }
            _ if self.keyword("BETWEEN") => {
                self.advance();
                self.condition_operand()?;
                self.eat_keyword("AND")?;
                self.condition_operand()
            }
            _ if self.keyword("IN") => {
                self.advance();
                self.eat(TokenKind::LParen)?;
                self.condition_operand()?;
                while self.kind() == TokenKind::Comma {
                    self.advance();
                    self.condition_operand()?;
                }
                self.eat(TokenKind::RParen)
            }
            _ => Err(self.error()),
        }
    }

    // ---- Update ----

    fn update(&mut self) -> SyntaxResult {
        loop {
            if self.keyword("SET") {
                self.advance();
                self.set_action()?;
                while self.kind() == TokenKind::Comma {
                    self.advance();
                    self.set_action()?;
                }
            } else if self.keyword("REMOVE") {
                self.advance();
                self.path()?;
                while self.kind() == TokenKind::Comma {
                    self.advance();
                    self.path()?;
                }
            } else if self.keyword("ADD") || self.keyword("DELETE") {
                self.advance();
                self.path()?;
                self.eat(TokenKind::Value)?;
                while self.kind() == TokenKind::Comma {
                    self.advance();
                    self.path()?;
                    self.eat(TokenKind::Value)?;
                }
            } else {
                return Err(self.error());
            }
            if self.kind() == TokenKind::Eof {
                return Ok(());
            }
        }
    }

    fn set_action(&mut self) -> SyntaxResult {
        self.path()?;
        self.eat(TokenKind::Eq)?;
        self.update_value()
    }

    /// `operand [('+' | '-') operand]`. A parenthesized group is a complete
    /// operand: an operator after it is a syntax error (`(a + :x) - :y`).
    fn update_value(&mut self) -> SyntaxResult {
        let grouped = self.kind() == TokenKind::LParen;
        self.update_operand()?;
        if !grouped && matches!(self.kind(), TokenKind::Plus | TokenKind::Minus) {
            self.advance();
            self.update_operand()?;
        }
        Ok(())
    }

    /// AWS accepts a parenthesized update operand (`SET a = (b)`, probe P1)
    /// and arithmetic inside it (`SET a = (b + :x)`).
    fn update_operand(&mut self) -> SyntaxResult {
        match self.kind() {
            TokenKind::Value => {
                self.advance();
                Ok(())
            }
            TokenKind::LParen => {
                self.advance();
                self.update_value()?;
                self.eat(TokenKind::RParen)
            }
            TokenKind::Ident if self.at_call() => self.call(Self::update_operand),
            _ => self.path(),
        }
    }

    // ---- Projection ----

    fn projection(&mut self) -> SyntaxResult {
        self.path()?;
        while self.kind() == TokenKind::Comma {
            self.advance();
            self.path()?;
        }
        self.end()
    }
}

fn syntax_message(kind: ExpressionKind, source: &str, error: SyntaxError) -> String {
    let token = error.token;
    let token_text = if token.kind == TokenKind::Eof {
        "<EOF>"
    } else {
        source.get(token.start..token.end).unwrap_or_default()
    };
    let near_start = error.previous_start.unwrap_or(token.start);
    let near_end = if token.kind == TokenKind::Eof {
        token.end
    } else {
        error.next_end
    };
    let near = source.get(near_start..near_end).unwrap_or_default();
    format!("Invalid {kind}: Syntax error; token: \"{token_text}\", near: \"{near}\"")
}

/// First rule for every expression: empty input, then the grammar of `kind`.
/// A whitespace-only expression is a syntax error at `<EOF>`, not empty
/// (AWS probe P4).
pub(crate) fn validate_expression_syntax(
    expression: &str,
    kind: ExpressionKind,
    prefixed: bool,
) -> Result<(), String> {
    if expression.is_empty() {
        return Err(validation_message(
            format!("Invalid {kind}: The expression can not be empty;"),
            prefixed,
        ));
    }
    let mut grammar = Grammar::new(expression);
    let result = match kind {
        ExpressionKind::Condition | ExpressionKind::Filter | ExpressionKind::KeyCondition => {
            grammar.condition()
        }
        ExpressionKind::Update => grammar.update(),
        ExpressionKind::Projection => grammar.projection(),
    };
    result
        .map_err(|error| validation_message(syntax_message(kind, expression, error), prefixed))?;
    // Every operator needs at least one byte, so short expressions cannot
    // exceed the limit.
    if expression.len() > MAX_EXPRESSION_OPERATORS
        && kind != ExpressionKind::Projection
        && kind != ExpressionKind::Update
    {
        let count = operator_count(expression);
        if count > MAX_EXPRESSION_OPERATORS {
            return Err(validation_message(
                format!(
                    "Invalid {kind}: The expression contains too many operators; operator count: \
                     {count}"
                ),
                prefixed,
            ));
        }
    }
    Ok(())
}

/// A condition expression may hold at most this many operators and functions.
const MAX_EXPRESSION_OPERATORS: usize = 300;

/// Comparisons, `AND`, `OR`, `NOT`, `BETWEEN` (with its own `AND`), `IN` and
/// function calls each count once; an `IN` list counts once however long it is.
fn operator_count(source: &str) -> usize {
    let mut lexer = Lexer::new(source);
    let mut count = 0usize;
    let mut between_and_pending = false;
    let mut previous_is_function_name = false;
    loop {
        let token = lexer.next_token();
        let mut is_function_name = false;
        match token.kind {
            TokenKind::Eof => return count,
            TokenKind::Eq
            | TokenKind::Ne
            | TokenKind::Lt
            | TokenKind::Le
            | TokenKind::Gt
            | TokenKind::Ge => count += 1,
            TokenKind::LParen if previous_is_function_name => count += 1,
            TokenKind::Ident => {
                let word = source.get(token.start..token.end).unwrap_or_default();
                if word.eq_ignore_ascii_case("AND") {
                    if between_and_pending {
                        between_and_pending = false;
                    } else {
                        count += 1;
                    }
                } else if word.eq_ignore_ascii_case("BETWEEN") {
                    between_and_pending = true;
                    count += 1;
                } else if word.eq_ignore_ascii_case("OR")
                    || word.eq_ignore_ascii_case("NOT")
                    || word.eq_ignore_ascii_case("IN")
                {
                    count += 1;
                } else {
                    is_function_name = true;
                }
            }
            _ => {}
        }
        previous_is_function_name = is_function_name;
    }
}
