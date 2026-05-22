use std::collections::HashMap;

use storage_types::AttributeValue;

use crate::{
    Condition, SizeComparison,
    helpers::{attribute_value_list_to_strings, attribute_value_scalar_to_string},
};

#[derive(Debug, Clone, PartialEq)]
enum Token {
    // Literals
    Identifier(String),
    AttributeName(String),  // #name
    AttributeValue(String), // :value
    String(String),
    Number(String),

    // Operators
    Equal,            // =
    NotEqual,         // <>
    LessThan,         // <
    LessThanEqual,    // <=
    GreaterThan,      // >
    GreaterThanEqual, // >=

    // Keywords
    And,
    Or,
    Not,
    Between,
    In,

    // Functions
    AttributeExists,
    AttributeNotExists,
    AttributeType,
    BeginsWith,
    Contains,
    Size,

    // Punctuation
    LeftParen,
    RightParen,
    LeftBracket,
    RightBracket,
    Comma,
    Dot,

    // Special
    Eof,
}

#[derive(Debug)]
pub(crate) struct Lexer<'a> {
    input: &'a str,
    position: usize,
}

impl<'a> Lexer<'a> {
    pub(crate) fn new(input: &'a str) -> Self {
        Self { input, position: 0 }
    }

    fn current_char(&self) -> Option<char> {
        self.input
            .get(self.position..)
            .and_then(|remaining| remaining.chars().next())
    }

    fn advance(&mut self) {
        if let Some(ch) = self.current_char() {
            self.position += ch.len_utf8();
        }
    }

    fn skip_whitespace(&mut self) {
        while self.current_char().is_some_and(char::is_whitespace) {
            self.advance();
        }
    }

    fn read_string(&mut self, quote_char: char) -> Result<String, String> {
        let mut value = String::new();
        self.advance(); // Skip opening quote

        while let Some(ch) = self.current_char() {
            if ch == quote_char {
                self.advance(); // Skip closing quote
                return Ok(value);
            }
            if ch == '\\' {
                self.advance();
                if let Some(escaped) = self.current_char() {
                    match escaped {
                        'n' => value.push('\n'),
                        't' => value.push('\t'),
                        'r' => value.push('\r'),
                        '\\' => value.push('\\'),
                        '\'' => value.push('\''),
                        '"' => value.push('"'),
                        _ => {
                            value.push('\\');
                            value.push(escaped);
                        }
                    }
                    self.advance();
                } else {
                    value.push('\\');
                }
                continue;
            }
            value.push(ch);
            self.advance();
        }

        Err("Unterminated string literal".to_string())
    }

    fn read_identifier(&mut self) -> String {
        let mut value = String::new();

        while let Some(ch) = self.current_char() {
            if ch.is_alphanumeric() || ch == '_' {
                value.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        value
    }

    fn read_number(&mut self) -> String {
        let mut value = String::new();

        while let Some(ch) = self.current_char() {
            if ch.is_ascii_digit() || ch == '.' || ch == '-' || ch == '+' {
                value.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        value
    }

    fn next_token(&mut self) -> Result<Token, String> {
        loop {
            match self.current_char() {
                None => return Ok(Token::Eof),

                Some(ch) if ch.is_whitespace() => {
                    self.skip_whitespace();
                }

                Some('(') => {
                    self.advance();
                    return Ok(Token::LeftParen);
                }

                Some(')') => {
                    self.advance();
                    return Ok(Token::RightParen);
                }

                Some('[') => {
                    self.advance();
                    return Ok(Token::LeftBracket);
                }

                Some(']') => {
                    self.advance();
                    return Ok(Token::RightBracket);
                }

                Some(',') => {
                    self.advance();
                    return Ok(Token::Comma);
                }

                Some('.') => {
                    self.advance();
                    return Ok(Token::Dot);
                }

                Some('=') => {
                    self.advance();
                    return Ok(Token::Equal);
                }

                Some('<') => {
                    self.advance();
                    if self.current_char() == Some('>') {
                        self.advance();
                        return Ok(Token::NotEqual);
                    }
                    if self.current_char() == Some('=') {
                        self.advance();
                        return Ok(Token::LessThanEqual);
                    }
                    return Ok(Token::LessThan);
                }

                Some('>') => {
                    self.advance();
                    if self.current_char() == Some('=') {
                        self.advance();
                        return Ok(Token::GreaterThanEqual);
                    }
                    return Ok(Token::GreaterThan);
                }

                Some(quote @ ('"' | '\'')) => {
                    let value = self.read_string(quote)?;
                    return Ok(Token::String(value));
                }

                Some('#') => {
                    self.advance();
                    let name = self.read_identifier();
                    return Ok(Token::AttributeName(name));
                }

                Some(':') => {
                    self.advance();
                    let name = self.read_identifier();
                    return Ok(Token::AttributeValue(name));
                }

                Some(ch) if ch.is_ascii_digit() || ch == '-' => {
                    let number = self.read_number();
                    return Ok(Token::Number(number));
                }

                Some(ch) if ch.is_alphabetic() => {
                    let identifier = self.read_identifier();
                    let token = match identifier.to_uppercase().as_str() {
                        "AND" => Token::And,
                        "OR" => Token::Or,
                        "NOT" => Token::Not,
                        "BETWEEN" => Token::Between,
                        "IN" => Token::In,
                        "ATTRIBUTE_EXISTS" => Token::AttributeExists,
                        "ATTRIBUTE_NOT_EXISTS" => Token::AttributeNotExists,
                        "ATTRIBUTE_TYPE" => Token::AttributeType,
                        "BEGINS_WITH" => Token::BeginsWith,
                        "CONTAINS" => Token::Contains,
                        "SIZE" => Token::Size,
                        _ => Token::Identifier(identifier),
                    };
                    return Ok(token);
                }

                Some(ch) => return Err(format!("Unexpected character: {ch}")),
            }
        }
    }
}

fn between_upper_is_less_than_lower(lower: &AttributeValue, upper: &AttributeValue) -> bool {
    match (lower, upper) {
        (AttributeValue::N(lower), AttributeValue::N(upper)) => {
            compare_dynamodb_numbers(upper, lower).is_some_and(std::cmp::Ordering::is_lt)
        }
        (AttributeValue::S(lower), AttributeValue::S(upper)) => upper < lower,
        (AttributeValue::B(lower), AttributeValue::B(upper)) => {
            match (
                storage_types::dynamodb_binary::decode_base64_string(lower),
                storage_types::dynamodb_binary::decode_base64_string(upper),
            ) {
                (Ok(lower), Ok(upper)) => upper < lower,
                _ => upper < lower,
            }
        }
        _ => false,
    }
}

fn compare_dynamodb_numbers(left: &str, right: &str) -> Option<std::cmp::Ordering> {
    Some(
        left.parse::<f64>()
            .ok()?
            .total_cmp(&right.parse::<f64>().ok()?),
    )
}

fn dynamodb_attribute_value_display(value: &AttributeValue) -> String {
    match value {
        AttributeValue::S(value) => format!("AttributeValue: {{S:{value}}}"),
        AttributeValue::N(value) => format!("AttributeValue: {{N:{value}}}"),
        AttributeValue::B(value) => format!("AttributeValue: {{B:{value}}}"),
        AttributeValue::BOOL(value) => format!("AttributeValue: {{BOOL:{value}}}"),
        AttributeValue::NULL(value) => format!("AttributeValue: {{NULL:{value}}}"),
        AttributeValue::SS(values) => format!("AttributeValue: {{SS:{values:?}}}"),
        AttributeValue::NS(values) => format!("AttributeValue: {{NS:{values:?}}}"),
        AttributeValue::BS(values) => format!("AttributeValue: {{BS:{values:?}}}"),
        AttributeValue::L(values) => format!("AttributeValue: {{L:{values:?}}}"),
        AttributeValue::M(values) => format!("AttributeValue: {{M:{values:?}}}"),
    }
}

#[derive(Debug)]
pub(crate) struct Parser<'a> {
    lexer: Lexer<'a>,
    current_token: Token,
}

impl<'a> Parser<'a> {
    pub(crate) fn new(mut lexer: Lexer<'a>) -> Result<Self, String> {
        let current_token = lexer.next_token()?;
        Ok(Parser {
            lexer,
            current_token,
        })
    }

    fn advance(&mut self) -> Result<(), String> {
        self.current_token = self.lexer.next_token()?;
        Ok(())
    }

    fn expect_token(&mut self, expected: &Token) -> Result<(), String> {
        if self.current_token == *expected {
            self.advance()
        } else {
            Err(format!(
                "Expected {expected:?}, found {:?}",
                self.current_token
            ))
        }
    }

    fn parse_primary(
        &mut self,
        attribute_names: &HashMap<String, String>,
        attribute_values: &HashMap<String, AttributeValue>,
    ) -> Result<Condition, String> {
        match &self.current_token {
            Token::LeftParen => {
                self.advance()?;
                let condition = self.parse_or_expression(attribute_names, attribute_values)?;
                self.expect_token(&Token::RightParen)?;
                Ok(condition)
            }

            Token::AttributeExists => {
                self.advance()?;
                self.expect_token(&Token::LeftParen)?;
                let field = self.parse_attribute_path(attribute_names)?;
                self.expect_token(&Token::RightParen)?;
                Ok(Condition::Exists { field })
            }

            Token::AttributeNotExists => {
                self.advance()?;
                self.expect_token(&Token::LeftParen)?;
                let field = self.parse_attribute_path(attribute_names)?;
                self.expect_token(&Token::RightParen)?;
                Ok(Condition::NotExists { field })
            }

            Token::BeginsWith => {
                self.advance()?;
                self.expect_token(&Token::LeftParen)?;
                let field = self.parse_attribute_path(attribute_names)?;
                self.expect_token(&Token::Comma)?;
                let prefix = self.parse_attribute_value(attribute_values)?;
                self.expect_token(&Token::RightParen)?;
                Ok(Condition::BeginsWith { field, prefix })
            }

            Token::Contains => {
                self.advance()?;
                self.expect_token(&Token::LeftParen)?;
                let field = self.parse_attribute_path(attribute_names)?;
                self.expect_token(&Token::Comma)?;
                if self.next_token_starts_same_path(&field, attribute_names)? {
                    return Err(format!(
                        "Invalid ConditionExpression: The first operand must be distinct from the \
                         remaining operands for this operator or function; operator: contains, \
                         first operand: [{field}]"
                    ));
                }
                let value = self.parse_attribute_value(attribute_values)?;
                self.expect_token(&Token::RightParen)?;
                Ok(Condition::Contains { field, value })
            }

            Token::AttributeType => {
                self.advance()?;
                self.expect_token(&Token::LeftParen)?;
                let field = self.parse_attribute_path(attribute_names)?;
                self.expect_token(&Token::Comma)?;
                let attribute_type = match self.parse_attribute_value(attribute_values)? {
                    AttributeValue::S(attribute_type) => attribute_type,
                    _ => {
                        return Err("Invalid ConditionExpression: Incorrect operand type for \
                                    operator or function; operator or function: attribute_type"
                            .to_string());
                    }
                };
                self.expect_token(&Token::RightParen)?;
                Ok(Condition::AttributeType {
                    field,
                    attribute_type,
                })
            }

            Token::Size => {
                self.advance()?;
                self.expect_token(&Token::LeftParen)?;
                let field = self.parse_attribute_path(attribute_names)?;
                self.expect_token(&Token::RightParen)?;

                // SIZE function must be followed by a comparison operator
                let size_field = field;
                self.parse_size_comparison(size_field, attribute_values)
            }

            _ => {
                // Parse comparison expression (field op value)
                let field = self.parse_attribute_path(attribute_names)?;
                self.parse_comparison(field, attribute_names, attribute_values)
            }
        }
    }

    fn parse_attribute_path(
        &mut self,
        attribute_names: &HashMap<String, String>,
    ) -> Result<String, String> {
        let mut path = match &self.current_token {
            Token::AttributeName(name) => {
                let key = format!("#{name}");
                let actual_name = attribute_names.get(&key).ok_or_else(|| {
                    format!("Attribute name #{name} not found in ExpressionAttributeNames")
                })?;
                self.advance()?;
                actual_name.clone()
            }
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance()?;
                name
            }
            _ => {
                return Err(format!(
                    "Expected attribute path, found {:?}",
                    self.current_token
                ));
            }
        };

        loop {
            match &self.current_token {
                Token::Dot => {
                    self.advance()?;
                    path.push('.');
                    path.push_str(&self.parse_attribute_path_segment(attribute_names)?);
                }
                Token::LeftBracket => {
                    self.advance()?;
                    let index = match &self.current_token {
                        Token::Number(value) => value.clone(),
                        other => {
                            return Err(format!(
                                "Expected list index in attribute path, found {other:?}"
                            ));
                        }
                    };
                    self.advance()?;
                    self.expect_token(&Token::RightBracket)?;
                    path.push('[');
                    path.push_str(&index);
                    path.push(']');
                }
                _ => return Ok(path),
            }
        }
    }

    fn parse_attribute_path_segment(
        &mut self,
        attribute_names: &HashMap<String, String>,
    ) -> Result<String, String> {
        match &self.current_token {
            Token::AttributeName(name) => {
                let key = format!("#{name}");
                let actual_name = attribute_names.get(&key).ok_or_else(|| {
                    format!("Attribute name #{name} not found in ExpressionAttributeNames")
                })?;
                self.advance()?;
                Ok(actual_name.clone())
            }
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance()?;
                Ok(name)
            }
            other => Err(format!("Expected attribute path segment, found {other:?}")),
        }
    }

    fn next_token_starts_same_path(
        &self,
        field: &str,
        attribute_names: &HashMap<String, String>,
    ) -> Result<bool, String> {
        match &self.current_token {
            Token::Identifier(name) => Ok(name == field),
            Token::AttributeName(name) => {
                let key = format!("#{name}");
                let actual_name = attribute_names.get(&key).ok_or_else(|| {
                    format!("Attribute name #{name} not found in ExpressionAttributeNames")
                })?;
                Ok(actual_name == field)
            }
            _ => Ok(false),
        }
    }

    fn parse_attribute_value(
        &mut self,
        attribute_values: &HashMap<String, AttributeValue>,
    ) -> Result<AttributeValue, String> {
        match &self.current_token {
            Token::AttributeValue(name) => {
                let key = format!(":{name}");
                let value = attribute_values.get(&key).ok_or_else(|| {
                    format!("Attribute value :{name} not found in ExpressionAttributeValues")
                })?;
                self.advance()?;
                Ok(value.clone())
            }
            Token::String(value) => {
                let value = value.clone();
                self.advance()?;
                Ok(AttributeValue::S(value))
            }
            Token::Number(value) => {
                let value = value.clone();
                self.advance()?;
                Ok(AttributeValue::N(value))
            }
            _ => Err(format!("Expected value, found {:?}", self.current_token)),
        }
    }

    fn parse_value(
        &mut self,
        attribute_values: &HashMap<String, AttributeValue>,
    ) -> Result<String, String> {
        match &self.current_token {
            Token::AttributeValue(name) => {
                let key = format!(":{name}");
                let value = attribute_values.get(&key).ok_or_else(|| {
                    format!("Attribute value :{name} not found in ExpressionAttributeValues")
                })?;
                self.advance()?;
                Ok(attribute_value_scalar_to_string(value))
            }
            Token::String(value) => {
                let value = value.clone();
                self.advance()?;
                Ok(value)
            }
            Token::Number(value) => {
                let value = value.clone();
                self.advance()?;
                Ok(value)
            }
            _ => Err(format!("Expected value, found {:?}", self.current_token)),
        }
    }

    fn parse_value_list(
        &mut self,
        attribute_values: &HashMap<String, AttributeValue>,
    ) -> Result<Vec<String>, String> {
        let mut values = Vec::new();

        loop {
            // Value
            let v = self.parse_value(attribute_values)?;
            values.push(v);

            // Next token either comma or right paren
            match &self.current_token {
                Token::Comma => {
                    self.advance()?;
                }
                Token::RightParen => break,
                other => {
                    return Err(format!(
                        "Expected ',' or ')' in value list, found {other:?}"
                    ));
                }
            }
        }

        Ok(values)
    }

    fn parse_size_comparison(
        &mut self,
        field: String,
        attribute_values: &HashMap<String, AttributeValue>,
    ) -> Result<Condition, String> {
        let operator = match &self.current_token {
            Token::Equal => SizeComparison::Equal,
            Token::NotEqual => SizeComparison::NotEqual,
            Token::LessThan => SizeComparison::LessThan,
            Token::LessThanEqual => SizeComparison::LessThanEqual,
            Token::GreaterThan => SizeComparison::GreaterThan,
            Token::GreaterThanEqual => SizeComparison::GreaterThanEqual,
            _ => return Err("SIZE function must be followed by comparison operator".to_string()),
        };
        self.advance()?;
        let size_str = self.parse_value(attribute_values)?;
        let size = size_str
            .parse::<usize>()
            .map_err(|_| "Size must be a number")?;
        if operator == SizeComparison::Equal {
            Ok(Condition::Size { field, size })
        } else {
            Ok(Condition::SizeCompare {
                field,
                operator,
                size,
            })
        }
    }

    #[expect(clippy::too_many_lines)]
    fn parse_comparison(
        &mut self,
        field: String,
        _attribute_names: &HashMap<String, String>,
        attribute_values: &HashMap<String, AttributeValue>,
    ) -> Result<Condition, String> {
        match &self.current_token {
            Token::Equal => {
                self.advance()?;
                let value = self.parse_value(attribute_values)?;
                Ok(Condition::Equal { field, value })
            }

            Token::NotEqual => {
                self.advance()?;
                let value = self.parse_value(attribute_values)?;
                Ok(Condition::NotEqual { field, value })
            }

            Token::LessThan => {
                self.advance()?;
                let value = self.parse_value(attribute_values)?;
                Ok(Condition::LessThan { field, value })
            }

            Token::LessThanEqual => {
                self.advance()?;
                let value = self.parse_value(attribute_values)?;
                Ok(Condition::LessThanEqual { field, value })
            }

            Token::GreaterThan => {
                self.advance()?;
                let value = self.parse_value(attribute_values)?;
                Ok(Condition::GreaterThan { field, value })
            }

            Token::GreaterThanEqual => {
                self.advance()?;
                let value = self.parse_value(attribute_values)?;
                Ok(Condition::GreaterThanEqual { field, value })
            }

            Token::Between => {
                self.advance()?;
                let min_value = self.parse_attribute_value(attribute_values)?;
                self.expect_token(&Token::And)?;
                let max_value = self.parse_attribute_value(attribute_values)?;
                if between_upper_is_less_than_lower(&min_value, &max_value) {
                    return Err(format!(
                        "Invalid ConditionExpression: The BETWEEN operator requires upper bound \
                         to be greater than or equal to lower bound; lower bound operand: {}, \
                         upper bound operand: {}",
                        dynamodb_attribute_value_display(&min_value),
                        dynamodb_attribute_value_display(&max_value)
                    ));
                }
                let min = attribute_value_scalar_to_string(&min_value);
                let max = attribute_value_scalar_to_string(&max_value);
                Ok(Condition::Between { field, min, max })
            }

            Token::In => {
                self.advance()?;
                self.expect_token(&Token::LeftParen)?;
                // Special-case: allow a single attribute value placeholder that
                // is itself a collection (e.g., SS/NS/BS/L) to represent the
                // entire IN list. If more than one value is present (comma
                // separated), fall back to the standard list parsing.
                // First, check if the next token is a single attribute value placeholder.
                let single_placeholder_name = if let Token::AttributeValue(n) = &self.current_token
                {
                    Some(n.clone())
                } else {
                    None
                };

                if let Some(name) = single_placeholder_name {
                    let key = format!(":{name}");
                    let raw = attribute_values.get(&key).cloned();

                    // Consume the attribute value token to inspect what's next
                    self.advance()?;

                    if matches!(self.current_token, Token::RightParen) {
                        // Single placeholder case: expand collection into list
                        let values = match raw {
                            Some(v) => attribute_value_list_to_strings(&v),
                            None => {
                                return Err(format!(
                                    "Attribute value :{name} not found in \
                                     ExpressionAttributeValues"
                                ));
                            }
                        };
                        self.expect_token(&Token::RightParen)?;
                        Ok(Condition::In { field, values })
                    } else {
                        // Fallback: handle as a comma-separated list beginning
                        // with the scalar form of the first placeholder
                        let mut values = Vec::new();
                        match raw {
                            Some(v) => values.push(attribute_value_scalar_to_string(&v)),
                            None => {
                                return Err(format!(
                                    "Attribute value :{name} not found in \
                                     ExpressionAttributeValues"
                                ));
                            }
                        }

                        loop {
                            match &self.current_token {
                                Token::Comma => {
                                    self.advance()?;
                                    let v = self.parse_value(attribute_values)?;
                                    values.push(v);
                                }
                                Token::RightParen => break,
                                other => {
                                    return Err(format!(
                                        "Expected ',' or ')' in value list, found {other:?}"
                                    ));
                                }
                            }
                        }

                        self.expect_token(&Token::RightParen)?;
                        Ok(Condition::In { field, values })
                    }
                } else {
                    let values = self.parse_value_list(attribute_values)?;
                    self.expect_token(&Token::RightParen)?;
                    Ok(Condition::In { field, values })
                }
            }

            _ => Err(format!(
                "Expected comparison operator after field {field}, found {:?}",
                self.current_token
            )),
        }
    }

    fn parse_and_expression(
        &mut self,
        attribute_names: &HashMap<String, String>,
        attribute_values: &HashMap<String, AttributeValue>,
    ) -> Result<Condition, String> {
        let mut left = self.parse_not_expression(attribute_names, attribute_values)?;

        while matches!(self.current_token, Token::And) {
            self.advance()?;
            let right = self.parse_not_expression(attribute_names, attribute_values)?;

            // Combine with existing AND conditions or create new one
            left = match left {
                Condition::And { mut conditions } => {
                    conditions.push(right);
                    Condition::And { conditions }
                }
                _ => Condition::And {
                    conditions: vec![left, right],
                },
            };
        }

        Ok(left)
    }

    fn parse_not_expression(
        &mut self,
        attribute_names: &HashMap<String, String>,
        attribute_values: &HashMap<String, AttributeValue>,
    ) -> Result<Condition, String> {
        if matches!(self.current_token, Token::Not) {
            self.advance()?;
            let condition = self.parse_not_expression(attribute_names, attribute_values)?;
            return Ok(Condition::Not {
                condition: Box::new(condition),
            });
        }

        self.parse_primary(attribute_names, attribute_values)
    }

    fn parse_or_expression(
        &mut self,
        attribute_names: &HashMap<String, String>,
        attribute_values: &HashMap<String, AttributeValue>,
    ) -> Result<Condition, String> {
        let mut left = self.parse_and_expression(attribute_names, attribute_values)?;

        while matches!(self.current_token, Token::Or) {
            self.advance()?;
            let right = self.parse_and_expression(attribute_names, attribute_values)?;

            // Combine with existing OR conditions or create new one
            left = match left {
                Condition::Or { mut conditions } => {
                    conditions.push(right);
                    Condition::Or { conditions }
                }
                _ => Condition::Or {
                    conditions: vec![left, right],
                },
            };
        }

        Ok(left)
    }

    pub(crate) fn parse(
        &mut self,
        attribute_names: &HashMap<String, String>,
        attribute_values: &HashMap<String, AttributeValue>,
    ) -> Result<Condition, String> {
        let condition = self.parse_or_expression(attribute_names, attribute_values)?;

        if !matches!(self.current_token, Token::Eof) {
            return Err(format!(
                "Unexpected token at end of expression: {:?}",
                self.current_token
            ));
        }

        Ok(condition)
    }
}
