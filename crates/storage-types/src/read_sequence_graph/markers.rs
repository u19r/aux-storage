use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{AttributeValue, ReadSequenceNodeOperation};

/// Private wire marker used while a graph operation is bound to its inputs.
pub(crate) const READ_SEQUENCE_INPUT_MARKER_PREFIX: &str = "\u{1f}aux-read-sequence-input:\u{1f}";

/// Private wire marker used for an `S` value composed from declared inputs.
pub(crate) const READ_SEQUENCE_STRING_TEMPLATE_PREFIX: &str =
    "\u{1f}aux-read-sequence-string-template:\u{1f}";

/// Escape for literal strings which begin with the private input marker.
pub const READ_SEQUENCE_INPUT_LITERAL_ESCAPE_PREFIX: &str =
    "\u{1f}aux-read-sequence-literal:\u{1f}";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadSequenceStringTemplatePart<'a> {
    Literal(&'a str),
    Input(&'a str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadSequenceStringTemplateError {
    MissingInput,
    UnclosedInput,
    UnexpectedClosingBrace,
    InvalidInput,
}

impl std::fmt::Display for ReadSequenceStringTemplateError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::MissingInput => "template must contain at least one {input}",
            Self::UnclosedInput => "template contains an unclosed input",
            Self::UnexpectedClosingBrace => "template contains an unexpected closing brace",
            Self::InvalidInput => {
                "template input names must contain only ASCII letters, numbers, or _"
            }
        })
    }
}

impl std::error::Error for ReadSequenceStringTemplateError {}

pub struct ReadSequenceStringTemplateParts<'a> {
    template: &'a str,
    cursor: usize,
    pending_input: Option<&'a str>,
    saw_input: bool,
    finished: bool,
}

impl<'a> ReadSequenceStringTemplateParts<'a> {
    #[must_use]
    pub const fn new(template: &'a str) -> Self {
        Self {
            template,
            cursor: 0,
            pending_input: None,
            saw_input: false,
            finished: false,
        }
    }

    fn fail(
        &mut self,
        error: ReadSequenceStringTemplateError,
    ) -> Option<Result<ReadSequenceStringTemplatePart<'a>, ReadSequenceStringTemplateError>> {
        self.finished = true;
        Some(Err(error))
    }

    fn input_part(
        &mut self,
        remaining: &'a str,
        relative_brace: usize,
    ) -> Option<Result<ReadSequenceStringTemplatePart<'a>, ReadSequenceStringTemplateError>> {
        let Some(input_suffix) = remaining.get(relative_brace + 1..) else {
            return self.fail(ReadSequenceStringTemplateError::InvalidInput);
        };
        let Some(relative_end) = input_suffix.find('}') else {
            return self.fail(ReadSequenceStringTemplateError::UnclosedInput);
        };
        let Some(input) = input_suffix.get(..relative_end) else {
            return self.fail(ReadSequenceStringTemplateError::InvalidInput);
        };
        if input.is_empty()
            || !input
                .bytes()
                .all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
        {
            return self.fail(ReadSequenceStringTemplateError::InvalidInput);
        }

        self.cursor += relative_brace + relative_end + 2;
        self.saw_input = true;
        if relative_brace == 0 {
            return Some(Ok(ReadSequenceStringTemplatePart::Input(input)));
        }
        self.pending_input = Some(input);
        let Some(literal) = remaining.get(..relative_brace) else {
            return self.fail(ReadSequenceStringTemplateError::InvalidInput);
        };
        Some(Ok(ReadSequenceStringTemplatePart::Literal(literal)))
    }
}

impl<'a> Iterator for ReadSequenceStringTemplateParts<'a> {
    type Item = Result<ReadSequenceStringTemplatePart<'a>, ReadSequenceStringTemplateError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(input) = self.pending_input.take() {
            return Some(Ok(ReadSequenceStringTemplatePart::Input(input)));
        }
        if self.finished {
            return None;
        }
        if self.cursor == self.template.len() {
            self.finished = true;
            return (!self.saw_input).then_some(Err(ReadSequenceStringTemplateError::MissingInput));
        }

        let Some(remaining) = self.template.get(self.cursor..) else {
            return self.fail(ReadSequenceStringTemplateError::InvalidInput);
        };
        let Some(relative_brace) = remaining.find(['{', '}']) else {
            self.cursor = self.template.len();
            return Some(Ok(ReadSequenceStringTemplatePart::Literal(remaining)));
        };
        if remaining.as_bytes().get(relative_brace) == Some(&b'}') {
            return self.fail(ReadSequenceStringTemplateError::UnexpectedClosingBrace);
        }

        self.input_part(remaining, relative_brace)
    }
}

impl Serialize for ReadSequenceNodeOperation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut value = match self {
            Self::Get(request) => serde_json::json!({ "Get": request }),
            Self::BatchGet(request) => serde_json::json!({ "BatchGet": request }),
            Self::Query(request) => serde_json::json!({ "Query": request }),
        };
        restore_input_markers(&mut value);
        value.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ReadSequenceNodeOperation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let mut value = JsonValue::deserialize(deserializer)?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| serde::de::Error::custom("ReadSequence operation must be an object"))?;
        if object.len() != 1 {
            return Err(serde::de::Error::custom(
                "ReadSequence operation must specify exactly one of Get, BatchGet, or Query",
            ));
        }
        let Some((kind, payload)) = object
            .iter_mut()
            .next()
            .map(|(kind, payload)| (kind.clone(), payload))
        else {
            return Err(serde::de::Error::custom(
                "ReadSequence operation must specify exactly one operation",
            ));
        };
        replace_input_markers(payload).map_err(serde::de::Error::custom)?;
        match kind.as_str() {
            "Get" => serde_json::from_value(payload.clone())
                .map(Self::Get)
                .map_err(serde::de::Error::custom),
            "BatchGet" => serde_json::from_value(payload.clone())
                .map(Self::BatchGet)
                .map_err(serde::de::Error::custom),
            "Query" => serde_json::from_value(payload.clone())
                .map(Self::Query)
                .map_err(serde::de::Error::custom),
            _ => Err(serde::de::Error::unknown_field(
                &kind,
                &["Get", "BatchGet", "Query"],
            )),
        }
    }
}

fn replace_input_markers(value: &mut JsonValue) -> Result<(), String> {
    match value {
        JsonValue::Object(fields) if fields.len() == 1 && fields.contains_key("FromInput") => {
            if let Some(JsonValue::String(name)) = fields.get("FromInput") {
                *value = serde_json::json!({
                    "S": format!("{READ_SEQUENCE_INPUT_MARKER_PREFIX}{name}")
                });
            } else if let Some(child) = fields.get_mut("FromInput") {
                replace_input_markers(child)?;
            }
        }
        JsonValue::Object(fields) if fields.len() == 1 && fields.contains_key("StringTemplate") => {
            if let Some(JsonValue::String(template)) = fields.get("StringTemplate") {
                for part in ReadSequenceStringTemplateParts::new(template) {
                    part.map_err(|error| format!("invalid ReadSequence StringTemplate: {error}"))?;
                }
                *value = serde_json::json!({
                    "S": format!("{READ_SEQUENCE_STRING_TEMPLATE_PREFIX}{template}")
                });
            } else if let Some(child) = fields.get_mut("StringTemplate") {
                replace_input_markers(child)?;
            }
        }
        JsonValue::Object(fields)
            if fields.len() == 1
                && fields.get("S").is_some_and(|value| {
                    matches!(value, JsonValue::String(string)
                        if string.starts_with(READ_SEQUENCE_INPUT_MARKER_PREFIX)
                            || string.starts_with(READ_SEQUENCE_STRING_TEMPLATE_PREFIX)
                            || string.starts_with(READ_SEQUENCE_INPUT_LITERAL_ESCAPE_PREFIX))
                }) =>
        {
            if let Some(JsonValue::String(string)) = fields.get_mut("S") {
                *string = format!("{READ_SEQUENCE_INPUT_LITERAL_ESCAPE_PREFIX}{string}");
            }
        }
        JsonValue::Object(fields) => {
            for child in fields.values_mut() {
                replace_input_markers(child)?;
            }
        }
        JsonValue::Array(values) => {
            for child in values {
                replace_input_markers(child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn restore_input_markers(value: &mut JsonValue) {
    match value {
        JsonValue::Object(fields) => {
            if fields.len() == 1
                && let Some(JsonValue::String(string)) = fields.get_mut("S")
                && let Some(unescaped) =
                    string.strip_prefix(READ_SEQUENCE_INPUT_LITERAL_ESCAPE_PREFIX)
            {
                *string = unescaped.to_string();
                return;
            }
            if fields.len() == 1
                && let Some(JsonValue::String(marker)) = fields.get("S")
                && let Some(name) = marker.strip_prefix(READ_SEQUENCE_INPUT_MARKER_PREFIX)
            {
                *value = serde_json::json!({ "FromInput": name });
                return;
            }
            if fields.len() == 1
                && let Some(JsonValue::String(marker)) = fields.get("S")
                && let Some(template) = marker.strip_prefix(READ_SEQUENCE_STRING_TEMPLATE_PREFIX)
            {
                *value = serde_json::json!({ "StringTemplate": template });
                return;
            }
            for child in fields.values_mut() {
                restore_input_markers(child);
            }
        }
        JsonValue::Array(values) => {
            for child in values {
                restore_input_markers(child);
            }
        }
        _ => {}
    }
}

pub fn read_sequence_input_marker_name(value: &AttributeValue) -> Option<&str> {
    match value {
        AttributeValue::S(value) => value.strip_prefix(READ_SEQUENCE_INPUT_MARKER_PREFIX),
        _ => None,
    }
}

/// Build the internal typed value used by Rust callers constructing a graph
/// without going through the JSON `FromInput` marker.
#[must_use]
pub fn read_sequence_input_marker(name: &str) -> AttributeValue {
    AttributeValue::S(format!("{READ_SEQUENCE_INPUT_MARKER_PREFIX}{name}"))
}

#[must_use]
pub fn read_sequence_string_template(template: &str) -> AttributeValue {
    AttributeValue::S(format!("{READ_SEQUENCE_STRING_TEMPLATE_PREFIX}{template}"))
}

#[must_use]
pub fn read_sequence_string_template_name(value: &AttributeValue) -> Option<&str> {
    match value {
        AttributeValue::S(value) => value.strip_prefix(READ_SEQUENCE_STRING_TEMPLATE_PREFIX),
        _ => None,
    }
}

#[must_use]
pub fn read_sequence_input_literal(value: &str) -> AttributeValue {
    AttributeValue::S(format!(
        "{READ_SEQUENCE_INPUT_LITERAL_ESCAPE_PREFIX}{value}"
    ))
}

#[must_use]
pub fn read_sequence_input_literal_name(value: &AttributeValue) -> Option<&str> {
    match value {
        AttributeValue::S(value) => value.strip_prefix(READ_SEQUENCE_INPUT_LITERAL_ESCAPE_PREFIX),
        _ => None,
    }
}

#[must_use]
pub fn read_sequence_operation_contains_literal_escape(
    operation: &ReadSequenceNodeOperation,
) -> bool {
    let contains_key = |key: &crate::KeyAttributes| {
        key.iter()
            .any(|(_, value)| read_sequence_attribute_contains_literal_escape(value))
    };
    match operation {
        ReadSequenceNodeOperation::Get(request) => contains_key(&request.key),
        ReadSequenceNodeOperation::BatchGet(request) => request
            .request_items
            .values()
            .flat_map(|keys| keys.keys.iter())
            .any(contains_key),
        ReadSequenceNodeOperation::Query(request) => {
            request
                .expression_attribute_values
                .as_ref()
                .is_some_and(|values| {
                    values
                        .values()
                        .any(read_sequence_attribute_contains_literal_escape)
                })
                || request
                    .exclusive_start_key
                    .as_ref()
                    .and_then(|key| match key {
                        crate::ExclusiveStartKey::Key(key) => Some(key),
                        crate::ExclusiveStartKey::Token(_) => None,
                    })
                    .is_some_and(contains_key)
        }
    }
}

fn read_sequence_attribute_contains_literal_escape(value: &AttributeValue) -> bool {
    if read_sequence_input_literal_name(value).is_some() {
        return true;
    }
    match value {
        AttributeValue::L(values) => values
            .iter()
            .any(read_sequence_attribute_contains_literal_escape),
        AttributeValue::M(values) => values
            .values()
            .any(read_sequence_attribute_contains_literal_escape),
        _ => false,
    }
}
