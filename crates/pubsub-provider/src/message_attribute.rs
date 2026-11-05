//! SNS attributes retain one logical type and value through durable delivery.

use std::borrow::Cow;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};

use crate::{PubsubError, PubsubResult, PubsubValidationKind};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "Type", content = "Value")]
pub enum MessageAttribute {
    String(String),
    #[serde(rename = "String.Array")]
    StringArray(String),
    Number(String),
    Binary(Vec<u8>),
}

impl From<String> for MessageAttribute {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl MessageAttribute {
    pub fn try_from_wire(data_type: &str, value: String) -> PubsubResult<Self> {
        let mut attribute = match data_type {
            "String" => Self::String(value),
            "String.Array" => Self::StringArray(value),
            "Number" => Self::Number(value),
            "Binary" => Self::Binary(STANDARD.decode(value).map_err(|_| invalid_attribute())?),
            _ => return Err(invalid_attribute()),
        };
        attribute.validate()?;
        Ok(attribute)
    }

    pub fn data_type(&self) -> &'static str {
        match self {
            Self::String(_) => "String",
            Self::StringArray(_) => "String.Array",
            Self::Number(_) => "Number",
            Self::Binary(_) => "Binary",
        }
    }

    pub fn wire_value(&self) -> Cow<'_, str> {
        match self {
            Self::String(value) | Self::StringArray(value) | Self::Number(value) => {
                Cow::Borrowed(value)
            }
            Self::Binary(value) => Cow::Owned(STANDARD.encode(value)),
        }
    }

    pub fn byte_len(&self) -> usize {
        match self {
            Self::String(value) | Self::StringArray(value) | Self::Number(value) => value.len(),
            Self::Binary(value) => value.len(),
        }
    }

    pub(crate) fn validate(&mut self) -> PubsubResult<()> {
        if self.byte_len() == 0 {
            return Err(invalid_attribute());
        }
        match self {
            Self::Number(value) => validate_number(value),
            Self::StringArray(value) => {
                let values: Vec<serde_json::Value> =
                    serde_json::from_str(value).map_err(|_| invalid_attribute())?;
                if values
                    .iter()
                    .any(|value| value.is_array() || value.is_object())
                {
                    return Err(invalid_attribute());
                }
                Ok(())
            }
            Self::String(_) | Self::Binary(_) => Ok(()),
        }
    }
}

fn validate_number(value: &mut String) -> PubsubResult<()> {
    let canonical =
        storage_types::canonical_dynamodb_number(value).map_err(|_| invalid_attribute())?;
    let fraction = canonical
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    let magnitude =
        storage_types::compare_dynamodb_numbers(canonical.trim_start_matches('-'), "1000000000")
            .map_err(|_| invalid_attribute())?;
    if fraction > 5 || magnitude.is_gt() {
        return Err(invalid_attribute());
    }
    *value = canonical;
    Ok(())
}

fn invalid_attribute() -> PubsubError {
    PubsubError::validation(PubsubValidationKind::InvalidMessageAttribute)
}

impl crate::PublishRequest {
    pub(crate) fn validate(&mut self) -> PubsubResult<()> {
        if self.message.is_empty() {
            return Err(PubsubError::validation(PubsubValidationKind::EmptyMessage));
        }
        if self.subject.as_ref().is_some_and(|subject| {
            subject.is_empty()
                || subject.chars().count() >= crate::constants::SNS_SUBJECT_CHARACTER_LIMIT
                || subject.chars().any(char::is_control)
        }) {
            return Err(PubsubError::validation(
                PubsubValidationKind::InvalidSubject,
            ));
        }
        let mut size = self.message.len();
        for (name, value) in &mut self.message_attributes {
            validate_attribute_name(name)?;
            value.validate()?;
            size += name.len() + value.data_type().len() + value.byte_len();
        }
        if size > crate::constants::SNS_DEFAULT_MAX_MESSAGE_SIZE_BYTES {
            return Err(PubsubError::validation(
                PubsubValidationKind::MessageTooLarge,
            ));
        }
        Ok(())
    }
}

fn validate_attribute_name(name: &str) -> PubsubResult<()> {
    let lower = name.to_ascii_lowercase();
    if !is_attribute_name_syntax_valid(name)
        || name.starts_with('.')
        || name.ends_with('.')
        || name.contains("..")
        || lower.starts_with("aws.")
        || lower.starts_with("amazon.")
    {
        return Err(invalid_attribute());
    }
    Ok(())
}

fn is_attribute_name_syntax_valid(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= crate::constants::SNS_MAX_ATTRIBUTE_NAME_BYTES
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}
