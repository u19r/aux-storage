use storage_types::UserStreamName;
use stream_provider::{CursorName, StreamError, StreamResult, StreamValidationKind};

use crate::constants::{
    CURSOR_NAME_ALLOWED_MIDDLE_CHARS, MAX_CURSOR_NAME_LEN, MAX_STREAM_ITEM_DATA_BYTES,
    MAX_STREAM_NAME_LEN, MAX_STREAM_TTL_SECONDS, STREAM_NAME_ALLOWED_MIDDLE_CHARS,
};

pub(crate) trait UserStreamNameValidation {
    fn validate_stream_name(&self) -> StreamResult<()>;
}

impl UserStreamNameValidation for UserStreamName {
    fn validate_stream_name(&self) -> StreamResult<()> {
        let name = self.as_str();

        validate_not_empty(name, "Stream name", MAX_STREAM_NAME_LEN)?;
        validate_name_format(name, "Stream name", STREAM_NAME_ALLOWED_MIDDLE_CHARS)?;

        Ok(())
    }
}

pub(crate) trait CursorNameValidation {
    fn validate_cursor_name(&self) -> StreamResult<()>;
}

impl CursorNameValidation for CursorName {
    fn validate_cursor_name(&self) -> StreamResult<()> {
        let name = self.as_str();

        validate_not_empty(name, "Cursor name", MAX_CURSOR_NAME_LEN)?;
        validate_name_format(name, "Cursor name", CURSOR_NAME_ALLOWED_MIDDLE_CHARS)?;

        Ok(())
    }
}

/// Validate that a string is not empty and meets length requirements
pub fn validate_not_empty(value: &str, field_name: &str, max_length: usize) -> StreamResult<()> {
    if value.is_empty() {
        return Err(StreamError::validation_with_detail(
            StreamValidationKind::EmptyName,
            format_args!("{field_name} cannot be empty"),
        ));
    }

    if value.len() > max_length {
        return Err(StreamError::validation_with_detail(
            StreamValidationKind::NameTooLong,
            format_args!("{field_name} must not exceed {max_length} characters"),
        ));
    }

    Ok(())
}

/// Validate that a string starts and ends with alphanumeric characters
/// and contains only allowed characters in the middle
pub fn validate_name_format(
    value: &str,
    field_name: &str,
    allowed_middle_chars: &[char],
) -> StreamResult<()> {
    let chars: Vec<char> = value.chars().collect();

    for (i, ch) in chars.iter().enumerate() {
        if i == 0 || i == chars.len() - 1 {
            // First and last characters must be alphanumeric
            if !ch.is_alphanumeric() {
                return Err(StreamError::validation_with_detail(
                    StreamValidationKind::InvalidNameBoundary,
                    format_args!("{field_name} must start and end with alphanumeric characters"),
                ));
            }
        } else {
            // Middle characters can be alphanumeric or in the allowed list
            if !ch.is_alphanumeric() && !allowed_middle_chars.contains(ch) {
                let allowed_chars_str = allowed_middle_chars
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                let allowed_suffix = if allowed_chars_str.is_empty() {
                    String::new()
                } else {
                    format!(", {allowed_chars_str}")
                };
                return Err(StreamError::validation_with_detail(
                    StreamValidationKind::InvalidNameCharacters,
                    format_args!(
                        "{field_name} can only contain alphanumeric characters{allowed_suffix}"
                    ),
                ));
            }
        }
    }

    Ok(())
}

/// Validate TTL bounds (1 second to 1 year)
pub fn validate_ttl_seconds(ttl_seconds: u32) -> StreamResult<()> {
    if ttl_seconds == 0 || ttl_seconds > MAX_STREAM_TTL_SECONDS {
        return Err(StreamError::validation_with_detail(
            StreamValidationKind::InvalidTtl,
            "TTL must be between 1 second and 1 year (31,536,000 seconds)",
        ));
    }
    Ok(())
}

/// Validate item data size (max 1MB)
pub fn validate_item_data_size(data: &[u8]) -> StreamResult<()> {
    if data.len() > MAX_STREAM_ITEM_DATA_BYTES {
        return Err(StreamError::validation_with_detail(
            StreamValidationKind::ItemDataTooLarge,
            "Item data cannot exceed 1 MiB (1,048,576 bytes)",
        ));
    }

    if data.is_empty() {
        return Err(StreamError::validation_with_detail(
            StreamValidationKind::ItemDataEmpty,
            "Item data cannot be empty",
        ));
    }

    Ok(())
}
