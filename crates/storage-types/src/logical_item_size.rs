use crate::{
    AttributeValue, StorageError, StorageResult,
    constants::MAX_ITEM_SIZE_BYTES,
    dynamodb_limits::{
        dynamodb_number_size, validate_attribute_value, validate_item_attribute_name,
    },
};

/// UpdateItem's text for a result item over the size limit.
pub const UPDATE_ITEM_TOO_LARGE_MESSAGE: &str =
    "Item size to update has exceeded the maximum allowed size";

/// The text of an item over the size limit.
pub(crate) const ITEM_TOO_LARGE_MESSAGE: &str = "Item size has exceeded the maximum allowed size";

/// Validated DynamoDB logical item size in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalItemSize(u64);

impl LogicalItemSize {
    pub fn try_from_entries<'a>(
        entries: impl IntoIterator<Item = (&'a str, &'a AttributeValue)>,
    ) -> StorageResult<Self> {
        Self::try_from_entries_with_field(entries, "Item")
    }

    /// The size of an item an UpdateItem produced; AWS refuses an oversized
    /// result with its own text.
    pub fn try_from_updated_entries<'a>(
        entries: impl IntoIterator<Item = (&'a str, &'a AttributeValue)>,
    ) -> StorageResult<Self> {
        let size = Self::entries_size(entries, "Item")?;
        Self::from_checked_size_with(size, UPDATE_ITEM_TOO_LARGE_MESSAGE)
    }

    pub(crate) fn try_from_entries_with_field<'a>(
        entries: impl IntoIterator<Item = (&'a str, &'a AttributeValue)>,
        field: &str,
    ) -> StorageResult<Self> {
        Self::from_checked_size(Self::entries_size(entries, field)?)
    }

    fn entries_size<'a>(
        entries: impl IntoIterator<Item = (&'a str, &'a AttributeValue)>,
        field: &str,
    ) -> StorageResult<u64> {
        entries.into_iter().try_fold(0_u64, |size, (name, value)| {
            validate_item_attribute_name(name, || format!("{field} attribute name"))
                .map_err(StorageError::validation)?;
            validate_attribute_value(value, 0, field).map_err(StorageError::validation)?;
            let value_size = attribute_value_size(value)?;
            let name_size = checked_len(name.len())?;
            checked_add(checked_add(size, name_size)?, value_size)
        })
    }

    /// Count logical bytes in attributes already validated by their owning
    /// write boundary. Unlike an item, a stream record can contain several
    /// complete images.
    pub fn sum_attribute_bytes<'a>(
        entries: impl IntoIterator<Item = (&'a str, &'a AttributeValue)>,
    ) -> StorageResult<u64> {
        entries.into_iter().try_fold(0_u64, |size, (name, value)| {
            let value_size = attribute_value_size(value)?;
            checked_add(checked_add(size, checked_len(name.len())?)?, value_size)
        })
    }

    pub(crate) fn from_dynamo_json(
        bytes: &[u8],
        field: &str,
        excluded_names: &[&str],
    ) -> StorageResult<Self> {
        Self::from_checked_size(Self::size_from_dynamo_json(bytes, field, excluded_names)?)
    }

    pub(crate) fn size_from_dynamo_json(
        bytes: &[u8],
        field: &str,
        excluded_names: &[&str],
    ) -> StorageResult<u64> {
        crate::wire_item::write::inspect_write_attributes::<false>(bytes, field, excluded_names)
            .map(|(size, _)| size)
            .map_err(|error| {
                StorageError::internal(&format!("decode wire item logical size failed: {error}"))
            })
    }

    fn from_checked_size(size: u64) -> StorageResult<Self> {
        Self::from_checked_size_with(size, ITEM_TOO_LARGE_MESSAGE)
    }

    fn from_checked_size_with(size: u64, too_large_message: &str) -> StorageResult<Self> {
        if size == 0 {
            return Err(StorageError::validation(
                "logical item size must be positive",
            ));
        }
        if size > MAX_ITEM_SIZE_BYTES as u64 {
            return Err(StorageError::validation(too_large_message));
        }
        Ok(Self(size))
    }
}

impl TryFrom<u64> for LogicalItemSize {
    type Error = StorageError;

    fn try_from(value: u64) -> StorageResult<Self> {
        Self::from_checked_size(value)
    }
}

impl TryFrom<i64> for LogicalItemSize {
    type Error = StorageError;

    fn try_from(value: i64) -> StorageResult<Self> {
        let value = u64::try_from(value)
            .map_err(|_| StorageError::validation("logical item size must be positive"))?;
        Self::try_from(value)
    }
}

impl From<LogicalItemSize> for u64 {
    fn from(value: LogicalItemSize) -> Self {
        value.0
    }
}

impl From<LogicalItemSize> for i64 {
    fn from(value: LogicalItemSize) -> Self {
        value.0 as i64
    }
}

fn attribute_value_size(value: &AttributeValue) -> StorageResult<u64> {
    match value {
        AttributeValue::S(value) => checked_len(value.len()),
        AttributeValue::N(value) => checked_len(dynamodb_number_size(value)),
        AttributeValue::B(value) => binary_size(value),
        AttributeValue::SS(values) => values.iter().try_fold(0_u64, |size, value| {
            checked_add(size, checked_len(value.len())?)
        }),
        AttributeValue::NS(values) => values.iter().try_fold(0_u64, |size, value| {
            checked_add(size, checked_len(dynamodb_number_size(value))?)
        }),
        AttributeValue::BS(values) => values
            .iter()
            .try_fold(0_u64, |size, value| checked_add(size, binary_size(value)?)),
        AttributeValue::BOOL(_) | AttributeValue::NULL(_) => Ok(1),
        AttributeValue::L(values) => {
            let size = checked_add(3, checked_len(values.len())?)?;
            values.iter().try_fold(size, |size, value| {
                checked_add(size, attribute_value_size(value)?)
            })
        }
        AttributeValue::M(values) => {
            let size = checked_add(3, checked_len(values.len())?)?;
            values.iter().try_fold(size, |size, (name, value)| {
                let size = checked_add(size, checked_len(name.len())?)?;
                checked_add(size, attribute_value_size(value)?)
            })
        }
    }
}

// Attribute values have already passed recursive Base64 validation. Count
// the decoded bytes from data characters, including unpadded final groups.
fn binary_size(value: &str) -> StorageResult<u64> {
    let digits = value.trim_end_matches('=').len();
    checked_len(digits / 4 * 3 + digits % 4 * 3 / 4)
}

fn checked_len(value: usize) -> StorageResult<u64> {
    u64::try_from(value)
        .map_err(|_| StorageError::internal(&"logical item size length conversion overflowed"))
}

fn checked_add(left: u64, right: u64) -> StorageResult<u64> {
    left.checked_add(right)
        .ok_or_else(|| StorageError::internal(&"logical item size arithmetic overflowed"))
}
