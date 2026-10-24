//! Canonical ordered primary and projection identities shared by all providers.

use crate::{
    AttributeValue, IndexKey, IndexKeyPrefix, ItemKey, TableKey,
    item_key::{ItemKeyEnum, ItemKeyError},
};

impl ItemKey {
    pub fn sorted_storage_suffix(&self) -> Result<Vec<u8>, ItemKeyError> {
        match self {
            ItemKey::Table(key) => key.sorted_storage_suffix(),
            ItemKey::Index(key) => key.sorted_storage_suffix(),
            ItemKey::IndexPrefix(key) => key.sorted_storage_suffix(),
        }
    }

    pub fn sorted_storage_prefix(&self) -> Result<Vec<u8>, ItemKeyError> {
        match self {
            ItemKey::IndexPrefix(key) if key.range_key.is_some() => {
                key.sorted_storage_range_prefix()
            }
            _ => self.sorted_storage_suffix(),
        }
    }
}

impl TableKey {
    pub fn sorted_storage_suffix(&self) -> Result<Vec<u8>, ItemKeyError> {
        serialize_key_part_values(&self.hash_key, self.range_key.as_ref())
    }
}

impl IndexKeyPrefix {
    pub fn sorted_storage_suffix(&self) -> Result<Vec<u8>, ItemKeyError> {
        serialize_index_key_part_values(&self.hash_key, self.range_key.as_ref())
    }

    fn sorted_storage_range_prefix(&self) -> Result<Vec<u8>, ItemKeyError> {
        let mut bytes = self.sorted_storage_suffix()?;
        // The final zero terminates a complete GSI range value. A prefix
        // scan instead includes every continuation of the encoded value.
        let terminator = bytes.pop();
        debug_assert_eq!(terminator, Some(0));
        Ok(bytes)
    }
}

impl IndexKey {
    pub fn sorted_storage_suffix(&self) -> Result<Vec<u8>, ItemKeyError> {
        let mut key_parts =
            serialize_index_key_part_values(&self.hash_key, self.range_key.as_ref())?;
        let table_key_bytes = self.table_key.sorted_storage_suffix()?;
        ItemKey::append_length_prefixed_key(&mut key_parts, &table_key_bytes)?;
        Ok(key_parts)
    }
}

pub(super) fn serialize_key_part_values(
    hash_key: &AttributeValue,
    range_key: Option<&AttributeValue>,
) -> Result<Vec<u8>, ItemKeyError> {
    let mut parts = Vec::new();

    append_length_prefixed_attribute_value(&mut parts, hash_key)?;

    if let Some(range_key) = range_key {
        append_attribute_value(&mut parts, range_key)?;
    }

    Ok(parts)
}

pub(super) fn serialize_index_key_part_values(
    hash_key: &AttributeValue,
    range_key: Option<&AttributeValue>,
) -> Result<Vec<u8>, ItemKeyError> {
    let mut parts = Vec::new();
    append_length_prefixed_attribute_value(&mut parts, hash_key)?;
    if let Some(range_key) = range_key {
        for byte in serialize_attribute_value(range_key)? {
            // Keep the terminator distinct from every encoded value byte so
            // a complete GSI range value is never a prefix of its extension.
            parts.push(1);
            parts.push(byte);
        }
        parts.push(0);
    }
    Ok(parts)
}

fn append_length_prefixed_attribute_value(
    parts: &mut Vec<u8>,
    attribute_value: &AttributeValue,
) -> Result<(), ItemKeyError> {
    let prefix_start = parts.len();
    parts.extend_from_slice(&[0; 2]);
    let value_start = parts.len();
    append_attribute_value(parts, attribute_value)?;
    let length = u16::try_from(parts.len() - value_start).map_err(|_| {
        ItemKeyEnum::Validation("Key attribute exceeds the ordered key format".into())
    })?;
    parts[prefix_start..value_start].copy_from_slice(&length.to_be_bytes());
    Ok(())
}

impl ItemKey {
    pub(crate) fn append_length_prefixed_key(
        parts: &mut Vec<u8>,
        data: &[u8],
    ) -> Result<(), ItemKeyError> {
        let length = u16::try_from(data.len()).map_err(|_| {
            ItemKeyEnum::Validation("Composite key exceeds the ordered key format".into())
        })?;
        parts.extend(length.to_be_bytes());
        parts.extend(data);
        Ok(())
    }
}

pub(super) fn serialize_attribute_value(
    attribute_value: &AttributeValue,
) -> Result<Vec<u8>, ItemKeyError> {
    let mut bytes = Vec::new();
    append_attribute_value(&mut bytes, attribute_value)?;
    Ok(bytes)
}

fn append_attribute_value(
    bytes: &mut Vec<u8>,
    attribute_value: &AttributeValue,
) -> Result<(), ItemKeyError> {
    match attribute_value {
        AttributeValue::S(s) => {
            bytes.extend_from_slice(s.as_bytes());
            Ok(())
        }
        AttributeValue::B(encoded) => {
            let decoded = crate::dynamodb_binary::decode_base64_string(encoded)
                .map_err(|error| ItemKeyEnum::Deserialization(error.to_string()))?;
            bytes.extend_from_slice(&decoded);
            Ok(())
        }
        AttributeValue::N(n) => crate::dynamodb_number::append_dynamodb_number_key_bytes(n, bytes)
            .map_err(|message| ItemKeyEnum::Deserialization(message).into()),
        _ => Err(ItemKeyEnum::Validation(
            "Only S, N, and B types are supported for keys".to_string(),
        )
        .into()),
    }
}
