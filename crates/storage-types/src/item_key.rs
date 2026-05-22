use std::collections::HashMap;

use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    AttributeDefinition, AttributeValue, AttributeValueLookup, IndexName, KeyAttributeType,
    KeyAttributes, KeySchemaElement, KeyType, StoredTableInfo, TableName, err_context,
};

#[derive(Debug, Error)]
pub enum ItemKeyEnum {
    #[error("Item key not a number")]
    Serialization(String),
    #[error("Item key not a number")]
    Deserialization(String),
    #[error("Item key validation error: {0}")]
    Validation(String),
}

err_context!(ItemKeyError, ItemKeyEnum);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableKey {
    pub table_name: TableName,
    pub hash_key: AttributeValue,
    pub range_key: Option<AttributeValue>,
}

impl TableKey {
    #[must_use]
    pub fn new(
        table_name: TableName,
        hash_key: AttributeValue,
        range_key: Option<AttributeValue>,
    ) -> Self {
        Self {
            table_name,
            hash_key,
            range_key,
        }
    }

    pub fn from_key_schema(
        table_name: TableName,
        key_schema: &[KeySchemaElement],
        item: &impl AttributeValueLookup,
    ) -> Result<Self, ItemKeyError> {
        let (hash_key, range_key) = keys_for_schema(key_schema, item)?;
        Ok(Self::new(table_name, hash_key, range_key))
    }

    /// NOTE: Not sorted! Use for page token only
    pub fn hash_range_key_part(&self) -> Result<Vec<u8>, ItemKeyError> {
        hash_range_key_part_for_values(&self.hash_key, self.range_key.as_ref())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexKey {
    pub table_name: TableName,
    pub index_id: IndexName,
    pub hash_key: AttributeValue,
    pub range_key: Option<AttributeValue>,
    pub table_key: TableKey,
}

impl IndexKey {
    #[must_use]
    pub fn new(
        table_name: TableName,
        index_id: IndexName,
        hash_key: AttributeValue,
        range_key: Option<AttributeValue>,
        table_key: TableKey,
    ) -> Self {
        Self {
            table_name,
            index_id,
            hash_key,
            range_key,
            table_key,
        }
    }

    pub fn from_key_schema_for_index(
        table_name: TableName,
        table_key_schema: &[KeySchemaElement],
        index_id: &IndexName,
        index_key_schema: &[KeySchemaElement],
        item: &impl AttributeValueLookup,
    ) -> Result<Option<Self>, ItemKeyError> {
        let Ok((hash_key, range_key)) = keys_for_schema(index_key_schema, item) else {
            // If the index key schema is not satisfied, return None
            return Ok(None);
        };

        let table_key = TableKey::from_key_schema(table_name.clone(), table_key_schema, item)?;

        Ok(Some(Self::new(
            table_name,
            index_id.clone(),
            hash_key,
            range_key,
            table_key,
        )))
    }

    /// NOTE: Not sorted! Use for page token only
    pub fn hash_range_key_part(&self) -> Result<Vec<u8>, ItemKeyError> {
        let mut parts = hash_range_key_part_for_values(&self.hash_key, self.range_key.as_ref())?;
        parts.extend(self.table_key.hash_range_key_part()?);
        Ok(parts)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexKeyPrefix {
    pub table_name: TableName,
    pub index_id: IndexName,
    pub hash_key: AttributeValue,
    pub range_key: Option<AttributeValue>,
}

impl IndexKeyPrefix {
    #[must_use]
    pub fn new(
        table_name: TableName,
        index_id: IndexName,
        hash_key: AttributeValue,
        range_key: Option<AttributeValue>,
    ) -> Self {
        Self {
            table_name,
            index_id,
            hash_key,
            range_key,
        }
    }

    /// NOTE: Not sorted! Use for page token only
    pub fn hash_range_key_part(&self) -> Result<Vec<u8>, ItemKeyError> {
        hash_range_key_part_for_values(&self.hash_key, self.range_key.as_ref())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ItemKey {
    Table(TableKey),
    Index(IndexKey),
    IndexPrefix(IndexKeyPrefix),
}

impl From<TableKey> for ItemKey {
    fn from(value: TableKey) -> Self {
        Self::Table(value)
    }
}

impl From<IndexKey> for ItemKey {
    fn from(value: IndexKey) -> Self {
        Self::Index(value)
    }
}

impl From<IndexKeyPrefix> for ItemKey {
    fn from(value: IndexKeyPrefix) -> Self {
        Self::IndexPrefix(value)
    }
}

impl ItemKey {
    #[must_use]
    pub fn table_key(
        table_name: TableName,
        hash_key: AttributeValue,
        range_key: Option<AttributeValue>,
    ) -> Self {
        TableKey::new(table_name, hash_key, range_key).into()
    }

    #[must_use]
    pub fn index_key(
        table_name: TableName,
        index_id: IndexName,
        hash_key: AttributeValue,
        range_key: Option<AttributeValue>,
        table_key: TableKey,
    ) -> Self {
        IndexKey::new(table_name, index_id, hash_key, range_key, table_key).into()
    }

    #[must_use]
    pub fn index_prefix(
        table_name: TableName,
        index_id: IndexName,
        hash_key: AttributeValue,
        range_key: Option<AttributeValue>,
    ) -> Self {
        IndexKeyPrefix::new(table_name, index_id, hash_key, range_key).into()
    }

    pub fn from_key_schema(
        table_name: TableName,
        key_schema: &[KeySchemaElement],
        item: &impl AttributeValueLookup,
    ) -> Result<Self, ItemKeyError> {
        TableKey::from_key_schema(table_name, key_schema, item).map(ItemKey::Table)
    }

    pub fn from_key_schema_for_index(
        table_name: TableName,
        table_key_schema: &[KeySchemaElement],
        index_id: &IndexName,
        index_key_schema: &[KeySchemaElement],
        item: &impl AttributeValueLookup,
    ) -> Result<Option<Self>, ItemKeyError> {
        IndexKey::from_key_schema_for_index(
            table_name,
            table_key_schema,
            index_id,
            index_key_schema,
            item,
        )
        .map(|key| key.map(ItemKey::Index))
    }

    #[must_use]
    pub fn table_name(&self) -> &TableName {
        match self {
            ItemKey::Table(key) => &key.table_name,
            ItemKey::Index(key) => &key.table_name,
            ItemKey::IndexPrefix(key) => &key.table_name,
        }
    }

    #[must_use]
    pub fn index_id(&self) -> Option<&IndexName> {
        match self {
            ItemKey::Index(key) => Some(&key.index_id),
            ItemKey::IndexPrefix(key) => Some(&key.index_id),
            ItemKey::Table(_) => None,
        }
    }

    #[must_use]
    pub fn hash_key(&self) -> &AttributeValue {
        match self {
            ItemKey::Table(key) => &key.hash_key,
            ItemKey::Index(key) => &key.hash_key,
            ItemKey::IndexPrefix(key) => &key.hash_key,
        }
    }

    #[must_use]
    pub fn range_key(&self) -> Option<&AttributeValue> {
        match self {
            ItemKey::Table(key) => key.range_key.as_ref(),
            ItemKey::Index(key) => key.range_key.as_ref(),
            ItemKey::IndexPrefix(key) => key.range_key.as_ref(),
        }
    }

    #[must_use]
    pub fn table_key_ref(&self) -> Option<&TableKey> {
        match self {
            ItemKey::Index(key) => Some(&key.table_key),
            ItemKey::Table(_) | ItemKey::IndexPrefix(_) => None,
        }
    }

    /// NOTE: Not sorted! Use for page token only
    pub fn hash_range_key_part(&self) -> Result<Vec<u8>, ItemKeyError> {
        match self {
            ItemKey::Table(key) => key.hash_range_key_part(),
            ItemKey::Index(key) => key.hash_range_key_part(),
            ItemKey::IndexPrefix(key) => key.hash_range_key_part(),
        }
    }

    pub fn next_page_token(&self) -> Result<String, ItemKeyError> {
        let parts = self.hash_range_key_part()?;
        Ok(URL_SAFE.encode(parts))
    }

    pub fn add_length_prefixed_part(parts: &mut Vec<u8>, data: &[u8]) {
        // Use 2 bytes: 10 bits for length, 2 bits for version (00), 4 bits reserved for
        // flags. Cap length at 1023 and convert safely.
        let length_u16 = u16::try_from(data.len()).unwrap_or(u16::MAX);
        let length = length_u16.min(1023);
        // Version is 00 (0), flags are 0000 (0)
        let prefix = length << 6; // Shift length to upper 10 bits, version/flags = 0
        parts.extend(prefix.to_be_bytes());
        parts.extend(data);
    }

    pub fn item_key_from_next_page_token(
        next_page_token: &str,
        table_info: &StoredTableInfo,
        index_name: &Option<IndexName>,
    ) -> Result<Option<Self>, ItemKeyError> {
        let decoded = URL_SAFE.decode(next_page_token).map_err(|_error| {
            ItemKeyEnum::Deserialization("Invalid next page token".to_string())
        })?;
        let decoded_parts = split_next_token_to_keys(&decoded);

        let mut keys = KeyAttributes::new();

        if let Some(index_name) = index_name {
            // For GSI, expect 2-4 parts: gsi_hash, optional gsi_range, table_hash, optional
            // table_range
            if decoded_parts.len() < 2 || decoded_parts.len() > 4 {
                return Err(invalid_token());
            }

            let index_key_schema = table_info
                .global_secondary_indexes
                .as_ref()
                .and_then(|indexes| indexes.iter().find(|i| i.index_name == *index_name))
                .map_or(&table_info.key_schema, |idx| &idx.key_schema);

            // GSI hash key (first part)
            let gsi_hash_schema = index_key_schema
                .iter()
                .find(|k| k.key_type == KeyType::Hash)
                .ok_or_else(|| {
                    ItemKeyEnum::Validation("Missing GSI hash key schema".to_string())
                })?;
            let gsi_hash_string = token_part_str(&decoded_parts[0])?;
            insert_attribute_string_to_map(
                &mut keys,
                gsi_hash_string,
                gsi_hash_schema,
                &table_info.attribute_definitions,
            );

            let mut part_index = 1;

            // GSI range key (second part, if present)
            let gsi_range_schema = index_key_schema
                .iter()
                .find(|k| k.key_type == KeyType::Range);
            if let Some(gsi_range_schema) = gsi_range_schema {
                if part_index >= decoded_parts.len() {
                    return Err(invalid_token());
                }
                let gsi_range_string = token_part_str(&decoded_parts[part_index])?;
                insert_attribute_string_to_map(
                    &mut keys,
                    gsi_range_string,
                    gsi_range_schema,
                    &table_info.attribute_definitions,
                );
                part_index += 1;
            }

            // Table hash key (third part)
            if part_index >= decoded_parts.len() {
                return Err(invalid_token());
            }
            let table_hash_schema = table_info
                .key_schema
                .iter()
                .find(|k| k.key_type == KeyType::Hash)
                .ok_or_else(|| {
                    ItemKeyEnum::Validation("Missing table hash key schema".to_string())
                })?;
            let table_hash_string = token_part_str(&decoded_parts[part_index])?;
            insert_attribute_string_to_map(
                &mut keys,
                table_hash_string,
                table_hash_schema,
                &table_info.attribute_definitions,
            );
            part_index += 1;

            // Table range key (fourth part, if present)
            let table_range_schema = table_info
                .key_schema
                .iter()
                .find(|k| k.key_type == KeyType::Range);
            if let Some(table_range_schema) = table_range_schema {
                if part_index >= decoded_parts.len() {
                    return Err(invalid_token());
                }
                let table_range_string = token_part_str(&decoded_parts[part_index])?;
                insert_attribute_string_to_map(
                    &mut keys,
                    table_range_string,
                    table_range_schema,
                    &table_info.attribute_definitions,
                );
            }

            Ok(Self::from_key_schema_for_index(
                table_info.table_name.clone(),
                &table_info.key_schema,
                index_name,
                index_key_schema,
                &keys,
            )?)
        } else {
            // For table, expect 1-2 parts: hash_key, optional range_key
            if decoded_parts.is_empty() || decoded_parts.len() > 2 {
                return Err(invalid_token());
            }

            // Table hash key (first part)
            let hash_schema = table_info
                .key_schema
                .iter()
                .find(|k| k.key_type == KeyType::Hash)
                .ok_or_else(|| {
                    ItemKeyEnum::Validation("Missing table hash key schema".to_string())
                })?;
            let hash_string = token_part_str(&decoded_parts[0])?;
            insert_attribute_string_to_map(
                &mut keys,
                hash_string,
                hash_schema,
                &table_info.attribute_definitions,
            );

            // Table range key (second part, if present)
            let range_schema = table_info
                .key_schema
                .iter()
                .find(|k| k.key_type == KeyType::Range);
            if let Some(range_schema) = range_schema {
                if decoded_parts.len() < 2 {
                    return Err(invalid_token());
                }
                let range_string = token_part_str(&decoded_parts[1])?;
                insert_attribute_string_to_map(
                    &mut keys,
                    range_string,
                    range_schema,
                    &table_info.attribute_definitions,
                );
            }

            let item_key = Self::from_key_schema(
                table_info.table_name.clone(),
                &table_info.key_schema,
                &keys,
            )?;
            Ok(Some(item_key))
        }
    }

    pub fn last_evaluated_key_from_last_item(
        last_item: &HashMap<String, AttributeValue>,
        table_info: &StoredTableInfo,
        index_name: &Option<IndexName>,
    ) -> Result<Option<String>, ItemKeyError> {
        if let Some(index_name) = index_name {
            let index_key_schema = table_info
                .global_secondary_indexes
                .as_ref()
                .and_then(|indexes| indexes.iter().find(|i| i.index_name == *index_name))
                .map_or(&table_info.key_schema, |idx| &idx.key_schema);

            let Ok(Some(key)) = IndexKey::from_key_schema_for_index(
                TableName::new(""),
                &table_info.key_schema,
                index_name,
                index_key_schema,
                last_item,
            ) else {
                return Ok(None);
            };

            // For GSI, encode up to 4 parts: gsi_hash, optional gsi_range, table_hash,
            // optional table_range
            let mut parts = Vec::new();

            // Add GSI hash key
            let gsi_hash_string = scalar_key_str(&key.hash_key)?;
            let gsi_hash_bytes = gsi_hash_string.as_bytes();
            Self::add_length_prefixed_part(&mut parts, gsi_hash_bytes);

            // Add GSI range key if present
            if let Some(gsi_range_key) = &key.range_key {
                let gsi_range_string = scalar_key_str(gsi_range_key)?;
                let gsi_range_bytes = gsi_range_string.as_bytes();
                Self::add_length_prefixed_part(&mut parts, gsi_range_bytes);
            }

            // Add table hash key
            let table_hash_string = scalar_key_str(&key.table_key.hash_key)?;
            let table_hash_bytes = table_hash_string.as_bytes();
            Self::add_length_prefixed_part(&mut parts, table_hash_bytes);

            // Add table range key if present
            if let Some(table_range_key) = &key.table_key.range_key {
                let table_range_string = scalar_key_str(table_range_key)?;
                let table_range_bytes = table_range_string.as_bytes();
                Self::add_length_prefixed_part(&mut parts, table_range_bytes);
            }

            let base64_encoded = URL_SAFE.encode(&parts);
            Ok(Some(base64_encoded))
        } else {
            let Ok(key) =
                TableKey::from_key_schema(TableName::new(""), &table_info.key_schema, last_item)
            else {
                return Ok(None);
            };

            // For table, encode up to 2 parts: hash_key, optional range_key
            let mut parts = Vec::new();

            // Add hash key
            let hash_string = scalar_key_str(&key.hash_key)?;
            let hash_bytes = hash_string.as_bytes();
            Self::add_length_prefixed_part(&mut parts, hash_bytes);

            // Add range key if present
            if let Some(range_key) = &key.range_key {
                let range_string = scalar_key_str(range_key)?;
                let range_bytes = range_string.as_bytes();
                Self::add_length_prefixed_part(&mut parts, range_bytes);
            }

            let base64_encoded = URL_SAFE.encode(&parts);
            Ok(Some(base64_encoded))
        }
    }
}

fn hash_range_key_part_for_values(
    hash_key: &AttributeValue,
    range_key: Option<&AttributeValue>,
) -> Result<Vec<u8>, ItemKeyError> {
    let mut parts = Vec::new();

    let hash_string = scalar_key_str(hash_key)?;
    let hash_bytes = hash_string.as_bytes();
    ItemKey::add_length_prefixed_part(&mut parts, hash_bytes);

    if let Some(range_key) = range_key {
        let range_string = scalar_key_str(range_key)?;
        let range_bytes = range_string.as_bytes();
        ItemKey::add_length_prefixed_part(&mut parts, range_bytes);
    }

    Ok(parts)
}

fn scalar_key_str(value: &AttributeValue) -> Result<&str, ItemKeyError> {
    value.inner_str().map_err(|err| {
        ItemKeyEnum::Validation(format!("Key attribute must be scalar: {err}")).into()
    })
}

fn token_part_str(value: &[u8]) -> Result<&str, ItemKeyError> {
    std::str::from_utf8(value).map_err(|_| invalid_token())
}

pub(crate) fn keys_for_schema(
    key_schema: &[KeySchemaElement],
    item: &impl AttributeValueLookup,
) -> Result<(AttributeValue, Option<AttributeValue>), ItemKeyError> {
    let mut hash_key = None;
    let mut range_key = None;
    for key_element in key_schema {
        let Some(attr_value) = item.get_attribute_value(&key_element.attribute_name) else {
            return Err(ItemKeyEnum::Validation(format!(
                "Missing key attribute: {}",
                key_element.attribute_name
            )))?;
        };

        match key_element.key_type {
            KeyType::Hash => {
                hash_key = Some(attr_value.clone());
            }
            KeyType::Range => {
                range_key = Some(attr_value.clone());
            }
        }
    }
    let hash_key =
        hash_key.ok_or_else(|| ItemKeyEnum::Validation("Missing hash key schema".to_string()))?;
    Ok((hash_key, range_key))
}

pub(crate) fn split_next_token_to_keys(key: &[u8]) -> Vec<Vec<u8>> {
    let mut parts = Vec::new();
    let mut pos = 0;

    while pos < key.len() {
        if pos + 2 > key.len() {
            // Not enough bytes for prefix, add remaining as one part
            parts.push(key[pos..].to_vec());
            break;
        }

        // Read 2-byte prefix
        let prefix = u16::from_be_bytes([key[pos], key[pos + 1]]);
        let length = (prefix >> 6) as usize; // Extract upper 10 bits for length
        pos += 2;

        if pos + length > key.len() {
            // Length exceeds remaining bytes, add remaining as one part
            parts.push(key[pos - 2..].to_vec());
            break;
        }

        // Extract the data part
        parts.push(key[pos..pos + length].to_vec());
        pos += length;
    }

    parts
}

pub(crate) fn string_to_attribute_value(
    s: &str,
    attribute_type: &KeyAttributeType,
) -> AttributeValue {
    match attribute_type {
        crate::KeyAttributeType::S => AttributeValue::S(s.to_string()),
        crate::KeyAttributeType::N => AttributeValue::N(s.to_string()),
        crate::KeyAttributeType::B => AttributeValue::B(s.to_string()),
    }
}

pub(crate) fn insert_attribute_string_to_map(
    map: &mut KeyAttributes,
    value: &str,
    key_schema: &KeySchemaElement,
    attribute_definitions: &[AttributeDefinition],
) {
    let attribute_type = attribute_definitions
        .iter()
        .find(|def| def.attribute_name == key_schema.attribute_name)
        .map_or(&KeyAttributeType::S, |def| &def.attribute_type); // Default to S if not found

    let attribute_value = string_to_attribute_value(value, attribute_type);
    map.insert(key_schema.attribute_name.clone(), attribute_value);
}

pub(crate) fn invalid_token() -> ItemKeyError {
    ItemKeyEnum::Deserialization("Invalid next page token".to_string()).into()
}
