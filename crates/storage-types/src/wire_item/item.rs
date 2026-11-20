use std::{borrow::Cow, collections::HashMap};

use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{
    AttributeMap, AttributeValue, IndexName, ItemKey, KeySchemaElement, KeyType, KeysAndAttributes,
    LogicalItemSize, StorageError, StorageResult, StoredTableInfo, TableName, TimestampMillis,
    TimestampSeconds,
    constants::{
        CREATED_AT_ALIAS_ATTR, CREATED_AT_ATTR, ENTITY_TYPE_ATTR, ENTITY_TYPE_NAME_ALIAS,
        EXPIRES_AT_ALIAS_ATTR, EXPIRES_AT_ATTR, UPDATED_AT_ALIAS_ATTR, UPDATED_AT_ATTR,
    },
    verify_key_attributes,
    wire_item::projection::{
        encode_dynamo_attribute_values, parse_complete_dynamo_attribute_value_field,
        parse_dynamo_attribute_fields, parse_dynamo_attribute_value_field,
        parse_dynamo_attribute_values, parse_dynamo_bool_field, parse_dynamo_number_field_i64,
        parse_dynamo_scalar_fields,
        parse_dynamo_scalar_fields_with_aliases as parse_dynamo_scalar_fields_with_aliases_impl,
        parse_dynamo_string_field,
    },
};

#[derive(Clone, Serialize, Deserialize, Default)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct BatchGetWireItemResponse {
    pub responses: Option<crate::VecMap<TableName, Vec<WireItem>>>,
    pub unprocessed_keys: Option<crate::VecMap<TableName, KeysAndAttributes>>,
    pub consumed_capacity: Option<Vec<crate::ConsumedCapacity>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub enum WireItem {
    DynamoJson { data: Vec<u8> },
}

pub trait TryFromWireItem: Sized {
    fn try_from_wire_item(item: &WireItem) -> StorageResult<Self>;
}

pub trait TryIntoWireItem: Sized {
    fn try_into_wire_item(&self) -> StorageResult<WireItem>;
}

pub trait WireAttributeDecode: Sized {
    fn decode(raw: Option<&str>, field: &str) -> StorageResult<Self>;
}

pub fn encode_wire_attribute<T>(value: &T, field: &str) -> StorageResult<AttributeValue>
where T: Serialize {
    let json = serde_json::to_value(value).map_err(|err| {
        StorageError::internal(&format!(
            "encode wire attribute {field} into json value failed: {err}"
        ))
    })?;
    crate::attribute_value_from_json_value(json).map_err(|err| {
        StorageError::internal(&format!("encode wire attribute {field} failed: {err}"))
    })
}

/// Decode a DynamoDB boolean while distinguishing an absent field from an
/// explicitly null field and rejecting a present value with another type.
pub fn decode_wire_bool(item: &WireItem, field: &str) -> StorageResult<Option<bool>> {
    match item.attribute_value(field)? {
        None => Ok(None),
        Some(AttributeValue::BOOL(value)) => Ok(Some(value)),
        Some(AttributeValue::NULL(true)) => Ok(None),
        Some(_) => Err(StorageError::internal(&format!(
            "invalid {field} field: expected BOOL"
        ))),
    }
}

impl<T> WireAttributeDecode for Option<T>
where T: WireAttributeDecode
{
    fn decode(raw: Option<&str>, field: &str) -> StorageResult<Self> {
        match raw {
            Some(value) => T::decode(Some(value), field).map(Some),
            None => Ok(None),
        }
    }
}

impl WireAttributeDecode for String {
    fn decode(raw: Option<&str>, field: &str) -> StorageResult<Self> {
        required_raw_scalar(raw, field).map(ToString::to_string)
    }
}

impl WireAttributeDecode for bool {
    fn decode(raw: Option<&str>, field: &str) -> StorageResult<Self> {
        let raw = required_raw_scalar(raw, field)?;
        match raw {
            "true" => Ok(true),
            "false" => Ok(false),
            "1" => Ok(true),
            "0" => Ok(false),
            _ => Err(StorageError::internal(&format!(
                "invalid {field} field: expected boolean"
            ))),
        }
    }
}

impl WireAttributeDecode for Vec<u8> {
    fn decode(raw: Option<&str>, field: &str) -> StorageResult<Self> {
        crate::dynamodb_binary::parse_required_dynamo_binary(raw, field)
    }
}

impl WireAttributeDecode for Vec<String> {
    fn decode(raw: Option<&str>, field: &str) -> StorageResult<Self> {
        let raw = required_raw_scalar(raw, field)?;
        Ok(vec![raw.to_string()])
    }
}

macro_rules! impl_wire_attribute_decode_from_parse {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl WireAttributeDecode for $ty {
                fn decode(raw: Option<&str>, field: &str) -> StorageResult<Self> {
                    let raw = required_raw_scalar(raw, field)?;
                    raw.parse::<$ty>()
                        .map_err(|err| StorageError::internal(&format!("invalid {field} field: {err}")))
                }
            }
        )+
    };
}

impl_wire_attribute_decode_from_parse!(
    u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, f32, f64
);

impl WireAttributeDecode for TimestampMillis {
    fn decode(raw: Option<&str>, field: &str) -> StorageResult<Self> {
        let raw = required_raw_scalar(raw, field)?;
        let millis = raw.parse::<i64>().map_err(|err| {
            StorageError::internal(&format!("invalid {field} field milliseconds: {err}"))
        })?;
        Ok(TimestampMillis::from_timestamp(millis))
    }
}

impl WireAttributeDecode for TimestampSeconds {
    fn decode(raw: Option<&str>, field: &str) -> StorageResult<Self> {
        let raw = required_raw_scalar(raw, field)?;
        let seconds = raw.parse::<i64>().map_err(|err| {
            StorageError::internal(&format!(
                "invalid {field} field seconds (must be i64): {err}"
            ))
        })?;
        Ok(TimestampSeconds::from(seconds))
    }
}

impl WireAttributeDecode for Timestamp {
    fn decode(raw: Option<&str>, field: &str) -> StorageResult<Self> {
        let raw = required_raw_scalar(raw, field)?;
        parse_datetime_auto(raw, field)
    }
}

fn required_raw_scalar<'a>(raw: Option<&'a str>, field: &str) -> StorageResult<&'a str> {
    raw.ok_or_else(|| StorageError::internal(&format!("missing required field {field}")))
}

fn parse_datetime_auto(raw: &str, field: &str) -> StorageResult<Timestamp> {
    if let Ok(parsed) = raw.parse::<Timestamp>() {
        return Ok(parsed);
    }
    if let Ok(timestamp) = raw.parse::<i64>() {
        // Heuristic: 13+ digit epoch values are milliseconds, otherwise
        // seconds.
        let parsed = if raw.len() >= 13 {
            Timestamp::from_millisecond(timestamp)
        } else {
            Timestamp::from_second(timestamp)
        };
        return parsed.map_err(|_| {
            StorageError::internal(&format!(
                "invalid {field} field timestamp value: {timestamp}"
            ))
        });
    }
    Err(StorageError::internal(&format!(
        "invalid {field} field datetime format"
    )))
}

impl TryFromWireItem for AttributeMap {
    fn try_from_wire_item(item: &WireItem) -> StorageResult<Self> {
        Self::try_from(item)
    }
}

impl TryFromWireItem for HashMap<String, AttributeValue> {
    fn try_from_wire_item(item: &WireItem) -> StorageResult<Self> {
        AttributeMap::try_from(item).map(AttributeMap::into_hashmap)
    }
}

impl TryFrom<&WireItem> for AttributeMap {
    type Error = StorageError;

    fn try_from(item: &WireItem) -> StorageResult<Self> {
        match item {
            WireItem::DynamoJson { data } => AttributeMap::from_dynamo_json(data),
        }
    }
}

impl TryFrom<WireItem> for AttributeMap {
    type Error = StorageError;

    fn try_from(item: WireItem) -> StorageResult<Self> {
        match item {
            WireItem::DynamoJson { data } => AttributeMap::from_dynamo_json(&data),
        }
    }
}

impl TryFrom<&AttributeMap> for WireItem {
    type Error = StorageError;

    fn try_from(attributes: &AttributeMap) -> StorageResult<Self> {
        let data = serde_json::to_vec(attributes).map_err(|err| {
            StorageError::internal(&format!("encode wire item attributes failed: {err}"))
        })?;
        Ok(Self::DynamoJson { data })
    }
}

// Legacy SQL/KV writes and external callers still pass HashMap items. New
// operation paths use AttributeMap; remove this conversion with those callers.
impl TryFrom<&HashMap<String, AttributeValue>> for WireItem {
    type Error = StorageError;

    fn try_from(attributes: &HashMap<String, AttributeValue>) -> StorageResult<Self> {
        let data = serde_json::to_vec(attributes).map_err(|err| {
            StorageError::internal(&format!("encode wire item attributes failed: {err}"))
        })?;
        Ok(Self::DynamoJson { data })
    }
}

impl TryFrom<AttributeMap> for WireItem {
    type Error = StorageError;

    fn try_from(attributes: AttributeMap) -> StorageResult<Self> {
        Self::try_from(&attributes)
    }
}

impl TryIntoWireItem for HashMap<String, AttributeValue> {
    fn try_into_wire_item(&self) -> StorageResult<WireItem> {
        WireItem::try_from(self)
    }
}

impl TryIntoWireItem for WireItem {
    fn try_into_wire_item(&self) -> StorageResult<WireItem> {
        Ok(self.clone())
    }
}

impl WireItem {
    #[must_use]
    pub fn dynamo_json(data: Vec<u8>) -> Self {
        Self::DynamoJson { data }
    }

    /// Verify the addressed key without completing or rewriting the image.
    /// False means a component is missing; conflicting or invalid values fail.
    /// Canonical images must agree across split keys and residual attributes.
    pub fn has_verified_keys(&self, key: &crate::KeyAttributes) -> StorageResult<bool> {
        verify_key_attributes(key, |name| self.canonical_key_attribute_value(name))
    }

    fn canonical_key_attribute_value(&self, field: &str) -> StorageResult<Option<AttributeValue>> {
        match self {
            Self::DynamoJson { data } => parse_complete_dynamo_attribute_value_field(data, field),
        }
    }

    /// Complete a legacy item with its addressed key, rejecting a stored
    /// image whose key attributes identify another item. Complete matching
    /// wire items retain their original representation and allocation.
    /// This path retains its primary-key lookup; canonical images instead
    /// require `has_verified_keys` to verify agreement between represented
    /// keys.
    pub fn with_verified_keys(self, key: &crate::KeyAttributes) -> StorageResult<Self> {
        if self.has_complete_write_keys(key)? {
            return Ok(self);
        }

        self.complete_write_keys(key)
    }

    pub(super) fn has_complete_write_keys(
        &self,
        key: &crate::KeyAttributes,
    ) -> StorageResult<bool> {
        verify_key_attributes(key, |name| self.key_attribute_value(name))
    }

    pub(super) fn complete_write_keys(self, key: &crate::KeyAttributes) -> StorageResult<Self> {
        let attributes = AttributeMap::try_from(&self)?.with_verified_keys(key)?;
        Self::try_from(&attributes)
    }

    fn key_attribute_value(&self, field: &str) -> StorageResult<Option<AttributeValue>> {
        self.attribute_value(field)
    }

    #[must_use]
    pub fn payload_len(&self) -> usize {
        match self {
            Self::DynamoJson { data } => data.len(),
        }
    }

    pub fn logical_size(&self) -> StorageResult<LogicalItemSize> {
        match self {
            Self::DynamoJson { data } => LogicalItemSize::from_dynamo_json(data, "WireItem", &[]),
        }
    }

    pub fn into_attribute_map(self) -> StorageResult<AttributeMap> {
        AttributeMap::try_from(self)
    }

    /// Decode through a borrowed item when the caller must retain the wire
    /// bytes. The consuming form above can move split key values instead of
    /// cloning them.
    pub fn to_attribute_map(&self) -> StorageResult<AttributeMap> {
        AttributeMap::try_from(self)
    }

    pub fn try_decode<T>(&self) -> StorageResult<T>
    where T: TryFromWireItem {
        T::try_from_wire_item(self)
    }

    pub fn scalar_attributes<'a>(
        &'a self,
        fields: &[&str],
    ) -> StorageResult<Vec<Option<Cow<'a, str>>>> {
        self.scalar_fields(fields)
    }

    pub fn string_attribute<'a>(&'a self, field: &str) -> StorageResult<Option<Cow<'a, str>>> {
        match self {
            Self::DynamoJson { data } => {
                let mut value = parse_dynamo_string_field(data.as_slice(), field)?;
                if value.is_none()
                    && let Some(alias) = alias_field(field)
                {
                    value = parse_dynamo_string_field(data.as_slice(), alias)?;
                }
                Ok(normalize_entity_type_scalar(field, value))
            }
        }
    }

    pub fn required_string_attribute(&self, field: &str) -> StorageResult<String> {
        let values = self.scalar_fields(&[field])?;
        values
            .first()
            .and_then(|value| value.as_ref())
            .map(ToString::to_string)
            .ok_or_else(|| {
                StorageError::internal(&format!("missing required string field {field}"))
            })
    }

    pub fn number_attribute_i64(&self, field: &str) -> StorageResult<Option<i64>> {
        match self {
            Self::DynamoJson { data } => {
                let mut value = parse_dynamo_number_field_i64(data.as_slice(), field)?;
                if value.is_none()
                    && let Some(alias) = alias_field(field)
                {
                    value = parse_dynamo_number_field_i64(data.as_slice(), alias)?;
                }
                Ok(value)
            }
        }
    }

    pub fn bool_attribute(&self, field: &str) -> StorageResult<Option<bool>> {
        match self {
            Self::DynamoJson { data } => parse_dynamo_bool_field(data.as_slice(), field),
        }
    }

    /// Reads one field, falling back to its timestamp alias, or for
    /// `entity_type` to the stored `et`, when the requested name is absent. Use
    /// `select_attribute_values` to inspect exact persisted field names.
    pub fn attribute_value(&self, field: &str) -> StorageResult<Option<AttributeValue>> {
        match self {
            Self::DynamoJson { data } => {
                let mut value = parse_dynamo_attribute_value_field(data.as_slice(), field)?;
                if value.is_none()
                    && let Some(alias) = alias_field(field)
                {
                    value = parse_dynamo_attribute_value_field(data.as_slice(), alias)?;
                }
                Ok(normalize_entity_type_attribute_value(field, value))
            }
        }
    }

    /// Decodes exact persisted field names in one pass, preserving the
    /// requested order. Unlike `attribute_value`, this does not resolve the
    /// entity-type name alias; an absent requested name retains its own
    /// `None` slot.
    pub fn select_attribute_values(
        &self,
        fields: &[&str],
    ) -> StorageResult<Vec<Option<AttributeValue>>> {
        match self {
            Self::DynamoJson { data } => parse_dynamo_attribute_values(data, fields),
        }
    }

    /// Builds a wire item containing only the named attributes.
    pub fn project_wire_attributes(&self, fields: &[&str]) -> StorageResult<Self> {
        let values = self.select_attribute_values(fields)?;
        Ok(Self::DynamoJson {
            data: encode_dynamo_attribute_values(fields, &values)?,
        })
    }

    pub fn project_attributes(&self, fields: &[&str]) -> StorageResult<AttributeMap> {
        match self {
            Self::DynamoJson { data } => parse_dynamo_attribute_fields(data, fields),
        }
    }

    /// Extract only the source key attributes; response projection never
    /// defines a cursor.
    pub fn item_key(
        &self,
        table_info: &StoredTableInfo,
        index_name: &Option<IndexName>,
    ) -> StorageResult<Option<ItemKey>> {
        let index = index_name
            .as_ref()
            .map(|name| {
                table_info
                    .global_secondary_indexes
                    .iter()
                    .flatten()
                    .find(|index| index.index_name == *name)
                    .ok_or_else(|| {
                        StorageError::internal("range source index is absent from metadata")
                    })
            })
            .transpose()?;
        let mut fields = Vec::with_capacity(4);
        for element in table_info
            .key_schema
            .iter()
            .chain(index.into_iter().flat_map(|index| &index.key_schema))
        {
            let name = element.attribute_name.as_str();
            if !fields.contains(&name) {
                fields.push(name);
            }
        }
        let attributes = self.project_attributes(&fields)?;
        if fields.iter().any(|name| !attributes.contains_key(name)) {
            return Ok(None);
        }
        match index {
            Some(index) => ItemKey::from_key_schema_for_index(
                table_info.table_name.clone(),
                &table_info.key_schema,
                &index.index_name,
                &index.key_schema,
                &attributes,
            )
            .map_err(|error| StorageError::internal(&format!("decode range source key: {error}"))),
            None => ItemKey::from_key_schema(
                table_info.table_name.clone(),
                &table_info.key_schema,
                &attributes,
            )
            .map(Some)
            .map_err(|error| StorageError::internal(&format!("decode range source key: {error}"))),
        }
    }

    pub fn last_evaluated_key(
        &self,
        table_info: &StoredTableInfo,
        index_name: &Option<IndexName>,
    ) -> StorageResult<Option<String>> {
        self.item_key(table_info, index_name)?
            .map(|key| {
                key.next_page_token().map_err(|error| {
                    StorageError::internal(&format!("encode range source key: {error}"))
                })
            })
            .transpose()
    }

    pub fn ttl_value_and_table_key_token(
        &self,
        table_info: &StoredTableInfo,
        ttl_attribute: &str,
    ) -> StorageResult<Option<(i64, String)>> {
        let hash_key = hash_key_name(&table_info.key_schema)?;
        let range_key = range_key_name(&table_info.key_schema);
        let mut fields = Vec::with_capacity(3);
        fields.push(ttl_attribute);
        fields.push(hash_key);
        if let Some(name) = range_key {
            fields.push(name);
        }

        let values = self.scalar_fields(&fields)?;
        let ttl = values[0]
            .as_deref()
            .and_then(|value| value.parse::<i64>().ok());
        let Some(ttl) = ttl else {
            return Ok(None);
        };

        let hash = values[1]
            .as_deref()
            .ok_or_else(|| StorageError::internal("ttl index token missing hash key attribute"))?;
        let range = if range_key.is_some() {
            Some(values[2].as_deref().ok_or_else(|| {
                StorageError::internal("ttl index token missing range key attribute")
            })?)
        } else {
            None
        };

        let mut parts = Vec::with_capacity(length_prefixed_capacity(&[
            Some(hash.as_bytes()),
            range.map(str::as_bytes),
        ]));
        ItemKey::append_length_prefixed_key(&mut parts, hash.as_bytes())?;
        if let Some(range) = range {
            ItemKey::append_length_prefixed_key(&mut parts, range.as_bytes())?;
        }

        Ok(Some((ttl, URL_SAFE.encode(&parts))))
    }

    fn scalar_fields<'a>(&'a self, fields: &[&str]) -> StorageResult<Vec<Option<Cow<'a, str>>>> {
        match self {
            Self::DynamoJson { data } => {
                let mut values = parse_dynamo_scalar_fields_with_aliases(data.as_slice(), fields)?;
                for (index, field) in fields.iter().enumerate() {
                    values[index] = normalize_entity_type_scalar(field, values[index].take());
                }
                Ok(values)
            }
        }
    }
}

fn parse_dynamo_scalar_fields_with_aliases<'a>(
    bytes: &'a [u8],
    fields: &[&str],
) -> StorageResult<Vec<Option<Cow<'a, str>>>> {
    if fields.iter().all(|field| alias_field(field).is_none()) {
        return parse_dynamo_scalar_fields(bytes, fields);
    }

    let aliases = fields
        .iter()
        .map(|field| alias_field(field))
        .collect::<SmallVec<[Option<&str>; 8]>>();
    parse_dynamo_scalar_fields_with_aliases_impl(bytes, fields, aliases.as_slice())
}

fn length_prefixed_capacity(parts: &[Option<&[u8]>]) -> usize {
    parts.iter().flatten().map(|part| part.len() + 2).sum()
}

fn alias_field(field: &str) -> Option<&'static str> {
    match field {
        ENTITY_TYPE_NAME_ALIAS => Some(ENTITY_TYPE_ATTR),
        CREATED_AT_ATTR => Some(CREATED_AT_ALIAS_ATTR),
        CREATED_AT_ALIAS_ATTR => Some(CREATED_AT_ATTR),
        UPDATED_AT_ATTR => Some(UPDATED_AT_ALIAS_ATTR),
        UPDATED_AT_ALIAS_ATTR => Some(UPDATED_AT_ATTR),
        EXPIRES_AT_ATTR => Some(EXPIRES_AT_ALIAS_ATTR),
        EXPIRES_AT_ALIAS_ATTR => Some(EXPIRES_AT_ATTR),
        _ => None,
    }
}

fn normalize_entity_type_scalar<'a>(
    field: &str,
    value: Option<Cow<'a, str>>,
) -> Option<Cow<'a, str>> {
    let _ = field;
    value
}

fn normalize_entity_type_attribute_value(
    field: &str,
    value: Option<AttributeValue>,
) -> Option<AttributeValue> {
    let _ = field;
    value
}

fn hash_key_name(key_schema: &[KeySchemaElement]) -> StorageResult<&str> {
    key_schema
        .iter()
        .find(|key| key.key_type == KeyType::Hash)
        .map(|key| key.attribute_name.as_str())
        .ok_or_else(|| StorageError::internal("missing hash key in table schema"))
}

fn range_key_name(key_schema: &[KeySchemaElement]) -> Option<&str> {
    key_schema
        .iter()
        .find(|key| key.key_type == KeyType::Range)
        .map(|key| key.attribute_name.as_str())
}
