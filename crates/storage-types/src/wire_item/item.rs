use std::{borrow::Cow, collections::HashMap};

use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{
    AttributeMap, AttributeValue, IndexName, ItemKey, KeySchemaElement, KeyType, KeysAndAttributes,
    LogicalItemSize, StorageError, StorageResult, StoredTableInfo, TableName, TimestampMillis,
    TimestampSeconds,
    constants::{
        CREATED_AT_ALIAS_ATTR, CREATED_AT_ATTR, ENTITY_TYPE_ATTR, EXPIRES_AT_ALIAS_ATTR,
        EXPIRES_AT_ATTR, LEGACY_ENTITY_TYPE_ATTR, UPDATED_AT_ALIAS_ATTR, UPDATED_AT_ATTR,
    },
    verify_key_attributes,
    wire_item::{
        key_attributes::{
            WireItemKeyAttributes, attribute_from_key_attributes, blob_is_empty_json_object,
            number_from_key_attributes, scalar_from_key_attributes, string_from_key_attributes,
        },
        projection::{
            encode_dynamo_attribute_values, parse_complete_dynamo_attribute_value_field,
            parse_dynamo_attribute_fields, parse_dynamo_attribute_value_field,
            parse_dynamo_attribute_values, parse_dynamo_bool_field, parse_dynamo_number_field_i64,
            parse_dynamo_scalar_fields,
            parse_dynamo_scalar_fields_with_aliases as parse_dynamo_scalar_fields_with_aliases_impl,
            parse_dynamo_string_field,
        },
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BatchGetWireItemResponse {
    pub responses: Option<crate::VecMap<TableName, Vec<WireItem>>>,
    pub unprocessed_keys: Option<crate::VecMap<TableName, KeysAndAttributes>>,
    pub consumed_capacity: Option<serde_json::Value>,
}

#[expect(
    clippy::large_enum_variant,
    reason = "LocalSplit is latency-sensitive on read path, avoid extra heap indirection"
)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireItem {
    DynamoJson {
        data: Vec<u8>,
    },
    LocalSplit {
        primary_key: WireItemKeyAttributes,
        secondary_key: Option<WireItemKeyAttributes>,
        non_key_attributes_blob: Option<Vec<u8>>,
    },
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
                "invalid {field} field: {raw}"
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

impl WireAttributeDecode for DateTime<Utc> {
    fn decode(raw: Option<&str>, field: &str) -> StorageResult<Self> {
        let raw = required_raw_scalar(raw, field)?;
        parse_datetime_auto(raw, field)
    }
}

fn required_raw_scalar<'a>(raw: Option<&'a str>, field: &str) -> StorageResult<&'a str> {
    raw.ok_or_else(|| StorageError::internal(&format!("missing required field {field}")))
}

fn parse_datetime_auto(raw: &str, field: &str) -> StorageResult<DateTime<Utc>> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(raw) {
        return Ok(parsed.with_timezone(&Utc));
    }
    if let Ok(timestamp) = raw.parse::<i64>() {
        // Heuristic: 13+ digit epoch values are milliseconds, otherwise
        // seconds.
        let parsed = if raw.len() >= 13 {
            Utc.timestamp_millis_opt(timestamp).single()
        } else {
            Utc.timestamp_opt(timestamp, 0).single()
        };
        return parsed.ok_or_else(|| {
            StorageError::internal(&format!(
                "invalid {field} field timestamp value: {timestamp}"
            ))
        });
    }
    Err(StorageError::internal(&format!(
        "invalid {field} field datetime format: {raw}"
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
            WireItem::LocalSplit {
                primary_key,
                secondary_key,
                non_key_attributes_blob,
            } => {
                let mut attributes = decode_non_key_attributes(non_key_attributes_blob.as_deref())?;
                insert_key_attributes(&mut attributes, primary_key);
                if let Some(secondary_key) = secondary_key {
                    insert_key_attributes(&mut attributes, secondary_key);
                }
                Ok(attributes)
            }
        }
    }
}

impl TryFrom<WireItem> for AttributeMap {
    type Error = StorageError;

    fn try_from(item: WireItem) -> StorageResult<Self> {
        match item {
            WireItem::DynamoJson { data } => AttributeMap::from_dynamo_json(&data),
            WireItem::LocalSplit {
                primary_key,
                secondary_key,
                non_key_attributes_blob,
            } => {
                let mut attributes = decode_non_key_attributes(non_key_attributes_blob.as_deref())?;
                insert_owned_key_attributes(&mut attributes, primary_key);
                if let Some(secondary_key) = secondary_key {
                    insert_owned_key_attributes(&mut attributes, secondary_key);
                }
                Ok(attributes)
            }
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

fn insert_key_attributes(attributes: &mut AttributeMap, keys: &WireItemKeyAttributes) {
    attributes.insert(keys.hash_key_name.to_string(), keys.hash_key.clone());
    if let (Some(name), Some(value)) = (&keys.sort_key_name, &keys.sort_key) {
        attributes.insert(name.to_string(), value.clone());
    }
}

fn insert_owned_key_attributes(attributes: &mut AttributeMap, keys: WireItemKeyAttributes) {
    attributes.insert(keys.hash_key_name.into_owned(), keys.hash_key);
    if let (Some(name), Some(value)) = (keys.sort_key_name, keys.sort_key) {
        attributes.insert(name.into_owned(), value);
    }
}

fn decode_non_key_attributes(blob: Option<&[u8]>) -> StorageResult<AttributeMap> {
    match blob {
        Some(blob) if !blob.is_empty() && !blob_is_empty_json_object(blob) => {
            serde_json::from_slice(blob).map_err(|err| {
                StorageError::internal(&format!(
                    "decode wire item non-key attributes failed: {err}"
                ))
            })
        }
        _ => Ok(AttributeMap::new()),
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

    #[must_use]
    pub fn local_split(
        primary_key: WireItemKeyAttributes,
        secondary_key: Option<WireItemKeyAttributes>,
        non_key_attributes_blob: Option<Vec<u8>>,
    ) -> Self {
        Self::LocalSplit {
            primary_key,
            secondary_key,
            non_key_attributes_blob,
        }
    }

    /// Verify the addressed key without completing or rewriting the image.
    /// False means a component is missing; conflicting or invalid values fail.
    /// Canonical images must agree across split keys and residual attributes.
    pub fn has_verified_keys(&self, key: &crate::KeyAttributes) -> StorageResult<bool> {
        if let Self::LocalSplit {
            primary_key,
            secondary_key,
            ..
        } = self
        {
            let entries = std::iter::once(primary_key)
                .chain(secondary_key)
                .flat_map(|keys| {
                    std::iter::once((keys.hash_key_name.as_ref(), &keys.hash_key))
                        .chain(keys.sort_key_name.as_deref().zip(keys.sort_key.as_ref()))
                });
            for (name, value) in entries {
                verify_key_attributes(key, |requested| {
                    Ok((name == requested).then(|| value.clone()))
                })?;
            }
        }
        verify_key_attributes(key, |name| self.canonical_key_attribute_value(name))
    }

    fn canonical_key_attribute_value(&self, field: &str) -> StorageResult<Option<AttributeValue>> {
        match self {
            Self::DynamoJson { data } => parse_complete_dynamo_attribute_value_field(data, field),
            Self::LocalSplit {
                primary_key,
                secondary_key,
                non_key_attributes_blob,
            } => {
                let value = non_key_attributes_blob
                    .as_deref()
                    .filter(|bytes| !bytes.is_empty())
                    .map(|bytes| parse_complete_dynamo_attribute_value_field(bytes, field))
                    .transpose()?
                    .flatten();
                Ok(value.or_else(|| {
                    std::iter::once(primary_key)
                        .chain(secondary_key)
                        .find_map(|keys| attribute_from_key_attributes(keys, field))
                        .cloned()
                }))
            }
        }
    }

    /// Complete a legacy item with its addressed key, rejecting a stored
    /// image whose key attributes identify another item. Complete matching
    /// wire items retain their original representation and allocation.
    /// This path retains its primary-key lookup; canonical images instead
    /// require `has_verified_keys` to verify agreement between represented
    /// keys.
    pub fn with_verified_keys(self, key: &crate::KeyAttributes) -> StorageResult<Self> {
        if verify_key_attributes(key, |name| self.key_attribute_value(name))? {
            return Ok(self);
        }

        let attributes = AttributeMap::try_from(&self)?.with_verified_keys(key)?;
        Self::try_from(&attributes)
    }

    fn key_attribute_value(&self, field: &str) -> StorageResult<Option<AttributeValue>> {
        if let Self::LocalSplit {
            primary_key,
            secondary_key,
            ..
        } = self
            && let Some(value) = attribute_from_key_attributes(primary_key, field).or_else(|| {
                secondary_key
                    .as_ref()
                    .and_then(|keys| attribute_from_key_attributes(keys, field))
            })
        {
            return Ok(Some(value.clone()));
        }
        self.attribute_value(field)
    }

    #[must_use]
    pub fn payload_len(&self) -> usize {
        match self {
            Self::DynamoJson { data } => data.len(),
            Self::LocalSplit {
                primary_key,
                secondary_key,
                non_key_attributes_blob,
            } => {
                primary_key.payload_len()
                    + secondary_key
                        .as_ref()
                        .map_or(0, WireItemKeyAttributes::payload_len)
                    + non_key_attributes_blob.as_ref().map_or(0, Vec::len)
            }
        }
    }

    pub fn logical_size(&self) -> StorageResult<LogicalItemSize> {
        match self {
            Self::DynamoJson { data } => LogicalItemSize::from_dynamo_json(data, "WireItem", &[]),
            Self::LocalSplit {
                primary_key,
                secondary_key,
                non_key_attributes_blob,
            } => {
                let key_entries = canonical_key_entries(primary_key, secondary_key.as_ref());
                let key_names = key_entries
                    .iter()
                    .map(|(name, _)| *name)
                    .collect::<Vec<_>>();
                let key_size = LogicalItemSize::try_from_entries(key_entries.iter().copied())?;
                let non_key_size = non_key_attributes_blob
                    .as_deref()
                    .filter(|blob| !blob.is_empty() && !blob_is_empty_json_object(blob))
                    .map_or(Ok(0), |blob| {
                        LogicalItemSize::size_from_dynamo_json(blob, "WireItem", &key_names)
                    })?;
                let size = u64::from(key_size)
                    .checked_add(non_key_size)
                    .ok_or_else(|| {
                        StorageError::internal(&"logical item size arithmetic overflowed")
                    })?;
                LogicalItemSize::try_from(size)
            }
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
            Self::LocalSplit {
                primary_key,
                secondary_key,
                non_key_attributes_blob,
            } => {
                if let Some(blob) = non_key_attributes_blob
                    && !blob.is_empty()
                    && !blob_is_empty_json_object(blob.as_slice())
                {
                    let mut projected = parse_dynamo_string_field(blob.as_slice(), field)?;
                    if projected.is_none()
                        && let Some(alias) = alias_field(field)
                    {
                        projected = parse_dynamo_string_field(blob.as_slice(), alias)?;
                    }
                    if projected.is_some() {
                        return Ok(normalize_entity_type_scalar(field, projected));
                    }
                }
                if let Some(value) = string_from_key_attributes(primary_key, field).or_else(|| {
                    alias_field(field)
                        .and_then(|alias| string_from_key_attributes(primary_key, alias))
                }) {
                    return Ok(normalize_entity_type_scalar(
                        field,
                        Some(Cow::Borrowed(value)),
                    ));
                }
                if let Some(secondary_key) = secondary_key
                    && let Some(value) =
                        string_from_key_attributes(secondary_key, field).or_else(|| {
                            alias_field(field)
                                .and_then(|alias| string_from_key_attributes(secondary_key, alias))
                        })
                {
                    return Ok(normalize_entity_type_scalar(
                        field,
                        Some(Cow::Borrowed(value)),
                    ));
                }
                Ok(None)
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
            Self::LocalSplit {
                primary_key,
                secondary_key,
                non_key_attributes_blob,
            } => {
                if let Some(blob) = non_key_attributes_blob
                    && !blob.is_empty()
                    && !blob_is_empty_json_object(blob.as_slice())
                {
                    let mut value = parse_dynamo_number_field_i64(blob.as_slice(), field)?;
                    if value.is_none()
                        && let Some(alias) = alias_field(field)
                    {
                        value = parse_dynamo_number_field_i64(blob.as_slice(), alias)?;
                    }
                    if let Some(value) = value {
                        return Ok(Some(value));
                    }
                }
                let mut value = number_from_key_attributes(primary_key, field)?;
                if value.is_none()
                    && let Some(alias) = alias_field(field)
                {
                    value = number_from_key_attributes(primary_key, alias)?;
                }
                if let Some(value) = value {
                    return Ok(Some(value));
                }
                if let Some(secondary_key) = secondary_key {
                    let mut value = number_from_key_attributes(secondary_key, field)?;
                    if value.is_none()
                        && let Some(alias) = alias_field(field)
                    {
                        value = number_from_key_attributes(secondary_key, alias)?;
                    }
                    if let Some(value) = value {
                        return Ok(Some(value));
                    }
                }

                Ok(None)
            }
        }
    }

    pub fn bool_attribute(&self, field: &str) -> StorageResult<Option<bool>> {
        match self {
            Self::DynamoJson { data } => parse_dynamo_bool_field(data.as_slice(), field),
            Self::LocalSplit {
                non_key_attributes_blob,
                ..
            } => {
                if let Some(blob) = non_key_attributes_blob
                    && !blob.is_empty()
                    && !blob_is_empty_json_object(blob.as_slice())
                {
                    return parse_dynamo_bool_field(blob.as_slice(), field);
                }
                Ok(None)
            }
        }
    }

    /// Reads one field, falling back to its canonical or legacy timestamp or
    /// entity-type alias when the requested name is absent. Use
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
            Self::LocalSplit {
                primary_key,
                secondary_key,
                non_key_attributes_blob,
            } => {
                if let Some(blob) = non_key_attributes_blob
                    && !blob.is_empty()
                    && !blob_is_empty_json_object(blob.as_slice())
                {
                    let mut value = parse_dynamo_attribute_value_field(blob.as_slice(), field)?;
                    if value.is_none()
                        && let Some(alias) = alias_field(field)
                    {
                        value = parse_dynamo_attribute_value_field(blob.as_slice(), alias)?;
                    }
                    if value.is_some() {
                        return Ok(normalize_entity_type_attribute_value(field, value));
                    }
                }
                if let Some(value) =
                    attribute_from_key_attributes(primary_key, field).or_else(|| {
                        alias_field(field)
                            .and_then(|alias| attribute_from_key_attributes(primary_key, alias))
                    })
                {
                    return Ok(normalize_entity_type_attribute_value(
                        field,
                        Some(value.clone()),
                    ));
                }
                if let Some(secondary_key) = secondary_key
                    && let Some(value) = attribute_from_key_attributes(secondary_key, field)
                        .or_else(|| {
                            alias_field(field).and_then(|alias| {
                                attribute_from_key_attributes(secondary_key, alias)
                            })
                        })
                {
                    return Ok(normalize_entity_type_attribute_value(
                        field,
                        Some(value.clone()),
                    ));
                }
                Ok(None)
            }
        }
    }

    /// Decodes exact persisted field names in one pass, preserving the
    /// requested order. Unlike `attribute_value`, this does not resolve
    /// canonical or legacy aliases; an absent requested name retains its
    /// own `None` slot.
    pub fn select_attribute_values(
        &self,
        fields: &[&str],
    ) -> StorageResult<Vec<Option<AttributeValue>>> {
        match self {
            Self::DynamoJson { data } => parse_dynamo_attribute_values(data, fields),
            Self::LocalSplit {
                primary_key,
                secondary_key,
                non_key_attributes_blob,
            } => {
                let mut values = match non_key_attributes_blob {
                    Some(blob) if !blob.is_empty() && !blob_is_empty_json_object(blob) => {
                        parse_dynamo_attribute_values(blob, fields)?
                    }
                    _ => vec![None; fields.len()],
                };
                for (field, value) in fields.iter().zip(&mut values) {
                    if value.is_some() {
                        continue;
                    }
                    *value = attribute_from_key_attributes(primary_key, field)
                        .or_else(|| {
                            secondary_key
                                .as_ref()
                                .and_then(|keys| attribute_from_key_attributes(keys, field))
                        })
                        .cloned();
                }
                Ok(values)
            }
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
            Self::LocalSplit {
                primary_key,
                secondary_key,
                non_key_attributes_blob,
            } => {
                let mut attributes = match non_key_attributes_blob {
                    Some(blob) if !blob.is_empty() && !blob_is_empty_json_object(blob) => {
                        parse_dynamo_attribute_fields(blob, fields)?
                    }
                    _ => AttributeMap::new(),
                };
                for field in fields {
                    let value = attribute_from_key_attributes(primary_key, field).or_else(|| {
                        secondary_key
                            .as_ref()
                            .and_then(|keys| attribute_from_key_attributes(keys, field))
                    });
                    if let Some(value) = value {
                        attributes.insert(*field, value.clone());
                    }
                }
                Ok(attributes)
            }
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
        ItemKey::add_length_prefixed_part(&mut parts, hash.as_bytes());
        if let Some(range) = range {
            ItemKey::add_length_prefixed_part(&mut parts, range.as_bytes());
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
            Self::LocalSplit {
                primary_key,
                secondary_key,
                non_key_attributes_blob,
                ..
            } => {
                let mut values = if let Some(blob) = non_key_attributes_blob
                    && !blob.is_empty()
                    && !blob_is_empty_json_object(blob.as_slice())
                {
                    parse_dynamo_scalar_fields(blob.as_slice(), fields)?
                } else {
                    vec![None; fields.len()]
                };
                for (index, field) in fields.iter().enumerate() {
                    if values[index].is_some() {
                        values[index] = normalize_entity_type_scalar(field, values[index].take());
                        continue;
                    }
                    let value = scalar_from_key_attributes(primary_key, field)
                        .or_else(|| {
                            secondary_key
                                .as_ref()
                                .and_then(|secondary| scalar_from_key_attributes(secondary, field))
                        })
                        .or_else(|| {
                            alias_field(field).and_then(|alias| {
                                scalar_from_key_attributes(primary_key, alias).or_else(|| {
                                    secondary_key.as_ref().and_then(|secondary| {
                                        scalar_from_key_attributes(secondary, alias)
                                    })
                                })
                            })
                        });
                    values[index] = value.map(Cow::Borrowed);
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

pub(super) fn canonical_key_entries<'a>(
    primary_key: &'a WireItemKeyAttributes,
    secondary_key: Option<&'a WireItemKeyAttributes>,
) -> SmallVec<[(&'a str, &'a AttributeValue); 4]> {
    let mut entries = SmallVec::new();
    for keys in std::iter::once(primary_key).chain(secondary_key) {
        insert_canonical_key_entry(&mut entries, keys.hash_key_name.as_ref(), &keys.hash_key);
        if let (Some(name), Some(value)) = (&keys.sort_key_name, &keys.sort_key) {
            insert_canonical_key_entry(&mut entries, name.as_ref(), value);
        }
    }
    entries
}

pub(super) fn canonical_key_entries_mut<'a>(
    primary_key: &'a mut WireItemKeyAttributes,
    secondary_key: Option<&'a mut WireItemKeyAttributes>,
) -> SmallVec<[(&'a str, &'a mut AttributeValue); 4]> {
    let mut entries = SmallVec::new();
    for keys in std::iter::once(primary_key).chain(secondary_key) {
        insert_canonical_key_entry(
            &mut entries,
            keys.hash_key_name.as_ref(),
            &mut keys.hash_key,
        );
        if let (Some(name), Some(value)) = (&keys.sort_key_name, &mut keys.sort_key) {
            insert_canonical_key_entry(&mut entries, name.as_ref(), value);
        }
    }
    entries
}

fn insert_canonical_key_entry<'a, T>(
    entries: &mut SmallVec<[(&'a str, T); 4]>,
    name: &'a str,
    value: T,
) {
    if let Some(index) = entries.iter().position(|(existing, _)| *existing == name) {
        entries[index].1 = value;
    } else {
        entries.push((name, value));
    }
}

fn length_prefixed_capacity(parts: &[Option<&[u8]>]) -> usize {
    parts.iter().flatten().map(|part| part.len() + 2).sum()
}

fn alias_field(field: &str) -> Option<&'static str> {
    match field {
        LEGACY_ENTITY_TYPE_ATTR => Some(ENTITY_TYPE_ATTR),
        ENTITY_TYPE_ATTR => Some(LEGACY_ENTITY_TYPE_ATTR),
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
