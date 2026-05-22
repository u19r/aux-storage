use std::{borrow::Cow, collections::HashMap};

use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    AttributeMap, AttributeValue, IndexName, ItemKey, KeySchemaElement, KeyType, KeysAndAttributes,
    StorageError, StorageResult, StoredTableInfo, TableName, TimestampMillis, TimestampSeconds,
    single_table_entity::{ENTITY_TYPE_ATTR, LEGACY_ENTITY_TYPE_ATTR},
    wire_item::{
        key_attributes::{
            WireItemKeyAttributes, attribute_from_key_attributes, blob_is_empty_json_object,
            number_from_key_attributes, scalar_from_key_attributes, string_from_key_attributes,
        },
        projection::{
            parse_dynamo_attribute_value_field, parse_dynamo_bool_field,
            parse_dynamo_number_field_i64, parse_dynamo_scalar_fields, parse_dynamo_string_field,
        },
    },
};

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

const CREATED_AT_ATTR: &str = "created_at";
const UPDATED_AT_ATTR: &str = "updated_at";
const EXPIRES_AT_ATTR: &str = "expires_at";
const CREATED_AT_ALIAS_ATTR: &str = "c_at";
const UPDATED_AT_ALIAS_ATTR: &str = "u_at";
const EXPIRES_AT_ALIAS_ATTR: &str = "e_at";

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

pub fn decode_wire_field<T>(item: &WireItem, raw: Option<&str>, field: &str) -> StorageResult<T>
where T: WireAttributeDecode + DeserializeOwned {
    if raw.is_some() {
        return T::decode(raw, field);
    }

    match item.attribute_value(field)? {
        Some(value) => {
            let plain_json = value.to_plain_json().map_err(|err| {
                StorageError::internal(&format!(
                    "decode wire attribute {field} into plain json failed: {err}"
                ))
            })?;
            serde_json::from_str::<T>(plain_json.as_str()).map_err(|err| {
                StorageError::internal(&format!("decode wire attribute {field} failed: {err}"))
            })
        }
        None => T::decode(None, field),
    }
}

pub fn decode_wire_field_json<T>(
    item: &WireItem,
    raw: Option<&str>,
    field: &str,
) -> StorageResult<T>
where
    T: DeserializeOwned,
{
    if let Some(raw) = raw {
        if let Ok(decoded) = serde_json::from_str::<T>(raw) {
            return Ok(decoded);
        }
        return decode_quoted_scalar_json(raw, field);
    }

    match item.attribute_value(field)? {
        Some(value) => {
            let plain_json = value.to_plain_json().map_err(|err| {
                StorageError::internal(&format!(
                    "decode wire attribute {field} into plain json failed: {err}"
                ))
            })?;
            serde_json::from_str::<T>(plain_json.as_str()).map_err(|err| {
                StorageError::internal(&format!("decode wire attribute {field} failed: {err}"))
            })
        }
        None => serde_json::from_str::<T>("null")
            .map_err(|_| StorageError::internal(&format!("missing required field {field}"))),
    }
}

pub fn decode_wire_serde_string<T>(raw: &str, field: &str) -> StorageResult<T>
where T: DeserializeOwned {
    decode_quoted_scalar_json(raw, field)
        .map_err(|err| StorageError::internal(&format!("invalid {field} field: {err}")))
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

fn decode_quoted_scalar_json<T>(raw: &str, field: &str) -> StorageResult<T>
where T: DeserializeOwned {
    let quoted = serde_json::to_string(raw).map_err(|err| {
        StorageError::internal(&format!(
            "decode wire attribute {field} scalar quoting failed: {err}"
        ))
    })?;
    serde_json::from_str::<T>(quoted.as_str()).map_err(|err| {
        StorageError::internal(&format!("decode wire attribute {field} failed: {err}"))
    })
}

fn parse_datetime_auto(raw: &str, field: &str) -> StorageResult<DateTime<Utc>> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(raw) {
        return Ok(parsed.with_timezone(&Utc));
    }
    if let Ok(timestamp) = raw.parse::<i64>() {
        // Heuristic: 13+ digit epoch values are milliseconds, otherwise seconds.
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

impl TryFromWireItem for HashMap<String, AttributeValue> {
    fn try_from_wire_item(item: &WireItem) -> StorageResult<Self> {
        item.to_attribute_map()
    }
}

impl TryIntoWireItem for HashMap<String, AttributeValue> {
    fn try_into_wire_item(&self) -> StorageResult<WireItem> {
        WireItem::from_attribute_map(self)
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

    pub fn from_attribute_map(item: &HashMap<String, AttributeValue>) -> StorageResult<Self> {
        let data = serde_json::to_vec(item).map_err(|err| {
            StorageError::internal(&format!("encode wire item map failed: {err}"))
        })?;
        Ok(Self::DynamoJson { data })
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

    pub fn into_attribute_map(self) -> StorageResult<HashMap<String, AttributeValue>> {
        match self {
            Self::DynamoJson { data } => serde_json::from_slice::<HashMap<String, AttributeValue>>(
                data.as_slice(),
            )
            .map_err(|err| {
                StorageError::internal(&format!("decode wire dynamo json item into map: {err}"))
            }),
            Self::LocalSplit {
                primary_key,
                secondary_key,
                non_key_attributes_blob,
            } => {
                let non_keys = if let Some(blob) = non_key_attributes_blob
                    && !blob.is_empty()
                    && !blob_is_empty_json_object(blob.as_slice())
                {
                    Some(
                        serde_json::from_slice::<AttributeMap>(blob.as_slice()).map_err(|err| {
                            StorageError::internal(&format!(
                                "decode sqlite non-key attributes blob into map: {err}"
                            ))
                        })?,
                    )
                } else {
                    None
                };

                let key_count = 1 + usize::from(secondary_key.is_some());
                let mut attributes = HashMap::with_capacity(
                    key_count + non_keys.as_ref().map_or(0, AttributeMap::len),
                );
                primary_key.append_to_attribute_map(&mut attributes);
                if let Some(secondary_key) = secondary_key {
                    secondary_key.append_to_attribute_map(&mut attributes);
                }
                if let Some(non_keys) = non_keys {
                    for attribute in non_keys {
                        attributes.insert(attribute.name, attribute.value);
                    }
                }
                Ok(attributes)
            }
        }
    }

    pub fn to_attribute_map(&self) -> StorageResult<HashMap<String, AttributeValue>> {
        self.clone().into_attribute_map()
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
                if let Some(value) = number_from_key_attributes(primary_key, field).or_else(|| {
                    alias_field(field)
                        .and_then(|alias| number_from_key_attributes(primary_key, alias))
                }) {
                    return Ok(Some(value));
                }
                if let Some(secondary_key) = secondary_key
                    && let Some(value) =
                        number_from_key_attributes(secondary_key, field).or_else(|| {
                            alias_field(field)
                                .and_then(|alias| number_from_key_attributes(secondary_key, alias))
                        })
                {
                    return Ok(Some(value));
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

    pub fn last_evaluated_key(
        &self,
        table_info: &StoredTableInfo,
        index_name: &Option<IndexName>,
    ) -> StorageResult<Option<String>> {
        if let Some(index_name) = index_name {
            let index_key_schema = table_info
                .global_secondary_indexes
                .as_ref()
                .and_then(|indexes| indexes.iter().find(|idx| idx.index_name == *index_name))
                .map_or(&table_info.key_schema, |idx| &idx.key_schema);
            let table_hash_key = hash_key_name(&table_info.key_schema)?;
            let table_range_key = range_key_name(&table_info.key_schema);
            let gsi_hash_key = hash_key_name(index_key_schema)?;
            let gsi_range_key = range_key_name(index_key_schema);

            let mut fields = Vec::with_capacity(4);
            fields.push(gsi_hash_key);
            if let Some(name) = gsi_range_key {
                fields.push(name);
            }
            fields.push(table_hash_key);
            if let Some(name) = table_range_key {
                fields.push(name);
            }

            let values = self.scalar_fields(&fields)?;
            let mut index = 0usize;
            let gsi_hash = values[index].as_deref();
            index += 1;

            let gsi_range = if gsi_range_key.is_some() {
                let value = values[index].as_deref();
                index += 1;
                value
            } else {
                None
            };

            let table_hash = values[index].as_deref();
            index += 1;

            let table_range = if table_range_key.is_some() {
                values[index].as_deref()
            } else {
                None
            };

            let Some(gsi_hash) = gsi_hash else {
                return Ok(None);
            };
            let Some(table_hash) = table_hash else {
                return Ok(None);
            };
            if gsi_range_key.is_some() && gsi_range.is_none() {
                return Ok(None);
            }
            if table_range_key.is_some() && table_range.is_none() {
                return Ok(None);
            }

            let mut parts = Vec::with_capacity(length_prefixed_capacity(&[
                Some(gsi_hash.as_bytes()),
                gsi_range.map(str::as_bytes),
                Some(table_hash.as_bytes()),
                table_range.map(str::as_bytes),
            ]));
            ItemKey::add_length_prefixed_part(&mut parts, gsi_hash.as_bytes());
            if let Some(gsi_range) = gsi_range {
                ItemKey::add_length_prefixed_part(&mut parts, gsi_range.as_bytes());
            }
            ItemKey::add_length_prefixed_part(&mut parts, table_hash.as_bytes());
            if let Some(table_range) = table_range {
                ItemKey::add_length_prefixed_part(&mut parts, table_range.as_bytes());
            }

            return Ok(Some(URL_SAFE.encode(&parts)));
        }

        let hash_key = hash_key_name(&table_info.key_schema)?;
        let range_key = range_key_name(&table_info.key_schema);
        let mut fields = Vec::with_capacity(2);
        fields.push(hash_key);
        if let Some(name) = range_key {
            fields.push(name);
        }
        let values = self.scalar_fields(&fields)?;
        let hash = values[0].as_deref();
        let range = if range_key.is_some() {
            values[1].as_deref()
        } else {
            None
        };
        let Some(hash) = hash else {
            return Ok(None);
        };
        if range_key.is_some() && range.is_none() {
            return Ok(None);
        }

        let mut parts = Vec::with_capacity(length_prefixed_capacity(&[
            Some(hash.as_bytes()),
            range.map(str::as_bytes),
        ]));
        ItemKey::add_length_prefixed_part(&mut parts, hash.as_bytes());
        if let Some(range) = range {
            ItemKey::add_length_prefixed_part(&mut parts, range.as_bytes());
        }
        Ok(Some(URL_SAFE.encode(&parts)))
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
                let mut values = parse_dynamo_scalar_fields(data.as_slice(), fields)?;
                for (index, field) in fields.iter().enumerate() {
                    if values[index].is_none()
                        && let Some(alias) = alias_field(field)
                    {
                        let alias_value = parse_dynamo_scalar_fields(data.as_slice(), &[alias])?;
                        values[index] = alias_value.into_iter().next().flatten();
                    }
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BatchGetWireItemResponse {
    pub responses: Option<HashMap<TableName, Vec<WireItem>>>,
    pub unprocessed_keys: Option<HashMap<TableName, KeysAndAttributes>>,
    pub consumed_capacity: Option<serde_json::Value>,
}
