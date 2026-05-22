//! Single-table entity trait scaffold.
//! This will evolve to provide key derivation logic for entities persisted
//! inside per-tenant single-table physical storage.

use std::{borrow::Cow, collections::HashMap};

/// Keys returned are logical (no tenant id prefix because each tenant has its
/// own physical table). Implementations should keep pk stable (category token)
/// and derive sk & optional GSIs at call time.
pub trait SingleTableEntity {
    const STORAGE_ENTITY_TYPE: &'static str;
    const ENTITY_TYPE: &'static str = Self::STORAGE_ENTITY_TYPE;

    /// Partition key/category token (e.g. "U", "TOG"). Now returns
    /// an owned `String` to permit dynamic composition (e.g. per-user bucket
    /// keys like `U#<id>|AUTHN`). Implementations should keep this value
    /// stable across calls for the same entity state.
    fn pk(&self) -> String;

    /// Sort key (full value). Typically includes identifiers and possible
    /// compound segments (e.g. "`TOG#grp_123#U#user_456`").
    fn sk(&self) -> String;

    #[must_use]
    fn pk_cow(&self) -> Cow<'_, str> {
        Cow::Owned(self.pk())
    }

    #[must_use]
    fn sk_cow(&self) -> Cow<'_, str> {
        Cow::Owned(self.sk())
    }

    #[must_use]
    fn table_key(&self) -> crate::KeyOwned {
        self.entity_key().into()
    }

    #[must_use]
    fn entity_key(&self) -> crate::EntityKey {
        let pk = self.pk_cow().into_owned();
        let sk = self.sk_cow().into_owned();
        crate::EntityKey::pk_sk(pk, sk)
    }

    #[must_use]
    fn table_key_map(&self) -> HashMap<String, crate::AttributeValue> {
        self.entity_key().into_map()
    }

    /// Optional GSI1 pair (pk, sk)
    fn gsi1(&self) -> Option<(String, String)> {
        None
    }

    /// Optional GSI2 pair (pk, sk)
    fn gsi2(&self) -> Option<(String, String)> {
        None
    }
    fn gsi3(&self) -> Option<(String, String)> {
        None
    }
    fn gsi4(&self) -> Option<(String, String)> {
        None
    }
    fn gsi5(&self) -> Option<(String, String)> {
        None
    }
}

/// Wrapper used when writing to storage to carry computed keys alongside the
/// raw entity payload for serialization.
#[derive(Debug, Clone)]
pub struct TableEntity<T> {
    pub pk: String,
    pub sk: String,
    pub storage_entity_type: &'static str,
    pub entity_type: &'static str,
    pub gsi1: Option<(String, String)>,
    pub gsi2: Option<(String, String)>,
    pub gsi3: Option<(String, String)>,
    pub gsi4: Option<(String, String)>,
    pub gsi5: Option<(String, String)>,
    pub payload: T,
}

impl<T: SingleTableEntity> From<T> for TableEntity<T> {
    fn from(value: T) -> Self {
        let pk = value.pk();
        let sk = value.sk();
        let gsi1 = value.gsi1();
        let gsi2 = value.gsi2();
        let gsi3 = value.gsi3();
        let gsi4 = value.gsi4();
        let gsi5 = value.gsi5();
        Self {
            pk,
            sk,
            storage_entity_type: T::STORAGE_ENTITY_TYPE,
            entity_type: T::ENTITY_TYPE,
            gsi1,
            gsi2,
            gsi3,
            gsi4,
            gsi5,
            payload: value,
        }
    }
}

use crate::{AttributeValue, TryIntoWireItem, WireItem, to_hashmap};

pub const ENTITY_TYPE_ATTR: &str = "et";
pub const LEGACY_ENTITY_TYPE_ATTR: &str = "entity_type";
pub const CREATED_AT_ATTR: &str = "created_at";
pub const UPDATED_AT_ATTR: &str = "updated_at";
pub const EXPIRES_AT_ATTR: &str = "expires_at";
pub const CREATED_AT_ALIAS_ATTR: &str = "c_at";
pub const UPDATED_AT_ALIAS_ATTR: &str = "u_at";
pub const EXPIRES_AT_ALIAS_ATTR: &str = "e_at";

/// Build a storage item `HashMap` (attribute name -> `AttributeValue`) from any
/// `SingleTableEntity` implementor by serializing its payload and adding the
/// standard single-table metadata keys (pk, sk, `et`, and GSIs when
/// present). The payload fields must not collide with these reserved names.
pub fn to_item_map<T: SingleTableEntity + serde::Serialize>(
    entity: &T,
) -> Result<std::collections::HashMap<String, AttributeValue>, crate::ConversionError> {
    let mut map = to_hashmap(entity)?;
    normalize_timestamp_attribute_aliases(&mut map);
    // Insert mandatory keys
    let pk_val = entity.pk_cow().into_owned();
    let sk_val = entity.sk_cow().into_owned();
    map.insert("pk".to_string(), AttributeValue::S(pk_val));
    map.insert("sk".to_string(), AttributeValue::S(sk_val));
    map.insert(
        ENTITY_TYPE_ATTR.to_string(),
        AttributeValue::S(T::ENTITY_TYPE.to_string()),
    );
    if let Some((gpk, gsk)) = entity.gsi1() {
        map.insert("gsi1pk".to_string(), AttributeValue::S(gpk));
        map.insert("gsi1sk".to_string(), AttributeValue::S(gsk));
    }
    if let Some((gpk, gsk)) = entity.gsi2() {
        map.insert("gsi2pk".to_string(), AttributeValue::S(gpk));
        map.insert("gsi2sk".to_string(), AttributeValue::S(gsk));
    }
    if let Some((gpk, gsk)) = entity.gsi3() {
        map.insert("gsi3pk".to_string(), AttributeValue::S(gpk));
        map.insert("gsi3sk".to_string(), AttributeValue::S(gsk));
    }
    if let Some((gpk, gsk)) = entity.gsi4() {
        map.insert("gsi4pk".to_string(), AttributeValue::S(gpk));
        map.insert("gsi4sk".to_string(), AttributeValue::S(gsk));
    }
    if let Some((gpk, gsk)) = entity.gsi5() {
        map.insert("gsi5pk".to_string(), AttributeValue::S(gpk));
        map.insert("gsi5sk".to_string(), AttributeValue::S(gsk));
    }
    Ok(map)
}

fn normalize_timestamp_attribute_aliases(map: &mut HashMap<String, AttributeValue>) {
    normalize_timestamp_attribute_alias(map, CREATED_AT_ATTR, CREATED_AT_ALIAS_ATTR);
    normalize_timestamp_attribute_alias(map, UPDATED_AT_ATTR, UPDATED_AT_ALIAS_ATTR);
    normalize_timestamp_attribute_alias(map, EXPIRES_AT_ATTR, EXPIRES_AT_ALIAS_ATTR);
}

fn normalize_timestamp_attribute_alias(
    map: &mut HashMap<String, AttributeValue>,
    long_name: &str,
    short_name: &str,
) {
    if map.contains_key(short_name) {
        let _ = map.remove(long_name);
        return;
    }
    if let Some(value) = map.remove(long_name) {
        map.insert(short_name.to_string(), value);
    }
}

/// Build a write-ready wire item from a `SingleTableEntity`.
///
/// This keeps the public API compatible with existing `to_item_map` call sites
/// while enabling write-path migration toward `WireItem`-based operations.
pub fn to_wire_item<T: SingleTableEntity + serde::Serialize>(
    entity: &T,
) -> Result<WireItem, crate::ConversionError> {
    let map = to_item_map(entity)?;
    WireItem::from_attribute_map(&map)
        .map_err(|err| crate::ConversionError::Serialization(err.to_string()))
}

/// Build a write-ready wire item using the derived write encoder path.
///
/// This path avoids materializing a `HashMap<String, AttributeValue>` for the
/// payload itself and instead stitches key metadata directly into the encoded
/// Dynamo wire JSON object.
pub fn to_wire_item_fast<T: SingleTableEntity + TryIntoWireItem>(
    entity: &T,
) -> Result<WireItem, crate::ConversionError> {
    let payload_data = match entity
        .try_into_wire_item()
        .map_err(|err| crate::ConversionError::Serialization(err.to_string()))?
    {
        WireItem::DynamoJson { data } => data,
        other @ WireItem::LocalSplit { .. } => {
            let map = other
                .into_attribute_map()
                .map_err(|err| crate::ConversionError::Serialization(err.to_string()))?;
            serde_json::to_vec(&map)
                .map_err(|err| crate::ConversionError::Serialization(err.to_string()))?
        }
    };
    let metadata_data = encode_single_table_metadata(entity)?;
    let bytes = merge_dynamo_object_bytes(payload_data.as_slice(), metadata_data.as_slice())?;
    Ok(WireItem::dynamo_json(bytes))
}

fn encode_single_table_metadata<T: SingleTableEntity>(
    entity: &T,
) -> Result<Vec<u8>, crate::ConversionError> {
    let mut metadata = HashMap::with_capacity(13);
    metadata.insert(
        "pk".to_string(),
        AttributeValue::S(entity.pk_cow().into_owned()),
    );
    metadata.insert(
        "sk".to_string(),
        AttributeValue::S(entity.sk_cow().into_owned()),
    );
    metadata.insert(
        ENTITY_TYPE_ATTR.to_string(),
        AttributeValue::S(T::ENTITY_TYPE.to_string()),
    );
    if let Some((gpk, gsk)) = entity.gsi1() {
        metadata.insert("gsi1pk".to_string(), AttributeValue::S(gpk));
        metadata.insert("gsi1sk".to_string(), AttributeValue::S(gsk));
    }
    if let Some((gpk, gsk)) = entity.gsi2() {
        metadata.insert("gsi2pk".to_string(), AttributeValue::S(gpk));
        metadata.insert("gsi2sk".to_string(), AttributeValue::S(gsk));
    }
    if let Some((gpk, gsk)) = entity.gsi3() {
        metadata.insert("gsi3pk".to_string(), AttributeValue::S(gpk));
        metadata.insert("gsi3sk".to_string(), AttributeValue::S(gsk));
    }
    if let Some((gpk, gsk)) = entity.gsi4() {
        metadata.insert("gsi4pk".to_string(), AttributeValue::S(gpk));
        metadata.insert("gsi4sk".to_string(), AttributeValue::S(gsk));
    }
    if let Some((gpk, gsk)) = entity.gsi5() {
        metadata.insert("gsi5pk".to_string(), AttributeValue::S(gpk));
        metadata.insert("gsi5sk".to_string(), AttributeValue::S(gsk));
    }
    serde_json::to_vec(&metadata)
        .map_err(|err| crate::ConversionError::Serialization(err.to_string()))
}

fn merge_dynamo_object_bytes(
    payload_data: &[u8],
    metadata_data: &[u8],
) -> Result<Vec<u8>, crate::ConversionError> {
    let payload_trimmed = trim_ascii_whitespace(payload_data);
    let metadata_trimmed = trim_ascii_whitespace(metadata_data);
    if !is_json_object(payload_trimmed) || !is_json_object(metadata_trimmed) {
        return Err(crate::ConversionError::Serialization(
            "wire item encode expected JSON objects".to_string(),
        ));
    }
    if payload_trimmed == b"{}" {
        return Ok(metadata_trimmed.to_vec());
    }
    if metadata_trimmed == b"{}" {
        return Ok(payload_trimmed.to_vec());
    }

    let mut out = Vec::with_capacity(payload_trimmed.len() + metadata_trimmed.len() - 1);
    out.extend_from_slice(&payload_trimmed[..payload_trimmed.len() - 1]);
    out.push(b',');
    out.extend_from_slice(&metadata_trimmed[1..]);
    Ok(out)
}

fn trim_ascii_whitespace(bytes: &[u8]) -> &[u8] {
    let mut start = 0usize;
    let mut end = bytes.len();
    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &bytes[start..end]
}

fn is_json_object(bytes: &[u8]) -> bool {
    bytes.len() >= 2 && bytes[0] == b'{' && bytes[bytes.len() - 1] == b'}'
}

#[must_use]
pub fn matches_entity_type<T: SingleTableEntity>(map: &HashMap<String, AttributeValue>) -> bool {
    entity_type_from_item(map).is_some_and(|entity_type| entity_type == T::STORAGE_ENTITY_TYPE)
}

#[must_use]
pub fn storage_entity_type_from_item(
    map: &HashMap<String, AttributeValue>,
) -> Option<crate::StorageEntityType> {
    match map
        .get(ENTITY_TYPE_ATTR)
        .or_else(|| map.get(LEGACY_ENTITY_TYPE_ATTR))
    {
        Some(AttributeValue::S(value)) => crate::StorageEntityType::parse_db(value),
        _ => None,
    }
}

#[must_use]
pub fn entity_type_from_item(map: &HashMap<String, AttributeValue>) -> Option<&str> {
    match map
        .get(ENTITY_TYPE_ATTR)
        .or_else(|| map.get(LEGACY_ENTITY_TYPE_ATTR))
    {
        Some(AttributeValue::S(value)) => Some(value.as_str()),
        _ => None,
    }
}
