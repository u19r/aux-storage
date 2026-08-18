//! Typed single-table entity keys, indexer metadata, and wire encoding.

use std::{borrow::Cow, collections::HashMap};

use serde::Serialize;

/// Keys returned are logical (no tenant id prefix because each tenant has its
/// own physical table). Implementations should keep pk stable (category token)
/// and derive sk & optional GSIs at call time.
pub trait SingleTableEntity {
    const STORAGE_ENTITY_TYPE: &'static str;
    const ENTITY_TYPE: &'static str = Self::STORAGE_ENTITY_TYPE;
    /// Ordered item indexers generated from entity field annotations.
    const INDEXERS: &'static [EntityIndexer] = &[];

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

/// One entity-owned item indexer attribute and its public ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntityIndexer {
    attribute_name: &'static str,
    ordinal: u8,
}

impl EntityIndexer {
    #[doc(hidden)]
    #[must_use]
    pub const fn from_derive(attribute_name: &'static str, ordinal: u8) -> Self {
        assert!(ordinal < crate::MAX_INDEXERS_CAPACITY);
        Self {
            attribute_name,
            ordinal,
        }
    }

    #[must_use]
    pub const fn attribute_name(self) -> &'static str {
        self.attribute_name
    }

    #[must_use]
    pub const fn ordinal(self) -> u8 {
        self.ordinal
    }

    #[must_use]
    pub fn one_from_query(
        self,
        node: impl Into<String>,
        on_missing: crate::ReadSequenceOnMissing,
    ) -> crate::ReadSequenceNodeInput {
        self.query_input(node, crate::ReadSequenceInputCardinality::One, on_missing)
    }

    #[must_use]
    pub fn many_from_query(
        self,
        node: impl Into<String>,
        on_missing: crate::ReadSequenceOnMissing,
    ) -> crate::ReadSequenceNodeInput {
        self.query_input(node, crate::ReadSequenceInputCardinality::Many, on_missing)
    }

    fn query_input(
        self,
        node: impl Into<String>,
        cardinality: crate::ReadSequenceInputCardinality,
        on_missing: crate::ReadSequenceOnMissing,
    ) -> crate::ReadSequenceNodeInput {
        let item = match cardinality {
            crate::ReadSequenceInputCardinality::One => "0",
            crate::ReadSequenceInputCardinality::Many => "*",
        };
        crate::ReadSequenceNodeInput {
            from: crate::ReadSequenceFromInput {
                node: node.into(),
                select: crate::ReadSequenceSelector(format!(
                    "$.Query.Items[{item}].{}",
                    self.attribute_name
                )),
            },
            mapped_key_source: Some(self.into()),
            cardinality,
            on_missing,
        }
    }
}

/// Encoded single-table entity with its generated ordered indexer declaration.
#[derive(Debug, Clone)]
pub struct WireEntity {
    item: WireItem,
    indexers: WireEntityIndexers,
}

#[derive(Debug, Clone)]
enum WireEntityIndexers {
    Derived(&'static [EntityIndexer]),
    Request(Option<Vec<String>>),
}

impl WireEntity {
    /// Wrap an encoded item that intentionally has no indexer declaration.
    #[must_use]
    pub fn unindexed(item: WireItem) -> Self {
        Self {
            item,
            indexers: WireEntityIndexers::Request(None),
        }
    }

    #[must_use]
    pub fn item(&self) -> &WireItem {
        &self.item
    }

    #[doc(hidden)]
    pub fn item_mut(&mut self) -> &mut WireItem {
        &mut self.item
    }

    /// Returns the ordered wire attribute names declared for this entity.
    /// Derived declarations allocate only when a backend needs owned names;
    /// request declarations remain borrowed.
    #[must_use]
    pub fn indexer_names(&self) -> Option<Cow<'_, [String]>> {
        match &self.indexers {
            WireEntityIndexers::Derived([]) | WireEntityIndexers::Request(None) => None,
            WireEntityIndexers::Derived(indexers) => Some(Cow::Owned(
                indexers
                    .iter()
                    .map(|indexer| indexer.attribute_name.to_string())
                    .collect(),
            )),
            WireEntityIndexers::Request(Some(indexers)) => Some(Cow::Borrowed(indexers)),
        }
    }

    #[must_use]
    pub fn into_write_parts(self) -> (WireItem, Option<Vec<String>>) {
        let indexers = match self.indexers {
            WireEntityIndexers::Derived([]) => None,
            WireEntityIndexers::Derived(indexers) => Some(
                indexers
                    .iter()
                    .map(|indexer| indexer.attribute_name.to_string())
                    .collect(),
            ),
            WireEntityIndexers::Request(indexers) => indexers,
        };
        (self.item, indexers)
    }

    pub(crate) fn from_write_parts(item: WireItem, indexers: Option<Vec<String>>) -> Self {
        Self {
            item,
            indexers: WireEntityIndexers::Request(indexers),
        }
    }

    fn new<T: SingleTableEntity>(item: WireItem) -> Self {
        Self {
            item,
            indexers: WireEntityIndexers::Derived(T::INDEXERS),
        }
    }
}

use crate::{AttributeValue, TryIntoWireItem, WireItem};

pub const ENTITY_TYPE_ATTR: &str = "et";
pub const LEGACY_ENTITY_TYPE_ATTR: &str = "entity_type";
pub const CREATED_AT_ATTR: &str = "created_at";
pub const UPDATED_AT_ATTR: &str = "updated_at";
pub const EXPIRES_AT_ATTR: &str = "expires_at";
pub const CREATED_AT_ALIAS_ATTR: &str = "c_at";
pub const UPDATED_AT_ALIAS_ATTR: &str = "u_at";
pub const EXPIRES_AT_ALIAS_ATTR: &str = "e_at";

/// Build a write-ready entity envelope using the derived write encoder path.
///
/// This path avoids materializing a `HashMap<String, AttributeValue>` for the
/// payload itself and instead stitches key metadata directly into the encoded
/// Dynamo wire JSON object.
pub fn to_wire_entity<T: SingleTableEntity + TryIntoWireItem>(
    entity: &T,
) -> Result<WireEntity, crate::ConversionError> {
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
    Ok(WireEntity::new::<T>(WireItem::dynamo_json(bytes)))
}

/// Encode an entity as a logical wire item for callers that do not own a
/// typed write envelope.
///
/// New writes that use entity indexers must retain the [`WireEntity`] returned
/// by [`to_wire_entity`] so the ordered declaration is forwarded alongside the
/// item. This item-only helper remains for active callers that deliberately
/// construct the surrounding request themselves; it does not manufacture an
/// indexer declaration.
pub fn to_wire_item_fast<T: SingleTableEntity + TryIntoWireItem>(
    entity: &T,
) -> Result<WireItem, crate::ConversionError> {
    Ok(to_wire_entity(entity)?.into_write_parts().0)
}

/// Encode a manually serialized single-table entity as a logical wire item.
///
/// This is the migration seam for entities that have a hand-written
/// `StoredEntity` implementation rather than the `WireItemEncode` derive.
/// Typed writes should prefer [`to_wire_entity`] so indexer declarations stay
/// attached to the item envelope.
pub fn to_wire_item<T: SingleTableEntity + Serialize>(
    entity: &T,
) -> Result<WireItem, crate::ConversionError> {
    let attributes = to_item_map(entity)?;
    WireItem::from_attribute_map(&attributes)
        .map_err(|err| crate::ConversionError::Serialization(err.to_string()))
}

/// Serialize a single-table entity into the attribute map used by the
/// non-encoded write builders and test fixtures.
///
/// This retains the logical entity metadata (`pk`, `sk`, entity type, and
/// configured GSIs) alongside the serialized fields. Callers that construct
/// an encoded write request should prefer [`to_wire_entity`] so indexer
/// declarations remain attached to the request envelope.
pub fn to_item_map<T: SingleTableEntity + Serialize>(
    entity: &T,
) -> Result<HashMap<String, AttributeValue>, crate::ConversionError> {
    let value = serde_json::to_value(entity)
        .map_err(|err| crate::ConversionError::Serialization(err.to_string()))?;
    let serde_json::Value::Object(fields) = value else {
        return Err(crate::ConversionError::Serialization(
            "single-table entity must serialize as a JSON object".to_string(),
        ));
    };
    let mut attributes = fields
        .into_iter()
        .map(|(name, value)| {
            crate::attribute_value_from_json_value(value).map(|value| (name, value))
        })
        .collect::<Result<HashMap<_, _>, _>>()?;
    normalize_timestamp_aliases(&mut attributes);
    attributes.extend(entity_metadata(entity));
    Ok(attributes)
}

fn normalize_timestamp_aliases(attributes: &mut HashMap<String, AttributeValue>) {
    for (long_name, alias) in [
        (CREATED_AT_ATTR, CREATED_AT_ALIAS_ATTR),
        (UPDATED_AT_ATTR, UPDATED_AT_ALIAS_ATTR),
        (EXPIRES_AT_ATTR, EXPIRES_AT_ALIAS_ATTR),
    ] {
        if let Some(value) = attributes.remove(long_name) {
            attributes.entry(alias.to_string()).or_insert(value);
        }
    }
}

fn encode_single_table_metadata<T: SingleTableEntity>(
    entity: &T,
) -> Result<Vec<u8>, crate::ConversionError> {
    serde_json::to_vec(&entity_metadata(entity))
        .map_err(|err| crate::ConversionError::Serialization(err.to_string()))
}

fn entity_metadata<T: SingleTableEntity>(entity: &T) -> HashMap<String, AttributeValue> {
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
    metadata
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
