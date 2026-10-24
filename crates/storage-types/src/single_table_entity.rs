//! Typed single-table entity keys, indexer metadata, and wire encoding.

use std::{borrow::Cow, collections::HashMap};

use serde::Serialize;

pub use crate::constants::{
    CREATED_AT_ALIAS_ATTR, CREATED_AT_ATTR, ENTITY_TYPE_ATTR, EXPIRES_AT_ALIAS_ATTR,
    EXPIRES_AT_ATTR, LEGACY_ENTITY_TYPE_ATTR, UPDATED_AT_ALIAS_ATTR, UPDATED_AT_ATTR,
};

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

    /// Bind a decoded entity to its addressed row before its identity selects
    /// a destination. Split physical keys and residual keys must agree; the
    /// canonical wire reader retains the existing entity-type alias format.
    fn validate_stored_identity(&self, source: &crate::WireItem) -> crate::StorageResult<()> {
        let key = crate::KeyAttributes::from_iter([
            ("pk".into(), crate::AttributeValue::S(self.pk())),
            ("sk".into(), crate::AttributeValue::S(self.sk())),
        ]);
        let entity_types = source.select_attribute_values(&[
            ENTITY_TYPE_ATTR,
            crate::constants::LEGACY_ENTITY_TYPE_ATTR,
        ])?;
        if !source.has_verified_keys(&key)?
            || !matches!(entity_types.as_slice(),
                [Some(crate::AttributeValue::S(value)), None]
                | [None, Some(crate::AttributeValue::S(value))]
                    if value == Self::STORAGE_ENTITY_TYPE)
        {
            return Err(crate::StorageError::internal(
                "stored entity identity mismatch",
            ));
        }
        Ok(())
    }

    /// Verify the independent physical source address before entity identity
    /// selects a destination. A range image alone cannot prove its table/key.
    fn validate_addressed_identity(
        &self,
        table: &crate::TableName,
        address: &crate::ItemKey,
    ) -> crate::StorageResult<()> {
        let key = match address {
            crate::ItemKey::Table(key) => key,
            crate::ItemKey::Index(key) => &key.table_key,
            crate::ItemKey::IndexPrefix(_) => {
                return Err(crate::StorageError::internal(
                    "entity source lacks a table key",
                ));
            }
        };
        if &key.table_name != table
            || key.hash_key != crate::AttributeValue::S(self.pk())
            || key.range_key != Some(crate::AttributeValue::S(self.sk()))
        {
            return Err(crate::StorageError::internal(
                "addressed entity identity mismatch",
            ));
        }
        Ok(())
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

/// Build a write-ready entity envelope using the derived write encoder path.
///
/// This path avoids materializing a `HashMap<String, AttributeValue>` for the
/// payload itself and instead stitches key metadata directly into the encoded
/// Dynamo wire JSON object. Generated keys precede the payload and replace
/// duplicate payload names; TTL and indexers precede ordinary metadata.
pub fn to_wire_entity<T: SingleTableEntity + TryIntoWireItem>(
    entity: &T,
) -> Result<WireEntity, crate::ConversionError> {
    let payload = entity
        .try_into_wire_item()
        .map_err(|err| crate::ConversionError::Serialization(err.to_string()))?;
    let payload_data = payload
        .dynamo_json_bytes()
        .map_err(|err| crate::ConversionError::Serialization(err.to_string()))?;
    let metadata_data = serde_json::to_vec(&SingleTableMetadata(entity))
        .map_err(|err| crate::ConversionError::Serialization(err.to_string()))?;
    let bytes = crate::wire_item::attributes::merge_attribute_bytes(&payload_data, &metadata_data)
        .map_err(|err| crate::ConversionError::Serialization(err.to_string()))?;
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
    Ok(to_wire_entity(entity)?.item)
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
    let attributes = to_attribute_map(entity)?;
    WireItem::try_from(&attributes)
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
    Ok(to_attribute_map(entity)?.into_hashmap())
}

/// Encode a manually serialized entity without allocating an intermediate
/// HashMap.
pub fn to_attribute_map<T: SingleTableEntity + Serialize>(
    entity: &T,
) -> Result<crate::AttributeMap, crate::ConversionError> {
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
        .collect::<Result<crate::AttributeMap, _>>()?;
    normalize_timestamp_aliases(&mut attributes);
    for (name, value) in entity_metadata(entity) {
        attributes.insert(name, AttributeValue::S(value.into_owned()));
    }
    attributes.order_single_table_attributes(T::INDEXERS);
    Ok(attributes)
}

fn normalize_timestamp_aliases(attributes: &mut crate::AttributeMap) {
    for (long_name, alias) in [
        (CREATED_AT_ATTR, CREATED_AT_ALIAS_ATTR),
        (UPDATED_AT_ATTR, UPDATED_AT_ALIAS_ATTR),
        (EXPIRES_AT_ATTR, EXPIRES_AT_ALIAS_ATTR),
    ] {
        if let Some(value) = attributes.remove(long_name)
            && !attributes.contains_key(alias)
        {
            attributes.insert(alias, value);
        }
    }
}

struct SingleTableMetadata<'a, T>(&'a T);

#[derive(Serialize)]
struct MetadataString<'a> {
    #[serde(rename = "S")]
    value: &'a str,
}

impl<T: SingleTableEntity> Serialize for SingleTableMetadata<'_, T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        let mut map = serializer.serialize_map(None)?;
        for (name, value) in entity_metadata(self.0) {
            map.serialize_entry(name, &MetadataString { value: &value })?;
        }
        map.end()
    }
}

fn entity_metadata<T: SingleTableEntity>(
    entity: &T,
) -> impl Iterator<Item = (&'static str, Cow<'_, str>)> {
    let primary = [
        ("pk", entity.pk_cow()),
        ("sk", entity.sk_cow()),
        (ENTITY_TYPE_ATTR, Cow::Borrowed(T::ENTITY_TYPE)),
    ];
    let secondary = [
        ("gsi1pk", "gsi1sk", entity.gsi1()),
        ("gsi2pk", "gsi2sk", entity.gsi2()),
        ("gsi3pk", "gsi3sk", entity.gsi3()),
        ("gsi4pk", "gsi4sk", entity.gsi4()),
        ("gsi5pk", "gsi5sk", entity.gsi5()),
    ];
    primary
        .into_iter()
        .chain(secondary.into_iter().flat_map(|(pk, sk, pair)| {
            pair.into_iter().flat_map(move |(partition, sort)| {
                [(pk, Cow::Owned(partition)), (sk, Cow::Owned(sort))]
            })
        }))
}

#[must_use]
pub fn matches_entity_type<T: SingleTableEntity>(map: &impl crate::AttributeValueLookup) -> bool {
    entity_type_from_item(map).is_some_and(|entity_type| entity_type == T::STORAGE_ENTITY_TYPE)
}

#[must_use]
pub fn storage_entity_type_from_item(
    map: &impl crate::AttributeValueLookup,
) -> Option<crate::StorageEntityType> {
    match map
        .get_attribute_value(ENTITY_TYPE_ATTR)
        .or_else(|| map.get_attribute_value(LEGACY_ENTITY_TYPE_ATTR))
    {
        Some(AttributeValue::S(value)) => crate::StorageEntityType::parse_db(value),
        _ => None,
    }
}

#[must_use]
pub fn entity_type_from_item(map: &impl crate::AttributeValueLookup) -> Option<&str> {
    match map
        .get_attribute_value(ENTITY_TYPE_ATTR)
        .or_else(|| map.get_attribute_value(LEGACY_ENTITY_TYPE_ATTR))
    {
        Some(AttributeValue::S(value)) => Some(value.as_str()),
        _ => None,
    }
}
