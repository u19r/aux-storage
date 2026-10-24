use std::borrow::Cow;

use serde::{Serialize, de::DeserializeSeed, ser::SerializeMap};

use super::{WireItem, attributes::parse_attributes};
use crate::{
    AttributeMap, AttributeValue, IndexerDeclaration, StorageError, StorageResult,
    constants::INDEXER_TUPLE_OFFSET,
    indexed_wire_item::{
        IndexedAttributeScan, IndexerMarker, IndexerMarkerSeed, indexer_tuple_index,
    },
};

pub(crate) fn encode_indexed_residual(
    item: &WireItem,
    declaration: &IndexerDeclaration,
) -> StorageResult<Vec<u8>> {
    let bytes = item.dynamo_json_bytes()?;
    encode_residual(parse_attributes(&bytes, &[])?, declaration)
}

pub(crate) fn encode_residual<'a, T: Serialize + 'a + ?Sized>(
    attributes: impl IntoIterator<Item = (Cow<'a, str>, &'a T)>,
    declaration: &'a IndexerDeclaration,
) -> StorageResult<Vec<u8>> {
    let mut entries = attributes
        .into_iter()
        .filter(|(name, _)| {
            !declaration
                .names()
                .iter()
                .any(|indexed| indexed == name.as_ref())
        })
        .map(|(name, value)| (name, ResidualValue::Attribute(value)))
        .collect::<Vec<_>>();
    entries.extend(
        declaration
            .names()
            .iter()
            .enumerate()
            .map(|(ordinal, name)| {
                (
                    Cow::Borrowed(name.as_str()),
                    ResidualValue::Indexer(IndexerMarker {
                        tuple_index: indexer_tuple_index(ordinal),
                    }),
                )
            }),
    );
    entries.sort_unstable_by(|left, right| {
        let priority = |entry: &(Cow<'_, str>, ResidualValue<'_, T>)| {
            let ordinal = match &entry.1 {
                ResidualValue::Indexer(marker) => Some(marker.tuple_index - INDEXER_TUPLE_OFFSET),
                ResidualValue::Attribute(_) => None,
            };
            crate::wire_attribute_priority(&entry.0, ordinal)
        };
        priority(left)
            .cmp(&priority(right))
            .then_with(|| left.0.cmp(&right.0))
    });
    serde_json::to_vec(&ResidualAttributes(entries))
        .map_err(|error| StorageError::internal(&error.to_string()))
}

#[derive(Serialize)]
#[serde(untagged)]
enum ResidualValue<'a, T: ?Sized> {
    Attribute(&'a T),
    Indexer(IndexerMarker),
}

struct ResidualAttributes<'a, T: ?Sized>(Vec<(Cow<'a, str>, ResidualValue<'a, T>)>);

impl<T: Serialize + ?Sized> Serialize for ResidualAttributes<'_, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, value) in &self.0 {
            map.serialize_entry(name, value)?;
        }
        map.end()
    }
}

/// Restore indexer slots while retaining every other attribute as borrowed
/// JSON. The marker scan proves one valid marker per declared slot before
/// rebuilding and validating the logical wire item.
pub(crate) fn restore_indexed_residual(
    indexed: crate::IndexedWireItem,
) -> StorageResult<(WireItem, IndexerDeclaration)> {
    let (bytes, mut slots) = indexed.into_parts();
    let mut entries = parse_attributes(&bytes, &[])?;
    let mut scan = IndexedAttributeScan::default();
    let mut names = vec![String::new(); slots.len()];
    let mut replacements = AttributeMap::new();
    for (name, value) in &entries {
        let mut deserializer = serde_json::Deserializer::from_str(value.get());
        let marker = IndexerMarkerSeed
            .deserialize(&mut deserializer)
            .map_err(|_| StorageError::internal("stored_item_corruption:marker_shape"))?;
        let Some(index) = marker else {
            continue;
        };
        scan.record_marker::<serde_json::Error>(name, index, slots.len())
            .map_err(|_| StorageError::internal("stored_item_corruption:invalid_marker"))?;
        let ordinal = index - INDEXER_TUPLE_OFFSET;
        if let Some(value) = slots[ordinal].take() {
            replacements.insert(name.to_string(), AttributeValue::S(value));
        }
        names[ordinal] = name.to_string();
    }
    scan.validate_marker_set(slots.len())?;
    entries.retain(|(name, _)| {
        !names.iter().any(|indexer| indexer == name.as_ref()) || replacements.contains_key(name)
    });
    let mut restored = Vec::with_capacity(bytes.len());
    serde_json::to_writer(
        &mut restored,
        &super::attributes::UpdatedAttributes {
            entries,
            replacements: &replacements,
        },
    )?;
    let item = WireItem::dynamo_json(restored);
    item.validate_for_write("Item")
        .map_err(|_| StorageError::internal("stored_item_corruption:residual_attribute_value"))?;
    let declaration = IndexerDeclaration::try_new(
        names,
        crate::MaxIndexers::try_from(crate::constants::MAX_INDEXERS_CAPACITY)?,
    )
    .map_err(|_| StorageError::internal("stored_item_corruption:indexer_declaration"))?;
    Ok((item, declaration))
}
