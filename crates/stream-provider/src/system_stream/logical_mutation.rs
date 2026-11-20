use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Serialize, ser::SerializeMap};
use storage_types::{
    AttributeValueLookup, KeyAttributes, StorageError, StorageResult, SystemStreamSourceTableName,
    TableName, WireItem, normalize_key_attribute_value_for_write,
};

/// Validated routing facts prepared before entering the writer.
pub struct DynamoMutationRoute {
    pub(crate) key: KeyAttributes,
    pub(crate) source_table_name: SystemStreamSourceTableName,
    routing_key: Vec<u8>,
}

impl DynamoMutationRoute {
    pub fn verify_item(&self, item: WireItem) -> StorageResult<storage_types::VerifiedWireItem> {
        storage_types::VerifiedWireItem::try_new(item, &self.key)
    }

    pub fn try_new(
        table_name: &TableName,
        key_schema: &[storage_types::KeySchemaElement],
        key: &KeyAttributes,
    ) -> StorageResult<Self> {
        let key = normalized_route_key(key);
        let source_table_name = SystemStreamSourceTableName::try_from(table_name)
            .map_err(|error| StorageError::validation(error.to_string()))?;
        let routing_key = canonical_route_key(&source_table_name, key_schema, &key)?;
        Ok(Self {
            key,
            source_table_name,
            routing_key,
        })
    }

    pub fn routing_key(&self) -> &[u8] {
        &self.routing_key
    }
}

fn normalized_route_key(key: &KeyAttributes) -> KeyAttributes {
    KeyAttributes::from_iter(key.iter().map(|(name, value)| {
        (
            name.to_owned(),
            normalize_key_attribute_value_for_write(value),
        )
    }))
}

/// Canonical routing bytes for one logical mutation: source table plus
/// normalized partition key. Shared so KV and SQL route the same
/// logical write to the same lineage.
pub fn canonical_route_key(
    source_table_name: &SystemStreamSourceTableName,
    key_schema: &[storage_types::KeySchemaElement],
    key: &impl AttributeValueLookup,
) -> StorageResult<Vec<u8>> {
    storage_types::storage_serde::to_bytes(&CanonicalRouteEvidenceRef {
        source_table_name,
        key: CanonicalKeyRef {
            attributes: key,
            key_schema,
        },
    })
}

struct CanonicalRouteEvidenceRef<'a, L: AttributeValueLookup> {
    source_table_name: &'a SystemStreamSourceTableName,
    key: CanonicalKeyRef<'a, L>,
}

struct CanonicalKeyRef<'a, L: AttributeValueLookup> {
    attributes: &'a L,
    key_schema: &'a [storage_types::KeySchemaElement],
}

impl<L: AttributeValueLookup> Serialize for CanonicalRouteEvidenceRef<'_, L> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("source_table_name", self.source_table_name)?;
        map.serialize_entry("key", &self.key)?;
        map.end()
    }
}

impl<L: AttributeValueLookup> Serialize for CanonicalKeyRef<'_, L> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut map = serializer.serialize_map(None)?;
        for key in self
            .key_schema
            .iter()
            .filter(|key| key.key_type == storage_types::KeyType::Hash)
        {
            if let Some(value) = self.attributes.get_attribute_value(&key.attribute_name) {
                match value {
                    storage_types::AttributeValue::N(number) => {
                        let canonical = storage_types::canonical_dynamodb_number(number)
                            .map_err(serde::ser::Error::custom)?;
                        map.serialize_entry(
                            &key.attribute_name,
                            &storage_types::AttributeValue::N(canonical),
                        )?;
                    }
                    storage_types::AttributeValue::B(binary) => {
                        let bytes = storage_types::dynamodb_binary::decode_base64_string(binary)
                            .map_err(serde::ser::Error::custom)?;
                        map.serialize_entry(
                            &key.attribute_name,
                            &storage_types::AttributeValue::B(STANDARD.encode(bytes)),
                        )?;
                    }
                    _ => map.serialize_entry(&key.attribute_name, value)?,
                }
            }
        }
        map.end()
    }
}
