use serde::{Serialize, ser::SerializeMap};
use storage_types::{
    AttributeValueLookup, ItemStreamVersion, KeyAttributes, StorageError, StorageResult,
    SystemStreamSourceTableName, TableName, TimestampMillis, WireItem,
    normalize_key_attribute_value_for_write,
};

/// Backend-neutral inputs for one logical SystemStream record.
///
/// Both KV and SQL name the same effect inputs once, here: the logical table,
/// its key schema, both images, declarations, versioning, and provenance.
/// Physical pointer rows stay backend-private; only the logical effect
/// travels through this shape.
pub struct LogicalSystemStreamMutationInput<'a> {
    pub history_id: storage_types::TableHistoryId,
    pub route: &'a DynamoMutationRoute,
    pub item: Option<storage_types::VerifiedWireItem>,
    pub old_item: Option<&'a storage_types::VerifiedWireItem>,
    pub previous_version: Option<ItemStreamVersion>,
    pub indexers: &'a [String],
    pub item_stream_version: ItemStreamVersion,

    pub created_at: TimestampMillis,
    pub user_identity: Option<storage_types::StreamUserIdentity>,
}

/// Backend-neutral logical mutation retaining validated wire images without
/// decoding maps.
#[derive(Clone, Debug)]
pub struct LogicalSystemStreamMutation {
    pub history_id: storage_types::TableHistoryId,
    pub source_table_name: SystemStreamSourceTableName,
    pub key: KeyAttributes,
    pub item: Option<WireItem>,
    pub predecessor_payload_bytes: Option<usize>,
    pub previous_version: Option<ItemStreamVersion>,
    pub item_stream_version: ItemStreamVersion,
    pub indexers: Vec<String>,

    pub created_at: TimestampMillis,
    pub user_identity: Option<storage_types::StreamUserIdentity>,
}

impl TryFrom<LogicalSystemStreamMutationInput<'_>> for LogicalSystemStreamMutation {
    type Error = StorageError;

    fn try_from(input: LogicalSystemStreamMutationInput<'_>) -> StorageResult<Self> {
        if input.old_item.is_some() != input.previous_version.is_some()
            || input.previous_version.is_some_and(|previous| {
                previous.get() == 0 || previous >= input.item_stream_version
            })
        {
            return Err(StorageError::internal("Invalid exact stream predecessor"));
        }
        let key = input.route.key.clone();
        let predecessor_payload_bytes = input
            .old_item
            .map(|item| item.dynamo_json_bytes().map(|bytes| bytes.len()))
            .transpose()?;
        if input.item.is_none() && input.previous_version.is_none() {
            return Err(StorageError::internal(
                "Deletion history requires its exact predecessor",
            ));
        }
        Ok(Self {
            history_id: input.history_id,
            source_table_name: input.route.source_table_name.clone(),
            item: input.item.map(WireItem::from),
            predecessor_payload_bytes,
            previous_version: input.previous_version,
            key,
            item_stream_version: input.item_stream_version,
            indexers: input.indexers.to_vec(),

            created_at: input.created_at,
            user_identity: input.user_identity,
        })
    }
}

/// Validated routing facts prepared before entering the writer.
pub struct DynamoMutationRoute {
    key: KeyAttributes,
    source_table_name: SystemStreamSourceTableName,
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
/// key-schema-ordered key attributes. Shared so KV and SQL route the same
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
        for key in self.key_schema {
            if let Some(value) = self.attributes.get_attribute_value(&key.attribute_name) {
                map.serialize_entry(&key.attribute_name, value)?;
            }
        }
        map.end()
    }
}
