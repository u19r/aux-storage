use serde::{Serialize, ser::SerializeMap};
use storage_types::{
    AttributeValueLookup, ItemStreamVersion, KeyAttributes, StorageError, StorageResult,
    StreamItemId, SystemStreamSourceTableName, TableName, TimestampMillis, WireItem,
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
    pub item: PreparedStreamImage,
    pub old_item: Option<storage_types::VerifiedWireItem>,
    pub indexers: &'a [String],
    pub old_indexers: Option<&'a [String]>,
    pub stream_item_id: StreamItemId,
    pub item_stream_version: ItemStreamVersion,
    pub is_deleted: bool,

    pub created_at: TimestampMillis,
}

/// Backend-neutral logical mutation retaining validated wire images without
/// decoding maps.
#[derive(Clone, Debug)]
pub struct LogicalSystemStreamMutation {
    pub history_id: storage_types::TableHistoryId,
    pub stream_item_id: StreamItemId,
    pub source_table_name: SystemStreamSourceTableName,
    pub key: KeyAttributes,
    pub item: WireItem,
    pub item_json: Vec<u8>,
    pub old_item: Option<WireItem>,
    pub(crate) old_source: Option<storage_types::StoredImageSource>,
    pub item_stream_version: ItemStreamVersion,
    pub indexers: Vec<String>,
    pub old_indexers: Option<Vec<String>>,

    pub is_deleted: bool,
    pub created_at: TimestampMillis,
    pub routing_key: Vec<u8>,
}

impl From<LogicalSystemStreamMutationInput<'_>> for LogicalSystemStreamMutation {
    fn from(input: LogicalSystemStreamMutationInput<'_>) -> Self {
        let key = input.route.key.clone();
        let PreparedStreamImage { item, json } = input.item;
        Self {
            history_id: input.history_id,
            stream_item_id: input.stream_item_id,
            source_table_name: input.route.source_table_name.clone(),
            item,
            item_json: json,
            old_source: input
                .old_item
                .as_ref()
                .and_then(storage_types::VerifiedWireItem::stored_source),
            old_item: input.old_item.map(WireItem::from),
            key,
            item_stream_version: input.item_stream_version,
            indexers: input.indexers.to_vec(),
            old_indexers: input.old_indexers.map(<[_]>::to_vec),

            is_deleted: input.is_deleted,
            created_at: input.created_at,
            routing_key: input.route.routing_key.clone(),
        }
    }
}

impl LogicalSystemStreamMutation {
    pub(crate) async fn restore_source_image<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &mut self,
        attempt: &mut A,
    ) -> StorageResult<()> {
        let Some(old) = &self.old_item else {
            if self.old_source.is_some() {
                return Err(StorageError::internal(
                    "Absent stream predecessor has source custody",
                ));
            }
            return Ok(());
        };
        let source = self
            .old_source
            .ok_or_else(|| StorageError::internal("Stream predecessor has no source custody"))?;
        if source.address.history() != self.history_id {
            return Err(StorageError::internal(
                "Stream predecessor source belongs to another incarnation",
            ));
        }
        let exact = crate::retained_image::load_stored_image(attempt, source).await?;
        crate::retained_image::require_materialized_image(&exact, old)?;
        if self.is_deleted {
            self.item_json = exact.dynamo_json_bytes()?.into_owned();
            self.item = exact.clone();
        }
        self.old_item = Some(exact);
        Ok(())
    }
}

/// Key-complete accepted image; native consumers share its raw JSON spelling.
pub struct PreparedStreamImage {
    item: WireItem,
    json: Vec<u8>,
}

impl PreparedStreamImage {
    pub fn try_new(route: &DynamoMutationRoute, item: WireItem) -> StorageResult<Self> {
        Self::try_from(storage_types::VerifiedWireItem::try_new(item, &route.key)?)
    }
}

impl TryFrom<storage_types::VerifiedWireItem> for PreparedStreamImage {
    type Error = StorageError;

    fn try_from(item: storage_types::VerifiedWireItem) -> StorageResult<Self> {
        let json = item.dynamo_json_bytes()?.into_owned();
        Ok(Self {
            item: item.into(),
            json,
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
