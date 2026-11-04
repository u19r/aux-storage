//! One logical Dynamo write becomes the same stream rows on SQL and KV.
//!
//! The backends own row keys, statements, and native transaction staging.
//! Image encoding, reference identity and payload admission
//! are decided once here after the operation has allocated a stream position.

use storage_types::{
    BoundTableInfo, ItemHistoryAddress, ItemKey, KeyAttributes, StorageError, StorageResult,
    StreamName, SystemStreamPosition, SystemStreamScope,
};

use crate::{LogicalSystemStreamMutation, StreamItem, StreamWriteKind};

pub struct DynamoStreamWriteInput<'a> {
    pub table: &'a BoundTableInfo,
    pub mutation: LogicalSystemStreamMutation,
    pub source_scope: SystemStreamScope,
    pub position: SystemStreamPosition,
}

impl DynamoStreamWriteInput<'_> {
    fn require_bound_identity(&self) -> StorageResult<()> {
        let table = self.table;
        let mutation = &self.mutation;
        if mutation.history_id != table.history_id
            || mutation.source_table_name.as_str() != table.table_name.as_ref()
        {
            return Err(StorageError::internal(
                "stream mutation does not match its bound table",
            ));
        }
        Ok(())
    }
}

/// Materialized history plus two metadata indexes share one native commit.
pub struct DynamoStreamWritePlan {
    pub rows: [StreamWriteKind<StreamItem>; 2],
    pub record: storage_types::SystemHistoryRecord,
}

impl TryFrom<DynamoStreamWriteInput<'_>> for DynamoStreamWritePlan {
    type Error = StorageError;

    fn try_from(input: DynamoStreamWriteInput<'_>) -> StorageResult<Self> {
        input.require_bound_identity()?;
        let previous_bytes = input.mutation.predecessor_payload_bytes;
        let metadata = input.history_metadata()?;
        let mutation = input.mutation;
        let change = match mutation.item {
            None => storage_types::ItemHistoryChange::Deleted { key: mutation.key },
            Some(item) => storage_types::ItemHistoryChange::Image {
                item,
                indexers: storage_types::IndexerDeclaration::try_new(
                    mutation.indexers,
                    input.table.max_indexers,
                )?,
            },
        };
        let item = StreamItem::try_from(storage_types::ItemHistoryEntry::try_new(
            metadata.clone(),
            change,
        )?)?;
        let pointer =
            storage_types::ItemHistoryPointer::try_new(metadata, item.data.len(), previous_bytes)?;
        let table = StreamItem::try_from(&pointer)?;
        Ok(Self {
            rows: [StreamWriteKind::Item(item), StreamWriteKind::Table(table)],
            record: pointer.try_into()?,
        })
    }
}

impl DynamoStreamWriteInput<'_> {
    fn history_metadata(&self) -> StorageResult<storage_types::ItemHistoryMetadata> {
        storage_types::ItemHistoryMetadata::try_new(
            dynamo_item_history_address(self.table, &self.mutation.key)?,
            self.mutation.item_stream_version,
            self.mutation.previous_version,
            storage_types::ItemHistoryPublication::Event {
                kind: if self.mutation.item.is_none() {
                    storage_types::ItemHistoryKind::Deleted
                } else {
                    storage_types::ItemHistoryKind::Image
                },
                source_scope: self.source_scope.clone(),
                table_position: self.position.clone(),
                created_at: self.mutation.created_at,
            },
        )
    }
}

pub fn dynamo_item_history_address(
    table: &BoundTableInfo,
    key: &KeyAttributes,
) -> StorageResult<ItemHistoryAddress> {
    let item_key = ItemKey::from_key_schema(table.table_name.clone(), &table.key_schema, key)
        .map_err(|error| StorageError::internal(&format!("stream item key error: {error}")))?;
    Ok(ItemHistoryAddress {
        history_id: table.history_id,
        stream_name: StreamName::table_item_stream(&table.table_name, &item_key)?,
    })
}
