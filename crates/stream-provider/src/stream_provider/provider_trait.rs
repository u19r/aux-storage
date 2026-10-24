//! Stream provider for DynamoDB-compatible change streams.
use async_trait::async_trait;
use storage_types::{
    AttributeMap, DurationSeconds, ItemStreamVersion, KeyAttributes, KeySchemaElement,
    StreamItemId, StreamName, StreamRecord, UserStreamName,
};

use super::pointer_read::{PointerReadPurpose, PointerStreamRead};
use crate::{
    constants::STREAM_LIMITS,
    errors::{StreamError, StreamInternalKind, StreamResult, StreamValidationKind},
    newtypes::CursorName,
    protected_cursor::{
        BootstrapCursorName, ProtectedCursorAdvanceRequest, ProtectedCursorPage,
        ProtectedCursorReadRequest,
    },
    system_stream::{
        SystemStreamLineage, SystemStreamPage, SystemStreamPageRequest, SystemStreamSplitRequest,
        SystemStreamSplitResult,
    },
    types::{
        CursorPage, CursorPosition, PointerRecordsResult, StoredStreamPointer, Stream,
        StreamCursor, StreamDataType, StreamItem, StreamPage, StreamPartitioningMode,
    },
};

/// Validate that a limit is between 1 and 1000 inclusive
pub fn validate_limit(limit: u32) -> StreamResult<()> {
    STREAM_LIMITS
        .validate(limit)
        .map_err(|_| StreamError::validation(StreamValidationKind::InvalidLimit))
        .map(|_| ())
}

#[async_trait]
pub trait StreamProvider: Send + Sync {
    /// Initialize the stream storage backend
    async fn initialize_stream(&self) -> StreamResult<()>;

    /// Create a new stream with optional TTL
    async fn create_stream(
        &self,
        stream_name: UserStreamName,
        ttl_seconds: Option<DurationSeconds>,
        partitioning_mode: StreamPartitioningMode,
    ) -> StreamResult<StreamName>;

    /// Delete a stream and all its items and cursors
    async fn delete_stream(&self, stream_name: UserStreamName) -> StreamResult<()>;

    /// Get stream information
    async fn get_stream(&self, stream_name: UserStreamName) -> StreamResult<Option<Stream>>;

    /// Append an item to the stream
    async fn append_item(
        &self,
        stream_name: StreamName,
        item_data: &[u8],
        partition_key: Option<&str>,
    ) -> StreamResult<StreamItemId>;

    /// Read items forward (chronological order - oldest first)
    async fn read_forward(
        &self,
        stream_name: StreamName,
        exclusive_start_key: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage>;

    /// Read items backward (reverse chronological order - newest first)
    async fn read_backward(
        &self,
        stream_name: StreamName,
        exclusive_start_key: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage>;

    /// Read one logical SystemStream page.  Providers that do not own a
    /// logical scope/frontier inherit the fail-closed contract; the SQLite
    /// reference provider implements the transaction and issued-token catalog
    /// behind this boundary.
    async fn get_system_stream_page(
        &self,
        _request: SystemStreamPageRequest,
    ) -> StreamResult<SystemStreamPage> {
        Err(StreamError::from(storage_types::StorageError::unsupported(
            "SystemStream logical lineage is unavailable from this backend",
        )))
    }

    /// Return the provider's logical lineage catalog.  This is primarily a
    /// conformance/readiness seam; public records contain positions, never
    /// physical provider identifiers.
    async fn system_stream_lineage_catalog(
        &self,
        _source_scope: storage_types::SystemStreamScope,
    ) -> StreamResult<Vec<SystemStreamLineage>> {
        Err(StreamError::from(storage_types::StorageError::unsupported(
            "SystemStream logical lineage is unavailable from this backend",
        )))
    }

    /// Return the database-owned logical SystemStream scope. This is a
    /// discovery seam for internal clients; records still validate the scope
    /// supplied by each page request.
    async fn system_stream_scope(&self) -> StreamResult<storage_types::SystemStreamScope> {
        Err(StreamError::from(storage_types::StorageError::unsupported(
            "SystemStream logical scope is unavailable from this backend",
        )))
    }

    /// Atomically split one logical SystemStream lineage.
    ///
    /// A provider must compare the complete request with the durable
    /// operation receipt before reading or writing split state. An exact
    /// receipt is returned as `Replayed`; an unknown operation with any
    /// partial child publication is rejected. A fresh operation must use one
    /// native transaction that records the receipt before applying effects,
    /// seals the exact active parent with a scope/generation/lineage write
    /// fence, creates both child catalog rows and initialized sequences, and
    /// publishes private routing/topology.
    /// Existing checkpoints remain immutable and a sealed parent is never
    /// appendable. The result may contain only logical identities and must be
    /// returned after the transaction commits; page requests are unchanged.
    async fn split_system_stream(
        &self,
        _request: SystemStreamSplitRequest,
    ) -> StreamResult<SystemStreamSplitResult> {
        Err(StreamError::from(storage_types::StorageError::unsupported(
            "SystemStream logical split is unavailable from this backend",
        )))
    }

    /// Decode a provider-owned stream pointer into its logical representation.
    async fn decode_stored_stream_pointer(
        &self,
        pointer_item: &StreamItem,
    ) -> StreamResult<StoredStreamPointer> {
        storage_types::storage_serde::from_bytes(&pointer_item.data).map_err(|error| {
            StreamError::internal_with_detail(
                StreamInternalKind::ParseStreamPointer,
                format_args!("pointer {}: {error}", pointer_item.id),
            )
        })
    }

    async fn read_item_stream_backward_from_pointer(
        &self,
        history: storage_types::ItemHistoryAddress,
        pointer_stream_item_id: StreamItemId,
        target_item_stream_version: ItemStreamVersion,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        let page = self
            .read_item_history(
                history,
                Some(StreamItemId::from(target_item_stream_version).increment()),
                limit,
            )
            .await?;
        if page.items.first().map(|item| item.id)
            != Some(StreamItemId::from(target_item_stream_version))
        {
            return Err(StreamError::internal_with_detail(
                StreamInternalKind::ParseStreamPointer,
                format_args!(
                    "pointer {pointer_stream_item_id} targets missing item version \
                     {target_item_stream_version}"
                ),
            ));
        }
        Ok(page)
    }

    /// Read physical history using the identity retained by a stored pointer.
    async fn read_item_history(
        &self,
        history: storage_types::ItemHistoryAddress,
        exclusive_start: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        self.read_backward(history.physical_stream_name(), exclusive_start, limit)
            .await
    }

    /// Create a cursor at the specified position
    async fn create_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
        position: CursorPosition,
    ) -> StreamResult<()>;

    /// Delete a cursor
    async fn delete_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
    ) -> StreamResult<()>;

    /// Read from a cursor without advancing the cursor position
    async fn read_from_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
        limit: u32,
    ) -> StreamResult<CursorPage>;

    /// Read a bootstrap page under the provider-owned protected cursor.
    ///
    /// Implementations must authenticate the cursor identity, boundary, and
    /// current position in one native/consistent operation. A default is not
    /// provided because an unverified empty page must never authorize
    /// activation.
    async fn read_from_protected_cursor(
        &self,
        _request: ProtectedCursorReadRequest,
    ) -> StreamResult<ProtectedCursorPage> {
        Err(StreamError::continuity(
            crate::StreamContinuityKind::Unsupported,
            "provider has no protected cursor implementation",
        ))
    }

    /// Atomically create or reconcile a typed bootstrap cursor and return its
    /// authoritative retained position. Generic cursor operations must not be
    /// used to acquire bootstrap retention authority.
    async fn create_protected_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: BootstrapCursorName,
        _position: CursorPosition,
    ) -> StreamResult<StreamCursor> {
        Err(StreamError::continuity(
            crate::StreamContinuityKind::Unsupported,
            "provider has no protected cursor creation implementation",
        ))
    }

    /// Read a typed bootstrap cursor for restart reconciliation. Generic
    /// cursor reads must not be used to recover protected retention state.
    async fn get_protected_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: BootstrapCursorName,
    ) -> StreamResult<Option<StreamCursor>> {
        Err(StreamError::continuity(
            crate::StreamContinuityKind::Unsupported,
            "provider has no protected cursor reconciliation implementation",
        ))
    }

    /// Atomically authenticate and monotonically advance a protected cursor.
    async fn advance_protected_cursor(
        &self,
        _request: ProtectedCursorAdvanceRequest,
    ) -> StreamResult<StreamItemId> {
        Err(StreamError::continuity(
            crate::StreamContinuityKind::Unsupported,
            "provider has no protected cursor advance implementation",
        ))
    }

    /// Delete a typed bootstrap cursor after its steady-state checkpoint is
    /// durable. Generic deletion must not be able to remove protected state.
    async fn delete_protected_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: BootstrapCursorName,
    ) -> StreamResult<()> {
        Err(StreamError::continuity(
            crate::StreamContinuityKind::Unsupported,
            "provider has no protected cursor deletion implementation",
        ))
    }

    /// Advance the cursor position to a specific item
    async fn advance_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
        to_item_id: StreamItemId,
    ) -> StreamResult<()>;

    /// Get cursor information
    async fn get_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
    ) -> StreamResult<Option<StreamCursor>>;

    fn get_key_attributes(
        &self,
        item: &AttributeMap,
        key_schema: &[KeySchemaElement],
    ) -> StreamResult<KeyAttributes> {
        let mut keys = KeyAttributes::with_capacity(key_schema.len());
        for schema_key in key_schema {
            let value = item.get(&schema_key.attribute_name).ok_or_else(|| {
                StreamError::internal(StreamInternalKind::InvalidOrMissingKeyAttribute)
            })?;
            keys.insert(schema_key.attribute_name.clone(), value.clone());
        }
        Ok(keys)
    }

    async fn get_items_from_pointer_stream(
        &self,
        pointer_stream_name: StreamName,
        starting_item_id: Option<StreamItemId>,
        limit: Option<u32>,
    ) -> StreamResult<PointerRecordsResult> {
        PointerStreamRead {
            provider: self,
            purpose: PointerReadPurpose::Direct,
        }
        .read(pointer_stream_name, starting_item_id, limit)
        .await
    }

    /// Read a pointer stream for a durable background consumer.
    ///
    /// Historical pointers remain valid after newer images are published.
    /// Referenced images use the same exact-target reader as direct reads.
    /// Providers can override this method to omit deleted table incarnations
    /// when their background consumer contract permits that omission.
    async fn get_items_from_pointer_stream_for_background(
        &self,
        pointer_stream_name: StreamName,
        starting_item_id: Option<StreamItemId>,
        limit: Option<u32>,
    ) -> StreamResult<PointerRecordsResult> {
        PointerStreamRead {
            provider: self,
            purpose: PointerReadPurpose::Background,
        }
        .read(pointer_stream_name, starting_item_id, limit)
        .await
    }

    /// Decode a background record. A provider may omit a deleted source when
    /// its durable consumer contract allows that omission.
    async fn decode_stored_stream_pointer_for_background(
        &self,
        pointer_item: &StreamItem,
    ) -> StreamResult<Option<StoredStreamPointer>> {
        self.decode_stored_stream_pointer(pointer_item)
            .await
            .map(Some)
    }

    async fn get_stream_records_from_pointer_stream(
        &self,
        pointer_stream_name: StreamName,
        key_schema: &[KeySchemaElement],
        starting_item_id: Option<StreamItemId>,
        limit: Option<u32>,
    ) -> StreamResult<(Vec<StreamRecord>, Option<StreamItemId>)> {
        let PointerRecordsResult {
            records: results,
            last_evaluated_key: last_evaluated_pointer,
            last_scanned_key,
            has_more: _,
        } = self
            .get_items_from_pointer_stream(pointer_stream_name, starting_item_id, limit)
            .await?;

        let mut image_items = Vec::new();

        for (pointer, item_data) in results {
            let new_image = item_data.first();
            let old_image = item_data.get(1);
            let Some(new_image) = new_image else {
                continue;
            };

            let mut record_old_image: Option<AttributeMap> = None;
            let mut record_new_image: Option<AttributeMap> = None;

            if !matches!(new_image.data_type, StreamDataType::DeleteMarker) {
                record_new_image = Some(
                    storage_types::storage_serde::from_bytes(new_image.data.as_slice()).map_err(
                        |e| StreamError::internal_with_detail(StreamInternalKind::ParseNewImage, e),
                    )?,
                );
            }

            if let Some(old_image) = old_image
                && !matches!(old_image.data_type, StreamDataType::DeleteMarker)
            {
                record_old_image = Some(
                    storage_types::storage_serde::from_bytes(old_image.data.as_slice()).map_err(
                        |e| StreamError::internal_with_detail(StreamInternalKind::ParseOldImage, e),
                    )?,
                );
            }

            let item_for_key = match (&record_old_image, &record_new_image) {
                (Some(_) | None, Some(new)) => new,
                (Some(old), None) => old,
                (None, None) => {
                    continue;
                }
            };

            let keys = self.get_key_attributes(item_for_key, key_schema)?;

            let stream_record = StreamRecord {
                cursor: Some(pointer.stream_item_id.to_string()),
                source_table_name: Some(pointer.table_name),
                keys,
                sequence_number: pointer.stream_item_id.to_string(),
                old_image: record_old_image,
                new_image: record_new_image,
                system_position: None,
            };

            image_items.push(stream_record);
        }

        let checkpoint_pointer = last_evaluated_pointer
            .or_else(|| image_items.is_empty().then_some(last_scanned_key).flatten());

        Ok((image_items, checkpoint_pointer))
    }
}
