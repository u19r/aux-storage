//! Stream provider for DynamoDB-compatible change streams.
use std::collections::HashMap;

use async_trait::async_trait;
use storage_types::{
    AttributeValue, DurationSeconds, ItemStreamVersion, KeySchemaElement, StreamItemId, StreamName,
    StreamRecord, UserStreamName,
};

use crate::{
    constants::STREAM_LIMITS,
    errors::{StreamError, StreamInternalKind, StreamResult, StreamValidationKind},
    newtypes::CursorName,
    types::{
        CursorPage, CursorPosition, PointerRecordsResult, StoredStreamPointer, Stream,
        StreamCursor, StreamDataType, StreamItem, StreamPage, StreamPartitioningMode,
        StreamPointer,
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

    async fn read_item_stream_backward_from_version(
        &self,
        stream_name: StreamName,
        exclusive_start_version: ItemStreamVersion,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        self.read_backward(
            stream_name,
            Some(StreamItemId::from(exclusive_start_version)),
            limit,
        )
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
        item: &HashMap<String, AttributeValue>,
        key_schema: &[KeySchemaElement],
    ) -> StreamResult<HashMap<String, AttributeValue>> {
        let mut key_attributes = HashMap::new();

        for (attr_name, attr_value) in item {
            let key_attribute = key_schema.iter().any(|i| *attr_name == i.attribute_name);

            if key_attribute {
                key_attributes.insert(attr_name.clone(), attr_value.clone());
            }
        }

        for table_schema_key in key_schema {
            if !key_attributes.contains_key(&table_schema_key.attribute_name) {
                return Err(StreamError::internal(
                    StreamInternalKind::InvalidOrMissingKeyAttribute,
                ));
            }
        }

        Ok(key_attributes)
    }

    async fn get_items_from_pointer_stream(
        &self,
        pointer_stream_name: StreamName,
        starting_item_id: Option<StreamItemId>,
        limit: Option<u32>,
    ) -> StreamResult<PointerRecordsResult> {
        let item_pointers = self
            .read_forward(
                pointer_stream_name.clone(),
                starting_item_id,
                STREAM_LIMITS.clamp(limit),
            )
            .await?;

        let mut tasks = Vec::new();
        let mut task_order = Vec::new();

        for pointer_item in item_pointers.items {
            let pointer_stream_name_dbg: String = (&pointer_stream_name).into();
            let stored_pointer = storage_types::storage_serde::from_bytes::<StoredStreamPointer>(
                pointer_item.data.as_slice(),
            )
            .map_err(|e| {
                tracing::error!(
                    pointer_stream = %pointer_stream_name_dbg,
                    pointer_id = %pointer_item.id,
                    error = %e,
                    "failed to parse stream pointer"
                );
                StreamError::internal_with_detail(
                    StreamInternalKind::ParseStreamPointer,
                    format_args!(
                        "pointer {} in {}: {e}",
                        pointer_item.id, pointer_stream_name_dbg
                    ),
                )
            })?;
            match stored_pointer {
                StoredStreamPointer::Embedded {
                    stream_name,
                    table_name,
                    item_stream_version,
                    items,
                    ..
                } => {
                    let pointer = StreamPointer {
                        stream_name,
                        table_name,
                        item_stream_version,
                        stream_item_id: pointer_item.id,
                    };
                    let embedded_items = items
                        .into_iter()
                        .map(|item| StreamItem {
                            id: pointer_item.id,
                            stream_name: None,
                            data: item.data,
                            data_type: item.data_type,
                            created_at: pointer_item.created_at,
                        })
                        .collect::<Vec<_>>();
                    task_order.push(TaskSlot::Ready((pointer, embedded_items)));
                }
                StoredStreamPointer::Pointer {
                    stream_name,
                    table_name,
                    item_stream_version,
                    ..
                } => {
                    let pointer = StreamPointer {
                        stream_name,
                        table_name,
                        item_stream_version,
                        stream_item_id: pointer_item.id,
                    };
                    let task = async move || {
                        let target_stream_dbg: String = (&pointer.stream_name).into();
                        tracing::debug!(
                            table = %pointer.table_name,
                            pointer_stream = %pointer_stream_name_dbg,
                            target_stream = %target_stream_dbg,
                            pointer_id = %pointer.stream_item_id,
                            target_version = %pointer.item_stream_version,
                            "stream-provider: fetching item images for pointer"
                        );
                        let items = match pointer.item_stream_version.checked_increment() {
                            Some(next_version) => {
                                self.read_item_stream_backward_from_version(
                                    pointer.stream_name.clone(),
                                    next_version,
                                    2,
                                )
                                .await
                            }
                            None => Err(StreamError::internal_with_detail(
                                StreamInternalKind::ParseStreamPointer,
                                "item stream version overflow",
                            )),
                        };

                        match &items {
                            Ok(page) => {
                                let types: Vec<_> =
                                    page.items.iter().map(|i| i.data_type).collect();
                                tracing::debug!(
                                    target_stream = %target_stream_dbg,
                                    types = ?types,
                                    len = page.items.len(),
                                    "stream-provider: fetched item images"
                                );
                            }
                            Err(e) => {
                                tracing::debug!(
                                    target_stream = %target_stream_dbg,
                                    error = %e,
                                    "stream-provider: error fetching item images"
                                );
                            }
                        }

                        (pointer, items)
                    };
                    let idx = tasks.len();
                    tasks.push(task);
                    task_order.push(TaskSlot::Pending(idx));
                }
            }
        }
        let results = futures::future::join_all(tasks.into_iter().map(|t| t())).await;

        let last_evaluated_key = if item_pointers.has_more {
            item_pointers.last_evaluated_key
        } else {
            None
        };

        Ok(PointerRecordsResult {
            last_evaluated_key,
            records: task_order
                .into_iter()
                .map(|slot| match slot {
                    TaskSlot::Ready(record) => record,
                    TaskSlot::Pending(idx) => {
                        let (stream_pointer, stream_page_result) = &results[idx];
                        (
                            stream_pointer.clone(),
                            stream_page_result
                                .as_ref()
                                .map(|r| r.items.clone())
                                .unwrap_or_default(),
                        )
                    }
                })
                .collect(),
        })
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
        } = self
            .get_items_from_pointer_stream(pointer_stream_name, starting_item_id, limit)
            .await?;

        let mut image_items = Vec::new();

        for (pointer, item_data) in results {
            let first_type = item_data.first().map(|i| i.data_type);
            let second_type = item_data.get(1).map(|i| i.data_type);
            tracing::debug!(
                table = %pointer.table_name,
                ptr_id = %pointer.stream_item_id,
                first_type = ?first_type,
                second_type = ?second_type,
                count = item_data.len(),
                "stream-provider: processing pointer images"
            );

            let new_image = item_data.first();
            let old_image = item_data.get(1);
            let Some(new_image) = new_image else {
                tracing::debug!(
                    table = %pointer.table_name,
                    ptr_id = %pointer.stream_item_id,
                    "stream-provider: pointer target image is missing; skipping"
                );
                continue;
            };

            let mut record_old_image: Option<HashMap<String, AttributeValue>> = None;
            let mut record_new_image: Option<HashMap<String, AttributeValue>> = None;

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

            if let Some(ref img) = record_new_image {
                let new_keys: Vec<_> = img.keys().cloned().collect();
                tracing::debug!(
                    ptr_id = %pointer.stream_item_id,
                    new_keys = ?new_keys,
                    "stream-provider: decoded new image keys"
                );
            }
            if let Some(ref img) = record_old_image {
                let old_keys: Vec<_> = img.keys().cloned().collect();
                tracing::debug!(
                    ptr_id = %pointer.stream_item_id,
                    old_keys = ?old_keys,
                    "stream-provider: decoded old image keys"
                );
            }

            let item_for_key = match (&record_old_image, &record_new_image) {
                (Some(_) | None, Some(new)) => new,
                (Some(old), None) => old,
                (None, None) => {
                    tracing::debug!(
                        ptr_id = %pointer.stream_item_id,
                        "stream-provider: both images were DeleteMarker or missing; skipping"
                    );
                    continue;
                }
            };

            let keys = self.get_key_attributes(item_for_key, key_schema)?;
            let key_names: Vec<_> = keys.keys().cloned().collect();
            tracing::debug!(
                ptr_id = %pointer.stream_item_id,
                keys = ?key_names,
                has_old = %record_old_image.is_some(),
                has_new = %record_new_image.is_some(),
                "stream-provider: built stream record"
            );

            let stream_record = StreamRecord {
                cursor: Some(pointer.stream_item_id.to_string()),
                keys,
                sequence_number: pointer.stream_item_id.to_string(),
                old_image: record_old_image,
                new_image: record_new_image,
            };

            image_items.push(stream_record);
        }

        Ok((image_items, last_evaluated_pointer))
    }

    /// Start the background TTL cleanup task
    async fn start_cleanup_task(&self, parallelism: usize) -> StreamResult<()>;

    /// Stop the background TTL cleanup task
    async fn stop_cleanup_task(&self) -> StreamResult<()>;

    /// Perform cleanup for streams with TTL (called by background task)
    async fn cleanup_expired_items(&self) -> StreamResult<u64>;
}

enum TaskSlot {
    Ready((StreamPointer, Vec<StreamItem>)),
    Pending(usize),
}
