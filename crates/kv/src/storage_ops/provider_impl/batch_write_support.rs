use crate::storage_ops::provider_impl::*;

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) async fn batch_existing_items_for_write_requests(
        &self,
        table_name: &TableName,
        table_identity: &TableIdentity,
        table_info: &StoredTableInfo,
        write_requests: &[WriteRequest],
    ) -> StorageResult<Vec<Option<HashMap<String, AttributeValue>>>> {
        let mut keys = Vec::new();
        let mut key_positions = Vec::new();
        let mut existing_items = vec![None; write_requests.len()];

        for (index, write_request) in write_requests.iter().enumerate() {
            let key = match write_request {
                WriteRequest {
                    put_request: Some(PutRequest { item, .. }),
                    delete_request: None,
                } => Some(ItemKey::from_key_schema(
                    table_info.table_name.clone(),
                    &table_info.key_schema,
                    item,
                )?),
                WriteRequest {
                    put_request: None,
                    delete_request: Some(DeleteRequest { key, .. }),
                } => Some(ItemKey::from_key_schema(
                    table_info.table_name.clone(),
                    &table_info.key_schema,
                    key,
                )?),
                _ => None,
            };

            if let Some(key) = key {
                keys.push(table_keys::item_key(table_identity, &key)?);
                key_positions.push(index);
            }
        }

        if keys.is_empty() {
            return Ok(existing_items);
        }

        #[cfg(test)]
        let started = std::time::Instant::now();
        let values = self.kv_store.multi_get(keys, true).await?;
        #[cfg(test)]
        provider_perf::record(
            "storage_provider",
            "batch_existing_multi_get",
            started.elapsed(),
        );
        for (position, value) in key_positions.into_iter().zip(values) {
            let wire_item = value
                .as_deref()
                .map(|bytes| {
                    decode_wire_item_from_storage_bytes(
                        self.kv_store.item_value_codec(),
                        bytes,
                        table_info.max_indexers,
                    )
                })
                .transpose()?;
            existing_items[position] = wire_item.map(WireItem::into_attribute_map).transpose()?;
        }

        tracing::debug!(
            table_name = %table_name,
            requested_items = write_requests.len(),
            loaded_items = existing_items.iter().filter(|item| item.is_some()).count(),
            "loaded existing batch write items"
        );

        Ok(existing_items)
    }
}

#[derive(Default)]
pub(super) struct TableBatchWriteResult {
    pub(super) items_updated: usize,
    pub(super) bytes_written: usize,
    pub(super) unprocessed_items: Vec<WriteRequest>,
}

pub(super) struct PreparedBatchWriteItem {
    pub(super) items: Vec<BatchItem>,
    pub(super) bytes_written: usize,
}

pub(super) enum FastEncodeBatchOutcome {
    Applied {
        items_updated: usize,
        bytes_written: usize,
    },
    NotAttempted,
}

impl FastEncodeBatchOutcome {
    pub(super) fn record(
        self,
        total_items_updated: &mut usize,
        total_bytes_written: &mut usize,
    ) -> bool {
        match self {
            Self::Applied {
                items_updated,
                bytes_written,
            } => {
                *total_items_updated += items_updated;
                *total_bytes_written += bytes_written;
                true
            }
            Self::NotAttempted => false,
        }
    }
}

pub(super) fn requested_write_tally(request: &BatchWriteItemRequest) -> WriteCostTally {
    let mut tally = WriteCostTally::default();
    for write_requests in request.request_items.values() {
        for write_request in write_requests {
            tally.record_write_request(write_request);
        }
    }
    tally
}

pub(super) fn requested_encode_write_tally(
    request: &BatchWriteItemEncodeRequest,
) -> WriteCostTally {
    let mut tally = WriteCostTally::default();
    for write_requests in request.request_items.values() {
        for write_request in write_requests {
            tally.record_encode_write_request(write_request);
        }
    }
    tally
}

pub(super) fn unprocessed_write_tally(response: &BatchWriteItemResponse) -> WriteCostTally {
    let mut tally = WriteCostTally::default();
    if let Some(unprocessed_items) = response.unprocessed_items.as_ref() {
        for write_requests in unprocessed_items.values() {
            for write_request in write_requests {
                tally.record_write_request(write_request);
            }
        }
    }
    tally
}

pub(super) fn batch_write_response(
    unprocessed_items: HashMap<TableName, Vec<WriteRequest>>,
) -> BatchWriteItemResponse {
    BatchWriteItemResponse {
        unprocessed_items: if unprocessed_items.is_empty() {
            None
        } else {
            Some(unprocessed_items)
        },
        item_collection_metrics: None,
        consumed_capacity: None,
    }
}

pub(super) fn is_terminal_batch_item_error(error: &StorageError) -> bool {
    matches!(error.to_enum(), StorageEnum::Validation { .. })
        || matches!(error.to_enum(), StorageEnum::KeyValidation(_))
        || matches!(error.to_enum(), StorageEnum::TableNotFound { .. })
}
