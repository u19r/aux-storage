//! Exact immutable history reads preserve the operation's native snapshot.

use std::num::NonZeroUsize;

use storage_types::{
    ItemHistoryEntry, ItemHistoryRecordHeader, ItemHistoryReference, StorageError, StorageResult,
    StreamItemId, TableHistoryId, TableHistoryState,
};
use stream_provider::{ItemHistoryReadAttempt, ItemHistoryReadBatch, StreamRowValue};

use crate::{KvAttempt, catalog::KvCatalogAttempt};

impl<A: KvAttempt> ItemHistoryReadAttempt for KvCatalogAttempt<'_, A> {
    async fn load_item_history_table_states(
        &mut self,
        histories: &[TableHistoryId],
    ) -> StorageResult<Vec<Option<TableHistoryState>>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let keys = histories
                .iter()
                .map(|history| {
                    crate::catalog::table_id_from_history(*history)
                        .map(crate::catalog::metadata_key)
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let records = catalog.attempt.get_batch(keys, catalog.protection).await?;
            if records.len() != histories.len() {
                return Err(invalid_history_read());
            }
            records
                .into_iter()
                .zip(histories)
                .map(|(record, history)| {
                    record
                        .map(|bytes| crate::catalog::decode_table_history_state(&bytes, *history))
                        .transpose()
                })
                .collect()
        }
        .await;
        operation.complete(result)
    }

    async fn load_item_history_headers(
        &mut self,
        read: ItemHistoryReadBatch<'_>,
    ) -> StorageResult<Vec<Option<ItemHistoryRecordHeader>>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let prefixes = load_history_prefixes(catalog.attempt, read, catalog.protection).await?;
            if prefixes.len() != read.references.len() {
                return Err(invalid_history_read());
            }
            prefixes
                .into_iter()
                .zip(read.references)
                .map(|(prefix, reference)| {
                    prefix
                        .map(|prefix| {
                            StreamRowValue::decode_history_header_prefix(
                                &prefix.bytes,
                                prefix.length,
                                (reference.address(), reference.version()),
                            )
                        })
                        .transpose()
                })
                .collect()
        }
        .await;
        operation.complete(result)
    }

    async fn load_item_history_entries(
        &mut self,
        read: ItemHistoryReadBatch<'_>,
    ) -> StorageResult<Vec<Option<ItemHistoryEntry>>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let keys = prepare_history_keys(read.references)?;
            let values = catalog
                .attempt
                .get_batch_bounded(keys, catalog.protection, read.maximum_bytes)
                .await?;
            if values.len() != read.references.len() {
                return Err(invalid_history_read());
            }
            values
                .into_iter()
                .zip(read.references)
                .map(|(value, reference)| {
                    value
                        .map(|bytes| {
                            stream_provider::decode_item_history_entry(
                                &bytes,
                                reference,
                                read.maximum_bytes.get(),
                            )
                        })
                        .transpose()
                })
                .collect()
        }
        .await;
        operation.complete(result)
    }
}

async fn load_history_prefixes<A: KvAttempt>(
    attempt: &A,
    read: ItemHistoryReadBatch<'_>,
    protection: storage_types::ReadProtection,
) -> StorageResult<Vec<Option<crate::value_prefix::KvValuePrefix>>> {
    let (keys, bound) = prepare_history_prefixes(read.references)?;
    let maximum_value = bound
        .get()
        .saturating_add(storage_types::LogicalItemSize::maximum_json_projection_bytes());
    let per_head = attempt.native_value_limit().min(maximum_value).max(1);
    let group_size = (crate::constants::HISTORY_METADATA_HEAD_BYTES / per_head).max(1);
    let mut prefixes = Vec::with_capacity(keys.len());
    let mut remaining = read.maximum_bytes.get();
    for keys in keys.chunks(group_size) {
        let group = attempt
            .get_batch_prefix_admitted(
                keys.to_vec(),
                protection,
                bound,
                crate::KvValueAllowance::new(maximum_value, per_head.saturating_mul(keys.len())),
            )
            .await?;
        for prefix in &group {
            remaining = remaining
                .checked_sub(prefix.as_ref().map_or(0, |prefix| prefix.bytes.len()))
                .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        }
        prefixes.extend(group);
    }
    Ok(prefixes)
}

fn prepare_history_prefixes(
    references: &[ItemHistoryReference],
) -> StorageResult<(Vec<Vec<u8>>, NonZeroUsize)> {
    let bound = references
        .iter()
        .try_fold(NonZeroUsize::MIN, |bound, reference| {
            Ok::<_, StorageError>(bound.max(StreamRowValue::history_metadata_prefix_bytes(
                reference.address(),
            )?))
        })?;
    Ok((prepare_history_keys(references)?, bound))
}

fn prepare_history_keys(references: &[ItemHistoryReference]) -> StorageResult<Vec<Vec<u8>>> {
    references
        .iter()
        .map(|reference| {
            super::range::item_key(
                &reference.address().physical_stream_name()?,
                StreamItemId::from(reference.version()),
            )
            .map(|(_, key)| key)
        })
        .collect()
}

fn invalid_history_read() -> StorageError {
    StorageError::internal("Invalid KV exact item-history read")
}
