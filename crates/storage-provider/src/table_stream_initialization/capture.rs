//! Protected current-image pages and concurrent-write predecessor anchors.
use std::{num::NonZeroUsize, sync::Arc};

use infrastructure_provider::{physical_attempt::PhysicalRecordOperation, record::RecordChange};
use storage_types::{
    BoundTableInfo, ItemHistoryChange, ItemHistoryEntry, ItemHistoryMetadata,
    ItemHistoryPublication, ItemHistoryReference, ItemRevision, ItemStreamVersion, KeyAttributes,
    StorageError, StorageResult, TableStatus,
};
use stream_provider::ItemHistoryReadBatch;

use super::record::store_change;
use crate::{DynamoAttempt, DynamoStreamWriteAttempt, TableUpdateChange};
/// Return true when this attempt committed one bounded initialization turn.
pub(super) async fn advance_table_stream_initialization(
    attempt: &mut (impl DynamoAttempt + crate::PrimaryStateScanAttempt),
    limit: u32,
) -> StorageResult<bool> {
    let mut settlement = PhysicalRecordOperation::new(attempt);
    let result = advance_initialization_page(settlement.as_mut(), limit).await;
    settlement.complete(result)
}

async fn advance_initialization_page(
    attempt: &mut (impl DynamoAttempt + crate::PrimaryStateScanAttempt),
    limit: u32,
) -> StorageResult<bool> {
    let Some((row, mut initialization)) =
        super::record::load_pending_initialization(attempt).await?
    else {
        return Ok(false);
    };
    let table = match attempt.read_catalog_stamp(&initialization.table).await? {
        Some(stamp) if stamp.history_id == initialization.history => Some(
            attempt
                .read_table_at_stamp(&initialization.table, stamp)
                .await?,
        ),
        _ => None,
    };
    let delete = RecordChange::Delete {
        key: row.key,
        expected: row.revision,
    };
    let Some(table) = table.filter(|table| {
        table.info.table_status == TableStatus::Updating && table.info.has_public_stream_request()
    }) else {
        store_change(attempt, delete).await?;
        return Ok(true);
    };
    initialization.after =
        capture_primary_page(attempt, Arc::clone(&table), initialization.after, limit).await?;
    if initialization.after.is_some() {
        store_change(attempt, initialization.prepare_store(Some(row.revision))?).await?;
    } else {
        let change = TableUpdateChange::complete_stream_initialization(&table.info);
        attempt
            .persist_table_update(table.as_ref().clone(), change)
            .await?;
        store_change(attempt, delete).await?;
    }
    Ok(true)
}

async fn capture_primary_page(
    attempt: &mut (impl DynamoAttempt + crate::PrimaryStateScanAttempt),
    table: Arc<BoundTableInfo>,
    after: Option<String>,
    limit: u32,
) -> StorageResult<Option<String>> {
    let scan = crate::PrimaryStateScan::try_new(Arc::clone(&table), after, Some(limit.min(32)))?;
    let page = attempt.scan_primary_state(&scan).await?;
    let observations = page
        .items
        .into_iter()
        .map(|row| {
            let state = crate::dynamo_put_item::ObservedWrite::try_new(row.state, &row.key)?;
            Ok((row.key, state))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let anchors = observations
        .iter()
        .filter_map(|(key, old)| {
            Some(ItemHistoryAnchor {
                key,
                item: old.item.as_ref()?,
                revision: old.revision?,
                indexers: &old.indexers,
            })
        })
        .collect::<Vec<_>>();
    ensure_item_history_anchors(attempt, &table, &anchors).await?;
    attempt.flush_dynamo_write().await?;
    Ok(page.continuation)
}

pub(crate) struct ItemHistoryAnchor<'a> {
    pub key: &'a KeyAttributes,
    pub item: &'a storage_types::VerifiedWireItem,
    pub revision: ItemRevision,
    pub indexers: &'a [String],
}

pub(crate) async fn ensure_item_history_anchors(
    attempt: &mut impl DynamoStreamWriteAttempt,
    table: &BoundTableInfo,
    anchors: &[ItemHistoryAnchor<'_>],
) -> StorageResult<()> {
    if anchors.is_empty() {
        return Ok(());
    }
    let references = anchors
        .iter()
        .map(|anchor| {
            ItemHistoryReference::try_new(
                stream_provider::dynamo_item_history_address(table, anchor.key)?,
                ItemStreamVersion::new(anchor.revision.get()),
            )
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let headers = load_anchor_headers(attempt, &references).await?;
    for ((anchor, reference), header) in anchors.iter().zip(references).zip(headers) {
        if let Some(header) = header {
            if header.metadata().reference() != &reference
                || header.metadata().kind() != storage_types::ItemHistoryKind::Image
            {
                return Err(StorageError::internal(
                    "Stream initialization found invalid current history",
                ));
            }
            continue;
        }
        let entry = anchor.prepare_entry(table, reference)?;
        attempt.stage_item_history(Some(entry), None)?;
    }
    Ok(())
}

impl ItemHistoryAnchor<'_> {
    fn prepare_entry(
        &self,
        table: &BoundTableInfo,
        reference: ItemHistoryReference,
    ) -> StorageResult<ItemHistoryEntry> {
        let metadata = ItemHistoryMetadata::try_new(
            reference.address().clone(),
            reference.version(),
            None,
            ItemHistoryPublication::PredecessorAnchor,
        )?;
        ItemHistoryEntry::try_new(
            metadata,
            ItemHistoryChange::Image {
                item: self.item.clone().into(),
                indexers: storage_types::IndexerDeclaration::try_new(
                    self.indexers.to_vec(),
                    table.max_indexers,
                )?,
            },
        )
    }
}

async fn load_anchor_headers(
    attempt: &mut impl DynamoStreamWriteAttempt,
    references: &[ItemHistoryReference],
) -> StorageResult<Vec<Option<storage_types::ItemHistoryRecordHeader>>> {
    let maximum = references.iter().try_fold(0usize, |size, reference| {
        size.checked_add(
            storage_types::ItemHistoryRecord::encoded_length_bound()
                + stream_provider::StreamRowValue::history_envelope_bytes(reference.address())?,
        )
        .ok_or_else(|| StorageError::internal("Stream initialization admission overflow"))
    })?;
    let headers = attempt
        .load_item_history_headers(ItemHistoryReadBatch {
            references,
            maximum_bytes: NonZeroUsize::new(maximum).ok_or_else(|| {
                StorageError::internal("Stream initialization header batch is empty")
            })?,
        })
        .await?;
    if headers.len() != references.len() {
        return Err(StorageError::internal(
            "Stream initialization header count differs",
        ));
    }
    Ok(headers)
}
