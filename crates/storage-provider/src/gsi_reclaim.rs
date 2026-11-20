//! Index retirement policy shared by all physical providers.

use infrastructure_provider::{
    batch::RecordBatch,
    physical_attempt::PhysicalRecordAttempt,
    record::{ConditionalWriteResult, RecordChange, RecordExpectation},
};
use storage_backfill::GsiBuildTarget;
use storage_types::{StorageResult, TableHistoryId};

use crate::{
    ReclaimAttempt, TableReclaimStep,
    gsi_reclaim_discovery::GsiReclaimDiscovery,
    gsi_reclaim_record::{GsiReclaimRecord, invalid_marker},
};

pub struct NativeGsiRetirement {
    pub index: storage_types::BoundGsi,
    pub unused_columns: Vec<storage_types::KeyColumn>,
}

/// Publish before removing the writable binding in this same native attempt.
pub async fn register_index_retirement(
    attempt: &mut impl PhysicalRecordAttempt,
    history: TableHistoryId,
    target: GsiBuildTarget,
    native_index: Option<storage_types::BoundGsi>,
) -> StorageResult<()> {
    let mut settlement_guard =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement_guard.as_mut();
    let result = async {
        let marker = GsiReclaimRecord::try_new(history, target, native_index)?;
        store_changes(attempt, &[marker.prepare_store(RecordExpectation::Absent)?]).await
    }
    .await;
    settlement_guard.complete(result)
}

/// Fresh protected authority checks precede every retirement page.
pub async fn reclaim_index_page(
    attempt: &mut impl ReclaimAttempt,
) -> StorageResult<TableReclaimStep> {
    let mut settlement_guard =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement_guard.as_mut();
    let result = async {
        let discovery = GsiReclaimDiscovery::load(attempt).await?;
        let Some(row) = discovery.select_marker(attempt).await? else {
            return Ok(TableReclaimStep::Complete);
        };
        let advance = discovery.prepare_advance(row.key.clone())?;
        let marker = GsiReclaimRecord::decode(&row)?;
        require_retired_binding(attempt, &marker).await?;
        let native = prepare_native_retirement(attempt, &marker).await?;
        let change = match attempt
            .delete_index_page(marker.history, &marker.target, native)
            .await?
        {
            TableReclaimStep::Advanced => None,
            TableReclaimStep::Complete => Some(RecordChange::Delete {
                key: row.key,
                expected: row.revision,
            }),
        };
        let mut changes = vec![advance];
        changes.extend(change);
        store_changes(attempt, &changes).await?;
        Ok(TableReclaimStep::Advanced)
    }
    .await;
    settlement_guard.complete(result)
}

async fn prepare_native_retirement(
    attempt: &mut impl ReclaimAttempt,
    marker: &GsiReclaimRecord,
) -> StorageResult<Option<NativeGsiRetirement>> {
    let Some(index) = &marker.native_index else {
        return Ok(None);
    };
    let current = if let Some(stamp) = attempt
        .read_catalog_stamp(&marker.target.table_name)
        .await?
    {
        Some(
            attempt
                .read_table_at_stamp(&marker.target.table_name, stamp)
                .await?,
        )
    } else {
        None
    };
    let unused_columns = index
        .columns
        .iter()
        .filter(|column| {
            current.as_ref().is_none_or(|table| {
                table.history_id != marker.history
                    || !table
                        .binding
                        .gsis
                        .iter()
                        .any(|gsi| !gsi.name.is_ttl_index() && gsi.columns.contains(column))
            })
        })
        .cloned()
        .collect();
    Ok(Some(NativeGsiRetirement {
        index: index.clone(),
        unused_columns,
    }))
}

async fn require_retired_binding(
    attempt: &mut impl ReclaimAttempt,
    marker: &GsiReclaimRecord,
) -> StorageResult<()> {
    let Some(stamp) = attempt
        .read_catalog_stamp(&marker.target.table_name)
        .await?
    else {
        return Ok(());
    };
    if attempt
        .load_index_incarnation(stamp.history_id, &marker.target)
        .await?
        .as_ref()
        == Some(&marker.target.incarnation)
    {
        return Err(invalid_marker());
    }
    Ok(())
}

async fn store_changes(
    attempt: &mut impl PhysicalRecordAttempt,
    changes: &[RecordChange],
) -> StorageResult<()> {
    let batch = RecordBatch::try_new(changes, GsiReclaimRecord::change_limits()?)?;
    if !matches!(
        attempt.store_records(&batch).await?,
        ConditionalWriteResult::Applied
    ) {
        return Err(storage_types::StorageError::write_miss(
            "gsi_retirement",
            storage_types::MissExpectation::Revision,
        ));
    }
    Ok(())
}
