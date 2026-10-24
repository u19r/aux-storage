//! Index retirement policy shared by all physical providers.

use std::future::Future;

use infrastructure_provider::{
    batch::RecordBatch,
    physical_attempt::PhysicalRecordAttempt,
    record::{ConditionalWriteResult, RecordChange, RecordExpectation},
};
use storage_backfill::GsiBuildTarget;
use storage_types::{StorageEnum, StorageResult, TableHistoryId, maintenance::TargetIncarnation};
use stream_provider::retained_image::{RetainedOwnerKind, RetainedOwnerRetirement};

use crate::{
    CatalogAttempt, TableReclaimStep,
    gsi_reclaim_discovery::GsiReclaimDiscovery,
    gsi_reclaim_record::{GsiReclaimPhase, GsiReclaimRecord, invalid_marker},
};

/// Physical index binding observations and exact bounded family deletion.
pub trait GsiReclaimAttempt:
    CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt
{
    fn load_index_incarnation(
        &mut self,
        history: TableHistoryId,
        target: &GsiBuildTarget,
    ) -> impl Future<Output = StorageResult<Option<TargetIncarnation>>> + Send;
    fn delete_index_page(
        &mut self,
        history: TableHistoryId,
        target: &GsiBuildTarget,
    ) -> impl Future<Output = StorageResult<TableReclaimStep>> + Send;
}

/// Publish before removing the writable binding in this same native attempt.
pub async fn register_index_retirement(
    attempt: &mut impl PhysicalRecordAttempt,
    history: TableHistoryId,
    target: GsiBuildTarget,
) -> StorageResult<()> {
    let mut settlement_guard =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement_guard.as_mut();
    let result = async {
        let marker = GsiReclaimRecord::try_new(history, target)?;
        store_changes(attempt, &[marker.prepare_store(RecordExpectation::Absent)?]).await
    }
    .await;
    settlement_guard.complete(result)
}

/// Fresh protected authority checks precede every retirement page.
pub async fn reclaim_index_page(
    attempt: &mut impl GsiReclaimAttempt,
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
        let mut marker = GsiReclaimRecord::decode(&row)?;
        require_retired_binding(attempt, &marker).await?;
        let change = match &marker.phase {
            GsiReclaimPhase::OwnerReceipts { after } => {
                let next = RetainedOwnerRetirement::try_new(
                    RetainedOwnerKind::Projection,
                    marker.history,
                )?
                .release_projection_page(
                    attempt,
                    &marker.target.incarnation,
                    &marker.target.index_name,
                    after.clone(),
                )
                .await?;
                marker.phase = match next {
                    Some(after) => GsiReclaimPhase::OwnerReceipts { after: Some(after) },
                    None => GsiReclaimPhase::PhysicalRows,
                };
                Some(marker.prepare_store(RecordExpectation::Revision(row.revision))?)
            }
            GsiReclaimPhase::PhysicalRows => {
                match attempt
                    .delete_index_page(marker.history, &marker.target)
                    .await?
                {
                    TableReclaimStep::Advanced => None,
                    TableReclaimStep::Complete => Some(RecordChange::Delete {
                        key: row.key,
                        expected: row.revision,
                    }),
                }
            }
        };
        let mut changes = vec![advance];
        changes.extend(change);
        store_changes(attempt, &changes).await?;
        Ok(TableReclaimStep::Advanced)
    }
    .await;
    settlement_guard.complete(result)
}

async fn require_retired_binding(
    attempt: &mut impl GsiReclaimAttempt,
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
        return Err(StorageEnum::TransactionConflict {
            message: "Index retirement authority changed".into(),
        }
        .into());
    }
    Ok(())
}
