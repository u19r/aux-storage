//! Reclamation of one deleted table incarnation outside the customer attempt.

use std::{future::Future, sync::Arc, time::Duration};

use scheduled_bg_jobs::{ExecutionContext, HandlerFuture, JobHandler, JobOutcome};
use serde::{Deserialize, Serialize};
use storage_backfill::GsiBuildTarget;
use storage_types::{StorageError, StorageResult, TableHistoryId, TableName};

/// The order is durable: each phase is drained before the next begins.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum TableReclaimPhase {
    ExternalData,
    MetadataCleanup,
    StreamEntries,
    ExplicitAppends,
    PrimaryItems,
    GsiProjections,
    TtlIndex,
    GsiNameLocks,
}

impl TableReclaimPhase {
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self {
            Self::ExternalData => Some(Self::MetadataCleanup),
            Self::MetadataCleanup => Some(Self::StreamEntries),
            Self::StreamEntries => Some(Self::ExplicitAppends),
            Self::ExplicitAppends => Some(Self::PrimaryItems),
            Self::PrimaryItems => Some(Self::GsiProjections),
            Self::GsiProjections => Some(Self::TtlIndex),
            Self::TtlIndex => Some(Self::GsiNameLocks),
            Self::GsiNameLocks => None,
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct TableReclaimTarget {
    pub history_id: TableHistoryId,
    pub table_name: TableName,
    pub phase: TableReclaimPhase,
    /// Physical GSI incarnations must survive name reuse during SQL
    /// reclamation.
    pub gsi_targets: Vec<GsiBuildTarget>,
}

/// A page advances or finds no ready work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableReclaimStep {
    Advanced,
    Complete,
}

/// Physical index binding observations and exact bounded family deletion.
///
/// One protected attempt. Physical stores only discover, delete, and persist.
pub trait ReclaimAttempt:
    crate::CatalogAttempt + infrastructure_provider::physical_attempt::PhysicalRecordAttempt
{
    fn load_reclaim_target(
        &mut self,
    ) -> impl Future<Output = StorageResult<Option<TableReclaimTarget>>> + Send;

    /// Delete one bounded family page.
    fn delete_reclaim_page(
        &mut self,
        target: &TableReclaimTarget,
    ) -> impl Future<Output = StorageResult<TableReclaimStep>> + Send;

    fn store_reclaim_phase(
        &mut self,
        target: &TableReclaimTarget,
        next: Option<TableReclaimPhase>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn load_index_incarnation(
        &mut self,
        history: TableHistoryId,
        target: &GsiBuildTarget,
    ) -> impl Future<Output = StorageResult<Option<storage_types::maintenance::TargetIncarnation>>> + Send;
    fn delete_index_page(
        &mut self,
        history: TableHistoryId,
        target: &GsiBuildTarget,
        native: Option<crate::NativeGsiRetirement>,
    ) -> impl Future<Output = StorageResult<TableReclaimStep>> + Send;
}

/// One shared phase transition inside the caller's protected native attempt.
pub async fn reclaim_table_page(
    attempt: &mut impl ReclaimAttempt,
) -> StorageResult<TableReclaimStep> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result =
        async {
            let index_step = crate::gsi_reclaim::reclaim_index_page(attempt).await?;
            let Some(target) = attempt.load_reclaim_target().await? else {
                return Ok(index_step);
            };
            if target.phase == TableReclaimPhase::ExternalData {
                return Err(StorageError::service_unavailable(1)
                    .with_context("Table reclamation awaits its external data owner"));
            }
            if let Some(gsi) = target.gsi_targets.iter().find(|gsi| {
                gsi.table_name != target.table_name || gsi.history_id != target.history_id
            }) {
                return Err(StorageError::internal(&format!(
                    "table reclaim target {} has GSI {} bound to table {}",
                    target.table_name, gsi.index_name, gsi.table_name,
                )));
            }
            let step = attempt.delete_reclaim_page(&target).await?;
            match step {
                TableReclaimStep::Advanced => Ok(TableReclaimStep::Advanced),
                TableReclaimStep::Complete => {
                    let next = target.phase.next();
                    attempt.store_reclaim_phase(&target, next).await?;
                    Ok(TableReclaimStep::Advanced)
                }
            }
        }
        .await;
    settlement.complete(result)
}

pub struct TableReclaimHandler {
    repository: Arc<dyn crate::maintenance::MaintenanceOperations>,
    interval: Duration,
}

impl TableReclaimHandler {
    pub fn try_new(
        repository: Arc<dyn crate::maintenance::MaintenanceOperations>,
        interval: Duration,
    ) -> StorageResult<Self> {
        if interval.is_zero() {
            return Err(StorageError::validation(
                "table reclaim interval must not be zero",
            ));
        }
        Ok(Self {
            repository,
            interval,
        })
    }
}

impl JobHandler for TableReclaimHandler {
    fn execute<'a>(
        &'a self,
        context: ExecutionContext<'a>,
        _payload: &'a [u8],
    ) -> HandlerFuture<'a> {
        let now = context.wall_now();
        let checkpoint = context.checkpoint().unwrap_or_default().to_vec();
        storage_common::maintenance_continuation::defer_unavailable_turn(
            async move {
                if context.is_cancelled() {
                    return Ok(JobOutcome::Cancelled);
                }
                let step = self
                    .repository
                    .reclaim_table_page()
                    .await
                    .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
                let delay = match step {
                    TableReclaimStep::Advanced => Duration::ZERO,
                    TableReclaimStep::Complete => self.interval,
                };
                let not_before = context.wall_now().checked_add(delay).map_err(|_| {
                    Box::new(StorageError::internal("table reclaim cadence overflow"))
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
                Ok(JobOutcome::Continue {
                    not_before,
                    checkpoint: Vec::<u8>::new().into(),
                })
            },
            now,
            self.interval,
            checkpoint,
        )
    }
}
