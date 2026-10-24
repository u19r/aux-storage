//! Reclamation of one deleted table incarnation outside the customer attempt.

use std::{future::Future, sync::Arc, time::Duration};

use async_trait::async_trait;
use scheduled_bg_jobs::{ExecutionContext, HandlerFuture, JobHandler, JobOutcome};
use serde::{Deserialize, Serialize};
use storage_backfill::GsiBuildTarget;
use storage_common::retry::retry_native_conflicts;
use storage_types::{StorageError, StorageResult, TableHistoryId, TableName, TimestampMillis};

use crate::{DYNAMO_WRITE_RETRY, DynamoPort};

/// The order is durable: each phase is drained before the next begins.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum TableReclaimPhase {
    MetadataCleanup,
    PrimaryItems,
    PrimaryTombstones,
    GsiProjections,
    GsiTombstones,
    TtlIndex,
    GsiNameLocks,
    NameReservation,
}

impl TableReclaimPhase {
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self {
            Self::MetadataCleanup => Some(Self::PrimaryItems),
            Self::PrimaryItems => Some(Self::PrimaryTombstones),
            Self::PrimaryTombstones => Some(Self::GsiProjections),
            Self::GsiProjections => Some(Self::GsiTombstones),
            Self::GsiTombstones => Some(Self::TtlIndex),
            Self::TtlIndex => Some(Self::GsiNameLocks),
            Self::GsiNameLocks => Some(Self::NameReservation),
            Self::NameReservation => None,
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct TableReclaimTarget {
    pub history_id: TableHistoryId,
    pub table_name: TableName,
    pub name_locked_until: TimestampMillis,
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

/// One protected attempt. Physical stores only discover, delete, and persist.
pub trait TableReclaimAttempt: crate::GsiReclaimAttempt {
    fn load_reclaim_target(
        &mut self,
        observed_at: TimestampMillis,
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
}

/// One shared phase transition inside the caller's protected native attempt.
pub async fn reclaim_table_page(
    attempt: &mut impl TableReclaimAttempt,
    observed_at: TimestampMillis,
) -> StorageResult<TableReclaimStep> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let index_step = crate::gsi_reclaim::reclaim_index_page(attempt).await?;
        let Some(target) = attempt.load_reclaim_target(observed_at).await? else {
            return Ok(index_step);
        };
        if let Some(gsi) = target
            .gsi_targets
            .iter()
            .find(|gsi| gsi.table_name != target.table_name)
        {
            return Err(StorageError::internal(&format!(
                "table reclaim target {} has GSI {} bound to table {}",
                target.table_name, gsi.index_name, gsi.table_name,
            )));
        }
        if release_reclaim_owners(attempt, &target).await? {
            return Ok(TableReclaimStep::Advanced);
        }
        match attempt.delete_reclaim_page(&target).await? {
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

async fn release_reclaim_owners(
    attempt: &mut impl TableReclaimAttempt,
    target: &TableReclaimTarget,
) -> StorageResult<bool> {
    let kind = match target.phase {
        TableReclaimPhase::PrimaryItems => {
            stream_provider::retained_image::RetainedOwnerKind::Primary
        }
        TableReclaimPhase::GsiProjections => {
            stream_provider::retained_image::RetainedOwnerKind::Projection
        }
        _ => return Ok(false),
    };
    stream_provider::retained_image::RetainedOwnerRetirement::try_new(kind, target.history_id)?
        .release_page(attempt)
        .await
}

#[async_trait]
pub trait TableReclaimRepository: Send + Sync {
    async fn reclaim_table_page(&self) -> StorageResult<TableReclaimStep>;

    /// Test cleanup observes name reservations as due while using the bounded
    /// production reclaim transition and its native attempt.
    async fn reclaim_table_page_at(
        &self,
        observed_at: TimestampMillis,
    ) -> StorageResult<TableReclaimStep>;
}

#[async_trait]
impl<P: DynamoPort> TableReclaimRepository for P
where for<'native> P::Attempt<'native>: TableReclaimAttempt
{
    async fn reclaim_table_page(&self) -> StorageResult<TableReclaimStep> {
        self.reclaim_table_page_at(self.now()).await
    }

    async fn reclaim_table_page_at(
        &self,
        observed_at: TimestampMillis,
    ) -> StorageResult<TableReclaimStep> {
        retry_native_conflicts(self, DYNAMO_WRITE_RETRY, || {
            self.with_dynamo_schema_attempt(move |attempt| {
                Box::pin(reclaim_table_page(attempt, observed_at))
            })
        })
        .await
    }
}

pub struct TableReclaimHandler {
    repository: Arc<dyn TableReclaimRepository>,
    interval: Duration,
}

impl TableReclaimHandler {
    pub fn try_new(
        repository: Arc<dyn TableReclaimRepository>,
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
