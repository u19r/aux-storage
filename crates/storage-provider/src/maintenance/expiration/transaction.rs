//! One shared expiration executor over physical catalog, index, and progress
//! ports.

use std::future::Future;

use storage_common::TtlIndexWrite;
use storage_types::{
    BoundTableInfo, StorageError, StorageResult,
    maintenance::{MaintenanceKind, StepResult, TargetIncarnation, TaskProgress},
};

use super::{
    ExpirationAttemptState, ExpirationCurrentObservation, ExpirationCurrentRead,
    ExpirationEffectPolicy, ExpirationIndexLocator, ExpirationPageWrite, ExpirationProgress,
    ExpirationProgressWrite, ExpirationScanPage, ExpirationScanRead, ExpirationTarget,
    ExpirationTargetRead, ExpirationTransaction, OrdinaryExpiration,
    read_expiration_candidates_in_attempt, read_expiration_target_in_attempt,
};
use crate::{
    CatalogAttempt, DeleteItemAttempt, DescribeTimeToLiveAttempt, GetItemAttempt,
    ListTablesAttempt, UpdateTimeToLiveAttempt, maintenance::MaintenancePort,
};

pub trait ExpirationPort:
    CatalogAttempt
    + DescribeTimeToLiveAttempt
    + UpdateTimeToLiveAttempt
    + ListTablesAttempt
    + GetItemAttempt
    + DeleteItemAttempt
    + MaintenancePort
{
    fn scan_expiration_index(
        &mut self,
        table: &BoundTableInfo,
        read: &ExpirationScanRead,
    ) -> impl Future<Output = StorageResult<(Vec<ExpirationIndexLocator>, bool)>> + Send;

    fn delete_expiration_index(
        &mut self,
        table: &BoundTableInfo,
        writes: impl Iterator<Item = TtlIndexWrite> + Send,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}

pub struct ExpirationAttempt<'a, P: ExpirationPort, S = OrdinaryExpiration> {
    pub(super) admission: S,
    pub(super) port: &'a mut P,
    pub(super) state: ExpirationAttemptState,
    progress_fence: Option<P::ProgressFence>,
}

impl<'a, P: ExpirationPort> ExpirationAttempt<'a, P> {
    pub fn new(port: &'a mut P) -> Self {
        Self {
            port,
            admission: OrdinaryExpiration,
            state: ExpirationAttemptState::default(),
            progress_fence: None,
        }
    }
}

impl<'a, P: ExpirationPort, S: ExpirationEffectPolicy<P>> ExpirationAttempt<'a, P, S> {
    pub fn with_admission(port: &'a mut P, admission: S) -> Self {
        Self {
            port,
            admission,
            state: ExpirationAttemptState::default(),
            progress_fence: None,
        }
    }
}

impl<P: ExpirationPort, S: ExpirationEffectPolicy<P>> ExpirationTransaction
    for ExpirationAttempt<'_, P, S>
{
    async fn read_expiration_admission(
        &mut self,
    ) -> StorageResult<(TargetIncarnation, Option<TaskProgress<ExpirationProgress>>)> {
        let (identity, progress, fence) = self
            .port
            .load_task_admission(MaintenanceKind::TtlSweep)
            .await?;
        self.progress_fence = Some(fence);
        Ok((identity, progress))
    }

    async fn read_maintenance_identity(&mut self) -> StorageResult<TargetIncarnation> {
        self.read_expiration_admission()
            .await
            .map(|(identity, _)| identity)
    }

    async fn read_expiration_progress(
        &mut self,
    ) -> StorageResult<Option<TaskProgress<ExpirationProgress>>> {
        self.read_expiration_admission()
            .await
            .map(|(_, progress)| progress)
    }

    async fn read_expiration_target(
        &mut self,
        read: &ExpirationTargetRead,
    ) -> StorageResult<Option<ExpirationTarget>> {
        let bound = read_expiration_target_in_attempt(self.port, read, &self.admission).await?;
        Ok(self.state.bind(bound))
    }

    async fn read_expiration_scan(
        &mut self,
        read: &ExpirationScanRead,
    ) -> StorageResult<ExpirationScanPage> {
        self.state.validate_scan(read)?;
        let table = self.state.table()?;
        let (rows, more) = self.port.scan_expiration_index(&table, read).await?;
        Ok(ExpirationScanPage {
            table_name: read.table_name.clone(),
            table_incarnation: read.table_incarnation.clone(),
            policy_generation: read.policy_generation,
            table_info: (*table).clone(),
            rows,
            more,
        })
    }

    async fn read_expiration_candidates(
        &mut self,
        read: &ExpirationCurrentRead,
    ) -> StorageResult<ExpirationCurrentObservation> {
        self.state.validate_policy(read.policy_generation)?;
        let table = self.state.table()?;
        read_expiration_candidates_in_attempt(self.port, &table, read).await
    }

    async fn write_expiration_progress(
        &mut self,
        write: ExpirationProgressWrite,
    ) -> StorageResult<StepResult<()>> {
        store_expiration_progress(self.port, self.progress_fence.as_ref(), write).await
    }

    async fn write_expiration_page(
        &mut self,
        write: ExpirationPageWrite,
    ) -> StorageResult<StepResult<()>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self.port);
        let port = settlement.as_mut();
        let result = async {
            self.state.validate_page(&write)?;
            let table = self.state.table()?;
            write
                .deletions
                .prepare_deletions(port, &self.admission, table, write.captured_at)
                .await?;
            store_expiration_progress(port, self.progress_fence.as_ref(), write.progress).await
        }
        .await;
        settlement.complete(result)
    }
}

async fn store_expiration_progress<P: ExpirationPort>(
    port: &mut P,
    fence: Option<&P::ProgressFence>,
    write: ExpirationProgressWrite,
) -> StorageResult<StepResult<()>> {
    ExpirationAttemptState::validate_progress(&write)?;
    let fence = fence.ok_or_else(|| {
        StorageError::internal("expiration progress was not admitted in this attempt")
    })?;
    port.store_task_progress(
        MaintenanceKind::TtlSweep,
        fence,
        write.expected_revision,
        write.next_progress,
    )
    .await?;
    Ok(write.result)
}
