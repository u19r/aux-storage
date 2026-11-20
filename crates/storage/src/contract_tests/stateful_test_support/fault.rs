use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use storage_provider::{
    MaintenanceOperations, StreamTrimDueMarker, StreamTrimScope, StreamTrimState, TableReclaimStep,
    maintenance::{
        ExpirationRequest, GsiBuildRequest, StreamDurationTrimStats, StreamRetentionWave,
    },
};
use storage_types::{
    StorageError, StorageResult, TableName, TimestampMillis, context::WrappedError as _,
    maintenance::StepResult,
};
use tokio::{sync::Notify, task::JoinHandle};

use super::{FixtureGap, StatefulTestBackend};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    MaintenanceCommit,
}

pub struct PendingFault {
    gate: Arc<PauseGate>,
    drive: JoinHandle<StorageResult<()>>,
}

impl PendingFault {
    pub async fn release(self) -> StorageResult<()> {
        self.gate.release.notify_one();
        self.drive
            .await
            .map_err(|error| StorageError::internal(&error))?
    }

    pub(super) fn new(gate: Arc<PauseGate>, drive: JoinHandle<StorageResult<()>>) -> Self {
        Self { gate, drive }
    }
}

pub(super) struct PauseGate {
    entered: Notify,
    release: Notify,
}

impl PauseGate {
    fn new() -> Self {
        Self {
            entered: Notify::new(),
            release: Notify::new(),
        }
    }

    pub(super) async fn wait_until_entered(&self) {
        self.entered.notified().await;
    }
}

#[derive(Default)]
pub(super) struct ExpirationFaults {
    pause: Mutex<Option<Arc<PauseGate>>>,
    lose_ack: AtomicBool,
}

impl ExpirationFaults {
    pub(super) fn arm_pause(&self) -> Arc<PauseGate> {
        let gate = Arc::new(PauseGate::new());
        *self.pause.lock().expect("expiration pause lock") = Some(Arc::clone(&gate));
        gate
    }

    pub(super) fn arm_lost_ack(&self) {
        self.lose_ack.store(true, Ordering::Release);
    }

    fn take_pause(&self) -> Option<Arc<PauseGate>> {
        self.pause.lock().expect("expiration pause lock").take()
    }
}

/// The fixture's maintenance value: expiration faults and the last GSI build
/// error, over the store's own operations.
pub(super) struct InstrumentedMaintenance {
    inner: Arc<dyn MaintenanceOperations>,
    faults: Arc<ExpirationFaults>,
    gsi_error: Arc<Mutex<Option<String>>>,
}

impl InstrumentedMaintenance {
    pub(super) fn new(
        inner: Arc<dyn MaintenanceOperations>,
        faults: Arc<ExpirationFaults>,
        gsi_error: Arc<Mutex<Option<String>>>,
    ) -> Self {
        Self {
            inner,
            faults,
            gsi_error,
        }
    }
}

#[async_trait]
impl MaintenanceOperations for InstrumentedMaintenance {
    async fn advance_expiration(
        &self,
        request: ExpirationRequest,
    ) -> StorageResult<StepResult<()>> {
        if let Some(gate) = self.faults.take_pause() {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        let result = self.inner.advance_expiration(request).await?;
        if self.faults.lose_ack.swap(false, Ordering::AcqRel) {
            return Err(StorageError::internal(
                "expiration commit acknowledgement was lost",
            ));
        }
        Ok(result)
    }

    async fn trim_transact_receipts_page(
        &self,
        after: Option<String>,
        limit: std::num::NonZeroU32,
    ) -> StorageResult<Option<String>> {
        self.inner.trim_transact_receipts_page(after, limit).await
    }

    async fn advance_gsi_build(&self, request: GsiBuildRequest) -> StorageResult<StepResult<()>> {
        let result = self.inner.advance_gsi_build(request).await;
        if let Err(error) = &result {
            let (kind, context) = error.recursive_context(Vec::new());
            *self.gsi_error.lock().expect("GSI diagnostic lock") =
                Some(format!("{kind:?}; context={context:?}"));
        }
        result
    }

    async fn trim_stream_marker_wave(
        &self,
        wave: StreamRetentionWave,
    ) -> StorageResult<StreamDurationTrimStats> {
        self.inner.trim_stream_marker_wave(wave).await
    }

    async fn list_due_stream_trim_markers(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        self.inner
            .list_due_stream_trim_markers(due_before, limit)
            .await
    }

    async fn load_stream_trim_states(
        &self,
        scopes: &[StreamTrimScope],
    ) -> StorageResult<Vec<Option<StreamTrimState>>> {
        self.inner.load_stream_trim_states(scopes).await
    }

    async fn trim_system_stream_page(
        &self,
        request: stream_provider::SystemStreamTrimPageRequest,
    ) -> StorageResult<stream_provider::SystemStreamTrimPageResult> {
        self.inner.trim_system_stream_page(request).await
    }

    async fn reclaim_table_page(&self) -> StorageResult<TableReclaimStep> {
        self.inner.reclaim_table_page().await
    }

    async fn read_ttl_sweep_policy(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Option<storage_common::TtlSweepPolicy>> {
        self.inner.read_ttl_sweep_policy(table_name).await
    }

    async fn execute_ttl_sweep_page(
        &self,
        command: storage_common::TtlSweepPageCommand,
    ) -> StorageResult<storage_common::TtlSweepPageResult> {
        self.inner.execute_ttl_sweep_page(command).await
    }

    async fn execute_ttl_sweep_request(
        &self,
        request: storage_common::TtlSweepRequest,
    ) -> StorageResult<storage_common::TtlSweepPageResult> {
        self.inner.execute_ttl_sweep_request(request).await
    }
}

impl StatefulTestBackend {
    pub async fn pause_before_commit(&self, point: FaultPoint) -> Result<PendingFault, FixtureGap> {
        let FaultPoint::MaintenanceCommit = point;
        Ok(self
            .maintenance
            .as_ref()
            .expect("fixture maintenance runtime is open")
            .pause_before_commit()
            .await)
    }

    pub fn lose_commit_ack(&self) -> Result<(), FixtureGap> {
        self.maintenance
            .as_ref()
            .expect("fixture maintenance runtime is open")
            .lose_commit_ack();
        Ok(())
    }
}
