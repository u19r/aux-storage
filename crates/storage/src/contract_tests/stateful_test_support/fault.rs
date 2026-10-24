use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use storage_provider::maintenance::{ExpirationRepository, ExpirationRequest};
use storage_types::{StorageError, StorageResult, maintenance::StepResult};
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

pub(super) struct FaultingExpirationRepository {
    repository: Arc<dyn ExpirationRepository>,
    faults: Arc<ExpirationFaults>,
}

impl FaultingExpirationRepository {
    pub(super) fn new(
        repository: Arc<dyn ExpirationRepository>,
        faults: Arc<ExpirationFaults>,
    ) -> Self {
        Self { repository, faults }
    }
}

#[async_trait]
impl ExpirationRepository for FaultingExpirationRepository {
    async fn advance_expiration(
        &self,
        request: ExpirationRequest,
    ) -> StorageResult<StepResult<()>> {
        if let Some(gate) = self.faults.take_pause() {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        let result = self.repository.advance_expiration(request).await?;
        if self.faults.lose_ack.swap(false, Ordering::AcqRel) {
            return Err(StorageError::internal(
                "expiration commit acknowledgement was lost",
            ));
        }
        Ok(result)
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
