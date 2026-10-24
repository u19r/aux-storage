use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use super::{
    AdmissionClass, AdmissionController, AdmissionRejection, AdmissionRejectionReason,
    metrics::{record_decision, record_rejection, record_state_metrics, state_metrics_snapshot},
    state::{Inner, lock_state},
};

/// A control-work lifetime, with a reserved slot when the connection configures
/// one. Zero reserve preserves unbounded scheduling while still owning
/// settlement.
pub struct ControlPermit {
    controller: Arc<Inner>,
    slot: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl AdmissionController {
    pub fn try_acquire_control(&self) -> Result<ControlPermit, AdmissionRejection> {
        let permit = self.register_control();
        if self.inner.config.control_reserve_concurrency == 0 {
            return Ok(permit);
        }
        if let Ok(slot) = Arc::clone(&self.inner.control_available).try_acquire_owned() {
            return Ok(self.claim_control_slot(permit, slot));
        }
        let mut state = lock_state(&self.inner);
        state.rejection_count = state.rejection_count.saturating_add(1);
        let rejection = AdmissionRejection::new(
            AdmissionRejectionReason::ControlReserveExhausted,
            self.inner.config.max_queue_wait_ms,
        );
        let snapshot = state_metrics_snapshot(&state, None);
        drop(state);
        record_rejection(&self.inner, AdmissionClass::PointRead, rejection.reason);
        record_state_metrics(&self.inner, snapshot);
        Err(rejection)
    }

    fn register_control(&self) -> ControlPermit {
        self.inner
            .control_outstanding
            .fetch_add(1, Ordering::SeqCst);
        if self.inner.config.control_reserve_concurrency == 0 {
            record_decision(&self.inner, AdmissionClass::PointRead, "control");
        }
        ControlPermit {
            controller: Arc::clone(&self.inner),
            slot: None,
        }
    }

    fn claim_control_slot(
        &self,
        mut permit: ControlPermit,
        slot: tokio::sync::OwnedSemaphorePermit,
    ) -> ControlPermit {
        permit.slot = Some(slot);
        let mut state = lock_state(&self.inner);
        state.control_in_flight = state.control_in_flight.saturating_add(1);
        let snapshot = state_metrics_snapshot(&state, None);
        drop(state);
        record_decision(&self.inner, AdmissionClass::PointRead, "control");
        record_state_metrics(&self.inner, snapshot);
        permit
    }

    pub(crate) async fn acquire_recovery(&self) -> Result<ControlPermit, AdmissionRejection> {
        let permit = self.register_control();
        if self.inner.config.control_reserve_concurrency == 0 {
            return Ok(permit);
        }
        let wait = Duration::from_millis(self.inner.config.max_queue_wait_ms);
        if let Ok(Ok(slot)) = tokio::time::timeout(
            wait,
            Arc::clone(&self.inner.control_available).acquire_owned(),
        )
        .await
        {
            return Ok(self.claim_control_slot(permit, slot));
        }
        drop(permit);
        let rejection = AdmissionRejection::new(
            AdmissionRejectionReason::ControlReserveExhausted,
            self.inner.config.max_queue_wait_ms,
        );
        self.finish_admission(
            AdmissionClass::PointRead,
            lock_state(&self.inner),
            None,
            Err(rejection),
        )
    }
}

impl Drop for ControlPermit {
    fn drop(&mut self) {
        if self.slot.is_some() {
            let mut state = lock_state(&self.controller);
            state.control_in_flight = state.control_in_flight.saturating_sub(1);
            let snapshot = state_metrics_snapshot(&state, None);
            drop(state);
            drop(self.slot.take());
            record_state_metrics(&self.controller, snapshot);
        }
        self.controller
            .control_outstanding
            .fetch_sub(1, Ordering::SeqCst);
        self.controller.native_idle.notify_waiters();
    }
}
