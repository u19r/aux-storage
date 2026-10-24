use std::sync::atomic::Ordering;

use super::{AdmissionController, AdmissionRegistry, state::lock_state};

impl AdmissionController {
    /// The host stops intake and joins dispatching workers before awaiting
    /// idle.
    pub(crate) async fn wait_for_native_idle(&self) {
        loop {
            let notified = self.inner.native_idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let is_idle = {
                let state = lock_state(&self.inner);
                state.in_flight == 0
                    && state.queue.is_empty()
                    && self.inner.control_outstanding.load(Ordering::SeqCst) == 0
            };
            if is_idle {
                return;
            }
            notified.await;
        }
    }
}

impl AdmissionRegistry {
    pub(crate) async fn wait_for_native_idle(&self) {
        futures::future::join_all(
            self.connection_ids()
                .filter_map(|id| self.for_connection(id))
                .map(AdmissionController::wait_for_native_idle),
        )
        .await;
    }
}
