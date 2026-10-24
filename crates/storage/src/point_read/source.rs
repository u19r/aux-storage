use std::{
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
};

use lru_ttl_cache::{RefreshDemand, RefreshPriority, RefreshQueue};
use metrics_facade::RequestCostContext;
use tokio::sync::oneshot;

use super::{PointReadEntry, PointReadRequest, generation::SourceRead};
use crate::types::StorageResult;

type Completion = oneshot::Sender<StorageResult<PointReadEntry>>;

#[derive(Clone)]
pub(super) struct SourceKey {
    pub(super) request: Arc<PointReadRequest>,
    completion: Arc<Mutex<Option<Completion>>>,
    pub(super) cost_context: RequestCostContext,
    pub(super) read: SourceRead,
    observer: super::observer::PointReadObserver,
    priority: RefreshPriority,
}

impl SourceKey {
    pub(super) fn new(
        request: Arc<PointReadRequest>,
        completion: Completion,
        read: SourceRead,
        observer: super::observer::PointReadObserver,
    ) -> Self {
        Self {
            request,
            completion: Arc::new(Mutex::new(Some(completion))),
            cost_context: RequestCostContext::capture(),
            read,
            observer,
            priority: RefreshPriority::current(),
        }
    }

    pub(super) fn demand(&self) -> RefreshDemand {
        self.priority.demand()
    }

    pub(super) fn observe(&self, event: super::PointReadEvent) {
        if let Some(observer) = self.observer {
            observer(event);
        }
    }

    pub(super) fn publish_result(&self, outcome: impl FnOnce() -> StorageResult<PointReadEntry>) {
        let completion = self
            .completion
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(completion) = completion {
            let result = if self.is_obsolete() {
                Err(crate::types::StorageError::service_unavailable(1))
            } else {
                outcome()
            };
            let result = if !self.is_obsolete() {
                result
            } else {
                Err(crate::types::StorageError::service_unavailable(1))
            };
            let _ = completion.send(result);
        }
    }

    pub(super) fn is_obsolete(&self) -> bool {
        self.read.is_obsolete()
    }

    pub(super) fn has_completed(&self) -> bool {
        self.completion
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_none()
    }
}

impl PartialEq for SourceKey {
    fn eq(&self, other: &Self) -> bool {
        self.read.id() == other.read.id()
    }
}
impl Eq for SourceKey {}
impl Hash for SourceKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.read.id().hash(state);
    }
}

pub(super) struct SourceAdmission {
    pub(super) key: SourceKey,
    pub(super) queue: RefreshQueue<SourceKey>,
}

impl SourceAdmission {
    pub(super) async fn receive_result(
        &self,
        mut receiver: oneshot::Receiver<StorageResult<PointReadEntry>>,
    ) -> Result<StorageResult<PointReadEntry>, oneshot::error::RecvError> {
        tokio::select! {
            biased;
            result = &mut receiver => result,
            _ = self.key.priority.promoted() => {
                self.queue.promote_cold(&self.key);
                receiver.await
            }
        }
    }
}

impl Drop for SourceAdmission {
    fn drop(&mut self) {
        self.key
            .publish_result(|| Err(crate::types::StorageError::service_unavailable(1)));
        self.queue.cancel(&self.key, self.key.read.id());
    }
}
