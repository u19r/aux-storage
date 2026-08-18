use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use tokio::sync::{Mutex, Notify};

use crate::{BackgroundJobName, errors::JobLockError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobLockAttempt {
    Acquired { lease_until_ms: i64 },
    Conflict { lease_until_ms: Option<i64> },
}

pub type JobLockResult<T> = Result<T, JobLockError>;

#[async_trait]
pub trait JobLockStore: Send + Sync {
    async fn try_acquire(
        &self,
        job_id: BackgroundJobName,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> JobLockResult<JobLockAttempt>;

    async fn renew(
        &self,
        job_id: BackgroundJobName,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> JobLockResult<bool>;
}

/// A one-shot lifecycle barrier for jobs which are registered before the
/// owning service has finished creating its system tables.
///
/// Registration intentionally remains eager so providers can keep one
/// canonical job setup path.  A gated lock store prevents the first lease
/// attempt from touching the backend until the service publishes that its
/// initialization is complete.
#[derive(Clone, Debug)]
pub struct JobStartGate {
    state: Arc<JobStartGateState>,
}

#[derive(Debug)]
struct JobStartGateState {
    open: AtomicBool,
    notify: Notify,
}

impl JobStartGate {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(JobStartGateState {
                open: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    /// Publish that the owning service has completed its startup sequence.
    pub fn open(&self) {
        if !self.state.open.swap(true, Ordering::Release) {
            self.state.notify.notify_waiters();
        }
    }

    #[must_use]
    pub fn is_open(&self) -> bool {
        self.state.open.load(Ordering::Acquire)
    }

    pub(crate) async fn wait_until_open(&self) {
        while !self.is_open() {
            let notified = self.state.notify.notified();
            if self.is_open() {
                break;
            }
            notified.await;
        }
    }
}

impl Default for JobStartGate {
    fn default() -> Self {
        Self::new()
    }
}

/// Delays lease operations until a [`JobStartGate`] is opened.
pub struct GatedJobLockStore {
    inner: Arc<dyn JobLockStore>,
    gate: JobStartGate,
}

impl std::fmt::Debug for GatedJobLockStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GatedJobLockStore")
            .finish_non_exhaustive()
    }
}

impl GatedJobLockStore {
    #[must_use]
    pub fn new(inner: Arc<dyn JobLockStore>, gate: JobStartGate) -> Self {
        Self { inner, gate }
    }
}

#[async_trait]
impl JobLockStore for GatedJobLockStore {
    async fn try_acquire(
        &self,
        job_id: BackgroundJobName,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> JobLockResult<JobLockAttempt> {
        self.gate.wait_until_open().await;
        self.inner.try_acquire(job_id, lease_until_ms, now_ms).await
    }

    async fn renew(
        &self,
        job_id: BackgroundJobName,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> JobLockResult<bool> {
        self.gate.wait_until_open().await;
        self.inner.renew(job_id, lease_until_ms, now_ms).await
    }
}

#[derive(Debug)]
pub struct InMemoryJobLockStore {
    _worker_id: String,
    leases: Mutex<HashMap<String, JobLeaseEntry>>,
}

#[derive(Debug, Clone, Copy)]
struct JobLeaseEntry {
    lease_until_ms: i64,
}

impl InMemoryJobLockStore {
    pub fn new(worker_id: impl Into<String>) -> Self {
        Self {
            _worker_id: worker_id.into(),
            leases: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl JobLockStore for InMemoryJobLockStore {
    async fn try_acquire(
        &self,
        job_id: BackgroundJobName,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> JobLockResult<JobLockAttempt> {
        let mut leases = self.leases.lock().await;
        match leases.get(job_id.as_str()) {
            Some(entry) if entry.lease_until_ms >= now_ms => Ok(JobLockAttempt::Conflict {
                lease_until_ms: Some(entry.lease_until_ms),
            }),
            _ => {
                leases.insert(
                    job_id.as_str().to_string(),
                    JobLeaseEntry { lease_until_ms },
                );
                Ok(JobLockAttempt::Acquired { lease_until_ms })
            }
        }
    }

    async fn renew(
        &self,
        job_id: BackgroundJobName,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> JobLockResult<bool> {
        let mut leases = self.leases.lock().await;
        match leases.get(job_id.as_str()) {
            Some(entry) if entry.lease_until_ms >= now_ms => {
                leases.insert(
                    job_id.as_str().to_string(),
                    JobLeaseEntry { lease_until_ms },
                );
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}
