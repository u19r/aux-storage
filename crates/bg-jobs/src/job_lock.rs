use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::Mutex;

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
