use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use tokio::time::{Duration, sleep};

use crate::{
    BackgroundJob, BackgroundJobName, ImmediateJobKind, JobConfig, JobLockAttempt, JobLockError,
    JobLockResult, JobLockStore, JobManager, PeriodicJobKind,
};

struct CountingJob {
    runs: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl BackgroundJob for CountingJob {
    async fn execute(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(true)
    }
}

struct AlwaysAcquireStore {
    acquire_calls: Arc<AtomicUsize>,
    renew_calls: Arc<AtomicUsize>,
    lease_offset_ms: Option<i64>,
}

#[async_trait::async_trait]
impl JobLockStore for AlwaysAcquireStore {
    async fn try_acquire(
        &self,
        _job_id: BackgroundJobName,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> JobLockResult<JobLockAttempt> {
        self.acquire_calls.fetch_add(1, Ordering::SeqCst);
        let lease_until_ms = self
            .lease_offset_ms
            .map_or(lease_until_ms, |offset| now_ms + offset);
        Ok(JobLockAttempt::Acquired { lease_until_ms })
    }

    async fn renew(
        &self,
        _job_id: BackgroundJobName,
        _lease_until_ms: i64,
        _now_ms: i64,
    ) -> JobLockResult<bool> {
        self.renew_calls.fetch_add(1, Ordering::SeqCst);
        Ok(true)
    }
}

struct AlwaysConflictStore {
    acquire_calls: Arc<AtomicUsize>,
    lease_offset_ms: i64,
}

#[async_trait::async_trait]
impl JobLockStore for AlwaysConflictStore {
    async fn try_acquire(
        &self,
        _job_id: BackgroundJobName,
        _lease_until_ms: i64,
        now_ms: i64,
    ) -> JobLockResult<JobLockAttempt> {
        self.acquire_calls.fetch_add(1, Ordering::SeqCst);
        Ok(JobLockAttempt::Conflict {
            lease_until_ms: Some(now_ms + self.lease_offset_ms),
        })
    }

    async fn renew(
        &self,
        _job_id: BackgroundJobName,
        _lease_until_ms: i64,
        _now_ms: i64,
    ) -> JobLockResult<bool> {
        Ok(false)
    }
}

struct ErroringStore;

#[async_trait::async_trait]
impl JobLockStore for ErroringStore {
    async fn try_acquire(
        &self,
        _job_id: BackgroundJobName,
        _lease_until_ms: i64,
        _now_ms: i64,
    ) -> JobLockResult<JobLockAttempt> {
        Err(JobLockError::store("lock failure"))
    }

    async fn renew(
        &self,
        _job_id: BackgroundJobName,
        _lease_until_ms: i64,
        _now_ms: i64,
    ) -> JobLockResult<bool> {
        Ok(false)
    }
}

struct RenewFailStore {
    acquire_calls: Arc<AtomicUsize>,
    renew_calls: Arc<AtomicUsize>,
    lease_offset_ms: i64,
}

#[async_trait::async_trait]
impl JobLockStore for RenewFailStore {
    async fn try_acquire(
        &self,
        _job_id: BackgroundJobName,
        _lease_until_ms: i64,
        now_ms: i64,
    ) -> JobLockResult<JobLockAttempt> {
        self.acquire_calls.fetch_add(1, Ordering::SeqCst);
        Ok(JobLockAttempt::Acquired {
            lease_until_ms: now_ms + self.lease_offset_ms,
        })
    }

    async fn renew(
        &self,
        _job_id: BackgroundJobName,
        _lease_until_ms: i64,
        _now_ms: i64,
    ) -> JobLockResult<bool> {
        let calls = self.renew_calls.fetch_add(1, Ordering::SeqCst);
        Ok(calls > 0)
    }
}

#[tokio::test]
async fn timer_job_runs_with_lock_acquired() {
    let runs = Arc::new(AtomicUsize::new(0));
    let acquire_calls = Arc::new(AtomicUsize::new(0));
    let renew_calls = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(AlwaysAcquireStore {
        acquire_calls: acquire_calls.clone(),
        renew_calls: renew_calls.clone(),
        lease_offset_ms: None,
    });
    let manager = JobManager::new(store);

    let job = CountingJob { runs: runs.clone() };
    manager
        .register_job(
            BackgroundJobName::Immediate {
                kind: ImmediateJobKind::Task,
            },
            job,
            JobConfig {
                start_immediately: true,
                sleep_duration: Duration::from_millis(10),
                jitter_percent: 0,
            },
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(40)).await;
    manager
        .stop_job(BackgroundJobName::Immediate {
            kind: ImmediateJobKind::Task,
        })
        .await
        .unwrap();

    assert!(runs.load(Ordering::SeqCst) > 0);
    assert!(acquire_calls.load(Ordering::SeqCst) > 0);
    assert!(renew_calls.load(Ordering::SeqCst) == 0);
}

#[tokio::test]
async fn timer_job_skips_on_lock_conflict() {
    let runs = Arc::new(AtomicUsize::new(0));
    let acquire_calls = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(AlwaysConflictStore {
        acquire_calls: acquire_calls.clone(),
        lease_offset_ms: 1_000,
    });
    let manager = JobManager::new(store);

    let job = CountingJob { runs: runs.clone() };
    manager
        .register_job(
            BackgroundJobName::Immediate {
                kind: ImmediateJobKind::Task,
            },
            job,
            JobConfig {
                start_immediately: true,
                sleep_duration: Duration::from_millis(10),
                jitter_percent: 0,
            },
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(40)).await;
    manager
        .stop_job(BackgroundJobName::Immediate {
            kind: ImmediateJobKind::Task,
        })
        .await
        .unwrap();

    assert_eq!(runs.load(Ordering::SeqCst), 0);
    assert!(acquire_calls.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn fast_job_renews_lock() {
    let runs = Arc::new(AtomicUsize::new(0));
    let acquire_calls = Arc::new(AtomicUsize::new(0));
    let renew_calls = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(AlwaysAcquireStore {
        acquire_calls: acquire_calls.clone(),
        renew_calls: renew_calls.clone(),
        lease_offset_ms: Some(500),
    });
    let manager = JobManager::new(store);

    let job = CountingJob { runs: runs.clone() };
    manager
        .register_job(
            BackgroundJobName::Periodic {
                kind: PeriodicJobKind::Maintenance,
            },
            job,
            JobConfig {
                start_immediately: true,
                sleep_duration: Duration::from_millis(200),
                jitter_percent: 0,
            },
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(400)).await;
    manager
        .stop_job(BackgroundJobName::Periodic {
            kind: PeriodicJobKind::Maintenance,
        })
        .await
        .unwrap();

    assert!(runs.load(Ordering::SeqCst) > 0);
    assert!(renew_calls.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn fast_job_conflict_sleeps_until_lease() {
    let runs = Arc::new(AtomicUsize::new(0));
    let acquire_calls = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(AlwaysConflictStore {
        acquire_calls: acquire_calls.clone(),
        lease_offset_ms: 200,
    });
    let manager = JobManager::new(store);

    let job = CountingJob { runs: runs.clone() };
    manager
        .register_job(
            BackgroundJobName::Immediate {
                kind: ImmediateJobKind::Task,
            },
            job,
            JobConfig {
                start_immediately: true,
                sleep_duration: Duration::from_millis(50),
                jitter_percent: 0,
            },
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;
    manager
        .stop_job(BackgroundJobName::Immediate {
            kind: ImmediateJobKind::Task,
        })
        .await
        .unwrap();

    assert_eq!(runs.load(Ordering::SeqCst), 0);
    assert!(acquire_calls.load(Ordering::SeqCst) <= 1);
}

#[tokio::test]
async fn job_skips_on_lock_error() {
    let runs = Arc::new(AtomicUsize::new(0));
    let manager = JobManager::new(Arc::new(ErroringStore));

    let job = CountingJob { runs: runs.clone() };
    manager
        .register_job(
            BackgroundJobName::Periodic {
                kind: PeriodicJobKind::Maintenance,
            },
            job,
            JobConfig {
                start_immediately: true,
                sleep_duration: Duration::from_millis(10),
                jitter_percent: 0,
            },
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(40)).await;
    manager
        .stop_job(BackgroundJobName::Periodic {
            kind: PeriodicJobKind::Maintenance,
        })
        .await
        .unwrap();

    assert_eq!(runs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn fast_job_reacquires_after_failed_renew() {
    let runs = Arc::new(AtomicUsize::new(0));
    let acquire_calls = Arc::new(AtomicUsize::new(0));
    let renew_calls = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(RenewFailStore {
        acquire_calls: acquire_calls.clone(),
        renew_calls: renew_calls.clone(),
        lease_offset_ms: 500,
    });
    let manager = JobManager::new(store);

    let job = CountingJob { runs: runs.clone() };
    manager
        .register_job(
            BackgroundJobName::Immediate {
                kind: ImmediateJobKind::Task,
            },
            job,
            JobConfig {
                start_immediately: true,
                sleep_duration: Duration::from_millis(200),
                jitter_percent: 0,
            },
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(400)).await;
    manager
        .stop_job(BackgroundJobName::Immediate {
            kind: ImmediateJobKind::Task,
        })
        .await
        .unwrap();

    assert!(runs.load(Ordering::SeqCst) > 0);
    assert!(renew_calls.load(Ordering::SeqCst) > 0);
    assert!(acquire_calls.load(Ordering::SeqCst) > 1);
}
