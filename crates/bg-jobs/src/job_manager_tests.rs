use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use tokio::time::{Duration, timeout};

use crate::{
    BackgroundJob, BackgroundJobName, ImmediateJobKind, JobConfig, JobManager,
    constants::MAX_CONCURRENT_JOBS, errors::JobError,
};

const JOB_SLEEP_SECS: u64 = 60;
const POLL_INTERVAL_MS: u64 = 10;
const WAIT_TIMEOUT_SECS: u64 = 2;
const LONG_SLEEP_SECS: u64 = 3600;

struct BlockingJob {
    running: Arc<AtomicUsize>,
    max_running: Arc<AtomicUsize>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl BackgroundJob for BlockingJob {
    async fn execute(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let current = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_running.fetch_max(current, Ordering::SeqCst);
        self.release.notified().await;
        self.running.fetch_sub(1, Ordering::SeqCst);
        Ok(true)
    }
}

struct NoopJob;

#[async_trait::async_trait]
impl BackgroundJob for NoopJob {
    async fn execute(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(false)
    }
}

#[tokio::test]
async fn job_manager_caps_concurrent_jobs() {
    let manager = JobManager::new_for_test();
    let running = Arc::new(AtomicUsize::new(0));
    let max_running = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());

    let job_ids = BackgroundJobName::all();
    assert!(job_ids.len() >= MAX_CONCURRENT_JOBS);

    for &job_id in job_ids {
        let job = BlockingJob {
            running: running.clone(),
            max_running: max_running.clone(),
            release: release.clone(),
        };
        let config = JobConfig {
            start_immediately: true,
            sleep_duration: Duration::from_secs(JOB_SLEEP_SECS),
            jitter_percent: 0,
        };
        manager.register_job(job_id, job, config).await.unwrap();
    }

    let wait_for_limit = async {
        loop {
            if max_running.load(Ordering::SeqCst) >= MAX_CONCURRENT_JOBS {
                break;
            }
            tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
    };

    timeout(Duration::from_secs(WAIT_TIMEOUT_SECS), wait_for_limit)
        .await
        .expect("jobs did not reach concurrency cap in time");

    let observed = max_running.load(Ordering::SeqCst);
    assert!(
        observed <= MAX_CONCURRENT_JOBS,
        "concurrency cap exceeded: {observed}"
    );

    release.notify_waiters();

    for &job_id in job_ids {
        let _ = manager.stop_job(job_id).await;
    }
}

#[test]
fn default_job_config_starts_immediately_with_hourly_timer() {
    assert_eq!(
        JobConfig::default(),
        JobConfig {
            start_immediately: true,
            sleep_duration: Duration::from_secs(LONG_SLEEP_SECS),
            jitter_percent: 0,
        }
    );
}

#[tokio::test]
async fn stopped_job_is_hidden_from_running_list_and_can_be_registered_again() {
    let manager = JobManager::new_for_test();
    let job_id = BackgroundJobName::Immediate {
        kind: ImmediateJobKind::Task,
    };
    let config = JobConfig {
        start_immediately: false,
        sleep_duration: Duration::from_secs(JOB_SLEEP_SECS),
        jitter_percent: 0,
    };

    manager
        .register_job(job_id, NoopJob, config.clone())
        .await
        .expect("register initial job");
    assert!(manager.is_job_running(job_id).await);
    assert!(matches!(
        manager.register_job(job_id, NoopJob, config.clone()).await,
        Err(JobError::JobAlreadyRunning)
    ));

    manager.stop_job(job_id).await.expect("stop running job");

    assert!(!manager.is_job_running(job_id).await);
    assert!(
        manager.list_jobs().await.is_empty(),
        "stopped jobs should not be reported as running"
    );
    assert!(matches!(
        manager.stop_job(job_id).await,
        Err(JobError::JobNotRunning)
    ));

    manager
        .register_job(job_id, NoopJob, config)
        .await
        .expect("re-register stopped job");
    manager
        .stop_job(job_id)
        .await
        .expect("stop re-registered job");
}
