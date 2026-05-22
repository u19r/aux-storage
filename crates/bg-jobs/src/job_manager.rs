use std::{
    collections::HashMap,
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Utc;
use tokio::{
    sync::{Mutex, Semaphore},
    task::JoinHandle,
    time::sleep,
};
use tracing::Instrument;

use crate::{
    BackgroundJobName,
    constants::{
        FAST_JOB_INTERVAL_THRESHOLD_MS, FAST_JOB_LOCK_RENEWAL_THRESHOLD_MS,
        FAST_JOB_LOCK_WINDOW_MS, MAX_CONCURRENT_JOBS,
    },
    errors::{JobError, JobLockError, JobResult},
    jitter::jittered,
    job_lock::{InMemoryJobLockStore, JobLockAttempt, JobLockResult, JobLockStore},
    worker::default_worker_id,
};

#[derive(Debug, Clone, PartialEq)]
pub struct JobConfig {
    pub start_immediately: bool,
    pub sleep_duration: Duration,
    pub jitter_percent: u8,
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            start_immediately: true,
            sleep_duration: Duration::from_secs(3600),
            jitter_percent: 0,
        }
    }
}

#[async_trait::async_trait]
pub trait BackgroundJob: Send + Sync {
    async fn execute(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Debug)]
pub struct JobHandle {
    pub(crate) job_id: BackgroundJobName,
    pub(crate) config: JobConfig,
}

impl JobHandle {
    #[must_use]
    pub const fn id(&self) -> BackgroundJobName {
        self.job_id
    }

    #[must_use]
    pub fn config(&self) -> &JobConfig {
        &self.config
    }
}

#[derive(Clone)]
pub struct JobManager {
    jobs: Arc<Mutex<HashMap<BackgroundJobName, JobState>>>,
    semaphore: Arc<Semaphore>,
    lock_store: Arc<dyn JobLockStore>,
}

struct JobState {
    handle: JoinHandle<()>,
    is_running: bool,
}

#[derive(Clone, Copy, Debug)]
enum JobLockPolicy {
    Slow { lease_ms: i64 },
    Fast { lease_ms: i64, renew_after_ms: i64 },
}

#[derive(Clone, Copy, Debug, Default)]
struct JobLockState {
    lease_until_ms: Option<i64>,
}

#[derive(Clone, Copy, Debug)]
enum JobLockOutcome {
    Proceed,
    Skip { sleep_override: Option<Duration> },
}

struct JobRunMetrics {
    job_id: BackgroundJobName,
}

impl JobRunMetrics {
    fn start(job_id: BackgroundJobName) -> Self {
        let job_id_owned = job_id.to_string();
        metrics_facade::counter!(
            metrics_facade::CounterMetric::BgJobsRunsTotalMetric,
            "job_id" => job_id_owned.clone()
        )
        .increment(1);
        metrics_facade::gauge!(
            metrics_facade::GaugeMetric::BgJobsRunningCountMetric,
            "job_id" => job_id_owned.clone()
        )
        .increment(1.0);
        Self { job_id }
    }
}

impl Drop for JobRunMetrics {
    fn drop(&mut self) {
        metrics_facade::gauge!(
            metrics_facade::GaugeMetric::BgJobsRunningCountMetric,
            "job_id" => self.job_id.to_string()
        )
        .decrement(1.0);
    }
}

impl Default for JobManager {
    fn default() -> Self {
        Self::new_for_test()
    }
}

impl JobManager {
    #[must_use]
    pub fn new(lock_store: Arc<dyn JobLockStore>) -> Self {
        Self {
            jobs: Arc::new(Mutex::new(HashMap::new())),
            semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_JOBS)),
            lock_store,
        }
    }

    #[must_use]
    pub fn new_for_test() -> Self {
        Self::new(Arc::new(InMemoryJobLockStore::new(default_worker_id())))
    }

    pub async fn register_job<J>(
        &self,
        job_id: BackgroundJobName,
        job: J,
        config: JobConfig,
    ) -> JobResult<JobHandle>
    where
        J: BackgroundJob + 'static,
    {
        let mut jobs = self.jobs.lock().await;

        if let Some(state) = jobs.get(&job_id) {
            if state.is_running {
                return Err(JobError::JobAlreadyRunning);
            }
            jobs.remove(&job_id);
        }

        if config.sleep_duration.is_zero() {
            tracing::warn!(
                job_id = %job_id,
                start_immediately = config.start_immediately,
                jitter_percent = config.jitter_percent,
                "job registered with zero sleep duration; this can cause busy loops"
            );
        }

        let job_arc = Arc::new(job);
        let jobs_clone = self.jobs.clone();
        let job_id_clone = job_id;
        let config_clone = config.clone();
        let config_clone_for_handle = config.clone();
        let semaphore = self.semaphore.clone();
        let lock_store = self.lock_store.clone();

        let task_name = format!("bg-job-{job_id_clone}");
        let handle = spawn_job_task(&task_name, async move {
            Self::run_timer_loop(
                job_arc,
                config_clone.start_immediately,
                config_clone.sleep_duration,
                config_clone.jitter_percent,
                jobs_clone,
                job_id_clone,
                semaphore,
                lock_store,
            )
            .await;
        })?;

        let job_handle = JobHandle {
            job_id,
            config: config_clone_for_handle,
        };

        let state = JobState {
            handle,
            is_running: true,
        };

        jobs.insert(job_id, state);

        Ok(job_handle)
    }

    pub async fn stop_job(&self, job_id: BackgroundJobName) -> JobResult<()> {
        let mut jobs = self.jobs.lock().await;

        if let Some(state) = jobs.get_mut(&job_id) {
            if !state.is_running {
                return Err(JobError::JobNotRunning);
            }

            state.handle.abort();
            state.is_running = false;
            Ok(())
        } else {
            Err(JobError::JobNotFound { job_id })
        }
    }

    pub async fn is_job_running(&self, job_id: BackgroundJobName) -> bool {
        let jobs = self.jobs.lock().await;
        jobs.get(&job_id).is_some_and(|state| state.is_running)
    }

    pub async fn list_jobs(&self) -> Vec<BackgroundJobName> {
        let jobs = self.jobs.lock().await;
        jobs.iter()
            .filter_map(|(job_id, state)| state.is_running.then_some(*job_id))
            .collect()
    }

    #[expect(clippy::too_many_arguments)]
    async fn run_timer_loop<J>(
        job: Arc<J>,
        start_immediately: bool,
        sleep_duration: Duration,
        jitter_percent: u8,
        jobs: Arc<Mutex<HashMap<BackgroundJobName, JobState>>>,
        job_id: BackgroundJobName,
        semaphore: Arc<Semaphore>,
        lock_store: Arc<dyn JobLockStore>,
    ) where
        J: BackgroundJob,
    {
        let lock_policy = Self::lock_policy_for_interval(sleep_duration);
        let mut lock_state = JobLockState::default();

        if !start_immediately {
            sleep(jittered(sleep_duration, jitter_percent)).await;
        }

        loop {
            {
                let jobs = jobs.lock().await;
                if !jobs.get(&job_id).is_some_and(|s| s.is_running) {
                    break;
                }
            }

            match Self::apply_job_lock(lock_store.as_ref(), lock_policy, &mut lock_state, job_id)
                .await
            {
                Ok(JobLockOutcome::Proceed) => {}
                Ok(JobLockOutcome::Skip { sleep_override }) => {
                    Self::handle_lock_skip(job_id, sleep_duration, jitter_percent, sleep_override)
                        .await;
                    continue;
                }
                Err(err) => {
                    Self::handle_lock_error(job_id, sleep_duration, jitter_percent, err).await;
                    continue;
                }
            }

            let Ok(permit) = semaphore.acquire().await else {
                break;
            };
            let _run_metrics = JobRunMetrics::start(job_id);
            let run_span = tracing::info_span!(
                "job.run",
                feature = "jobs",
                job_id = %job_id,
                job_run_runtime_ms = tracing::field::Empty,
                job_run_had_work = tracing::field::Empty,
                job_run_error = tracing::field::Empty
            );
            let run_start = Instant::now();
            let result = job.execute().instrument(run_span.clone()).await;
            let elapsed_ms = duration_ms_u64(run_start.elapsed());
            run_span.record("job_run_runtime_ms", elapsed_ms);
            metrics_facade::histogram!(
                metrics_facade::HistogramMetric::BgJobsRunDurationMsMetric,
                "job_id" => job_id.to_string()
            )
            .record(u64_to_f64(elapsed_ms));
            match result {
                Ok(did_work) => {
                    run_span.record("job_run_had_work", did_work);
                    let _ = elapsed_ms;
                }
                Err(err) => {
                    let err_display = err.to_string();
                    run_span.record("job_run_error", tracing::field::display(&err_display));
                    metrics_facade::counter!(
                        metrics_facade::CounterMetric::BgJobsRunErrorsTotalMetric,
                        "job_id" => job_id.to_string()
                    )
                    .increment(1);
                    if !err
                        .downcast_ref::<JobLockError>()
                        .is_some_and(|lock_error| {
                            matches!(lock_error, JobLockError::Contention { .. })
                        })
                    {
                        tracing::warn!(
                            job_id = %job_id,
                            elapsed_ms,
                            error = %err_display,
                            "job execution failed"
                        );
                    }
                }
            }
            drop(permit);

            sleep(jittered(sleep_duration, jitter_percent)).await;
        }
    }

    async fn handle_lock_skip(
        job_id: BackgroundJobName,
        sleep_duration: Duration,
        jitter_percent: u8,
        sleep_override: Option<Duration>,
    ) {
        metrics_facade::counter!(
            metrics_facade::CounterMetric::BgJobsLockSkipsTotalMetric,
            "job_id" => job_id.to_string()
        )
        .increment(1);
        let sleep_for = sleep_override.unwrap_or_else(|| jittered(sleep_duration, jitter_percent));
        sleep(sleep_for).await;
    }

    async fn handle_lock_error(
        job_id: BackgroundJobName,
        sleep_duration: Duration,
        jitter_percent: u8,
        error: JobLockError,
    ) {
        if !matches!(error, JobLockError::Contention { .. }) {
            tracing::warn!(
                job_id = %job_id,
                error = %error,
                "job lock check failed; skipping run"
            );
        }
        sleep(jittered(sleep_duration, jitter_percent)).await;
    }

    fn lock_policy_for_interval(sleep_duration: Duration) -> JobLockPolicy {
        let interval_ms = i64::try_from(sleep_duration.as_millis()).unwrap_or(i64::MAX);
        if interval_ms < FAST_JOB_INTERVAL_THRESHOLD_MS {
            JobLockPolicy::Fast {
                lease_ms: FAST_JOB_LOCK_WINDOW_MS,
                renew_after_ms: FAST_JOB_LOCK_RENEWAL_THRESHOLD_MS,
            }
        } else {
            JobLockPolicy::Slow {
                lease_ms: interval_ms,
            }
        }
    }

    async fn apply_job_lock(
        lock_store: &dyn JobLockStore,
        policy: JobLockPolicy,
        lock_state: &mut JobLockState,
        job_id: BackgroundJobName,
    ) -> JobLockResult<JobLockOutcome> {
        let now_ms = Self::now_ms();
        match policy {
            JobLockPolicy::Slow { lease_ms } => {
                let lease_until_ms = now_ms.saturating_add(lease_ms);
                match lock_store
                    .try_acquire(job_id, lease_until_ms, now_ms)
                    .await?
                {
                    JobLockAttempt::Acquired { lease_until_ms } => {
                        lock_state.lease_until_ms = Some(lease_until_ms);
                        Ok(JobLockOutcome::Proceed)
                    }
                    JobLockAttempt::Conflict { .. } => Ok(JobLockOutcome::Skip {
                        sleep_override: None,
                    }),
                }
            }
            JobLockPolicy::Fast {
                lease_ms,
                renew_after_ms,
            } => {
                if let Some(current_until) = lock_state.lease_until_ms {
                    if now_ms >= current_until {
                        lock_state.lease_until_ms = None;
                    } else if now_ms.saturating_add(renew_after_ms) >= current_until {
                        let new_until = now_ms.saturating_add(lease_ms);
                        let renewed = lock_store.renew(job_id, new_until, now_ms).await?;
                        if renewed {
                            lock_state.lease_until_ms = Some(new_until);
                        } else {
                            lock_state.lease_until_ms = None;
                        }
                    }
                }

                if lock_state.lease_until_ms.is_none() {
                    let new_until = now_ms.saturating_add(lease_ms);
                    match lock_store.try_acquire(job_id, new_until, now_ms).await? {
                        JobLockAttempt::Acquired { lease_until_ms } => {
                            lock_state.lease_until_ms = Some(lease_until_ms);
                            return Ok(JobLockOutcome::Proceed);
                        }
                        JobLockAttempt::Conflict { lease_until_ms } => {
                            let sleep_override =
                                lease_until_ms.and_then(|until| Self::sleep_until(until, now_ms));
                            return Ok(JobLockOutcome::Skip { sleep_override });
                        }
                    }
                }

                Ok(JobLockOutcome::Proceed)
            }
        }
    }

    fn sleep_until(lease_until_ms: i64, now_ms: i64) -> Option<Duration> {
        let wait_ms = lease_until_ms.saturating_sub(now_ms);
        if wait_ms <= 0 {
            None
        } else {
            let wait_ms_u64 = u64::try_from(wait_ms).unwrap_or(0);
            Some(Duration::from_millis(wait_ms_u64))
        }
    }

    fn now_ms() -> i64 {
        Utc::now().timestamp_millis()
    }
}

fn duration_ms_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[expect(clippy::cast_precision_loss)]
fn u64_to_f64(value: u64) -> f64 {
    value as f64
}

#[expect(clippy::unnecessary_wraps)]
#[expect(unused_variables)]
fn spawn_job_task<F>(task_name: &str, fut: F) -> JobResult<JoinHandle<()>>
where F: Future<Output = ()> + Send + 'static {
    #[cfg(tokio_unstable)]
    {
        tokio::task::Builder::new()
            .name(task_name)
            .spawn(fut)
            .map_err(|err| JobError::SpawnFailed {
                message: err.to_string(),
            })
    }

    #[cfg(not(tokio_unstable))]
    {
        Ok(tokio::spawn(fut))
    }
}
