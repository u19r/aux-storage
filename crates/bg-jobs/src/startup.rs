use std::{env, time::Duration};

use tracing::warn;

pub use crate::constants::ENV_JOBS_MODE;
use crate::{
    BackgroundJob, BackgroundJobName, JobConfig, JobManager,
    constants::{JOBS_MODE_ALL, JOBS_MODE_METRICS_ONLY},
    errors::JobStartupError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobsMode {
    All,
    MetricsOnly,
}

impl JobsMode {
    #[must_use]
    pub const fn allows_all_jobs(self) -> bool {
        matches!(self, Self::All)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct InProcessJobStartupConfig {
    pub jobs_enabled: bool,
    pub jitter_percent: u8,
    pub mode_env_key: &'static str,
}

impl InProcessJobStartupConfig {
    #[must_use]
    pub const fn with_mode_env_key(self, mode_env_key: &'static str) -> Self {
        Self {
            mode_env_key,
            ..self
        }
    }
}

#[derive(Clone)]
pub struct InProcessJobRuntime {
    pub(crate) manager: JobManager,
    pub(crate) mode: JobsMode,
    pub(crate) jitter_percent: u8,
}

impl InProcessJobRuntime {
    #[must_use]
    pub const fn mode(&self) -> JobsMode {
        self.mode
    }

    #[must_use]
    pub const fn jitter_percent(&self) -> u8 {
        self.jitter_percent
    }

    pub async fn register_timer_job<J, F>(
        &self,
        job_id: BackgroundJobName,
        interval_ms: u64,
        requires_all_mode: bool,
        build_job: F,
    ) -> bool
    where
        J: BackgroundJob + 'static,
        F: FnOnce() -> J,
    {
        if requires_all_mode && !self.mode.allows_all_jobs() {
            return false;
        }

        if interval_ms == 0 {
            return false;
        }

        let registration = self
            .manager
            .register_job(
                job_id,
                build_job(),
                JobConfig {
                    start_immediately: true,
                    sleep_duration: Duration::from_millis(interval_ms),
                    jitter_percent: self.jitter_percent,
                },
            )
            .await;
        if let Err(err) = registration {
            warn!(job_id = %job_id, error = %err, "background.job.register.failed");
            return false;
        }

        true
    }

    pub async fn register_optional_timer_job<J, F>(
        &self,
        job_id: BackgroundJobName,
        interval_ms: Option<u64>,
        missing_interval_message: &str,
        requires_all_mode: bool,
        build_job: F,
    ) -> bool
    where
        J: BackgroundJob + 'static,
        F: FnOnce() -> J,
    {
        let Some(interval_ms) = interval_ms else {
            let _ = (job_id, missing_interval_message);
            return false;
        };
        self.register_timer_job(job_id, interval_ms, requires_all_mode, build_job)
            .await
    }
}

pub fn build_in_process_runtime<F>(
    config: InProcessJobStartupConfig,
    build_manager: F,
) -> Result<Option<InProcessJobRuntime>, JobStartupError>
where
    F: FnOnce() -> JobManager,
{
    if !config.jobs_enabled {
        return Ok(None);
    }

    let mode = resolve_jobs_mode(config.mode_env_key)?;
    let manager = build_manager();

    Ok(Some(InProcessJobRuntime {
        manager,
        mode,
        jitter_percent: config.jitter_percent,
    }))
}

pub fn resolve_jobs_mode(mode_env_key: &'static str) -> Result<JobsMode, JobStartupError> {
    let Some(raw) = read_mode_env(mode_env_key) else {
        return Ok(JobsMode::All);
    };

    match raw.to_ascii_lowercase().as_str() {
        JOBS_MODE_ALL => Ok(JobsMode::All),
        JOBS_MODE_METRICS_ONLY => Ok(JobsMode::MetricsOnly),
        _ => Err(JobStartupError::InvalidJobsMode {
            env_key: mode_env_key,
            value: raw,
            expected_all: JOBS_MODE_ALL,
            expected_metrics_only: JOBS_MODE_METRICS_ONLY,
        }),
    }
}

fn read_mode_env(key: &str) -> Option<String> {
    let value = env::var(key).ok()?;
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    Some(value.to_string())
}
