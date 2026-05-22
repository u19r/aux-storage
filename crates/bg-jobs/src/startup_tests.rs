use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    BackgroundJob, BackgroundJobName, ImmediateJobKind, JobManager, PeriodicJobKind,
    startup::{
        InProcessJobRuntime, InProcessJobStartupConfig, JobsMode, build_in_process_runtime,
        resolve_jobs_mode,
    },
};

struct NoopJob;

#[async_trait]
impl BackgroundJob for NoopJob {
    async fn execute(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(false)
    }
}

#[test]
fn resolve_jobs_mode_defaults_to_all_when_missing() {
    let mode = resolve_jobs_mode("AUX_JOBS_MODE_NOT_SET_FOR_BG_JOBS_TEST")
        .expect("mode parse should succeed");
    assert_eq!(mode, JobsMode::All);
}

#[test]
fn jobs_mode_all_allows_database_jobs_but_metrics_only_does_not() {
    assert!(JobsMode::All.allows_all_jobs());
    assert!(!JobsMode::MetricsOnly.allows_all_jobs());
}

#[test]
fn startup_config_can_override_mode_env_key_without_changing_other_settings() {
    let config = InProcessJobStartupConfig {
        jobs_enabled: true,
        jitter_percent: 17,
        mode_env_key: "ORIGINAL_KEY",
    };

    let updated = config.with_mode_env_key("NEW_KEY");

    assert!(updated.jobs_enabled);
    assert_eq!(updated.jitter_percent, 17);
    assert_eq!(updated.mode_env_key, "NEW_KEY");
}

#[test]
fn build_runtime_returns_none_when_jobs_are_disabled() {
    let runtime = build_in_process_runtime(
        InProcessJobStartupConfig {
            jobs_enabled: false,
            jitter_percent: 0,
            mode_env_key: "AUX_JOBS_MODE_NOT_SET_FOR_DISABLED_TEST",
        },
        || panic!("manager should not be built when jobs are disabled"),
    )
    .expect("disabled runtime");

    assert!(runtime.is_none());
}

#[test]
fn build_runtime_uses_default_all_mode_and_preserves_jitter() {
    let runtime = build_in_process_runtime(
        InProcessJobStartupConfig {
            jobs_enabled: true,
            jitter_percent: 23,
            mode_env_key: "AUX_JOBS_MODE_NOT_SET_FOR_RUNTIME_TEST",
        },
        || {
            JobManager::new(Arc::new(crate::job_lock::InMemoryJobLockStore::new(
                "worker",
            )))
        },
    )
    .expect("runtime")
    .expect("enabled runtime");

    assert_eq!(runtime.mode(), JobsMode::All);
    assert_eq!(runtime.jitter_percent(), 23);
}

#[tokio::test]
async fn register_timer_job_is_skipped_in_metrics_only_mode() {
    let runtime = InProcessJobRuntime {
        manager: JobManager::new(Arc::new(crate::job_lock::InMemoryJobLockStore::new(
            "worker",
        ))),
        mode: JobsMode::MetricsOnly,
        jitter_percent: 0,
    };

    let registered = runtime
        .register_timer_job(
            BackgroundJobName::Immediate {
                kind: ImmediateJobKind::Task,
            },
            1000,
            true,
            || NoopJob,
        )
        .await;
    assert!(!registered);
    assert!(runtime.manager.list_jobs().await.is_empty());
}

#[tokio::test]
async fn register_optional_timer_job_skips_when_interval_missing() {
    let runtime = InProcessJobRuntime {
        manager: JobManager::new(Arc::new(crate::job_lock::InMemoryJobLockStore::new(
            "worker",
        ))),
        mode: JobsMode::All,
        jitter_percent: 0,
    };

    let registered = runtime
        .register_optional_timer_job(
            BackgroundJobName::Periodic {
                kind: PeriodicJobKind::Maintenance,
            },
            None,
            "missing interval in test",
            true,
            || NoopJob,
        )
        .await;
    assert!(!registered);
    assert!(runtime.manager.list_jobs().await.is_empty());
}

#[tokio::test]
async fn register_timer_job_skips_zero_interval_without_registering_job() {
    let runtime = InProcessJobRuntime {
        manager: JobManager::new(Arc::new(crate::job_lock::InMemoryJobLockStore::new(
            "worker",
        ))),
        mode: JobsMode::All,
        jitter_percent: 0,
    };

    let registered = runtime
        .register_timer_job(
            BackgroundJobName::Immediate {
                kind: ImmediateJobKind::Task,
            },
            0,
            false,
            || NoopJob,
        )
        .await;

    assert!(!registered);
    assert!(runtime.manager.list_jobs().await.is_empty());
}
