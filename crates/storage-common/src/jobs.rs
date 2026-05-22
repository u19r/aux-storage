//! Background job registration helpers.
use async_trait::async_trait;
use bg_jobs::{BackgroundJob, BackgroundJobName};

use crate::{GSI_BACKFILL_JOB, GSI_UPDATE_JOB, JobIntervalMillis};

/// Simple configuration for registering GSI related jobs.
#[derive(Debug, Clone, Copy)]
pub struct GsiJobConfig {
    pub update_interval_ms: JobIntervalMillis,
    pub backfill_interval_ms: JobIntervalMillis,
}

impl Default for GsiJobConfig {
    fn default() -> Self {
        Self {
            update_interval_ms: JobIntervalMillis(100),
            backfill_interval_ms: JobIntervalMillis(30_000),
        }
    }
}

/// Trait abstraction for a minimal job registrar (to decouple from concrete job
/// manager type).
#[async_trait]
pub trait RegistersJobs: Send + Sync {
    type Error;
    async fn register_timed_job<J>(
        &self,
        name: BackgroundJobName,
        interval_ms: JobIntervalMillis,
        job: J,
    ) -> Result<(), Self::Error>
    where
        J: BackgroundJob + 'static;
}

/// Register standard GSI jobs. Backend supplies concrete job implementations.
pub async fn register_gsi_jobs<R, J1, J2>(
    registrar: &R,
    cfg: GsiJobConfig,
    gsi_update_job: J1,
    gsi_backfill_job: J2,
) -> Result<(), R::Error>
where
    R: RegistersJobs + ?Sized,
    J1: BackgroundJob + 'static,
    J2: BackgroundJob + 'static,
{
    registrar
        .register_timed_job(GSI_UPDATE_JOB, cfg.update_interval_ms, gsi_update_job)
        .await?;
    registrar
        .register_timed_job(GSI_BACKFILL_JOB, cfg.backfill_interval_ms, gsi_backfill_job)
        .await?;
    Ok(())
}
