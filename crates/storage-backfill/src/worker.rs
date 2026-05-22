use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use futures::stream::{FuturesUnordered, StreamExt};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::{
    BACKFILL_BATCH_SIZE, BACKFILL_BATCH_SLEEP_MS, BACKFILL_LOCK_TTL_MS, BackfillBatchOutcome,
    BackfillError, BackfillLock, BackfillResult, BackfillResultType, BackfillState, BackfillStatus,
    GsiBackfillDescriptor, MAX_CONCURRENT_GSI_BACKFILLS, WorkerContext, traits::BackfillDriver,
};

#[derive(Debug, Clone)]
pub struct BackfillConfig {
    pub max_concurrent: usize,
    pub batch_size: usize,
    pub idle_sleep_ms: u64,
    pub lock_ttl_ms: i64,
}

impl Default for BackfillConfig {
    fn default() -> Self {
        Self {
            max_concurrent: MAX_CONCURRENT_GSI_BACKFILLS,
            batch_size: BACKFILL_BATCH_SIZE,
            idle_sleep_ms: BACKFILL_BATCH_SLEEP_MS,
            lock_ttl_ms: BACKFILL_LOCK_TTL_MS,
        }
    }
}

impl BackfillConfig {
    #[must_use]
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    #[must_use]
    pub fn with_max_concurrent(mut self, max_concurrent: usize) -> Self {
        self.max_concurrent = max_concurrent;
        self
    }
}

#[derive(Clone)]
pub struct BackfillCoordinator<D>
where D: BackfillDriver + 'static
{
    driver: Arc<D>,
    config: Arc<BackfillConfig>,
    worker_id: String,
}

impl<D> BackfillCoordinator<D>
where D: BackfillDriver + 'static
{
    pub fn new(driver: Arc<D>, config: BackfillConfig) -> Self {
        let worker_id = Uuid::now_v7().to_string();
        Self {
            driver,
            config: Arc::new(config),
            worker_id,
        }
    }

    fn build_context(&self) -> WorkerContext {
        WorkerContext::new(self.worker_id.clone())
    }

    #[instrument(skip_all, fields(feature = "storage", worker_id = %self.worker_id))]
    pub async fn run_once(&self) -> BackfillResultType {
        let enumerate_start = Instant::now();
        let mut ctx = self.build_context();
        let descriptors = self
            .driver
            .enumerate_states()
            .await
            .map_err(BackfillError::from)?;
        let enumerate_elapsed = enumerate_start.elapsed().as_millis() as f64;
        metrics_facade::histogram!(
            metrics_facade::HistogramMetric::BackfillEnumerateLatencyMs,
            "job" => "gsi-backfill"
        )
        .record(enumerate_elapsed);
        debug!(
            worker = %self.worker_id,
            descriptors = descriptors.len(),
            elapsed_ms = enumerate_elapsed,
            "backfill.enumerate"
        );

        if descriptors.is_empty() {
            metrics_facade::gauge!(
                metrics_facade::GaugeMetric::BackfillJobsConcurrentCount,
                "job" => "gsi-backfill"
            )
            .set(0.0);
            debug!(
                worker = %self.worker_id,
                active = 0,
                "backfill.concurrency"
            );
            debug!("no descriptors registered for backfill");
            return Ok(BackfillResult::Idle);
        }

        let mut acquired = Vec::new();
        let mut throttled_due_to_concurrency = false;
        for (descriptor, state) in descriptors {
            if acquired.len() >= self.config.max_concurrent {
                throttled_due_to_concurrency = true;
                break;
            }

            if matches!(state.status, BackfillStatus::Done) {
                continue;
            }

            ctx.refresh_now();
            if let Some(lock) = state.lock.as_ref() {
                if lock.owner_id == self.worker_id {
                    debug!(
                        worker = %self.worker_id,
                        table = %descriptor.table_name,
                        index = %descriptor.index_name,
                        "backfill.lock.reuse"
                    );
                    acquired.push((descriptor, state));
                    continue;
                }

                if !lock.is_expired(ctx.now) {
                    debug!(
                        worker = %self.worker_id,
                        table = %descriptor.table_name,
                        index = %descriptor.index_name,
                        owner = %lock.owner_id,
                        "backfill.lock.busy"
                    );
                    continue;
                }

                metrics_facade::counter!(
                    metrics_facade::CounterMetric::BackfillJobExpiredLockFoundCount,
                    "scope" => "job"
                )
                .increment(1);
                metrics_facade::counter!(
                    metrics_facade::CounterMetric::BackfillJobExpiredLockFoundCount,
                    "table" => descriptor.table_name.clone(),
                    "index" => descriptor.index_name.clone()
                )
                .increment(1);
                warn!(
                    table = %descriptor.table_name,
                    index = %descriptor.index_name,
                    worker = %self.worker_id,
                    "backfill.lock.expired_claimed"
                );
            }

            if let Some(acquired_state) = self
                .try_acquire_lock(&mut ctx, descriptor.clone(), state)
                .await?
            {
                info!(
                    worker = %self.worker_id,
                    table = %descriptor.table_name,
                    index = %descriptor.index_name,
                    status = ?acquired_state.status,
                    "backfill.schedule"
                );
                acquired.push((descriptor, acquired_state));
            } else {
                debug!(
                    worker = %self.worker_id,
                    table = %descriptor.table_name,
                    index = %descriptor.index_name,
                    "backfill.lock.contended"
                );
            }
        }

        if acquired.is_empty() {
            metrics_facade::gauge!(
                metrics_facade::GaugeMetric::BackfillJobsConcurrentCount,
                "job" => "gsi-backfill"
            )
            .set(0.0);
            debug!(
                worker = %self.worker_id,
                active = 0,
                "backfill.concurrency"
            );
            debug!("no backfill locks acquired");
            return Ok(BackfillResult::Idle);
        }

        if throttled_due_to_concurrency {
            metrics_facade::counter!(
                metrics_facade::CounterMetric::BackfillJobsThrottledCount,
                "job" => "gsi-backfill",
                "reason" => "max_concurrent"
            )
            .increment(1);
        }

        metrics_facade::gauge!(
            metrics_facade::GaugeMetric::BackfillJobsConcurrentCount,
            "job" => "gsi-backfill"
        )
        .set(acquired.len() as f64);
        debug!(
            worker = %self.worker_id,
            active = acquired.len(),
            "backfill.concurrency"
        );

        let mut work_done = false;
        let mut futures: FuturesUnordered<_> = acquired
            .into_iter()
            .map(|(descriptor, state)| {
                let driver = Arc::clone(&self.driver);
                let config = Arc::clone(&self.config);
                let worker_id = self.worker_id.clone();
                async move {
                    Self::process_descriptor(driver, config, worker_id, descriptor, state).await
                }
            })
            .collect();

        while let Some(result) = futures.next().await {
            match result {
                Ok(did_work) => work_done |= did_work,
                Err(err) => warn!(error = %err, "backfill descriptor failed"),
            }
        }

        Ok(BackfillResult::from(work_done))
    }

    async fn try_acquire_lock(
        &self,
        ctx: &mut WorkerContext,
        descriptor: GsiBackfillDescriptor,
        mut state: BackfillState,
    ) -> Result<Option<BackfillState>, BackfillError> {
        let acquire_start = Instant::now();
        let lock = BackfillLock {
            owner_id: self.worker_id.clone(),
            expires_at: ctx.now + self.config.lock_ttl_ms,
        };
        state.lock = Some(lock);
        state.status = BackfillStatus::Backfilling;
        state.refresh_updated_at(ctx.now);
        self.driver
            .persist_state(&descriptor, &state)
            .await
            .map_err(BackfillError::from)?;

        let refreshed = self
            .driver
            .reload_state(&descriptor)
            .await
            .map_err(BackfillError::from)?;

        match refreshed {
            Some(mut refreshed_state) => {
                if refreshed_state
                    .lock
                    .as_ref()
                    .is_some_and(|l| l.owner_id == self.worker_id)
                {
                    let latency_ms = acquire_start.elapsed().as_millis() as f64;
                    metrics_facade::histogram!(
                        metrics_facade::HistogramMetric::BackfillLockAcquireLatencyMs,
                        "job" => "gsi-backfill"
                    )
                    .record(latency_ms);
                    metrics_facade::histogram!(
                        metrics_facade::HistogramMetric::BackfillLockAcquireLatencyMs,
                        "table" => descriptor.table_name.clone(),
                        "index" => descriptor.index_name.clone()
                    )
                    .record(latency_ms);
                    info!(
                        worker = %self.worker_id,
                        table = %descriptor.table_name,
                        index = %descriptor.index_name,
                        latency_ms,
                        "backfill.lock.acquired"
                    );
                    refreshed_state.refresh_updated_at(ctx.now);
                    Ok(Some(refreshed_state))
                } else {
                    debug!(
                        worker = %self.worker_id,
                        table = %descriptor.table_name,
                        index = %descriptor.index_name,
                        "backfill.lock.lost"
                    );
                    Ok(None)
                }
            }
            None => {
                debug!(
                    worker = %self.worker_id,
                    table = %descriptor.table_name,
                    index = %descriptor.index_name,
                    "backfill.lock.state_missing"
                );
                Ok(None)
            }
        }
    }

    async fn process_descriptor<DImpl>(
        driver: Arc<DImpl>,
        config: Arc<BackfillConfig>,
        worker_id: String,
        descriptor: GsiBackfillDescriptor,
        mut state: BackfillState,
    ) -> Result<bool, BackfillError>
    where
        DImpl: BackfillDriver + 'static,
    {
        let config_ref = config.as_ref();
        let mut ctx = WorkerContext::new(worker_id.clone());

        ctx.refresh_now();
        if let Some(lock) = state.lock.as_mut() {
            lock.expires_at = ctx.now + config_ref.lock_ttl_ms;
        }
        driver
            .persist_state(&descriptor, &state)
            .await
            .map_err(BackfillError::from)?;

        let descriptor_table = descriptor.table_name.clone();
        let descriptor_index = descriptor.index_name.clone();
        let batch_start = Instant::now();
        let batch = driver
            .execute_batch(&descriptor, &state, config_ref.batch_size)
            .await
            .map_err(BackfillError::from)?;
        let elapsed_ms = batch_start.elapsed().as_millis() as f64;
        metrics_facade::histogram!(
            metrics_facade::HistogramMetric::BackfillJobRuntimeMs,
            "scope" => "job"
        )
        .record(elapsed_ms);
        metrics_facade::histogram!(
            metrics_facade::HistogramMetric::BackfillJobRuntimeMs,
            "table" => descriptor_table.clone(),
            "index" => descriptor_index.clone()
        )
        .record(elapsed_ms);

        let did_work = batch.did_work();
        let items_processed = batch.items_processed;
        let has_checkpoint = batch.next_token.is_some();
        let done = batch.done;

        info!(
            table = %descriptor_table,
            index = %descriptor_index,
            runtime_ms = elapsed_ms,
            items_processed,
            done,
            has_checkpoint,
            "backfill.batch"
        );

        ctx.refresh_now();
        Self::update_state_after_batch(
            &mut state,
            &descriptor,
            &driver,
            config_ref,
            batch,
            &mut ctx,
        )
        .await?;

        if !did_work {
            metrics_facade::counter!(
                metrics_facade::CounterMetric::BackfillJobIdleCount,
                "table" => descriptor_table.clone(),
                "index" => descriptor_index.clone()
            )
            .increment(1);
            info!(
                table = %descriptor_table,
                index = %descriptor_index,
                "backfill.batch.idle"
            );
            tokio::time::sleep(Duration::from_millis(config_ref.idle_sleep_ms)).await;
        }

        Ok(did_work)
    }

    async fn update_state_after_batch<DImpl>(
        state: &mut BackfillState,
        descriptor: &GsiBackfillDescriptor,
        driver: &Arc<DImpl>,
        config: &BackfillConfig,
        batch: BackfillBatchOutcome,
        ctx: &mut WorkerContext,
    ) -> Result<(), BackfillError>
    where
        DImpl: BackfillDriver + 'static,
    {
        state.scan_lek = batch.next_token;
        state.refresh_updated_at(ctx.now);

        if batch.done {
            info!(
                table = %descriptor.table_name,
                index = %descriptor.index_name,
                "backfill completed"
            );
            state.status = BackfillStatus::Done;
            state.lock = None;
        } else if let Some(lock) = state.lock.as_mut() {
            lock.expires_at = ctx.now + config.lock_ttl_ms;
        }

        driver
            .persist_state(descriptor, state)
            .await
            .map_err(BackfillError::from)?;
        Ok(())
    }
}
