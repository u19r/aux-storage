use crate::storage_ops::imports::{
    Arc, AtomicU32, BatchWriteItemRequest, DeleteRequest, HashMap, Instrument, Ordering,
    RetryPolicy, SortedKvDbStorageProvider, StorageError, StorageProvider, StorageResult,
    StoredTableInfo, StreamExt, TableName, TablePageKey, TimeToLiveStatus, TimestampMillis,
    TtlConfigRecord, TtlSweepLock, Utc, WriteRequest, constants, execute_with_retry,
    increment_bytes, ttl,
};

struct TtlSweepStats {
    deleted_items: usize,
    next_shard: u8,
    shards_checked: usize,
    throttled: bool,
    last_processed_shard: u8,
    last_processed_watermark: Option<i64>,
    retry_batches: u32,
    retry_attempts: u32,
    retry_failures: u32,
}

#[expect(clippy::cast_possible_truncation)]
pub(super) fn usize_to_u32(value: usize) -> u32 {
    value as u32
}

pub(super) fn adjust_ttl_shard_batch(
    table_name: &TableName,
    config: &mut TtlConfigRecord,
    runtime_ms: u64,
) {
    let interval_ms = constants::TTL_SWEEP_INTERVAL_MINUTES.saturating_mul(60_000);
    if let Some(new_batch) = config.update_adaptive_batch(
        runtime_ms,
        interval_ms,
        constants::TTL_SWEEP_MIN_SHARD_BATCH,
        constants::TTL_SWEEP_MAX_SHARD_BATCH,
        constants::TTL_SWEEP_INITIAL_SHARD_BATCH,
    ) {
        #[expect(clippy::cast_precision_loss)]
        let utilization = if interval_ms == 0 {
            0.0
        } else {
            runtime_ms as f64 / interval_ms as f64
        };
        tracing::info!(
            table = %table_name,
            runtime_ms,
            interval_ms,
            utilization,
            new_batch,
            "ttl.sweep.adjust_shard_batch"
        );
    }
}

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(job = %storage_common::TTL_SWEEP_JOB)
    )]
    pub(crate) async fn run_ttl_sweep(&self) -> StorageResult<bool> {
        let configs = self.list_ttl_configs().await?;
        if configs.is_empty() {
            return Ok(false);
        }

        let worker_id = uuid::Uuid::now_v7().to_string();
        let job_start = std::time::Instant::now();

        let mut processed_tables = 0_usize;
        let mut work_done = false;
        let mut total_deleted = 0_usize;
        let mut total_shards = 0_usize;
        let mut throttled_tables = 0_usize;
        let mut total_retry_batches = 0_u64;
        let mut total_retry_attempts = 0_u64;
        let mut total_retry_failures = 0_u64;

        for (table_name, mut config) in configs {
            if processed_tables >= constants::TTL_SWEEP_TABLE_CONCURRENCY {
                break;
            }

            if config.status != TimeToLiveStatus::Enabled {
                continue;
            }

            let now = TimestampMillis::now();
            let force_health_check = config
                .should_force_health_check(now, constants::TTL_SWEEP_HEALTH_CHECK_INTERVAL_MINUTES);

            if config.should_skip() && !force_health_check {
                config.consume_skip();
                config.touch();
                self.save_ttl_config(&table_name, &config).await?;
                tracing::debug!(
                    table = %table_name,
                    remaining = config.skip_runs_remaining,
                    "ttl.sweep.skipped"
                );
                continue;
            }

            if config.should_skip() && force_health_check {
                tracing::debug!(
                    table = %table_name,
                    "ttl.sweep.health_check_override"
                );
                config.skip_runs_remaining = 0;
                config.touch();
                self.save_ttl_config(&table_name, &config).await?;
            }

            let Some(mut config) = self
                .try_acquire_ttl_lock(&table_name, config, &worker_id, now)
                .await?
            else {
                continue;
            };

            let shard_batch = config.compute_shard_batch(
                constants::TTL_SWEEP_INITIAL_SHARD_BATCH,
                constants::TTL_SWEEP_MIN_SHARD_BATCH,
                constants::TTL_SWEEP_MAX_SHARD_BATCH,
            );

            let table_info = self.get_table_info(&table_name).await?;

            let table_span = tracing::info_span!(
                "ttl_sweep.table",
                job = %storage_common::TTL_SWEEP_JOB,
                table = %table_name,
                gsi = %config.gsi_name(),
                shard_batch = shard_batch
            );

            let sweep_start = std::time::Instant::now();
            config.last_sweep_started_at = Some(TimestampMillis::now());

            let sweep_stats = self
                .process_ttl_for_table(&table_name, &table_info, &config, shard_batch)
                .instrument(table_span.clone())
                .await?;

            let runtime_ms = u64::try_from(sweep_start.elapsed().as_millis()).unwrap_or(u64::MAX);
            processed_tables += 1;
            total_retry_batches =
                total_retry_batches.saturating_add(u64::from(sweep_stats.retry_batches));
            total_retry_attempts =
                total_retry_attempts.saturating_add(u64::from(sweep_stats.retry_attempts));
            total_retry_failures =
                total_retry_failures.saturating_add(u64::from(sweep_stats.retry_failures));
            config.last_sweep_runtime_ms = Some(runtime_ms);
            config.next_shard = sweep_stats.next_shard;
            config.sweep_lock = None;
            config.touch();
            if let Some(watermark) = sweep_stats.last_processed_watermark {
                config.last_processed_watermark = Some(watermark);
            }

            #[expect(clippy::cast_precision_loss)]
            {
                metrics_facade::histogram!(
                    metrics_facade::HistogramMetric::TtlSweepRuntimeMs,
                    "scope" => "table",
                    "table" => table_name.as_ref().to_string()
                )
                .record(runtime_ms as f64);
            }

            metrics_facade::counter!(
                metrics_facade::CounterMetric::TtlSweepTablesChecked,
                "scope" => "table",
                "table" => table_name.as_ref().to_string()
            )
            .increment(1);

            metrics_facade::counter!(
                metrics_facade::CounterMetric::TtlSweepTablesChecked,
                "scope" => "job"
            )
            .increment(1);

            metrics_facade::counter!(
                metrics_facade::CounterMetric::TtlSweepShardsChecked,
                "scope" => "table",
                "table" => table_name.as_ref().to_string()
            )
            .increment(sweep_stats.shards_checked as u64);

            metrics_facade::counter!(
                metrics_facade::CounterMetric::TtlSweepItemsDeleted,
                "scope" => "table",
                "table" => table_name.as_ref().to_string()
            )
            .increment(sweep_stats.deleted_items as u64);

            if sweep_stats.retry_batches > 0 {
                metrics_facade::counter!(
                    metrics_facade::CounterMetric::TtlSweepRetryBatches,
                    "scope" => "table",
                    "table" => table_name.as_ref().to_string()
                )
                .increment(u64::from(sweep_stats.retry_batches));
            }

            if sweep_stats.retry_attempts > 0 {
                metrics_facade::counter!(
                    metrics_facade::CounterMetric::TtlSweepRetryAttempts,
                    "scope" => "table",
                    "table" => table_name.as_ref().to_string()
                )
                .increment(u64::from(sweep_stats.retry_attempts));
            }

            if sweep_stats.retry_failures > 0 {
                metrics_facade::counter!(
                    metrics_facade::CounterMetric::TtlSweepRetryFailures,
                    "scope" => "table",
                    "table" => table_name.as_ref().to_string()
                )
                .increment(u64::from(sweep_stats.retry_failures));
            }

            total_deleted += sweep_stats.deleted_items;
            total_shards += sweep_stats.shards_checked;

            if sweep_stats.deleted_items > 0 {
                config.register_progress();
                work_done = true;
            } else {
                config.register_idle(constants::TTL_SWEEP_MAX_SKIP);
            }

            if sweep_stats.throttled {
                throttled_tables += 1;
                config.register_throttle();
                metrics_facade::counter!(
                    metrics_facade::CounterMetric::TtlSweepThrottledCount,
                    "scope" => "table",
                    "table" => table_name.as_ref().to_string()
                )
                .increment(1);
                tracing::info!(
                    table = %table_name,
                    gsi = %config.gsi_name(),
                    shard = sweep_stats.last_processed_shard,
                    shard_batch,
                    "ttl.sweep.throttled"
                );
            } else {
                config.reset_throttle();
            }

            let () = adjust_ttl_shard_batch(&table_name, &mut config, runtime_ms);

            tracing::info!(
                table = %table_name,
                gsi = %config.gsi_name(),
                deleted_items = sweep_stats.deleted_items,
                shards_checked = sweep_stats.shards_checked,
                next_shard = sweep_stats.next_shard,
                throttled = sweep_stats.throttled,
                retry_batches = sweep_stats.retry_batches,
                retry_attempts = sweep_stats.retry_attempts,
                retry_failures = sweep_stats.retry_failures,
                runtime_ms,
                "ttl.sweep.table_summary"
            );

            self.save_ttl_config(&table_name, &config)
                .instrument(table_span)
                .await?;
        }

        let job_runtime_ms = u64::try_from(job_start.elapsed().as_millis()).unwrap_or(u64::MAX);
        #[expect(clippy::cast_precision_loss)]
        {
            metrics_facade::histogram!(
                metrics_facade::HistogramMetric::TtlSweepRuntimeMs,
                "scope" => "job"
            )
            .record(job_runtime_ms as f64);
        }

        metrics_facade::counter!(
            metrics_facade::CounterMetric::TtlSweepShardsChecked,
            "scope" => "job"
        )
        .increment(total_shards as u64);

        metrics_facade::counter!(
            metrics_facade::CounterMetric::TtlSweepItemsDeleted,
            "scope" => "job"
        )
        .increment(total_deleted as u64);

        if total_retry_batches > 0 {
            metrics_facade::counter!(
                metrics_facade::CounterMetric::TtlSweepRetryBatches,
                "scope" => "job"
            )
            .increment(total_retry_batches);
        }

        if total_retry_attempts > 0 {
            metrics_facade::counter!(
                metrics_facade::CounterMetric::TtlSweepRetryAttempts,
                "scope" => "job"
            )
            .increment(total_retry_attempts);
        }

        if total_retry_failures > 0 {
            metrics_facade::counter!(
                metrics_facade::CounterMetric::TtlSweepRetryFailures,
                "scope" => "job"
            )
            .increment(total_retry_failures);
        }

        if throttled_tables > 0 {
            metrics_facade::counter!(
                metrics_facade::CounterMetric::TtlSweepThrottledCount,
                "scope" => "job"
            )
            .increment(throttled_tables as u64);
        }

        tracing::info!(
            worker_id = %worker_id,
            processed_tables,
            total_deleted,
            total_shards,
            throttled_tables,
            retry_batches = total_retry_batches,
            retry_attempts = total_retry_attempts,
            retry_failures = total_retry_failures,
            runtime_ms = job_runtime_ms,
            "ttl.sweep.job_summary"
        );

        Ok(work_done)
    }

    async fn process_ttl_for_table(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        config: &TtlConfigRecord,
        shard_batch: usize,
    ) -> StorageResult<TtlSweepStats> {
        let now_seconds = Utc::now().timestamp();
        let mut deleted_items = 0_usize;
        let mut shards_checked = 0_usize;
        let mut throttled = false;
        let mut last_processed_watermark = config.last_processed_watermark;
        let mut retry_batches = 0_u32;
        let mut retry_attempts = 0_u32;
        let mut retry_failures = 0_u32;
        let prefix = ttl::ttl_index_prefix(table_name);
        let start_prefix = ttl::ttl_index_range_start(table_name);
        let end_prefix = ttl::ttl_index_range_end(table_name, now_seconds);
        let range_end = increment_bytes(end_prefix);
        let should_write_to_stream = crate::backends::common::should_write_stream_entries(
            table_info,
            self.requires_immediate_gsi_updates(table_info),
        );
        let policy = RetryPolicy {
            max_attempts: constants::TTL_SWEEP_RETRY_MAX_ATTEMPTS,
            base_delay_ms: constants::TTL_SWEEP_RETRY_BASE_DELAY_MS,
            max_delay_ms: constants::TTL_SWEEP_RETRY_MAX_DELAY_MS,
            jitter: true,
        };

        let mut range_start = start_prefix.clone();
        for batch_index in 0..shard_batch {
            let range = self
                .kv_store
                .get_range(
                    &range_start,
                    &range_end,
                    Some(usize_to_u32(constants::TTL_SWEEP_ITEMS_PER_SHARD)),
                    None::<TablePageKey>,
                    true,
                )
                .await?;

            if range.items.is_empty() {
                break;
            }

            let last_key = range.items.last().map(|(key, _)| key.to_vec());
            shards_checked += 1;

            let mut delete_keys = Vec::new();
            for (key, _raw_value) in &range.items {
                let Some((ttl_value, token)) = ttl::parse_ttl_index_key(key, &prefix) else {
                    continue;
                };
                if ttl_value > now_seconds {
                    continue;
                }
                let key_map = ttl::ttl_index_key_map_from_token(&token, table_info)?;
                delete_keys.push(key_map);
                last_processed_watermark = Some(ttl_value);
            }

            if !delete_keys.is_empty() {
                deleted_items = deleted_items.saturating_add(delete_keys.len());
                let mut delete_batches = Vec::new();
                for chunk in delete_keys.chunks(constants::TTL_SWEEP_DELETE_BATCH_SIZE) {
                    let mut writes = Vec::with_capacity(chunk.len());
                    for key in chunk {
                        writes.push(WriteRequest {
                            put_request: None,
                            delete_request: Some(DeleteRequest {
                                key: key.clone().into(),
                                aux_item_stream_ttl_hours: None,
                            }),
                        });
                    }
                    let mut request_items = HashMap::new();
                    request_items.insert(table_name.clone(), writes);
                    delete_batches.push(BatchWriteItemRequest {
                        request_items,
                        return_consumed_capacity: None,
                        return_item_collection_metrics: None,
                    });
                }

                retry_batches =
                    retry_batches.saturating_add(u32::try_from(delete_batches.len()).unwrap_or(0));

                let mut results = futures::stream::iter(delete_batches.into_iter().map(|batch| {
                    let provider = self.clone();
                    let attempts = Arc::new(AtomicU32::new(0));
                    let failures = Arc::new(AtomicU32::new(0));
                    let attempts_for_closure = Arc::clone(&attempts);
                    let failures_for_closure = Arc::clone(&failures);
                    let table_name_clone = table_name.clone();
                    async move {
                        let result = execute_with_retry(policy, move |attempt| {
                            attempts_for_closure.store(attempt, Ordering::Relaxed);
                            if attempt > 1 {
                                failures_for_closure.fetch_add(1, Ordering::Relaxed);
                                tracing::warn!(
                                    table = %table_name_clone,
                                    attempt,
                                    "ttl.sweep.batch_retry"
                                );
                            }
                            let provider = provider.clone();
                            let batch = batch.clone();
                            async move {
                                let response = provider
                                    .batch_write_item(batch, should_write_to_stream)
                                    .await?;
                                if response.unprocessed_items.is_some() {
                                    Err(StorageError::internal(
                                        "ttl sweep delete batch unprocessed",
                                    ))
                                } else {
                                    Ok(())
                                }
                            }
                        })
                        .await;
                        (result, attempts, failures)
                    }
                }))
                .buffer_unordered(constants::TTL_SWEEP_DELETE_BATCH_CONCURRENCY)
                .collect::<Vec<_>>()
                .await;

                for (result, attempts, failures) in results.drain(..) {
                    let observed_attempts = attempts.load(Ordering::Relaxed);
                    let observed_failures = failures.load(Ordering::Relaxed);
                    if observed_attempts > 0 {
                        retry_attempts = retry_attempts.saturating_add(observed_attempts);
                    }
                    if observed_failures > 0 {
                        retry_failures = retry_failures.saturating_add(observed_failures);
                    }
                    result?;
                }
            }

            if range.has_more {
                if batch_index + 1 >= shard_batch {
                    throttled = true;
                    break;
                }
                if let Some(last_key) = last_key {
                    range_start = increment_bytes(last_key);
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        Ok(TtlSweepStats {
            deleted_items,
            next_shard: 0,
            shards_checked,
            throttled,
            last_processed_shard: 0,
            last_processed_watermark,
            retry_batches,
            retry_attempts,
            retry_failures,
        })
    }

    async fn try_acquire_ttl_lock(
        &self,
        table_name: &TableName,
        mut config: TtlConfigRecord,
        worker_id: &str,
        now: TimestampMillis,
    ) -> StorageResult<Option<TtlConfigRecord>> {
        if let Some(lock) = config.sweep_lock.as_ref() {
            if lock.owner_id != worker_id && !lock.is_expired(now) {
                tracing::debug!(
                    table = %table_name,
                    owner = %lock.owner_id,
                    "ttl.sweep.lock_held_by_peer"
                );
                return Ok(None);
            }

            if lock.owner_id != worker_id && lock.is_expired(now) {
                metrics_facade::counter!(
                    metrics_facade::CounterMetric::TtlSweepExpiredLockFoundCount,
                    "scope" => "job"
                )
                .increment(1);
                metrics_facade::counter!(
                    metrics_facade::CounterMetric::TtlSweepExpiredLockFoundCount,
                    "table" => table_name.as_ref().to_string()
                )
                .increment(1);
                tracing::info!(
                    table = %table_name,
                    expired_owner = %lock.owner_id,
                    "ttl.sweep.lock_expired"
                );
            }
        }

        config.sweep_lock = Some(TtlSweepLock::new(
            worker_id.to_string(),
            now,
            constants::TTL_SWEEP_LOCK_TTL_MS,
        ));
        config.touch();
        self.save_ttl_config(table_name, &config).await?;

        let refreshed = self.load_ttl_config(table_name).await?;
        let Some(refreshed_config) = refreshed else {
            tracing::warn!(
                table = %table_name,
                "ttl.sweep.lock_state_missing_after_save"
            );
            return Ok(None);
        };

        if refreshed_config
            .sweep_lock
            .as_ref()
            .is_some_and(|lock| lock.owner_id == worker_id)
        {
            tracing::debug!(
                table = %table_name,
                worker = %worker_id,
                "ttl.sweep.lock_acquired"
            );
            Ok(Some(refreshed_config))
        } else {
            tracing::debug!(
                table = %table_name,
                worker = %worker_id,
                "ttl.sweep.lock_claim_failed"
            );
            Ok(None)
        }
    }
}
