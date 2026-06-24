use crate::storage_ops::provider_impl::*;

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(crate) async fn trim_change_index_markers_older_than(
        &self,
        cutoff_created_at_ms: i64,
    ) -> StorageResult<usize> {
        let mut deleted_markers = 0usize;
        for slot in 0..crate::storage_ops::write_helpers::CHANGE_INDEX_SLOT_COUNT {
            let prefix = change_index_slot_prefix(slot);
            let range = self.kv_store.get_prefix(&prefix, true, None, true).await?;
            let mut batch = Vec::new();
            for (key, _) in range.items {
                let Some(marker) = parse_change_index_key(slot, &prefix, &key) else {
                    continue;
                };
                let Some(created_at_ms) = change_index_marker_created_at_ms(&marker.versionstamp)
                else {
                    continue;
                };
                if created_at_ms < cutoff_created_at_ms {
                    batch.push(BatchItem {
                        key: key.into_vec(),
                        value: None,
                    });
                    deleted_markers += 1;
                }
            }
            if !batch.is_empty() {
                self.kv_store.batch_write(batch).await?;
            }
        }
        Ok(deleted_markers)
    }

    pub(super) async fn run_job_impl(&self, name: BackgroundJobName) -> StorageResult<()> {
        match name {
            storage_common::GSI_UPDATE_JOB => self.run_gsi_update_job().await,
            storage_common::GSI_BACKFILL_JOB => self.run_gsi_backfill_job().await,
            storage_common::TTL_SWEEP_JOB => self.run_ttl_sweep_job().await,
            STREAM_TRIM_JOB => self.run_stream_trim_job().await,
            storage_common::PARTITION_RECONCILE_JOB => self.run_partition_reconcile_job().await,
            _ => Ok(()),
        }
    }

    pub(super) async fn list_change_index_markers_impl(
        &self,
        request: ListChangeIndexMarkersRequest,
    ) -> StorageResult<Vec<ChangeIndexMarker>> {
        let limit = u32::try_from(request.limit)
            .map_err(|_| StorageError::validation("change index list limit exceeds u32"))?;
        let prefix = change_index_slot_prefix(request.slot);
        let start = request.after_versionstamp.as_ref().map_or_else(
            || prefix.clone(),
            |after| {
                let mut start = prefix.clone();
                start.extend_from_slice(after.as_bytes());
                start.extend_from_slice(b"/\xff");
                start
            },
        );
        let exclusive_end = increment_bytes(prefix.clone());
        let range = self
            .kv_store
            .get_range(&start, &exclusive_end, Some(limit), None::<ItemKey>, true)
            .await?;
        let mut markers = Vec::with_capacity(range.items.len());
        for (key, _) in range.items {
            if let Some(marker) = parse_change_index_key(request.slot, &prefix, &key) {
                markers.push(marker);
            }
        }
        Ok(markers)
    }

    pub(super) async fn initialize_storage_impl(&self) -> StorageResult<()> {
        if !self.database_jobs_enabled {
            return Ok(());
        }

        let registrar = KvRegistrar {
            mgr: &self.job_manager,
        };
        self.register_gsi_jobs(&registrar).await?;
        self.register_ttl_job(&registrar).await?;
        self.register_stream_trim_job(&registrar).await?;
        self.start_partition_reconcile_task().await?;
        Ok(())
    }

    async fn run_gsi_update_job(&self) -> StorageResult<()> {
        if self.immediate_gsi_consistency {
            return Ok(());
        }
        loop {
            let progressed = self.process_gsi_updates().await?;
            if !progressed {
                return Ok(());
            }
        }
    }

    async fn run_gsi_backfill_job(&self) -> StorageResult<()> {
        let coordinator =
            BackfillCoordinator::new(std::sync::Arc::new(self.clone()), BackfillConfig::default());
        loop {
            let progressed = self.process_gsi_backfills_with(&coordinator).await?;
            if !progressed {
                return Ok(());
            }
        }
    }

    async fn run_ttl_sweep_job(&self) -> StorageResult<()> {
        loop {
            let cutoff_created_at_ms = TimestampMillis::now()
                .timestamp_millis()
                .saturating_sub(CHANGE_INDEX_MARKER_RETENTION_MS);
            self.trim_change_index_markers_older_than(cutoff_created_at_ms)
                .await?;
            let progressed = self.run_ttl_sweep().await?;
            if !progressed {
                return Ok(());
            }
        }
    }

    async fn run_stream_trim_job(&self) -> StorageResult<()> {
        loop {
            let progressed = self.run_stream_trim().await?;
            if !progressed {
                return Ok(());
            }
        }
    }

    async fn run_partition_reconcile_job(&self) -> StorageResult<()> {
        loop {
            let progressed = self.run_partition_reconcile().await?;
            if !progressed {
                return Ok(());
            }
        }
    }

    async fn register_gsi_jobs(&self, registrar: &KvRegistrar<'_>) -> StorageResult<()> {
        let gsi_cfg = self.database_job_intervals.gsi_config();
        let update_job = GsiUpdateJob::new_with_interval(
            std::sync::Arc::new(self.clone()),
            gsi_cfg.update_interval_ms,
        );
        let backfill_job = GsiBackfillJob::new(std::sync::Arc::new(self.clone()));
        if self.immediate_gsi_consistency {
            registrar
                .register_timed_job(GSI_BACKFILL_JOB, gsi_cfg.backfill_interval_ms, backfill_job)
                .await
                .map_err(|e| {
                    StorageError::internal(&format!("register gsi backfill job failed: {e}"))
                })?;
            return Ok(());
        }

        register_gsi_jobs(registrar, gsi_cfg, update_job, backfill_job)
            .await
            .map_err(|e| StorageError::internal(&format!("register gsi jobs failed: {e}")))
    }

    async fn register_ttl_job(&self, registrar: &KvRegistrar<'_>) -> StorageResult<()> {
        let ttl_job = crate::ttl::TtlSweepJob::new(std::sync::Arc::new(self.clone()));
        registrar
            .register_timed_job(
                storage_common::TTL_SWEEP_JOB,
                self.database_job_intervals.ttl_sweep_interval_ms,
                ttl_job,
            )
            .await
            .map_err(|e| StorageError::internal(&format!("register ttl sweep job failed: {e}")))
    }

    async fn register_stream_trim_job(&self, registrar: &KvRegistrar<'_>) -> StorageResult<()> {
        let trim_job = crate::stream::StreamTrimJob::new(std::sync::Arc::new(self.clone()));
        registrar
            .register_timed_job(
                STREAM_TRIM_JOB,
                self.database_job_intervals.stream_trim_interval_ms,
                trim_job,
            )
            .await
            .map_err(|e| StorageError::internal(&format!("register stream trim job failed: {e}")))
    }
}

struct KvRegistrar<'a> {
    mgr: &'a bg_jobs::JobManager,
}

#[async_trait]
impl RegistersJobs for KvRegistrar<'_> {
    type Error = StorageError;

    async fn register_timed_job<J>(
        &self,
        name: BackgroundJobName,
        interval_ms: JobIntervalMillis,
        job: J,
    ) -> Result<(), Self::Error>
    where
        J: BackgroundJob + 'static,
    {
        let config = JobConfig {
            start_immediately: true,
            sleep_duration: std::time::Duration::from_millis(interval_ms.0),
            jitter_percent: 10,
        };
        self.mgr
            .register_job(name, job, config)
            .await
            .map_err(|e| StorageError::internal(&format!("register job {name} failed: {e}")))?;
        Ok(())
    }
}
