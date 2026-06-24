use crate::{common::*, imports::*};

pub(crate) struct PartitionFamilyWorkload {
    pub(crate) name: String,
    pub(crate) profile: String,
    pub(crate) operation_count: u64,
    pub(crate) artifact_root: String,
    pub(crate) client_id: i32,
    pub(crate) client_count: i32,
    pub(crate) active_client_count: i32,
    pub(crate) setup_count: u64,
    pub(crate) start_count: u64,
    pub(crate) check_count: u64,
    pub(crate) append_count: u64,
    pub(crate) reconcile_count: u64,
    pub(crate) direct_scan_count: u64,
    pub(crate) split_count: u64,
    pub(crate) range_check_count: u64,
    pub(crate) read_back_count: u64,
    pub(crate) background_lease_event_count: u64,
    pub(crate) error_count: u64,
    pub(crate) stream_name: Option<StreamName>,
    pub(crate) expected_item_ids: Vec<storage_types::StreamItemId>,
    pub(crate) background_lease_events: Vec<BackgroundLeaseEvent>,
    pub(crate) context: WorkloadContext,
}

impl PartitionFamilyWorkload {
    pub(crate) fn new(name: String, context: WorkloadContext) -> Self {
        let profile = option_or_default_string(&context, OPTION_PROFILE, "smoke");
        let operation_count = option_or_default(&context, OPTION_OPERATION_COUNT, 0_u64);
        let _: u64 = option_or_default(&context, OPTION_HISTORY_SAMPLE_LIMIT, 0_u64);
        let artifact_root =
            option_or_default_string(&context, OPTION_ARTIFACT_ROOT, "run-artifacts/fdb-chaos");
        let client_id = context.client_id();
        let client_count = context.client_count();
        let active_client_count =
            option_or_default(&context, OPTION_ACTIVE_CLIENT_COUNT, 1_i32).clamp(1, client_count);
        Self {
            name,
            profile,
            operation_count,
            artifact_root,
            client_id,
            client_count,
            active_client_count,
            setup_count: 0,
            start_count: 0,
            check_count: 0,
            append_count: 0,
            reconcile_count: 0,
            direct_scan_count: 0,
            split_count: 0,
            range_check_count: 0,
            read_back_count: 0,
            background_lease_event_count: 0,
            error_count: 0,
            stream_name: None,
            expected_item_ids: Vec::new(),
            background_lease_events: Vec::new(),
            context,
        }
    }

    pub(crate) fn store(&self, db: SimDatabase) -> Result<FoundationDbKvStore, String> {
        FoundationDbKvStore::from_database(
            FoundationDbConfig {
                cluster_file_path: None,
                tenant_name: None,
                subspace_prefix: Some(b"aux-storage/fdb-chaos/partition-family/".to_vec()),
                cache_read_version_ms: 0,
                immediate_gsi_consistency: false,
                report_conflicting_keys: false,
            },
            db,
        )
        .map_err(|err| storage_error_detail(&err))
    }

    pub(crate) async fn provider(&self, db: SimDatabase) -> Result<Arc<FdbChaosProvider>, String> {
        let provider = Arc::new(
            SortedKvDbStorageProvider::new(self.store(db)?).with_database_jobs_enabled(false),
        );
        provider
            .initialize_storage()
            .await
            .map_err(|err| storage_error_detail(&err))?;
        provider
            .initialize_stream()
            .await
            .map_err(|err| err.to_string())?;
        Ok(provider)
    }

    pub(crate) fn is_active_client(&self) -> bool {
        self.client_id < self.active_client_count
    }

    pub(crate) fn stream_user_name(&self) -> UserStreamName {
        UserStreamName::new("FdbChaosPartitionFamily")
    }

    pub(crate) fn client_artifact_root(&self) -> PathBuf {
        PathBuf::from(&self.artifact_root).join(format!("client-{}", self.client_id))
    }

    pub(crate) fn sim_time_ms(&self) -> i64 {
        let now = self.context.now();
        if !now.is_finite() || now < 0.0 {
            return 1;
        }
        let millis = (now * 1_000.0).round();
        if millis >= i64::MAX as f64 {
            i64::MAX
        } else {
            (millis as i64).max(1)
        }
    }

    pub(crate) fn partition_reconcile_lease_key(&self, stream_name: &StreamName) -> String {
        format!(
            "partition-reconcile/ordered-log/{}",
            ordered_log_family_component(stream_name)
        )
    }

    pub(crate) fn begin_partition_reconcile_lease_resume(
        &mut self,
        stream_name: &StreamName,
    ) -> (String, i64) {
        let lease_key = self.partition_reconcile_lease_key(stream_name);
        let base_ms = self.sim_time_ms().saturating_add(
            i64::try_from(self.reconcile_count)
                .unwrap_or(i64::MAX)
                .saturating_mul(100),
        );
        let active_worker = format!("fdb-chaos-client-{}", self.client_id);
        self.background_lease_events
            .push(BackgroundLeaseEvent::acquire(
                lease_key.clone(),
                "fdb-chaos-reconcile-crashed",
                base_ms,
                base_ms.saturating_add(10),
            ));
        self.background_lease_events
            .push(BackgroundLeaseEvent::acquire(
                lease_key.clone(),
                active_worker.clone(),
                base_ms.saturating_add(20),
                base_ms.saturating_add(40),
            ));
        self.background_lease_events
            .push(BackgroundLeaseEvent::renew(
                lease_key.clone(),
                active_worker,
                base_ms.saturating_add(35),
                base_ms.saturating_add(70),
            ));
        self.background_lease_event_count = self.background_lease_events.len() as u64;
        (lease_key, base_ms.saturating_add(50))
    }

    pub(crate) fn record_partition_reconcile_commit(&mut self, lease_key: String, at_ms: i64) {
        self.background_lease_events
            .push(BackgroundLeaseEvent::commit(
                lease_key,
                format!("fdb-chaos-client-{}", self.client_id),
                at_ms,
                format!("partition-reconcile-run-{}", self.reconcile_count),
            ));
        self.background_lease_event_count = self.background_lease_events.len() as u64;
    }

    pub(crate) fn write_background_lease_artifacts(&self) -> Result<(), String> {
        if self.background_lease_events.is_empty() {
            return Ok(());
        }
        let root = self.client_artifact_root();
        fs::create_dir_all(&root).map_err(|err| {
            format!(
                "failed to create workload artifact directory {}: {err}",
                root.display()
            )
        })?;
        let mut lines = String::new();
        for event in &self.background_lease_events {
            lines.push_str(
                &serde_json::to_string(event)
                    .map_err(|err| format!("failed to serialize background lease event: {err}"))?,
            );
            lines.push('\n');
        }
        fs::write(root.join("background-lease-events.jsonl"), lines)
            .map_err(|err| format!("failed to write background lease events: {err}"))
    }

    pub(crate) fn hot_partition_keys(&self, partition_id: u16, count: usize) -> Vec<String> {
        let partitions = initial_partition_infos(DEFAULT_ORDERED_LOG_PARTITION_COUNT);
        let mut keys = Vec::with_capacity(count);
        let mut candidate = 0_u64;
        while keys.len() < count {
            let key = format!("hot-key-{candidate}");
            let hash = ordered_log_hash(key.as_bytes());
            if find_partition_for_hash(&partitions, hash)
                .is_some_and(|partition| partition.partition_id == partition_id)
            {
                keys.push(key);
            }
            candidate = candidate.saturating_add(1);
        }
        keys
    }

    pub(crate) async fn direct_partition_infos(
        &self,
        db: SimDatabase,
        stream_name: &StreamName,
    ) -> Result<Vec<PartitionInfo>, String> {
        let store = self.store(db)?;
        let family_component = ordered_log_family_component(stream_name);
        let prefix = partition_info_prefix(PartitionFamilyKind::OrderedLog, &family_component);
        let scan = store
            .direct_audit_scan_prefix(&prefix, 1024)
            .await
            .map_err(|err| storage_error_detail(&err))?;
        let mut partitions = Vec::new();
        for (_key, value) in scan.items {
            partitions
                .push(parse_partition_info(&value).map_err(|err| storage_error_detail(&err))?);
        }
        partitions.sort_by(|left, right| {
            left.hash_start_inclusive
                .cmp(&right.hash_start_inclusive)
                .then(left.partition_id.cmp(&right.partition_id))
        });
        Ok(partitions)
    }

    pub(crate) async fn write_hot_load_sample(
        &self,
        db: SimDatabase,
        stream_name: &StreamName,
        hot_keys: &[String],
    ) -> Result<(), String> {
        let store = self.store(db)?;
        let family_component = ordered_log_family_component(stream_name);
        let window_start_ms = partition_sample_window_start_ms(
            TimestampMillis::now().timestamp_millis(),
            PARTITION_LOAD_SAMPLE_WINDOW_SECONDS,
        );
        let publisher_id = format!("fdb-chaos-client-{}", self.client_id);
        let sample_key = partition_load_sample_key(
            PartitionFamilyKind::OrderedLog,
            &family_component,
            0,
            window_start_ms,
            &publisher_id,
        );
        let mut routing_key_bucket_bitmap = 0_u64;
        for key in hot_keys {
            routing_key_bucket_bitmap |= routing_key_bucket_bit(ordered_log_hash(key.as_bytes()));
        }
        let record = PartitionLoadSampleRecord {
            partition_id: 0,
            window_start_ms,
            publisher_id,
            sample: PartitionLoadSample {
                writes: 1_000,
                bytes: 512_000,
                conflicts: 0,
                routing_key_bucket_bitmap,
                queue_scan_work: 0,
                queue_claim_conflicts: 0,
                oldest_visible_age_ms: 0,
                visible_count: 0,
                invisible_count: 0,
            },
        };
        store
            .put(
                &sample_key,
                &partition_load_sample_bytes(&record).map_err(|err| storage_error_detail(&err))?,
                None,
            )
            .await
            .map_err(|err| storage_error_detail(&err))
    }

    pub(crate) fn check_writable_ranges(&self, partitions: &[PartitionInfo]) -> Result<(), String> {
        const HASH_SPACE_END: u128 = (u64::MAX as u128) + 1;

        let writable = partitions
            .iter()
            .filter(|partition| partition.state == PartitionState::Open)
            .collect::<Vec<_>>();
        if writable.is_empty() {
            return Err("ordered-log partition family has no writable partitions".to_string());
        }

        let mut expected_start = 0_u128;
        for partition in writable {
            let actual_start = u128::from(partition.hash_start_inclusive);
            if actual_start != expected_start {
                return Err(format!(
                    "writable partition range gap or overlap before partition {}: \
                     expected_start={} actual_start={}",
                    partition.partition_id, expected_start, actual_start
                ));
            }
            let actual_end = partition
                .hash_end_exclusive
                .map_or(HASH_SPACE_END, u128::from);
            if actual_end <= actual_start {
                return Err(format!(
                    "writable partition {} has empty range start={} end={}",
                    partition.partition_id, actual_start, actual_end
                ));
            }
            expected_start = actual_end;
        }
        if expected_start != HASH_SPACE_END {
            return Err(format!(
                "writable partition ranges ended at {expected_start}, expected {HASH_SPACE_END}"
            ));
        }
        Ok(())
    }

    pub(crate) fn trace_phase(&self, phase: &'static str) {
        self.context.trace(
            Severity::Info,
            "AuxStorageFdbChaosPartitionFamilyPhase",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Profile" => &self.profile,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "ClientCount" => self.client_count,
                "ActiveClientCount" => self.active_client_count,
                "OperationCount" => self.operation_count,
                "ArtifactRoot" => &self.artifact_root,
            ],
        );
    }

    pub(crate) fn trace_operation(&self, phase: &'static str, operation: &'static str) {
        self.context.trace(
            Severity::Info,
            "AuxStorageFdbChaosPartitionFamilyOperation",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Profile" => &self.profile,
                "Phase" => phase,
                "Operation" => operation,
                "ClientId" => self.client_id,
            ],
        );
    }

    pub(crate) fn trace_error(&mut self, phase: &'static str, error: String) {
        self.error_count += 1;
        self.context.trace(
            Severity::Error,
            "AuxStorageFdbChaosPartitionFamilyError",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "Error" => error,
            ],
        );
    }
}
