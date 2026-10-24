use kv_backend::{
    AttemptMode, KvAttempt as _, KvBackend as _, KvEntry, KvMutation, KvRange, ReadProtection,
    foundationdb::FoundationDbBackend,
};

use crate::{common::*, constants::*, imports::*};

pub(crate) struct KvSmokeWorkload {
    name: String,
    profile: String,
    artifact_root: String,
    client_id: i32,
    client_count: i32,
    operation_count: u64,
    setup_count: u64,
    start_count: u64,
    check_count: u64,
    write_count: u64,
    read_count: u64,
    multi_get_count: u64,
    audit_count: u64,
    error_count: u64,
    context: WorkloadContext,
}

impl KvSmokeWorkload {
    pub(crate) fn new(name: String, context: WorkloadContext) -> Self {
        let profile = option_or_default_string(&context, OPTION_PROFILE, "smoke");
        let operation_count = option_or_default(&context, OPTION_OPERATION_COUNT, 1_u64);
        let _: u64 = option_or_default(&context, OPTION_HISTORY_SAMPLE_LIMIT, 0_u64);
        let artifact_root =
            option_or_default_string(&context, OPTION_ARTIFACT_ROOT, "run-artifacts/fdb-chaos");
        let client_id = context.client_id();
        let client_count = context.client_count();
        Self {
            name,
            profile,
            artifact_root,
            client_id,
            client_count,
            operation_count,
            setup_count: 0,
            start_count: 0,
            check_count: 0,
            write_count: 0,
            read_count: 0,
            multi_get_count: 0,
            audit_count: 0,
            error_count: 0,
            context,
        }
    }

    fn store(&self, db: SimDatabase) -> FoundationDbBackend {
        FoundationDbBackend::from_simulation(db, Some(&self.subspace_prefix()), None)
    }

    async fn write_changes(
        &self,
        store: &FoundationDbBackend,
        changes: Vec<KvEntry>,
    ) -> Result<(), StorageError> {
        self.retry_attempt(store, AttemptMode::ReadWrite, || {
            let changes = changes
                .iter()
                .map(|entry| KvMutation::Put {
                    key: entry.key.clone(),
                    value: entry.value.clone(),
                })
                .collect();
            move |attempt: &mut kv_backend::foundationdb::FoundationDbAttempt| {
                Box::pin(async move { attempt.stage(changes) })
                    as futures::future::BoxFuture<'_, Result<(), StorageError>>
            }
        })
        .await
    }

    async fn retry_attempt<R, F>(
        &self,
        store: &FoundationDbBackend,
        mode: AttemptMode,
        make_operation: impl Fn() -> F,
    ) -> Result<R, StorageError>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(
                &'a mut kv_backend::foundationdb::FoundationDbAttempt,
            ) -> futures::future::BoxFuture<'a, Result<R, StorageError>>
            + Send
            + 'static,
    {
        for attempt in 1..=KV_SMOKE_NATIVE_ATTEMPTS {
            let result = store.with_attempt(mode, make_operation()).await;
            match result {
                Err(error)
                    if attempt < KV_SMOKE_NATIVE_ATTEMPTS
                        && matches!(error.as_ref(), StorageEnum::TransactionConflict { .. }) => {}
                result => return result,
            }
        }
        unreachable!("final native attempt returns its outcome")
    }

    fn subspace_prefix(&self) -> Vec<u8> {
        format!(
            "aux-storage/fdb-chaos/{}/client-{}/",
            self.profile, self.client_id
        )
        .into_bytes()
    }

    fn key(&self) -> Vec<u8> {
        b"kv-smoke/primary-item".to_vec()
    }

    fn value(&self) -> Vec<u8> {
        format!("kv-smoke-value-client-{}", self.client_id).into_bytes()
    }

    fn trace_phase(&self, phase: &'static str) {
        self.context.trace(
            Severity::Info,
            "AuxStorageFdbChaosKvSmokePhase",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Profile" => &self.profile,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "ClientCount" => self.client_count,
                "ArtifactRoot" => &self.artifact_root,
            ],
        );
    }

    fn trace_error(&mut self, phase: &'static str, error: String) {
        self.error_count += 1;
        self.context.trace(
            Severity::Error,
            "AuxStorageFdbChaosKvSmokeError",
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

impl RustWorkload for KvSmokeWorkload {
    async fn setup(&mut self, _db: SimDatabase) {
        self.setup_count += 1;
        self.trace_phase("setup");
    }

    async fn start(&mut self, db: SimDatabase) {
        self.start_count += 1;
        self.trace_phase("start");

        let store = self.store(db);
        let key = self.key();
        let value = self.value();
        if let Err(err) = self
            .write_changes(&store, vec![KvEntry::new(key.clone(), value.clone())])
            .await
        {
            self.trace_error("write", storage_error_detail(&err));
            return;
        }
        self.write_count += 1;

        match self
            .retry_attempt(&store, AttemptMode::ReadOnly, || {
                let key = key.clone();
                move |attempt: &mut kv_backend::foundationdb::FoundationDbAttempt| {
                    Box::pin(async move { attempt.get(&key, ReadProtection::Snapshot).await })
                        as futures::future::BoxFuture<'_, Result<Option<Vec<u8>>, StorageError>>
                }
            })
            .await
        {
            Ok(Some(actual)) if actual == value => {
                self.read_count += 1;
            }
            Ok(Some(actual)) => {
                self.trace_error(
                    "read",
                    format!(
                        "read value mismatch: expected {} bytes, got {}",
                        value.len(),
                        actual.len()
                    ),
                );
                return;
            }
            Ok(None) => {
                self.trace_error("read", "missing value after committed write".to_string());
                return;
            }
            Err(err) => {
                self.trace_error("read", storage_error_detail(&err));
                return;
            }
        }

        let batch_keys = (0..100)
            .map(|index| format!("kv-smoke/batch/{index}").into_bytes())
            .collect::<Vec<_>>();
        let batch_items = batch_keys
            .iter()
            .enumerate()
            .map(|(index, key)| {
                KvEntry::new(key.clone(), format!("batch-value-{index}").into_bytes())
            })
            .collect();
        if let Err(err) = self.write_changes(&store, batch_items).await {
            self.trace_error("multi_get_write", storage_error_detail(&err));
            return;
        }
        let repeat_count = self.operation_count.max(100);
        for _ in 0..repeat_count {
            let missing_key = b"kv-smoke/batch/missing".to_vec();
            let mut requested_keys = Vec::with_capacity(batch_keys.len() + 2);
            requested_keys.push(batch_keys[0].clone());
            requested_keys.push(batch_keys[0].clone());
            requested_keys.extend(batch_keys.iter().skip(1).cloned());
            requested_keys.push(missing_key);
            let values = match self
                .retry_attempt(&store, AttemptMode::ReadOnly, || {
                    let requested_keys = requested_keys.clone();
                    move |attempt: &mut kv_backend::foundationdb::FoundationDbAttempt| {
                        Box::pin(async move {
                            attempt
                                .get_batch(requested_keys, ReadProtection::Snapshot)
                                .await
                        })
                            as futures::future::BoxFuture<
                                '_,
                                Result<Vec<Option<Vec<u8>>>, StorageError>,
                            >
                    }
                })
                .await
            {
                Ok(values) => values,
                Err(err) => {
                    self.trace_error("multi_get", storage_error_detail(&err));
                    return;
                }
            };
            let duplicate_matches = values.first() == values.get(1)
                && values.first().and_then(Option::as_deref) == Some(b"batch-value-0".as_slice());
            let missing_is_none = values.last().is_some_and(Option::is_none);
            let present_values_match = values
                .iter()
                .skip(2)
                .take(batch_keys.len().saturating_sub(1))
                .enumerate()
                .all(|(index, value)| {
                    value.as_deref() == Some(format!("batch-value-{}", index + 1).as_bytes())
                });
            if values.len() != requested_keys.len()
                || !duplicate_matches
                || !missing_is_none
                || !present_values_match
            {
                self.trace_error(
                    "multi_get",
                    format!(
                        "multi-key result mismatch: requested={} returned={} duplicate_matches={} \
                         missing_is_none={} present_values_match={}",
                        requested_keys.len(),
                        values.len(),
                        duplicate_matches,
                        missing_is_none,
                        present_values_match,
                    ),
                );
                return;
            }
            self.multi_get_count += 1;
        }

        match self
            .retry_attempt(&store, AttemptMode::ReadOnly, || {
                |attempt: &mut kv_backend::foundationdb::FoundationDbAttempt| {
                    Box::pin(async move {
                        attempt
                            .scan(
                                KvRange {
                                    entry_bounds: None,
                                    begin: b"kv-smoke/primary-item".to_vec(),
                                    end: b"kv-smoke/primary-iten".to_vec(),
                                    reverse: false,
                                    limit: std::num::NonZeroU32::new(10)
                                        .expect("nonzero scan limit"),
                                    max_bytes: std::num::NonZeroUsize::new(1024 * 1024)
                                        .expect("nonzero scan budget"),
                                },
                                ReadProtection::Snapshot,
                            )
                            .await
                    })
                        as futures::future::BoxFuture<
                            '_,
                            Result<kv_backend::KvRangePage, StorageError>,
                        >
                }
            })
            .await
        {
            Ok(range) if range.entries.len() == 1 => {
                self.audit_count += 1;
            }
            Ok(range) => {
                self.trace_error(
                    "audit",
                    format!("expected one audit row, got {}", range.entries.len()),
                );
            }
            Err(err) => {
                self.trace_error("audit", storage_error_detail(&err));
            }
        }
    }

    async fn check(&mut self, _db: SimDatabase) {
        self.check_count += 1;
        self.trace_phase("check");
    }

    fn get_metrics(&self, mut out: Metrics) {
        out.extend([
            metric_val_u64("aux_storage_kv_smoke_setup_count", self.setup_count),
            metric_val_u64("aux_storage_kv_smoke_start_count", self.start_count),
            metric_val_u64("aux_storage_kv_smoke_check_count", self.check_count),
            metric_val_u64("aux_storage_kv_smoke_write_count", self.write_count),
            metric_val_u64("aux_storage_kv_smoke_read_count", self.read_count),
            metric_val_u64("aux_storage_kv_smoke_multi_get_count", self.multi_get_count),
            metric_val_u64("aux_storage_kv_smoke_audit_count", self.audit_count),
            metric_val_u64("aux_storage_kv_smoke_error_count", self.error_count),
        ]);
    }

    fn get_check_timeout(&self) -> f64 {
        60.0
    }
}
