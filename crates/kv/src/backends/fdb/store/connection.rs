use super::*;

impl FoundationDbKvStore {
    pub fn connect(config: FoundationDbConfig) -> StorageResult<Self> {
        let network = init_network(&config)?;
        let database = open_database(&config)?;
        Ok(Self {
            database: Arc::new(database),
            _network: network,
            config: Arc::new(config),
            runtime_partition_load_tracker: RuntimePartitionLoadTracker::default(),
        })
    }

    pub async fn check_reachable(&self, timeout: Duration) -> StorageResult<()> {
        let check = async {
            let trx = self.create_transaction()?;
            Self::configure_transaction(&trx, Some("kv.startup_check"), true)?;
            let _ = trx
                .get(b"__aux_healthcheck", false)
                .await
                .map_err(|err| map_fdb_error("foundationdb healthcheck read", err))?;
            Ok::<(), StorageError>(())
        };

        match time::timeout(timeout, check).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(StorageError::internal(&format!(
                "foundationdb server is not reachable: {err}"
            ))),
            Err(_) => Err(StorageError::internal(&format!(
                "foundationdb server is not reachable (timed out after {}s)",
                timeout.as_secs()
            ))),
        }
    }

    pub fn connect_default() -> StorageResult<Self> {
        Self::connect(FoundationDbConfig::default())
    }

    pub fn from_database(
        config: FoundationDbConfig,
        database: Arc<Database>,
    ) -> StorageResult<Self> {
        validate_simulated_database_config(&config)?;
        Ok(Self {
            database,
            _network: FoundationDbNetworkOwnership::Simulated,
            config: Arc::new(config),
            runtime_partition_load_tracker: RuntimePartitionLoadTracker::default(),
        })
    }

    #[must_use]
    pub fn database(&self) -> Arc<Database> {
        Arc::clone(&self.database)
    }

    #[must_use]
    pub fn config(&self) -> Arc<FoundationDbConfig> {
        Arc::clone(&self.config)
    }

    pub async fn direct_audit_scan_prefix(
        &self,
        prefix: &[u8],
        limit: u32,
    ) -> StorageResult<RangeResult> {
        self.get_prefix(prefix, true, Some(limit.max(1)), true)
            .await
    }

    pub(crate) fn create_transaction(&self) -> StorageResult<Transaction> {
        let trx = self
            .database
            .create_trx()
            .map_err(|err| map_fdb_error("create FoundationDB transaction", err))?;
        if self.config.report_conflicting_keys {
            trx.set_option(options::TransactionOption::ReportConflictingKeys)
                .map_err(|err| map_fdb_error("enable conflict key reporting", err))?;
        }
        Ok(trx)
    }

    pub(crate) fn uses_grv_cache(&self, consistent_read: bool) -> bool {
        !consistent_read && self.config.cache_read_version_ms > 0
    }

    pub(crate) fn configure_read_transaction(
        &self,
        trx: &Transaction,
        debug_id: Option<&str>,
        consistent_read: bool,
    ) -> StorageResult<()> {
        Self::configure_transaction(trx, debug_id, consistent_read)?;
        if !self.uses_grv_cache(consistent_read) {
            return Ok(());
        }
        trx.set_option(options::TransactionOption::UseGrvCache)
            .map_err(|err| map_fdb_error("enable use_grv_cache", err))
    }

    pub(crate) async fn prepare_uncached_read_version_fdb(
        &self,
        trx: &Transaction,
        consistent_read: bool,
    ) -> Result<(), FdbError> {
        if self.uses_grv_cache(consistent_read) {
            return Ok(());
        }

        let started_at = Instant::now();
        trx.get_read_version().await?;
        metrics_facade::histogram!(FOUNDATIONDB_GET_READ_VERSION_LATENCY_MS_METRIC)
            .record(started_at.elapsed().as_secs_f64() * 1000.0);
        Ok(())
    }

    pub(crate) async fn retry_transaction_after_fdb_error(
        &self,
        trx: Transaction,
        operation: &'static str,
        scope: &'static str,
        attempt: u32,
        err: FdbError,
        candidate_keys: &[Vec<u8>],
    ) -> StorageResult<Transaction> {
        record_fdb_operation(operation, "retry", 1);
        let error_code = err.code();
        let retryable = err.is_retryable();
        let on_error_started = Instant::now();
        let retry_result = trx.on_error(err).await;
        record_fdb_operation_latency(operation, "on_error", on_error_started.elapsed());
        match retry_result {
            Ok(mut new_trx) => {
                self.log_conflict_details(
                    &new_trx,
                    operation,
                    attempt,
                    retryable,
                    error_code,
                    candidate_keys,
                )
                .await;
                new_trx.reset();
                Ok(new_trx)
            }
            Err(retry_err) => Err(map_fdb_error(scope, retry_err)),
        }
    }

    pub(crate) fn configure_transaction(
        trx: &Transaction,
        debug_id: Option<&str>,
        consistent_read: bool,
    ) -> StorageResult<()> {
        if let Some(debug_id) = debug_id {
            trx.set_option(options::TransactionOption::DebugTransactionIdentifier(
                debug_id.to_string(),
            ))
            .map_err(|err| map_fdb_error("set debug transaction identifier", err))?;
        }

        if !consistent_read {
            trx.set_option(options::TransactionOption::CausalReadRisky)
                .map_err(|err| map_fdb_error("set causal read option", err))?;
            trx.set_option(options::TransactionOption::ReadYourWritesDisable)
                .map_err(|err| map_fdb_error("disable read-your-writes", err))?;
            trx.set_option(options::TransactionOption::SnapshotRywDisable)
                .map_err(|err| map_fdb_error("disable snapshot read-your-writes", err))?;
        }

        Ok(())
    }

    pub(crate) fn prefix_bytes(prefix: Option<&Vec<u8>>, key: &[u8]) -> Vec<u8> {
        keyspace::prefix_bytes(prefix, key)
    }

    pub(crate) async fn commit_transaction(
        path: &'static str,
        trx: Transaction,
    ) -> Result<(), TransactionCommitError> {
        let started = Instant::now();
        let result = trx.commit().await;
        record_fdb_operation_latency(path, "commit", started.elapsed());
        result.map(|_| ())
    }

    pub(crate) fn strip_prefix<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        keyspace::strip_prefix(key, self.config.subspace_prefix.as_ref())
    }

    pub(crate) fn prefix_slice(&self, key: &[u8]) -> Vec<u8> {
        Self::prefix_bytes(self.config.subspace_prefix.as_ref(), key)
    }

    pub(crate) fn collect_transact_write_keys(
        prefix: Option<&Vec<u8>>,
        operations: &[TransactWriteOperation],
    ) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        for operation in operations {
            match operation {
                TransactWriteOperation::Put { key, .. }
                | TransactWriteOperation::Delete { key, .. }
                | TransactWriteOperation::Check { key, .. }
                | TransactWriteOperation::CheckValue { key, .. }
                | TransactWriteOperation::Update { key, .. } => {
                    keys.push(Self::prefix_bytes(prefix, key));
                }
                TransactWriteOperation::PutTemplate { template, .. } => {
                    if let Some(mut versioned) = template.foundationdb_key() {
                        if let Some(prefix_bytes) = prefix {
                            let mut composed = prefix_bytes.clone();
                            composed.extend_from_slice(&versioned);
                            adjust_versionstamp_offset(&mut composed, prefix_bytes.len());
                            versioned = composed;
                        }
                        keys.push(versioned);
                    } else {
                        let key = template.rocks_key();
                        keys.push(Self::prefix_bytes(prefix, &key));
                    }
                }
            }
        }
        keys
    }

    pub(crate) fn collect_unchecked_write_keys(
        prefix: Option<&Vec<u8>>,
        operations: &[DirectWriteOperation],
    ) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        for operation in operations {
            match operation {
                DirectWriteOperation::Put { key, .. }
                | DirectWriteOperation::Delete { key }
                | DirectWriteOperation::CheckValue { key, .. } => {
                    keys.push(Self::prefix_bytes(prefix, key));
                }
                DirectWriteOperation::DeleteRange {
                    start,
                    exclusive_end,
                } => {
                    keys.push(Self::prefix_bytes(prefix, start));
                    keys.push(Self::prefix_bytes(prefix, exclusive_end));
                }
                DirectWriteOperation::PutTemplate { template, .. } => {
                    if let Some(mut versioned) = template.foundationdb_key() {
                        if let Some(prefix_bytes) = prefix {
                            let mut composed = prefix_bytes.clone();
                            composed.extend_from_slice(&versioned);
                            adjust_versionstamp_offset(&mut composed, prefix_bytes.len());
                            versioned = composed;
                        }
                        keys.push(versioned);
                    } else {
                        let key = template.rocks_key();
                        keys.push(Self::prefix_bytes(prefix, &key));
                    }
                }
            }
        }
        keys
    }

    pub(crate) fn collect_transact_write_table_keys(
        prefix: Option<&Vec<u8>>,
        operations: &[TransactWriteTableOperation],
    ) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        for operation in operations {
            if let Ok(key) = table_operation_primary_key(operation) {
                keys.push(Self::prefix_bytes(prefix, &key));
            }
        }
        keys
    }

    pub(crate) async fn read_key_prefix(
        trx: &Transaction,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, FdbError> {
        let start = prefix.to_vec();
        let end = increment_bytes(prefix.to_vec());
        let mut option = RangeOption::from((start, end));
        option.limit = Some(limit);
        option.mode = options::StreamingMode::WantAll;

        let mut iteration = 1;
        let mut out = Vec::new();

        loop {
            let values = trx.get_range(&option, iteration, false).await?;
            for kv in &values {
                out.push((kv.key().to_vec(), kv.value().to_vec()));
                if out.len() >= limit {
                    return Ok(out);
                }
            }

            if let Some(next) = option.next_range(&values) {
                option = next;
                iteration += 1;
            } else {
                break;
            }
        }

        Ok(out)
    }
}
