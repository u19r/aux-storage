use super::*;

impl FoundationDbKvStore {
    pub(crate) async fn atomic_read_modify_write_table_operation(
        &self,
        read_key: Vec<u8>,
        transform: AtomicTableWriteTransform,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<u8>> {
        let prefix = self.config.subspace_prefix.clone();
        let prefixed_read_key = Self::prefix_bytes(prefix.as_ref(), &read_key);
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        loop {
            attempt = attempt.saturating_add(1);
            Self::configure_transaction(&trx, None, true)?;
            trx.set_option(options::TransactionOption::ReadYourWritesDisable)
                .map_err(|error| map_fdb_error("disable atomic RMW read-your-writes", error))?;
            let current = trx
                .get(&prefixed_read_key, false)
                .await
                .map_err(|error| map_fdb_error("atomic RMW read item", error))?
                .map(|value| value.to_vec());
            let (operations, output) = match transform(current.as_deref())? {
                AtomicTableWriteDecision::NoWrite { output } => return Ok(output),
                AtomicTableWriteDecision::Write { operations, output } => (operations, output),
            };
            let stream_ids = self.build_stream_ids(&operations).await;
            match self
                .execute_transact_write_table_tx(
                    &trx,
                    &operations,
                    &stream_ids,
                    prefix.as_ref(),
                    immediate_gsi_consistency,
                )
                .await
            {
                Ok(execution) => match Self::commit_transaction("atomic_item_rmw", trx).await {
                    Ok(_) => {
                        self.record_ordered_log_writes(
                            &execution.ordered_log_writes,
                            u64::from(attempt.saturating_sub(1)),
                        );
                        return Ok(output);
                    }
                    Err(error) => match error.on_error().await {
                        Ok(mut new_trx) => {
                            new_trx.reset();
                            trx = new_trx;
                        }
                        Err(error) => {
                            return Err(map_fdb_error("atomic item RMW commit", error));
                        }
                    },
                },
                Err(FdbTableWriteExecutionError::Storage(error)) => return Err(error),
                Err(FdbTableWriteExecutionError::Fdb { scope, error }) => {
                    let candidate_keys =
                        Self::collect_transact_write_table_keys(prefix.as_ref(), &operations);
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "atomic_item_rmw",
                            scope,
                            attempt,
                            error,
                            &candidate_keys,
                        )
                        .await?;
                }
            }
        }
    }

    pub(crate) async fn transact_write_operation(
        &self,
        operations: Vec<TransactWriteOperation>,
    ) -> StorageResult<TransactWriteOutput> {
        if operations.is_empty() {
            return Ok(TransactWriteOutput::new(Vec::new()));
        }

        let prefix = self.config.subspace_prefix.clone();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        let mut set_count = 0u64;
        let mut clear_count = 0u64;
        let mut get_count = 0u64;
        let mut write_bytes = 0u64;
        let mut read_key_bytes = 0u64;
        let mut write_key_bytes = 0u64;
        let mut blind_writes = 0u64;
        let mut read_modify_writes = 0u64;
        for operation in &operations {
            match operation {
                TransactWriteOperation::Put {
                    key,
                    value,
                    condition,
                } => {
                    set_count = set_count.saturating_add(1);
                    write_bytes = write_bytes.saturating_add(value.len() as u64);
                    write_key_bytes = write_key_bytes
                        .saturating_add(Self::prefix_bytes(prefix.as_ref(), key).len() as u64);
                    if condition.is_some() {
                        get_count = get_count.saturating_add(1);
                        read_key_bytes = read_key_bytes
                            .saturating_add(Self::prefix_bytes(prefix.as_ref(), key).len() as u64);
                        read_modify_writes = read_modify_writes.saturating_add(1);
                    } else {
                        blind_writes = blind_writes.saturating_add(1);
                    }
                }
                TransactWriteOperation::PutTemplate {
                    template,
                    value,
                    condition,
                } => {
                    set_count = set_count.saturating_add(1);
                    write_bytes = write_bytes.saturating_add(value.len() as u64);
                    let key = template.rocks_key();
                    write_key_bytes = write_key_bytes
                        .saturating_add(Self::prefix_bytes(prefix.as_ref(), &key).len() as u64);
                    if condition.is_some() {
                        get_count = get_count.saturating_add(1);
                        read_key_bytes = read_key_bytes
                            .saturating_add(Self::prefix_bytes(prefix.as_ref(), &key).len() as u64);
                        read_modify_writes = read_modify_writes.saturating_add(1);
                    } else {
                        blind_writes = blind_writes.saturating_add(1);
                    }
                }
                TransactWriteOperation::Delete { key, condition } => {
                    clear_count = clear_count.saturating_add(1);
                    write_key_bytes = write_key_bytes
                        .saturating_add(Self::prefix_bytes(prefix.as_ref(), key).len() as u64);
                    if condition.is_some() {
                        get_count = get_count.saturating_add(1);
                        read_key_bytes = read_key_bytes
                            .saturating_add(Self::prefix_bytes(prefix.as_ref(), key).len() as u64);
                        read_modify_writes = read_modify_writes.saturating_add(1);
                    } else {
                        blind_writes = blind_writes.saturating_add(1);
                    }
                }
                TransactWriteOperation::Check { key, .. }
                | TransactWriteOperation::CheckValue { key, .. } => {
                    get_count = get_count.saturating_add(1);
                    read_key_bytes = read_key_bytes
                        .saturating_add(Self::prefix_bytes(prefix.as_ref(), key).len() as u64);
                }
                TransactWriteOperation::Update { key, condition, .. } => {
                    set_count = set_count.saturating_add(1);
                    get_count = get_count.saturating_add(1);
                    let prefixed_key = Self::prefix_bytes(prefix.as_ref(), key);
                    write_key_bytes = write_key_bytes.saturating_add(prefixed_key.len() as u64);
                    read_key_bytes = read_key_bytes.saturating_add(prefixed_key.len() as u64);
                    if condition.is_some() {
                        get_count = get_count.saturating_add(1);
                        read_key_bytes = read_key_bytes.saturating_add(prefixed_key.len() as u64);
                    }
                    read_modify_writes = read_modify_writes.saturating_add(1);
                }
            }
        }
        record_fdb_transaction_start("transact_write");

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            let execute_started = Instant::now();
            match self
                .execute_transact_write_tx(&trx, &operations, prefix.as_ref())
                .await
            {
                Ok((result, bindings, ordered_log_writes)) => {
                    let version_future = if bindings.is_empty() {
                        None
                    } else {
                        Some(trx.get_versionstamp())
                    };
                    record_fdb_operation_latency(
                        "transact_write",
                        "execute",
                        execute_started.elapsed(),
                    );
                    record_fdb_point_read("transact_write", false, get_count);
                    record_fdb_operation("transact_write", "set", set_count);
                    record_fdb_operation("transact_write", "clear", clear_count);
                    record_fdb_write_shape("transact_write", blind_writes, read_modify_writes);
                    record_fdb_operation_bytes("transact_write", "read_key", read_key_bytes);
                    record_fdb_operation_bytes("transact_write", "write", write_bytes);
                    record_fdb_operation_bytes("transact_write", "write_key", write_key_bytes);
                    record_fdb_operation("transact_write", "commit", 1);
                    match Self::commit_transaction("transact_write", trx).await {
                        Ok(_) => {
                            self.record_ordered_log_writes(
                                &ordered_log_writes,
                                u64::from(attempt.saturating_sub(1)),
                            );
                            let mut placeholder_versions = HashMap::new();
                            if let Some(fut) = version_future {
                                let committed = fut
                                    .await
                                    .map_err(|err| map_fdb_error("get versionstamp", err))?;
                                let data = committed.as_ref();
                                if data.len() != 10 {
                                    return Err(StorageError::internal(
                                        "unexpected versionstamp length",
                                    ));
                                }
                                let mut commit_bytes = [0u8; 10];
                                commit_bytes.copy_from_slice(data);
                                for (id, binding) in bindings {
                                    let mut bytes = [0u8; 12];
                                    bytes[..10].copy_from_slice(&commit_bytes);
                                    bytes[10..].copy_from_slice(&binding.user_bytes);
                                    placeholder_versions.insert(id, bytes);
                                }
                            }

                            return Ok(TransactWriteOutput {
                                items: result,
                                placeholder_versions,
                            });
                        }
                        Err(commit_err) => {
                            record_fdb_operation("transact_write", "retry", 1);
                            let error_code = commit_err.code();
                            let retryable = commit_err.is_retryable();
                            let on_error_started = Instant::now();
                            let retry_result = commit_err.on_error().await;
                            record_fdb_operation_latency(
                                "transact_write",
                                "on_error",
                                on_error_started.elapsed(),
                            );
                            match retry_result {
                                Ok(mut new_trx) => {
                                    let candidate_keys = Self::collect_transact_write_keys(
                                        prefix.as_ref(),
                                        &operations,
                                    );
                                    self.log_conflict_details(
                                        &new_trx,
                                        "transact_write",
                                        attempt,
                                        retryable,
                                        error_code,
                                        &candidate_keys,
                                    )
                                    .await;
                                    new_trx.reset();
                                    trx = new_trx;
                                }
                                Err(retry_err) => {
                                    return Err(map_fdb_error("transact_write commit", retry_err));
                                }
                            }
                        }
                    }
                }
                Err(storage_err) => return Err(storage_err),
            }
        }
    }

    pub(crate) async fn transact_write_unchecked_operation(
        &self,
        operations: Vec<DirectWriteOperation>,
    ) -> StorageResult<()> {
        if operations.is_empty() {
            return Ok(());
        }

        let prefix = self.config.subspace_prefix.clone();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        let mut set_count = 0u64;
        let mut clear_count = 0u64;
        let mut check_count = 0u64;
        let mut write_bytes = 0u64;
        let mut read_key_bytes = 0u64;
        let mut write_key_bytes = 0u64;
        let mut has_check = false;
        let mut range_clear_count = 0u64;
        for operation in &operations {
            match operation {
                DirectWriteOperation::Put { key, value } => {
                    set_count = set_count.saturating_add(1);
                    write_bytes = write_bytes.saturating_add(value.len() as u64);
                    write_key_bytes = write_key_bytes
                        .saturating_add(Self::prefix_bytes(prefix.as_ref(), key).len() as u64);
                }
                DirectWriteOperation::PutTemplate { template, value } => {
                    set_count = set_count.saturating_add(1);
                    write_bytes = write_bytes.saturating_add(value.len() as u64);
                    let key = template.rocks_key();
                    write_key_bytes = write_key_bytes
                        .saturating_add(Self::prefix_bytes(prefix.as_ref(), &key).len() as u64);
                }
                DirectWriteOperation::Delete { key } => {
                    clear_count = clear_count.saturating_add(1);
                    write_key_bytes = write_key_bytes
                        .saturating_add(Self::prefix_bytes(prefix.as_ref(), key).len() as u64);
                }
                DirectWriteOperation::DeleteRange {
                    start,
                    exclusive_end,
                } => {
                    clear_count = clear_count.saturating_add(1);
                    range_clear_count = range_clear_count.saturating_add(1);
                    write_key_bytes = write_key_bytes.saturating_add(
                        Self::prefix_bytes(prefix.as_ref(), start)
                            .len()
                            .saturating_add(
                                Self::prefix_bytes(prefix.as_ref(), exclusive_end).len(),
                            ) as u64,
                    );
                }
                DirectWriteOperation::CheckValue { key, .. } => {
                    check_count = check_count.saturating_add(1);
                    has_check = true;
                    read_key_bytes = read_key_bytes
                        .saturating_add(Self::prefix_bytes(prefix.as_ref(), key).len() as u64);
                }
            }
        }
        record_fdb_transaction_start("transact_write_unchecked");

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            let execute_started = Instant::now();
            let ordered_log_writes = match self
                .execute_transact_write_unchecked_tx(&trx, &operations, prefix.as_ref())
                .await
            {
                Ok(ordered_log_writes) => ordered_log_writes,
                Err(FdbTableWriteExecutionError::Storage(storage_err)) => {
                    return Err(storage_err);
                }
                Err(FdbTableWriteExecutionError::Fdb { scope, error }) => {
                    let candidate_keys =
                        Self::collect_unchecked_write_keys(prefix.as_ref(), &operations);
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "transact_write_unchecked",
                            scope,
                            attempt,
                            error,
                            &candidate_keys,
                        )
                        .await?;
                    continue;
                }
            };
            record_fdb_operation_latency(
                "transact_write_unchecked",
                "execute",
                execute_started.elapsed(),
            );

            record_fdb_point_read("transact_write_unchecked", false, check_count);
            record_fdb_operation("transact_write_unchecked", "set", set_count);
            record_fdb_operation("transact_write_unchecked", "clear", clear_count);
            record_fdb_operation("transact_write_unchecked", "range_clear", range_clear_count);
            let write_count = set_count.saturating_add(clear_count);
            if has_check {
                record_fdb_write_shape("transact_write_unchecked", 0, write_count);
            } else {
                record_fdb_write_shape("transact_write_unchecked", write_count, 0);
            }
            record_fdb_operation_bytes("transact_write_unchecked", "read_key", read_key_bytes);
            record_fdb_operation_bytes("transact_write_unchecked", "write", write_bytes);
            record_fdb_operation_bytes("transact_write_unchecked", "write_key", write_key_bytes);
            record_fdb_operation("transact_write_unchecked", "commit", 1);
            match Self::commit_transaction("transact_write_unchecked", trx).await {
                Ok(_) => {
                    self.record_ordered_log_writes(
                        &ordered_log_writes,
                        u64::from(attempt.saturating_sub(1)),
                    );
                    return Ok(());
                }
                Err(commit_err) => {
                    record_fdb_operation("transact_write_unchecked", "retry", 1);
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    let on_error_started = Instant::now();
                    let retry_result = commit_err.on_error().await;
                    record_fdb_operation_latency(
                        "transact_write_unchecked",
                        "on_error",
                        on_error_started.elapsed(),
                    );
                    match retry_result {
                        Ok(diagnostic_trx) => {
                            let candidate_keys =
                                Self::collect_unchecked_write_keys(prefix.as_ref(), &operations);
                            self.log_conflict_details(
                                &diagnostic_trx,
                                "transact_write_unchecked",
                                attempt,
                                retryable,
                                error_code,
                                &candidate_keys,
                            )
                            .await;
                            trx = self.create_transaction()?;
                        }
                        Err(retry_err) => {
                            return Err(map_fdb_error(
                                "transact_write_unchecked commit",
                                retry_err,
                            ));
                        }
                    }
                }
            }
        }
    }
}
