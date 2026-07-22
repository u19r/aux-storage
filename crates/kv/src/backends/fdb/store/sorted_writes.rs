use super::*;

impl FoundationDbKvStore {
    pub(crate) async fn put_operation(
        &self,
        key: &[u8],
        value: &[u8],
        condition: Option<Condition>,
    ) -> StorageResult<()> {
        let prefix = self.config.subspace_prefix.clone();
        let key_bytes = key.to_vec();
        let value_bytes = value.to_vec();
        let condition = condition.clone();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        let mut maybe_committed = false;
        record_fdb_transaction_start("put");

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            let prefixed_key = Self::prefix_bytes(prefix.as_ref(), &key_bytes);

            if let Some(condition) = &condition {
                let current = trx
                    .get(&prefixed_key, false)
                    .await
                    .map_err(|err| map_fdb_error("load key for conditional put", err))?;
                record_fdb_point_read("put", false, 1);
                record_fdb_operation_bytes("put", "read_key", prefixed_key.len() as u64);
                record_fdb_operation_bytes(
                    "put",
                    "read",
                    prefixed_key
                        .len()
                        .saturating_add(current.as_ref().map_or(0, |bytes| bytes.len()))
                        as u64,
                );

                if !evaluate_condition_bytes(current.as_deref(), condition) {
                    if maybe_committed {
                        return Err(StorageError::internal(
                            "maybe_committed: conditional put retry observed condition failure \
                             after a maybe-committed commit",
                        ));
                    }
                    return Err(StorageEnum::TransactionCanceled {
                        reasons: vec!["ConditionalCheckFailed".to_string()],
                    }
                    .into());
                }
            }

            trx.set(&prefixed_key, &value_bytes);
            record_fdb_operation("put", "set", 1);
            if condition.is_some() {
                record_fdb_write_shape("put", 0, 1);
            } else {
                record_fdb_write_shape("put", 1, 0);
            }
            record_fdb_operation_bytes("put", "write", value_bytes.len() as u64);
            record_fdb_operation_bytes("put", "write_key", prefixed_key.len() as u64);
            record_fdb_operation("put", "commit", 1);

            match trx.commit().await {
                Ok(_) => return Ok(()),
                Err(commit_err) => {
                    record_fdb_operation("put", "retry", 1);
                    maybe_committed |= commit_err.is_maybe_committed();
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    match commit_err.on_error().await {
                        Ok(mut new_trx) => {
                            let candidate_keys =
                                vec![Self::prefix_bytes(prefix.as_ref(), &key_bytes)];
                            self.log_conflict_details(
                                &new_trx,
                                "put",
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
                            return Err(map_fdb_error("put commit", retry_err));
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn delete_operation(&self, key: &[u8]) -> StorageResult<()> {
        let prefix = self.config.subspace_prefix.clone();
        let key_bytes = key.to_vec();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        record_fdb_transaction_start("delete");

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            let prefixed_key = Self::prefix_bytes(prefix.as_ref(), &key_bytes);
            trx.clear(&prefixed_key);
            record_fdb_operation("delete", "clear", 1);
            record_fdb_write_shape("delete", 1, 0);
            record_fdb_operation_bytes("delete", "write_key", prefixed_key.len() as u64);
            record_fdb_operation("delete", "commit", 1);

            match trx.commit().await {
                Ok(_) => return Ok(()),
                Err(commit_err) => {
                    record_fdb_operation("delete", "retry", 1);
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    match commit_err.on_error().await {
                        Ok(mut new_trx) => {
                            let candidate_keys =
                                vec![Self::prefix_bytes(prefix.as_ref(), &key_bytes)];
                            self.log_conflict_details(
                                &new_trx,
                                "delete",
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
                            return Err(map_fdb_error("delete commit", retry_err));
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn delete_prefix_operation(&self, prefix: Vec<u8>) -> StorageResult<()> {
        let start = self.prefix_slice(&prefix);
        let end = increment_bytes(start.clone());

        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;
            trx.clear_range(&start, &end);

            match trx.commit().await {
                Ok(_) => return Ok(()),
                Err(commit_err) => {
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    match commit_err.on_error().await {
                        Ok(mut new_trx) => {
                            let candidate_keys = vec![start.clone(), end.clone()];
                            self.log_conflict_details(
                                &new_trx,
                                "delete_prefix",
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
                            return Err(map_fdb_error("delete_prefix commit", retry_err));
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn get_range_operation(
        &self,
        start: &[u8],
        exclusive_end: &[u8],
        limit: Option<u32>,
        page_token: Option<impl SerializesToKey + Send + Sync>,
        consistent_read: bool,
    ) -> StorageResult<RangeResult> {
        let page_bytes = if let Some(token) = page_token {
            Some(token.serialize_to_bytes()?)
        } else {
            None
        };

        self.read_range(start, exclusive_end, limit, page_bytes, consistent_read)
            .await
    }
}
