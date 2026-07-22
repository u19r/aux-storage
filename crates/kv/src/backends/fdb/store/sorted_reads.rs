use super::*;

impl FoundationDbKvStore {
    pub(crate) async fn begin_read_context_operation(
        &self,
    ) -> StorageResult<Box<dyn SortedKvReadContext>> {
        let trx = self.create_transaction()?;
        self.configure_read_transaction(&trx, None, true)?;
        self.prepare_uncached_read_version_fdb(&trx, true)
            .await
            .map_err(|err| map_fdb_error("read context read version", err))?;
        record_fdb_transaction_start("read_context");
        Ok(Box::new(FoundationDbReadContext {
            store: self.clone(),
            trx,
        }))
    }

    pub(crate) async fn get_operation(
        &self,
        key: &[u8],
        consistent_read: bool,
    ) -> StorageResult<Option<Vec<u8>>> {
        let prefixed_key = self.prefix_slice(key);
        let candidate_keys = vec![prefixed_key.to_vec()];
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        record_fdb_transaction_start("get");

        loop {
            attempt += 1;
            self.configure_read_transaction(&trx, None, consistent_read)?;
            if let Err(err) = self
                .prepare_uncached_read_version_fdb(&trx, consistent_read)
                .await
            {
                trx = self
                    .retry_transaction_after_fdb_error(
                        trx,
                        "get",
                        "get read version",
                        attempt,
                        err,
                        &candidate_keys,
                    )
                    .await?;
                continue;
            }

            let get_started = Instant::now();
            let value = match trx.get(&prefixed_key, true).await {
                Ok(value) => value,
                Err(err) => {
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "get",
                            "read key",
                            attempt,
                            err,
                            &candidate_keys,
                        )
                        .await?;
                    continue;
                }
            };
            record_fdb_operation_latency("get", "point_read", get_started.elapsed());
            record_fdb_point_read("get", true, 1);
            record_fdb_operation_bytes("get", "read_key", prefixed_key.len() as u64);
            record_fdb_operation_bytes(
                "get",
                "read",
                prefixed_key
                    .len()
                    .saturating_add(value.as_ref().map_or(0, |bytes| bytes.len()))
                    as u64,
            );

            return Ok(value.map(|bytes| bytes.to_vec()));
        }
    }

    pub(crate) async fn multi_get_operation(
        &self,
        keys: Vec<Vec<u8>>,
        consistent_read: bool,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut trx = self.create_transaction()?;
        record_fdb_transaction_start("multi_get");
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            self.configure_read_transaction(&trx, None, consistent_read)?;
            let prefix = self.config.subspace_prefix.clone();
            let candidate_keys = keys
                .iter()
                .map(|key| Self::prefix_bytes(prefix.as_ref(), key))
                .collect::<Vec<_>>();
            if let Err(err) = self
                .prepare_uncached_read_version_fdb(&trx, consistent_read)
                .await
            {
                trx = self
                    .retry_transaction_after_fdb_error(
                        trx,
                        "multi_get",
                        "multi_get read version",
                        attempt,
                        err,
                        &candidate_keys,
                    )
                    .await?;
                continue;
            }

            let prefixed_keys = keys
                .iter()
                .map(|key| Self::prefix_bytes(prefix.as_ref(), key))
                .collect::<Vec<_>>();
            let read_started = Instant::now();
            match read_fdb_keys_sequential(&trx, &prefixed_keys, false).await {
                Ok(results) => {
                    record_fdb_operation_latency(
                        "multi_get",
                        "point_read_batch",
                        read_started.elapsed(),
                    );
                    record_fdb_point_read("multi_get", false, keys.len() as u64);
                    record_fdb_operation_bytes(
                        "multi_get",
                        "read_key",
                        prefixed_keys
                            .iter()
                            .map(|key| key.len() as u64)
                            .sum::<u64>(),
                    );
                    record_fdb_operation_bytes(
                        "multi_get",
                        "read",
                        keys.iter()
                            .map(|key| key.len() as u64)
                            .sum::<u64>()
                            .saturating_add(
                                results
                                    .iter()
                                    .map(|value| {
                                        value.as_ref().map_or(0, |bytes| bytes.len() as u64)
                                    })
                                    .sum::<u64>(),
                            ),
                    );
                    return Ok(results
                        .into_iter()
                        .map(|value| value.map(|bytes| bytes.to_vec()))
                        .collect());
                }
                Err(err) => {
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "multi_get",
                            "multi_get",
                            0,
                            err,
                            &candidate_keys,
                        )
                        .await?;
                }
            }
        }
    }
}
