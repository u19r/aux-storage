use std::time::Instant;

use storage_types::StorageResult;

use crate::{
    backends::fdb::{
        error::map_fdb_error,
        metrics::{
            record_fdb_operation_bytes, record_fdb_operation_latency, record_fdb_point_read,
            record_fdb_transaction_start,
        },
        read_context::FoundationDbReadContext,
        store::{FoundationDbKvStore, read_fdb_keys_concurrently},
    },
    sorted_kv_store::SortedKvReadContext,
};

impl FoundationDbKvStore {
    pub(crate) async fn begin_read_context_operation(
        &self,
    ) -> StorageResult<Box<dyn SortedKvReadContext>> {
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            self.configure_read_transaction(&trx, None, true)?;
            match self.prepare_uncached_read_version_fdb(&trx, true).await {
                Ok(()) => break,
                Err(error) if error.is_retryable() => {
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "read_context",
                            "read context read version",
                            attempt,
                            error,
                            0,
                        )
                        .await?;
                }
                Err(error) => {
                    return Err(map_fdb_error("read context read version", error));
                }
            }
        }
        record_fdb_transaction_start("read_context");
        Ok(Box::new(FoundationDbReadContext {
            store: self.clone(),
            trx,
            retryable_failure: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }))
    }

    pub(crate) async fn get_operation(
        &self,
        key: &[u8],
        consistent_read: bool,
    ) -> StorageResult<Option<Vec<u8>>> {
        let prefixed_key = self.prefix_slice(key);
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
                        1,
                    )
                    .await?;
                continue;
            }

            let get_started = Instant::now();
            let value = match trx.get(&prefixed_key, true).await {
                Ok(value) => value,
                Err(err) => {
                    trx = self
                        .retry_transaction_after_fdb_error(trx, "get", "read key", attempt, err, 1)
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
            let prefix = self.physical_prefix();
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
                        keys.len(),
                    )
                    .await?;
                continue;
            }

            let prefixed_keys = keys
                .iter()
                .map(|key| Self::prefix_bytes(prefix, key))
                .collect::<Vec<_>>();
            let prefixed_key_bytes = prefixed_keys
                .iter()
                .map(|key| key.len() as u64)
                .sum::<u64>();
            let read_started = Instant::now();
            match read_fdb_keys_concurrently(&trx, prefixed_keys, false).await {
                Ok(results) => {
                    record_fdb_operation_latency(
                        "multi_get",
                        "point_read_batch",
                        read_started.elapsed(),
                    );
                    record_fdb_point_read("multi_get", false, keys.len() as u64);
                    record_fdb_operation_bytes("multi_get", "read_key", prefixed_key_bytes);
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
                    return Ok(results);
                }
                Err(err) => {
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "multi_get",
                            "multi_get",
                            0,
                            err,
                            keys.len(),
                        )
                        .await?;
                }
            }
        }
    }
}
