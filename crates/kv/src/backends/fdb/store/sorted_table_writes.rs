use std::time::Instant;

use foundationdb::options;
#[cfg(test)]
use storage_common::provider_perf;
use storage_types::StorageResult;

use crate::{
    backends::fdb::{
        error::map_fdb_error,
        metrics::{record_fdb_operation, record_fdb_operation_latency},
        store::{FdbTableWriteExecutionError, FoundationDbKvStore},
    },
    sorted_kv_store::{BatchItem, OldNewItems, TransactWriteTableOperation},
};

impl FoundationDbKvStore {
    pub(crate) async fn transact_write_table_operation(
        &self,
        operations: Vec<TransactWriteTableOperation>,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<OldNewItems>> {
        if operations.is_empty() {
            return Ok(Vec::new());
        }

        let stream_ids = self.build_stream_ids(&operations).await;
        let prefix = self.config.subspace_prefix.clone();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            #[cfg(test)]
            provider_perf::record_amount("foundationdb", "table_write_attempt", 1);
            Self::configure_transaction(&trx, None, true)?;
            trx.set_option(options::TransactionOption::ReadYourWritesDisable)
                .map_err(|err| map_fdb_error("disable table-write read-your-writes", err))?;

            let execute_started = Instant::now();
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
                Ok(execution) => {
                    let execute_elapsed = execute_started.elapsed();
                    let execute_ms = execute_elapsed.as_secs_f64() * 1000.0;
                    record_fdb_operation_latency(
                        "transact_write_table",
                        "execute",
                        execute_elapsed,
                    );
                    record_fdb_operation("transact_write_table", "commit", 1);
                    let commit_started = Instant::now();
                    match Self::commit_transaction("transact_write_table", trx).await {
                        Ok(_) => {
                            let commit_elapsed = commit_started.elapsed();
                            #[cfg(test)]
                            provider_perf::record(
                                "foundationdb",
                                "table_write_commit",
                                commit_elapsed,
                            );
                            tracing::debug!(
                                attempt,
                                operation_count = operations.len(),
                                execute_ms,
                                commit_ms = commit_elapsed.as_secs_f64() * 1000.0,
                                ordered_log_write_count = execution.ordered_log_writes.len(),
                                "foundationdb transact_write_table committed"
                            );
                            self.record_ordered_log_writes(
                                &execution.ordered_log_writes,
                                u64::from(attempt.saturating_sub(1)),
                            );
                            return Ok(execution.results);
                        }
                        Err(commit_err) => {
                            let commit_ms = commit_started.elapsed().as_secs_f64() * 1000.0;
                            record_fdb_operation("transact_write_table", "retry", 1);
                            let error_code = commit_err.code();
                            let retryable = commit_err.is_retryable();
                            #[cfg(test)]
                            {
                                provider_perf::record_amount(
                                    "foundationdb",
                                    "table_write_commit_retry",
                                    1,
                                );
                                if retryable {
                                    provider_perf::record_amount(
                                        "foundationdb",
                                        "table_write_commit_retryable",
                                        1,
                                    );
                                } else {
                                    provider_perf::record_amount(
                                        "foundationdb",
                                        "table_write_commit_non_retryable",
                                        1,
                                    );
                                }
                            }
                            tracing::debug!(
                                attempt,
                                operation_count = operations.len(),
                                execute_ms,
                                commit_ms,
                                error_code,
                                retryable,
                                "foundationdb transact_write_table commit retry"
                            );
                            let on_error_started = Instant::now();
                            let retry_result = commit_err.on_error().await;
                            record_fdb_operation_latency(
                                "transact_write_table",
                                "on_error",
                                on_error_started.elapsed(),
                            );
                            match retry_result {
                                Ok(mut new_trx) => {
                                    let candidate_keys = Self::collect_transact_write_table_keys(
                                        prefix.as_ref(),
                                        &operations,
                                    );
                                    self.log_conflict_details(
                                        &new_trx,
                                        "transact_write_table",
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
                                    return Err(map_fdb_error(
                                        "transact_write_table commit",
                                        retry_err,
                                    ));
                                }
                            }
                        }
                    }
                }
                Err(FdbTableWriteExecutionError::Storage(storage_err)) => {
                    return Err(storage_err);
                }
                Err(FdbTableWriteExecutionError::Fdb { scope, error }) => {
                    let candidate_keys =
                        Self::collect_transact_write_table_keys(prefix.as_ref(), &operations);
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "transact_write_table",
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

    pub(crate) async fn batch_write_operation(&self, items: Vec<BatchItem>) -> StorageResult<()> {
        if items.is_empty() {
            return Ok(());
        }

        let prefix = self.config.subspace_prefix.clone();
        let prefixed_items: Vec<BatchItem> = items
            .into_iter()
            .map(|item| BatchItem {
                key: Self::prefix_bytes(prefix.as_ref(), &item.key),
                value: item.value,
            })
            .collect();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            for item in &prefixed_items {
                match &item.value {
                    Some(value) => trx.set(&item.key, value),
                    None => trx.clear(&item.key),
                }
            }

            match trx.commit().await {
                Ok(_) => return Ok(()),
                Err(commit_err) => {
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    match commit_err.on_error().await {
                        Ok(mut new_trx) => {
                            let candidate_keys: Vec<Vec<u8>> =
                                prefixed_items.iter().map(|item| item.key.clone()).collect();
                            self.log_conflict_details(
                                &new_trx,
                                "batch_write",
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
                            return Err(map_fdb_error("batch_write commit", retry_err));
                        }
                    }
                }
            }
        }
    }
}
