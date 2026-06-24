use std::time::Instant;

use foundationdb::{FdbError, KeySelector, RangeOption, Transaction, options};
use futures_util::TryStreamExt;
use storage_types::StorageResult;

use super::{
    error::map_fdb_error,
    metrics::{
        record_fdb_operation, record_fdb_operation_bytes, record_fdb_operation_latency,
        record_fdb_range_read, record_fdb_transaction_start,
    },
    store::FoundationDbKvStore,
};
use crate::{
    backends::common::{RangeKeyDecision, RangeScanSettings},
    sorted_kv_store::RangeResult,
};

pub(super) const DYNAMODB_RANGE_TARGET_BYTES: usize = 1024 * 1024;

struct FdbRangeReadAttempt {
    filtered: Vec<(Vec<u8>, Vec<u8>)>,
    backend_has_more: bool,
    entries_seen: u64,
    read_bytes: u64,
    elapsed: std::time::Duration,
}

pub(super) struct FoundationDbRangeReadOptions {
    pub(super) limit: Option<u32>,
    pub(super) page_token: Option<Vec<u8>>,
    pub(super) metrics_path: &'static str,
    pub(super) record_transaction_start: bool,
}

impl FoundationDbRangeReadOptions {
    pub(super) fn standalone(limit: Option<u32>, page_token: Option<Vec<u8>>) -> Self {
        Self {
            limit,
            page_token,
            metrics_path: "range",
            record_transaction_start: true,
        }
    }

    pub(super) fn read_context(limit: Option<u32>, page_token: Option<Vec<u8>>) -> Self {
        Self {
            limit,
            page_token,
            metrics_path: "read_context",
            record_transaction_start: false,
        }
    }
}

impl FoundationDbKvStore {
    pub(super) async fn read_range(
        &self,
        start: &[u8],
        exclusive_end: &[u8],
        limit: Option<u32>,
        page_token: Option<Vec<u8>>,
        consistent_read: bool,
    ) -> StorageResult<RangeResult> {
        self.read_range_with_retry(
            start,
            exclusive_end,
            FoundationDbRangeReadOptions::standalone(limit, page_token),
            consistent_read,
        )
        .await
    }

    async fn read_range_with_retry(
        &self,
        start: &[u8],
        exclusive_end: &[u8],
        options: FoundationDbRangeReadOptions,
        consistent_read: bool,
    ) -> StorageResult<RangeResult> {
        let FoundationDbRangeReadOptions {
            limit,
            page_token,
            metrics_path,
            record_transaction_start,
        } = options;
        let scan = RangeScanSettings::new(start, exclusive_end, limit, page_token)?;

        let (ordered_start, ordered_end) = scan.ordered_bounds();

        let begin_pref = if scan.forward() {
            match scan.page_token() {
                Some(token) if token >= ordered_start && token < ordered_end => {
                    KeySelector::first_greater_than(self.prefix_slice(token))
                }
                _ => KeySelector::first_greater_or_equal(self.prefix_slice(ordered_start)),
            }
        } else {
            KeySelector::first_greater_or_equal(self.prefix_slice(ordered_start))
        };

        let end_pref_ordered = if scan.forward() {
            KeySelector::first_greater_than(self.prefix_slice(ordered_end))
        } else {
            match scan.page_token() {
                Some(token) if token > ordered_start && token <= ordered_end => {
                    KeySelector::first_greater_or_equal(self.prefix_slice(token))
                }
                _ => KeySelector::first_greater_than(self.prefix_slice(ordered_end)),
            }
        };

        let candidate_keys = vec![begin_pref.key().to_vec(), end_pref_ordered.key().to_vec()];
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        if record_transaction_start {
            record_fdb_transaction_start(metrics_path);
        }

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
                        metrics_path,
                        "get FoundationDB read version",
                        attempt,
                        err,
                        &candidate_keys,
                    )
                    .await?;
                continue;
            }

            record_fdb_range_read(metrics_path, true, 1);
            record_fdb_operation_bytes(
                metrics_path,
                "read_key",
                begin_pref
                    .key()
                    .len()
                    .saturating_add(end_pref_ordered.key().len()) as u64,
            );

            let read_result = self
                .read_range_attempt(&trx, &scan, begin_pref.clone(), end_pref_ordered.clone())
                .await;

            match read_result {
                Ok(attempt_result) => {
                    record_fdb_operation_latency(
                        metrics_path,
                        "range_read",
                        attempt_result.elapsed,
                    );
                    record_fdb_operation(metrics_path, "range_entry", attempt_result.entries_seen);
                    record_fdb_operation_bytes(metrics_path, "read", attempt_result.read_bytes);
                    return Ok(
                        scan.finalize(attempt_result.filtered, attempt_result.backend_has_more)
                    );
                }
                Err(err) => {
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            metrics_path,
                            "scan range",
                            attempt,
                            err,
                            &candidate_keys,
                        )
                        .await?;
                }
            }
        }
    }

    pub(super) async fn read_range_with_transaction(
        &self,
        trx: &Transaction,
        start: &[u8],
        exclusive_end: &[u8],
        options: FoundationDbRangeReadOptions,
    ) -> StorageResult<RangeResult> {
        let FoundationDbRangeReadOptions {
            limit,
            page_token,
            metrics_path,
            record_transaction_start,
        } = options;
        let scan = RangeScanSettings::new(start, exclusive_end, limit, page_token)?;

        let (ordered_start, ordered_end) = scan.ordered_bounds();

        let begin_pref = if scan.forward() {
            match scan.page_token() {
                Some(token) if token >= ordered_start && token < ordered_end => {
                    KeySelector::first_greater_than(self.prefix_slice(token))
                }
                _ => KeySelector::first_greater_or_equal(self.prefix_slice(ordered_start)),
            }
        } else {
            KeySelector::first_greater_or_equal(self.prefix_slice(ordered_start))
        };

        let end_pref_ordered = if scan.forward() {
            KeySelector::first_greater_than(self.prefix_slice(ordered_end))
        } else {
            match scan.page_token() {
                Some(token) if token > ordered_start && token <= ordered_end => {
                    KeySelector::first_greater_or_equal(self.prefix_slice(token))
                }
                _ => KeySelector::first_greater_than(self.prefix_slice(ordered_end)),
            }
        };

        if record_transaction_start {
            record_fdb_transaction_start(metrics_path);
        }
        record_fdb_range_read(metrics_path, true, 1);
        record_fdb_operation_bytes(
            metrics_path,
            "read_key",
            begin_pref
                .key()
                .len()
                .saturating_add(end_pref_ordered.key().len()) as u64,
        );

        let attempt_result = self
            .read_range_attempt(trx, &scan, begin_pref, end_pref_ordered)
            .await
            .map_err(|err| map_fdb_error("scan range", err))?;

        record_fdb_operation_latency(metrics_path, "range_read", attempt_result.elapsed);
        record_fdb_operation(metrics_path, "range_entry", attempt_result.entries_seen);
        record_fdb_operation_bytes(metrics_path, "read", attempt_result.read_bytes);
        Ok(scan.finalize(attempt_result.filtered, attempt_result.backend_has_more))
    }

    async fn read_range_attempt(
        &self,
        trx: &Transaction,
        scan: &RangeScanSettings,
        begin_pref: KeySelector<'_>,
        end_pref_ordered: KeySelector<'_>,
    ) -> Result<FdbRangeReadAttempt, FdbError> {
        let option = dynamodb_range_option(
            begin_pref,
            end_pref_ordered,
            scan.fetch_limit(),
            !scan.forward(),
        );
        let mut stream = trx.get_ranges(option, true);
        let mut filtered = Vec::new();
        let mut backend_has_more = false;
        let fetch_limit = scan.fetch_limit();
        let mut entries_seen = 0u64;
        let mut read_bytes = 0u64;

        let range_started = Instant::now();
        loop {
            let values = match stream.try_next().await {
                Ok(Some(values)) => values,
                Ok(None) => break,
                Err(err) => return Err(err),
            };

            for kv in values.as_ref() {
                entries_seen = entries_seen.saturating_add(1);
                read_bytes = read_bytes
                    .saturating_add(kv.key().len().saturating_add(kv.value().len()) as u64);
                let original_key = self.strip_prefix(kv.key()).to_vec();
                let value = kv.value().to_vec();

                match scan.evaluate_key(&original_key) {
                    RangeKeyDecision::Include => {
                        filtered.push((original_key, value));
                        if filtered.len() >= fetch_limit {
                            backend_has_more = true;
                            break;
                        }
                    }
                    RangeKeyDecision::Skip => {}
                    RangeKeyDecision::Stop => {
                        backend_has_more = false;
                        break;
                    }
                }
            }

            if backend_has_more || filtered.len() >= fetch_limit {
                break;
            }
        }

        Ok(FdbRangeReadAttempt {
            filtered,
            backend_has_more,
            entries_seen,
            read_bytes,
            elapsed: range_started.elapsed(),
        })
    }
}

pub(super) fn dynamodb_range_option<'a>(
    begin: KeySelector<'a>,
    end: KeySelector<'a>,
    limit: usize,
    reverse: bool,
) -> RangeOption<'a> {
    let mut option = RangeOption::from((begin, end));
    option.limit = Some(limit);
    option.target_bytes = DYNAMODB_RANGE_TARGET_BYTES;
    option.reverse = reverse;
    option.mode = options::StreamingMode::WantAll;
    option
}
