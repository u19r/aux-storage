use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use foundationdb::{KeySelector, RangeOption, Transaction};
use storage_types::StorageResult;

use crate::{
    backends::fdb::{
        error::map_fdb_error,
        mapped_range::{self, MappedRangeAttemptError},
        metrics::{
            record_fdb_operation_bytes, record_fdb_operation_latency, record_fdb_point_read,
        },
        range_read::FoundationDbRangeReadOptions,
        store::{FoundationDbKvStore, read_fdb_keys_concurrently},
    },
    sorted_kv_store::{RangeValuesResult, RawKey, SortedKvReadContext},
};

pub(super) struct FoundationDbReadContext {
    pub(super) store: FoundationDbKvStore,
    pub(super) trx: Transaction,
    pub(super) retryable_failure: Arc<AtomicBool>,
}

/// Execute one mapped-range transaction attempt.  The store/transaction are
/// borrowed so the owning provider can rebuild both after a retryable FDB
/// error without ever publishing a page from the failed attempt.
pub(super) async fn read_sequence_mapped_range_attempt(
    trx: &Transaction,
    request: &storage_provider::ReadSequenceMappedRangeRequest,
    physical_prefix: &[u8],
) -> Result<storage_provider::ReadSequenceMappedRangePage, MappedRangeAttemptError> {
    let mut range = mapped_range_options(request);
    mapped_range::validate_request(&range).map_err(MappedRangeAttemptError::Storage)?;
    let mut entries = Vec::new();
    let mut iteration = 1;
    loop {
        let page = mapped_range::get_mapped_range_attempt(
            trx,
            &range,
            request.mapper.as_deref(),
            iteration,
            physical_prefix,
        )
        .await?;
        let page_more = page.more;
        let last_parent = page.entries.last().map(|entry| entry.parent_key.clone());
        entries.extend(page.entries);
        if !page_more {
            return Ok(storage_provider::ReadSequenceMappedRangePage {
                entries,
                more: false,
            });
        }
        if request.mapper.is_some() {
            // The C API exposes one `more` bit for the whole mapped result and
            // no independent secondary continuation. Advancing only the
            // primary selector could drop an incomplete child range, so the
            // caller must discard this attempt and use the ordinary DAG.
            return Ok(storage_provider::ReadSequenceMappedRangePage {
                entries,
                more: true,
            });
        }
        let Some(last_parent) = last_parent else {
            return Err(MappedRangeAttemptError::Storage(
                storage_types::StorageError::internal(
                    "mapped range returned an empty continuation page",
                ),
            ));
        };
        range = mapped_range_continuation_options(request, last_parent);
        iteration += 1;
    }
}

pub(super) fn prefix_mapped_range_request(
    store: &FoundationDbKvStore,
    mut request: storage_provider::ReadSequenceMappedRangeRequest,
) -> storage_provider::ReadSequenceMappedRangeRequest {
    let prefix = store.physical_prefix();
    request.begin = FoundationDbKvStore::prefix_bytes(prefix, &request.begin);
    request.end = FoundationDbKvStore::prefix_bytes(prefix, &request.end);
    request.exclusive_start = request
        .exclusive_start
        .map(|key| FoundationDbKvStore::prefix_bytes(prefix, &key));
    request.mapper = request
        .mapper
        .map(|mapper| FoundationDbKvStore::prefix_bytes(prefix, &mapper));
    request
}

fn mapped_range_options(
    request: &storage_provider::ReadSequenceMappedRangeRequest,
) -> RangeOption<'_> {
    let begin = if request.reverse {
        KeySelector::first_greater_or_equal(request.begin.as_slice())
    } else if let Some(exclusive_start) = request.exclusive_start.as_deref() {
        KeySelector::first_greater_than(exclusive_start)
    } else {
        KeySelector::first_greater_or_equal(request.begin.as_slice())
    };
    let end = if request.reverse {
        request.exclusive_start.as_deref().map_or_else(
            || KeySelector::first_greater_or_equal(request.end.as_slice()),
            KeySelector::first_greater_or_equal,
        )
    } else {
        KeySelector::first_greater_or_equal(request.end.as_slice())
    };
    mapped_range_options_with_selectors(request, begin, end)
}

fn mapped_range_continuation_options(
    request: &storage_provider::ReadSequenceMappedRangeRequest,
    last_parent: Vec<u8>,
) -> RangeOption<'_> {
    let begin = if request.reverse {
        KeySelector::first_greater_or_equal(request.begin.as_slice())
    } else {
        KeySelector::first_greater_than(last_parent.clone())
    };
    let end = if request.reverse {
        KeySelector::first_greater_or_equal(last_parent)
    } else {
        KeySelector::first_greater_or_equal(request.end.as_slice())
    };
    mapped_range_options_with_selectors(request, begin, end)
}

fn mapped_range_options_with_selectors<'a>(
    request: &'a storage_provider::ReadSequenceMappedRangeRequest,
    begin: KeySelector<'a>,
    end: KeySelector<'a>,
) -> RangeOption<'a> {
    RangeOption {
        begin,
        end,
        limit: None,
        target_bytes: request.target_bytes as usize,
        mode: foundationdb::options::StreamingMode::WantAll,
        reverse: request.reverse,
        ..RangeOption::default()
    }
}

#[async_trait::async_trait]
impl SortedKvReadContext for FoundationDbReadContext {
    fn take_retryable_read_failure(&self) -> bool {
        self.retryable_failure.swap(false, Ordering::AcqRel)
    }

    async fn get(&self, key: &[u8], _consistent_read: bool) -> StorageResult<Option<Vec<u8>>> {
        let prefixed_key = self.store.prefix_slice(key);

        let get_started = Instant::now();
        let value = match self.trx.get(&prefixed_key, true).await {
            Ok(value) => value,
            Err(error) => {
                if error.is_retryable() {
                    self.retryable_failure.store(true, Ordering::Release);
                }
                return Err(map_fdb_error("read context key", error));
            }
        };
        record_fdb_operation_latency("read_context", "point_read", get_started.elapsed());
        record_fdb_point_read("read_context", true, 1);
        record_fdb_operation_bytes("read_context", "read_key", prefixed_key.len() as u64);
        record_fdb_operation_bytes(
            "read_context",
            "read",
            prefixed_key
                .len()
                .saturating_add(value.as_ref().map_or(0, |bytes| bytes.len())) as u64,
        );

        Ok(value.map(|bytes| bytes.to_vec()))
    }

    async fn multi_get(
        &self,
        keys: Vec<Vec<u8>>,
        _consistent_read: bool,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let prefix = self.store.physical_prefix();
        let prefixed_keys = keys
            .iter()
            .map(|key| FoundationDbKvStore::prefix_bytes(prefix, key))
            .collect::<Vec<_>>();
        let read_key_bytes = prefixed_keys
            .iter()
            .map(|key| key.len() as u64)
            .sum::<u64>();

        let read_started = Instant::now();
        let results = match read_fdb_keys_concurrently(&self.trx, prefixed_keys, true).await {
            Ok(results) => results,
            Err(error) => {
                if error.is_retryable() {
                    self.retryable_failure.store(true, Ordering::Release);
                }
                return Err(map_fdb_error("read context multi_get", error));
            }
        };
        record_multi_get_metrics(&keys, read_key_bytes, &results, read_started);

        Ok(results)
    }

    async fn get_range_values(
        &self,
        start: &[u8],
        exclusive_end: &[u8],
        limit: Option<u32>,
        page_token: Option<RawKey>,
        _consistent_read: bool,
    ) -> StorageResult<RangeValuesResult> {
        let page_bytes = page_token.map(|token| token.0);
        let range = self
            .store
            .read_range_with_transaction(
                &self.trx,
                start,
                exclusive_end,
                FoundationDbRangeReadOptions::read_context(limit, page_bytes),
                Some(&self.retryable_failure),
            )
            .await?;
        Ok(range.into_values_result())
    }
}

fn record_multi_get_metrics(
    keys: &[Vec<u8>],
    read_key_bytes: u64,
    results: &[Option<Vec<u8>>],
    started: Instant,
) {
    let read_bytes = keys
        .iter()
        .map(|key| key.len() as u64)
        .sum::<u64>()
        .saturating_add(
            results
                .iter()
                .map(|value| value.as_ref().map_or(0, |bytes| bytes.len() as u64))
                .sum::<u64>(),
        );
    record_fdb_operation_latency("read_context", "point_read_batch", started.elapsed());
    record_fdb_point_read("read_context", true, keys.len() as u64);
    record_fdb_operation_bytes("read_context", "read_key", read_key_bytes);
    record_fdb_operation_bytes("read_context", "read", read_bytes);
}
