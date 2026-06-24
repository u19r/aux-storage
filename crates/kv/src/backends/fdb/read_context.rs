use std::time::Instant;

use foundationdb::Transaction;
use futures_util::future::try_join_all;
use storage_types::StorageResult;

use super::{
    error::map_fdb_error,
    metrics::{record_fdb_operation_bytes, record_fdb_operation_latency, record_fdb_point_read},
    range_read::FoundationDbRangeReadOptions,
    store::FoundationDbKvStore,
};
use crate::sorted_kv_store::{RangeValuesResult, RawKey, SortedKvReadContext};

pub(super) struct FoundationDbReadContext {
    pub(super) store: FoundationDbKvStore,
    pub(super) trx: Transaction,
}

#[async_trait::async_trait]
impl SortedKvReadContext for FoundationDbReadContext {
    async fn get(&self, key: &[u8], _consistent_read: bool) -> StorageResult<Option<Vec<u8>>> {
        let prefixed_key = self.store.prefix_slice(key);

        let get_started = Instant::now();
        let value = self
            .trx
            .get(&prefixed_key, true)
            .await
            .map_err(|err| map_fdb_error("read context key", err))?;
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

        let prefix = self.store.config().subspace_prefix.clone();
        let futures = keys
            .iter()
            .map(|key| {
                let prefixed = FoundationDbKvStore::prefix_bytes(prefix.as_ref(), key);
                self.trx.get(&prefixed, true)
            })
            .collect::<Vec<_>>();

        let read_started = Instant::now();
        let results = try_join_all(futures)
            .await
            .map_err(|err| map_fdb_error("read context multi_get", err))?;
        record_fdb_operation_latency("read_context", "point_read_batch", read_started.elapsed());
        record_fdb_point_read("read_context", true, keys.len() as u64);
        record_fdb_operation_bytes(
            "read_context",
            "read_key",
            keys.iter()
                .map(|key| FoundationDbKvStore::prefix_bytes(prefix.as_ref(), key).len() as u64)
                .sum::<u64>(),
        );
        record_fdb_operation_bytes(
            "read_context",
            "read",
            keys.iter()
                .map(|key| key.len() as u64)
                .sum::<u64>()
                .saturating_add(
                    results
                        .iter()
                        .map(|value| value.as_ref().map_or(0, |bytes| bytes.len() as u64))
                        .sum::<u64>(),
                ),
        );

        Ok(results
            .into_iter()
            .map(|value| value.map(|bytes| bytes.to_vec()))
            .collect())
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
            )
            .await?;
        Ok(range.into_values_result())
    }
}
