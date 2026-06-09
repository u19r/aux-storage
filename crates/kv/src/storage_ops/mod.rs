mod constants;
mod gsi;
pub(crate) mod imports;
mod logical_backfill;
mod logical_backfill_domains;
mod logical_backfill_records;
mod provider_impl;
mod query;
mod resolved_sync_apply;
pub(crate) mod stream_duration;
mod write_helpers;

pub(crate) use gsi::{GsiBackfillJob, GsiUpdateJob};
#[cfg(test)]
pub(crate) use provider_impl::{
    TransactConditionBindingCacheEntry, cached_transact_condition_binding,
    encode_requests_to_write_requests, normalize_conditional_transaction_error,
    normalized_attribute_map_for_write, normalized_wire_item_for_write,
    project_wire_item_table_key_and_ttl, ttl_index_direct_operations_for_wire_items,
    ttl_tracking_enabled, wire_item_key_token_from_item_key,
};
pub(crate) use provider_impl::{
    compute_items_bytes, decode_wire_item_from_storage_bytes, encode_wire_item_storage_bytes,
    now_ms_u64, record_provider_stage, record_query_result, record_read, record_write,
    should_log_job,
};
pub(crate) use write_helpers::{key_schema_for_gsi, project_gsi_item};

#[cfg(test)]
mod conditional_error_tests;
#[cfg(all(test, feature = "rocksdb-backend"))]
mod delete_item_tests;
#[cfg(all(
    test,
    any(feature = "rocksdb-backend", feature = "foundationdb-backend")
))]
mod logical_backfill_empty_domain_tests;
#[cfg(all(
    test,
    any(feature = "rocksdb-backend", feature = "foundationdb-backend")
))]
mod logical_backfill_tests;
#[cfg(test)]
mod provider_impl_tests;
#[cfg(test)]
mod provider_tests;
#[cfg(all(test, feature = "rocksdb-backend"))]
mod put_item_tests;
#[cfg(test)]
mod quint_sync_committed_stream_id_tests;
#[cfg(all(
    test,
    any(feature = "rocksdb-backend", feature = "foundationdb-backend")
))]
mod resolved_sync_apply_tests;
#[cfg(all(
    test,
    any(feature = "rocksdb-backend", feature = "foundationdb-backend")
))]
mod stream_duration_perf_tests;
#[cfg(test)]
mod stream_duration_tests;
#[cfg(all(test, feature = "rocksdb-backend"))]
mod update_item_tests;
