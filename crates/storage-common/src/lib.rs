//! Shared backend-agnostic helpers for storage providers.
//!
//! Goals:
//! - Reduce duplication between SQLite and KV storage providers.
//! - Keep logic pure & deterministic wherever possible.
//! - Provide safe, well-documented utilities for pagination, key schema
//!   validation, projection filtering, retry policies, and (eventually) update
//!   expression orchestration.
//!
//! This crate MUST NOT depend on backend-specific crates. It may depend on
//! `storage-types` and other pure parsing crates.
//!
//! This crate is an internal implementation crate, not a supported downstream
//! API.
#![doc(hidden)]

pub mod billing;
mod constants;
pub mod diagnostics;
pub mod durability;
mod errors;
mod gsi;
mod gsi_lag;
mod jobs;
pub mod key_template;
pub mod maintenance_continuation;
mod newtypes;
mod pagination;
pub mod partition;
pub mod point_results;
mod projection;
pub mod provider_perf;
pub mod retry;
pub mod telemetry;
mod tracing_utils;
pub mod ttl;

pub use billing::{
    STORAGE_BILLED_ITEM_OPS_TOTAL_METRIC, STORAGE_LOGICAL_ITEM_BYTES_TOTAL_METRIC, WriteCostTally,
    attr_map_payload_bytes, record_billed_item_ops, record_logical_item_bytes, record_query_result,
    record_read_cost, record_write_cost, serializable_payload_bytes, storage_payload_bytes,
    wire_items_payload_bytes,
};
pub use constants::{
    DEFAULT_GENERIC_LIMIT, DEFAULT_ORDERED_LOG_PARTITION_COUNT,
    DEFAULT_PARTITION_TARGET_BYTES_PER_SECOND, DEFAULT_PARTITION_TARGET_CONFLICTS_PER_WINDOW,
    DEFAULT_PARTITION_TARGET_OLDEST_VISIBLE_AGE_MS, DEFAULT_PARTITION_TARGET_WRITES_PER_SECOND,
    DEFAULT_QUERY_LIMIT, DEFAULT_SCAN_LIMIT, DEFAULT_STANDARD_QUEUE_PARTITION_COUNT,
    MAX_GENERIC_LIMIT, MAX_QUERY_LIMIT, MAX_SCAN_LIMIT, MAX_TTL_SWEEP_PAGE_LIMIT, MILLIS_PER_HOUR,
    PARTITION_LOAD_SAMPLE_RATE, PARTITION_PUBLISHER_MAX_BYTES, PARTITION_SAMPLE_MAX_AGE_MS,
    PARTITION_SAMPLE_PAGE_LIMIT, STREAM_TRIM_DELETE_BATCH_SIZE, STREAM_TRIM_READ_LIMIT,
    STREAM_TRIM_RETENTION_HOURS,
};
pub use errors::ensure;
pub use gsi::{GsiLifecycleAction, GsiTableUpdatePlan};
pub use gsi_lag::{GsiPropagationGovernor, apply_gsi_write_pressure};
pub use jobs::{DatabaseJobIntervals, JobRowState, StorageJob, job_row_state};
pub use newtypes::{IdempotencyToken, JobIntervalMillis};
pub use pagination::normalize_limit;
pub use projection::{apply_gsi_projection, gsi_projection_fields};
pub use tracing_utils::{record_limit, record_result, start_op_span};
pub use ttl::{
    PreparedTtlPut, TtlConfigActivation, TtlConfigRecord, TtlIndexKey, TtlIndexWrite,
    TtlIndexWriteBatch, TtlMaintenanceCursor, TtlPolicyGeneration, TtlSweepPageCommand,
    TtlSweepPageInput, TtlSweepPageResult, TtlSweepRequest, ttl_tracking_enabled,
};

#[cfg(test)]
mod pagination_tests;

#[cfg(test)]
mod billing_tests;

#[cfg(test)]
mod projection_tests;

#[cfg(test)]
mod retry_tests;

#[cfg(test)]
mod telemetry_tests;

#[cfg(test)]
mod ttl_tests;

#[cfg(test)]
mod errors_tests;

#[cfg(test)]
mod gsi_lag_tests;

#[cfg(test)]
mod newtypes_tests;

#[cfg(test)]
mod jobs_tests;

#[cfg(test)]
mod key_template_tests;

#[cfg(test)]
mod provider_perf_tests;

#[cfg(test)]
mod point_results_tests;
