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
mod jobs;
pub mod maintenance_continuation;
pub mod native_admission;
pub mod native_attempt_lifecycle;
pub mod native_observer;
pub mod native_outcome;
pub mod native_supervision;
mod newtypes;
mod pagination;
pub mod partition;
pub mod point_results;
mod projection;
pub mod retry;
pub mod telemetry;
pub mod ttl;
pub mod writer_occupancy;

pub use billing::{
    WriteCostTally, attr_map_payload_bytes, record_read_cost, record_write_cost,
    serializable_payload_bytes, storage_payload_bytes, wire_items_payload_bytes,
};
pub use constants::{
    DEFAULT_ORDERED_LOG_PARTITION_COUNT, DEFAULT_PARTITION_TARGET_BYTES_PER_SECOND,
    DEFAULT_PARTITION_TARGET_CONFLICTS_PER_WINDOW, DEFAULT_PARTITION_TARGET_OLDEST_VISIBLE_AGE_MS,
    DEFAULT_PARTITION_TARGET_WRITES_PER_SECOND, MAX_QUERY_LIMIT, MAX_SCAN_LIMIT,
    MAX_TTL_SWEEP_PAGE_LIMIT, PARTITION_LOAD_SAMPLE_RATE, PARTITION_PUBLISHER_MAX_BYTES,
    PARTITION_SAMPLE_MAX_AGE_MS, PARTITION_SAMPLE_PAGE_LIMIT,
};
pub use errors::ensure;
pub use gsi::{GsiLifecycleAction, GsiTableUpdatePlan};
pub use jobs::{DatabaseJobIntervals, StorageJob};
pub use newtypes::{IdempotencyToken, JobIntervalMillis};
pub use pagination::{normalize_limit, normalize_query_limit};
pub use projection::{apply_gsi_projection, gsi_projection_fields};
pub use ttl::{
    PreparedTtlPut, TtlConfigActivation, TtlConfigRecord, TtlIndexKey, TtlIndexWrite,
    TtlIndexWriteBatch, TtlMaintenanceCursor, TtlPolicyGeneration, TtlSweepPageCommand,
    TtlSweepPageInput, TtlSweepPageResult, TtlSweepPolicy, TtlSweepRequest, ttl_tracking_enabled,
};

#[cfg(test)]
mod pagination_tests;

#[cfg(test)]
mod durability_tests;

#[cfg(test)]
mod native_admission_tests;

#[cfg(test)]
mod native_attempt_lifecycle_tests;

#[cfg(test)]
mod native_observer_tests;

#[cfg(test)]
mod native_outcome_tests;

#[cfg(test)]
mod native_supervision_tests;

#[cfg(test)]
mod quint_durability_tests;

#[cfg(test)]
mod billing_tests;

#[cfg(test)]
mod projection_tests;

#[cfg(test)]
mod retry_tests;

#[cfg(test)]
mod ttl_tests;

#[cfg(test)]
mod errors_tests;

#[cfg(test)]
mod jobs_tests;

#[cfg(test)]
mod attempt_deadline_simulation_tests;
#[cfg(test)]
mod point_results_tests;
#[cfg(test)]
mod retry_one_deadline_tests;
