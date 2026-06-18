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

pub mod errors;
pub mod gsi;
pub mod gsi_lag;
pub mod gsi_write;
pub mod jobs;
pub mod key_schema;
pub mod newtypes;
pub mod pagination;
pub mod projection;
pub mod provider_perf;
pub mod read;
pub mod retry;
pub mod tracing_utils;
pub mod ttl;
pub mod validation;

pub use errors::{ensure, err_internal, err_validation};
pub use gsi::{GSI_BACKFILL_JOB, GSI_UPDATE_JOB, GsiBackfillPhase, STREAM_TRIM_JOB, TTL_SWEEP_JOB};
pub use gsi_lag::{
    GSI_LAG_CRITICAL_LIMIT_MS, GSI_LAG_HARD_LIMIT_MS, GSI_LAG_SOFT_LIMIT_MS, GSI_LAG_TARGET_MS,
    GsiLagPolicy, GsiPropagationGovernor, GsiWritePressure, apply_gsi_write_pressure,
    emit_gsi_lag_metrics, gsi_write_throttled_error, lag_ms_from_created_at, observe_gsi_lag,
};
pub use gsi_write::{
    GsiKeyPart, GsiKeyParts, GsiWriteAction, key_parts, key_parts_to_map, plan_gsi_write_actions,
    require_key_parts,
};
pub use jobs::{DatabaseJobIntervals, GsiJobConfig, RegistersJobs, register_gsi_jobs};
pub use newtypes::{BackfillCursor, IdempotencyToken, JobIntervalMillis, PageToken};
pub use pagination::{DEFAULT_GENERIC_LIMIT, MAX_GENERIC_LIMIT, normalize_limit};
pub use projection::{apply_gsi_projection, apply_projection};
pub use read::{ReadOrigin, ReadPlan};
pub use tracing_utils::{record_limit, record_result, start_op_span};
pub use ttl::{TtlConfigRecord, TtlSweepLock};
pub use validation::validate_create_table;

#[cfg(test)]
mod gsi_write_tests;

#[cfg(test)]
mod gsi_write_alloc_tests;

#[cfg(test)]
mod key_schema_tests;

#[cfg(test)]
mod pagination_tests;

#[cfg(test)]
mod projection_tests;

#[cfg(test)]
mod ttl_tests;

#[cfg(test)]
mod validation_tests;

#[cfg(test)]
mod errors_tests;

#[cfg(test)]
mod gsi_tests;

#[cfg(test)]
mod gsi_lag_tests;

#[cfg(test)]
mod newtypes_tests;

#[cfg(test)]
mod provider_perf_tests;
