//! Test-only fixtures and canonical audit evidence for storage profiles.
//!
//! This crate is a dev-dependency of provider crates. It supplies
//! representative fixture data and deterministic receipt serialization without
//! adding either to a provider's production surface.

mod audit;
mod canonical_json;
mod constants;
mod logical_profile;
mod logical_profile_shape;

pub use audit::{
    AuditRow, DigestEvidence, ExpectedObserved, blocked_environment_payload, canonical_digest,
    canonical_value, digest_rows, emit_audit_record, expected_observed,
};
pub use constants::{
    LOGICAL_PROFILE_INDEXES, LOGICAL_PROFILE_ITEMS, PROFILE_PAYLOAD_BYTES, VISIBLE_ATTRIBUTES,
};
pub use logical_profile::{
    logical_profile_batches, logical_profile_expected_index_rows, logical_profile_index_key,
    logical_profile_request,
};
pub use logical_profile_shape::{
    canonical_key as logical_profile_canonical_key, item as logical_profile_item_with_shape,
    key as logical_profile_key_with_shape, request as logical_profile_request_with_shape,
};
