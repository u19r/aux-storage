//! Provider-facing access to the common logical fence vocabulary.
//!
//! Fence identities are provider-neutral. The storage-provider crate owns
//! writer authorities and native key encoding, while storage-backfill owns
//! identity validation and canonical ordering shared by all providers.

pub(super) use storage_backfill::{
    canonical_fence_identities, validate_namespace, validate_routed_table_namespace,
};
