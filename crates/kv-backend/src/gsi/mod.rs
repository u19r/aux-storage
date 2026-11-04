//! GSI catalog records, protected lifecycle reads, and projection writes.

mod catalog;
mod discovery;
mod key_layout;
mod projection;
mod read;
mod scan;
mod transaction;
mod transition;
mod write;

pub(crate) use catalog::{delete_gsi_builds, register_gsi_builds};
pub(crate) use discovery::{read_candidate_binding, scan_build_candidates};
pub(crate) use key_layout::{
    gsi_name_lock_key, index_incarnation, ordered_projection_key, ordered_projection_prefix,
    ordered_projection_suffix,
};
pub(crate) use projection::{read_projection_page, read_projection_states};
pub(crate) use read::{
    decode_gsi_lifecycle, gsi_build_keys, read_activation_state, read_gsi_readiness,
};
pub(crate) use scan::read_scan_source_page;
pub(crate) use transition::apply_gsi_transition;
pub(crate) use write::{
    DecodedProjection, GsiWriteInput, PreparedGsiWrite, ProjectionChanges, prepare_gsi_writes,
};

#[cfg(test)]
mod read_tests;
