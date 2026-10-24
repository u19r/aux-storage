//! GSI catalog records, protected lifecycle reads, and projection writes.

mod catalog;
mod discovery;
mod hold;
mod images;
mod key_layout;
mod projection;
mod read;
mod scan;
mod stream;
mod transaction;
mod transition;
mod tree_codec;
mod tree_page;
mod tree_read;
mod tree_write;
mod write;

pub(crate) use catalog::{delete_gsi_builds, register_gsi_builds};
pub(crate) use discovery::{read_candidate_holds, scan_build_candidates};
pub(crate) use hold::load_trim_protection_rows;
pub(crate) use images::read_referenced_histories;
pub(crate) use key_layout::{
    gsi_name_lock_key, index_incarnation, ordered_projection_key, ordered_projection_prefix,
    ordered_projection_suffix, projection_address,
};
pub(crate) use projection::read_projection_states;
pub(crate) use read::{
    decode_gsi_lifecycle, gsi_build_keys, read_activation_state, read_gsi_readiness,
};
pub(crate) use scan::read_scan_source_page;
pub(crate) use stream::read_stream_source_page;
pub(crate) use transition::apply_gsi_transition;
pub(crate) use tree_page::read_tree_page;
pub(crate) use tree_read::TreeChanges;
pub(crate) use write::{GsiWriteInput, HydratedProjection, PreparedGsiWrite, prepare_gsi_writes};

#[cfg(test)]
mod read_tests;
