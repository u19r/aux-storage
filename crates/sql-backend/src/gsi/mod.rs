//! GSI catalog records, protected lifecycle reads, and projection writes.

mod catalog;
mod discovery;
mod hold;
mod images;
mod projection;
mod read;
mod scan;
mod sql_statements;
mod stream;
mod transaction;
mod transition;
mod write;

pub(crate) use catalog::{initialize_gsi, register_gsi_builds};
pub(crate) use discovery::{read_candidate_holds, scan_build_candidates};
pub(crate) use hold::load_trim_protection_rows;
pub(crate) use images::read_referenced_histories;
pub(crate) use projection::read_projection_states;
pub(crate) use read::{
    decode_gsi_lifecycle, gsi_lifecycle_statement, read_activation_state, read_gsi_readiness,
};
pub(crate) use scan::read_scan_source_page;
pub(crate) use stream::read_stream_source_page;
pub(crate) use transition::apply_gsi_transition;
pub(crate) use write::{GsiWriteInput, PreparedGsiWrite, prepare_gsi_writes};

#[cfg(test)]
mod read_tests;

mod retained_projection;
pub(crate) use retained_projection::{ProjectionPageScope, hydrate_projection_page};
