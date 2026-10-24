//! SQL statements for the GSI lowerer.

mod key;
mod read;
mod schema;
mod write;

pub(super) use key::BoundProjectionKey;
pub(super) use read::{
    ProjectionStateQuery, build_for_hold_statement, candidate_binding_statement,
    candidate_statement, gsi_build_statement, gsi_status_statement, minimum_hold_statement,
    projection_descriptor_statement, projection_state_statement, read_hold_statement,
    referenced_histories_statement, scan_source_statement, ttl_projection_state_statement,
};
pub(super) use schema::gsi_schema;
pub(super) use write::{
    GsiUpsertSql, activate_ttl_statement, delete_history_hold_statement, history_hold_statement,
    insert_build_statement, render_gsi_upsert, store_history_hold_statement,
    transition_state_statement,
};

#[cfg(test)]
mod write_tests;
