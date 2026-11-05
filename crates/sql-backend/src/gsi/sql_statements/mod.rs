//! SQL statements for the GSI lowerer.

mod key;
mod read;
mod schema;
mod write;

pub(super) use key::BoundProjectionKey;
pub(super) use read::{
    candidate_binding_statement, candidate_statement, gsi_build_statement, gsi_status_statement,
    projection_state_statement, scan_source_statement, ttl_projection_state_statement,
};
pub(super) use schema::gsi_schema;
pub(super) use write::{
    activate_ttl_statement, insert_build_statement, transition_state_statement,
};

#[cfg(test)]
mod write_tests;
