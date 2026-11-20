//! SQL statements for the GSI lowerer.

mod fill;
mod read;
mod schema;
mod write;

pub(crate) use fill::index_state_statement;
pub(super) use fill::{fill_row_statement, native_transition_statement, ttl_metadata_statement};
pub(super) use read::{
    ScanSourceStatement, candidate_binding_statement, candidate_statement, render_source_scan,
    scan_source_statement,
};
pub(super) use schema::gsi_schema;
pub(super) use write::{
    activate_ttl_statement, insert_build_statement, transition_state_statement,
};

#[cfg(test)]
mod write_tests;
