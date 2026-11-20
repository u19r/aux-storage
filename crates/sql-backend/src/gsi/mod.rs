//! GSI build records, binding readiness, and the TTL index build. User GSIs
//! are key columns and partial indexes on the item relation (W6F N3).

mod catalog;
mod discovery;
mod fill;
mod read;
mod scan;
mod sql_statements;
mod transaction;
mod transition;

pub(crate) use catalog::{initialize_gsi, register_gsi_builds};
pub(crate) use discovery::{read_candidate_binding, scan_build_candidates};
pub(crate) use scan::read_scan_source_page;
pub(crate) use sql_statements::index_state_statement;
pub(crate) use transition::apply_gsi_transition;
