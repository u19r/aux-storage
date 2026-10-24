//! Native source history and receiver-local first-seen canonical views.
pub mod ancestry_scope;
pub mod causal_read;
pub mod causal_stream;
pub mod checkpoint;
pub mod discovery;
pub mod encoded_response;
pub(crate) mod expiry_job;
pub mod hold_discovery;
pub mod portable_checkpoint;
pub mod read;
pub(crate) mod response_budget;
pub mod retention;
pub mod selection;
pub mod source_cut;
pub(crate) mod source_retention;
pub mod source_selection;
pub mod table_cut;
pub mod trim_authority;
pub mod view;

pub(crate) mod publication_scope;

pub(crate) mod source_floor;
mod source_floor_encoding;
mod source_floor_iteration;
