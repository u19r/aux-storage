//! Provider-neutral logical SystemStream pagination.
//!
//! The public storage API deliberately owns the wire representation of a
//! scope, checkpoint, and record position. This module owns the provider
//! boundary around those values: providers return a page with a server-issued
//! checkpoint and may expose their catalog for internal conformance tests,
//! while physical stream/table identifiers remain provider-private.

mod catalog;
mod checkpoint;
mod history;
#[cfg(test)]
mod history_tests;
mod logical_mutation;
#[cfg(test)]
mod logical_mutation_tests;
mod page;
mod page_preparation;
pub(crate) mod read;
mod snapshot_page;
mod split;
mod trim;
#[cfg(test)]
mod trim_tests;

pub use catalog::{SystemStreamLineage, SystemStreamLineageCatalog};
pub use checkpoint::{
    SystemStreamCheckpointIssue, SystemStreamCheckpointIssueInput, SystemStreamCheckpointLookup,
    SystemStreamCheckpointReceipt,
};
pub use history::{
    SystemHistoryReadAttempt, SystemStreamHistory, SystemStreamHistoryBounds,
    SystemStreamHistoryObservation, SystemStreamHistoryOrdering, SystemStreamHistoryPage,
    SystemStreamHistoryRead, SystemStreamHistoryRecord, SystemStreamWriteAllocation,
};
pub use logical_mutation::{
    DynamoMutationRoute, LogicalSystemStreamMutation, LogicalSystemStreamMutationInput,
    canonical_route_key,
};
pub use page::{SystemStreamIssuedCheckpoint, SystemStreamPage, SystemStreamPageRequest};
pub use split::{
    SystemStreamSplitChild, SystemStreamSplitDisposition, SystemStreamSplitOperationId,
    SystemStreamSplitReceipt, SystemStreamSplitRequest, SystemStreamSplitResult,
};
pub use trim::{
    SystemStreamTrimAttempt, SystemStreamTrimContinuation, SystemStreamTrimOperationId,
    SystemStreamTrimPageReceipt, SystemStreamTrimPageRequest, SystemStreamTrimPageResult,
    SystemStreamTrimRecord, execute_trim_page,
};
