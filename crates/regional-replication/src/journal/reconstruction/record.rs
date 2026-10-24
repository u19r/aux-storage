use replication_types::{event::order::WinnerProvenance, identity::resource::ControlOperationId};
use serde::{Deserialize, Serialize};
use storage_types::KeyAttributes;

/// A derived verification index. Immutable baseline/event authority owns the
/// image; this record selects a version and never replaces retained history.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct ExpectedWinner {
    pub operation: ControlOperationId,
    pub key: KeyAttributes,
    pub provenance: WinnerProvenance,
}
