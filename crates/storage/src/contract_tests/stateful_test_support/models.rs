use queue_provider::MessageId;
use storage_types::{
    IndexName, KeyAttributes, StreamItemId, SystemStreamScope, TableName, TimestampMillis,
};
use stream_provider::{BootstrapCursorName, SystemStreamLineage};

pub struct StatefulObservation {
    pub active: bool,
    pub visible_items: Vec<String>,
    pub effect_count: u64,
    pub progress_revision: u64,
    pub generation: u64,
}

#[derive(Clone)]
pub(super) struct ExpirationFixtureTarget {
    pub table_name: TableName,
    pub key: KeyAttributes,
}

#[derive(Clone)]
pub(super) struct GsiFixtureTarget {
    pub table_name: TableName,
    pub index_name: IndexName,
}

pub(super) struct StreamReclamationFixtureTarget {
    pub scope: SystemStreamScope,
    pub parent: SystemStreamLineage,
    pub cursor_name: BootstrapCursorName,
    /// Immutable physical history boundary authenticated by every held read.
    pub protected_boundary: StreamItemId,
    /// Mutable consumer position expected by the next held read.
    pub cursor_position: StreamItemId,
    pub cutoff_timestamp: TimestampMillis,
}

pub(super) struct QueueReclamationFixtureTarget {
    pub queue_url: String,
    pub message_id: MessageId,
}
