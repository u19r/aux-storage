//! Exact canonical entries selected by retention in the caller's native
//! attempt.

use storage_types::{StorageResult, SystemStreamPosition, TableHistoryId};

pub struct StreamEntryAddress {
    pub history_id: TableHistoryId,
    pub position: SystemStreamPosition,
}

pub trait StreamEntryDeleteAttempt:
    infrastructure_provider::physical_attempt::PhysicalRecordAttempt
{
    fn delete_stream_entries(
        &mut self,
        rows: &[StreamEntryAddress],
    ) -> impl Future<Output = StorageResult<()>> + Send;
}
