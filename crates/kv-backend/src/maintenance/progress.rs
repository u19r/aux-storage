//! Protected task progress shared by KV maintenance workflows.

use serde::de::DeserializeOwned;
use storage_provider::maintenance::ProgressWrite;
use storage_types::{
    StorageResult,
    maintenance::{MaintenanceKind, TaskProgress},
};

use super::key_layout::progress_key;
use crate::{KvAttempt, KvMutation};

pub(super) fn decode_progress<P: DeserializeOwned>(
    value: Option<Vec<u8>>,
    kind: MaintenanceKind,
) -> StorageResult<Option<TaskProgress<P>>> {
    value
        .map(|bytes| storage_provider::maintenance::decode_task_progress(&bytes, kind))
        .transpose()
}

/// Stage the checked progress write. Admission read this key with conflict
/// protection and `ProgressWrite::new` compared that read; both native
/// drivers reject a concurrent change at commit, so rereading cannot
/// strengthen the fence and would add a dependent network round on
/// FoundationDB.
pub(crate) fn store_progress<A: KvAttempt>(
    attempt: &mut A,
    write: ProgressWrite,
) -> StorageResult<()> {
    attempt.stage(vec![KvMutation::PutUnchunked {
        key: progress_key(write.kind),
        value: write.bytes,
    }])
}
