//! One protected KV batch for independent maintenance admission facts.

use serde::de::DeserializeOwned;
use storage_types::{
    StorageError, StorageResult,
    maintenance::{MaintenanceKind, TargetIncarnation, TaskProgress},
};

use super::{
    identity, key_layout,
    progress::{self, ProgressFence},
};
use crate::{KvAttempt, ReadProtection, constants::MAINTENANCE_IDENTITY_KEY};

pub(crate) async fn read_admission<A: KvAttempt, P: DeserializeOwned>(
    attempt: &mut A,
    kind: MaintenanceKind,
) -> StorageResult<(TargetIncarnation, Option<TaskProgress<P>>, ProgressFence)> {
    let values = attempt
        .get_batch(
            vec![
                MAINTENANCE_IDENTITY_KEY.to_vec(),
                key_layout::progress_key(kind),
            ],
            ReadProtection::Conflict,
        )
        .await?;
    let [identity, progress]: [_; 2] = values.try_into().map_err(|_| {
        StorageError::internal("KV maintenance admission returned the wrong number of values")
    })?;
    let identity = identity::decode_identity(identity)?
        .ok_or_else(|| StorageError::internal("KV maintenance identity is missing"))?;
    let progress = progress::decode_progress(progress, kind)?;
    let fence = ProgressFence::from_read(progress.as_ref());
    Ok((identity, progress, fence))
}
