//! One shared settlement for retention-marker finishes; backends only
//! compare-and-delete markers and store the states that follow.

use storage_types::{MissExpectation, StorageError, StorageResult, storage_serde};

use super::{
    StreamRetentionFinish, StreamRetentionFinishRecord, StreamRetentionPort, StreamTrimDueMarker,
    StreamTrimScope, StreamTrimStateRecord,
};

/// One retention-marker finish, checked and encoded once for the physical port.
pub struct PreparedRetentionFinish {
    pub marker: StreamTrimDueMarker,
    /// `storage_serde` bytes of `marker`: the stored marker payload a delete
    /// compares.
    pub marker_blob: Vec<u8>,
    pub record: StreamRetentionFinishRecord,
}

impl TryFrom<StreamRetentionFinish> for PreparedRetentionFinish {
    type Error = StorageError;

    fn try_from(finish: StreamRetentionFinish) -> StorageResult<Self> {
        let marker = finish.marker.clone();
        let marker_blob = storage_serde::to_bytes(&marker)?;
        let record: StreamRetentionFinishRecord = finish.try_into()?;
        if !record.valid_update {
            return Err(StorageError::internal(
                "stream trim finish has an invalid state update",
            ));
        }
        Ok(Self {
            marker,
            marker_blob,
            record,
        })
    }
}

/// A state record to store after its marker was deleted, with the scope that
/// keys it.
pub struct RetentionStateWrite {
    pub scope: StreamTrimScope,
    pub record: StreamTrimStateRecord,
}

impl PreparedRetentionFinish {
    /// `completed`: the port deleted the marker (present with `marker_blob`,
    /// and, when `record.expected_state_blob` is set, the stored state equal to
    /// it). A required state write whose marker or state moved is a miss.
    pub fn settle(self, completed: bool) -> StorageResult<Option<RetentionStateWrite>> {
        match (completed, self.record.state_write) {
            (false, Some(_)) => Err(StorageError::write_miss(
                "table_retention_state",
                MissExpectation::Revision,
            )),
            (false, None) => Ok(None),
            (true, None) => Ok(None),
            (true, Some(record)) => Ok(Some(RetentionStateWrite {
                scope: self.marker.scope,
                record,
            })),
        }
    }
}

/// Finish one wave's markers inside the caller's attempt: one
/// compare-and-delete call, then one store call when a state follows.
pub async fn finish_stream_trim_markers(
    attempt: &mut impl StreamRetentionPort,
    finishes: Vec<StreamRetentionFinish>,
) -> StorageResult<()> {
    if finishes.is_empty() {
        return Ok(());
    }
    let prepared = finishes
        .into_iter()
        .map(PreparedRetentionFinish::try_from)
        .collect::<StorageResult<Vec<_>>>()?;
    let completed = attempt.delete_stream_trim_markers(&prepared).await?;
    let mut states = Vec::new();
    for (finish, completed) in completed.zip(prepared) {
        if let Some(state) = finish.settle(completed)? {
            states.push(state);
        }
    }
    if states.is_empty() {
        Ok(())
    } else {
        attempt.store_stream_trim_states(states).await
    }
}
