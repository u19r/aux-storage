//! Public write semantics travel with the prepared batch into native execution.

use storage_types::StorageResult;

use super::{FinishedObservedBatch, PreparedTableBatch, finish_transact_members_with_observations};
use crate::{ValidatedWriteObservations, finish_batch_with_observations};

pub enum PreparedTableWrite {
    Ordinary(PreparedTableBatch),
    Transaction(PreparedTableBatch),
}

impl PreparedTableWrite {
    pub fn finish(
        self,
        observations: ValidatedWriteObservations,
    ) -> StorageResult<FinishedObservedBatch> {
        match self {
            Self::Ordinary(batch) => {
                finish_batch_with_observations(batch, observations).map_err(Into::into)
            }
            Self::Transaction(batch) => {
                finish_transact_members_with_observations(batch, observations)
            }
        }
    }
}
