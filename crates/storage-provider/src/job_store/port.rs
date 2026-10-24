use scheduled_bg_jobs::store::{Check, CommitOperationId};

use super::{
    CommittedTransaction, ConflictedTransaction, EstablishedCommit, TransactionCheckObservations,
};
use crate::RawOrderedJobStoreError;

/// Native transaction I/O for one checked JobStore commit.
///
/// Implementations may choose backend-specific read plans and physical writes,
/// but return observations without deciding whether the checks match.
#[allow(async_fn_in_trait)]
pub trait AtomicCommit {
    type Error: From<RawOrderedJobStoreError>;
    const MAX_SCHEDULED_JOB_VALUE_BYTES: usize;

    async fn load_established_commit(
        &mut self,
        operation_id: CommitOperationId,
    ) -> Result<Option<EstablishedCommit>, Self::Error>;

    async fn load_transaction_check_observations(
        &mut self,
        checks: &[Check],
    ) -> Result<TransactionCheckObservations, Self::Error>;

    /// Persist every supplied mutation and its commit receipt atomically.
    async fn save_committed_transaction(
        &mut self,
        transaction: CommittedTransaction<'_>,
    ) -> Result<(), Self::Error>;

    /// Persist the supplied conflict receipt without applying mutations.
    async fn save_conflicted_transaction(
        &mut self,
        transaction: ConflictedTransaction<'_>,
    ) -> Result<(), Self::Error>;
}
