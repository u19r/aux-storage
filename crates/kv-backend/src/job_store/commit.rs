use scheduled_bg_jobs::store::{Check, CommitOperationId, CommitOutcome, StoreTransaction};
use storage_provider::{
    RawOrderedJobStoreError,
    job_store::{
        AtomicCommit, CommittedTransaction, ConflictedTransaction, EstablishedCommit,
        EstablishedCommitOutcome, JobCommitError, TransactionCheckObservations, encode_transaction,
        execute_commit, run_native_job_attempt, settle_native_commit,
    },
};
use storage_types::AttemptMode;

use super::{KvRawOrderedJobStore, data_key, data_keys, point_allowance, receipt};
use crate::{
    KvAttempt, KvBackend, KvMutation, ReadProtection, constants::SCHEDULED_JOB_VALUE_BYTES,
};

pub(super) async fn commit<B: KvBackend + 'static>(
    store: &KvRawOrderedJobStore<B>,
    transaction: StoreTransaction,
) -> Result<CommitOutcome, RawOrderedJobStoreError> {
    let canonical = encode_transaction(&transaction)?;
    settle_native_commit(store.capabilities.max_transaction_duration(), |remaining| {
        commit_once(store, transaction.clone(), canonical.clone(), remaining)
    })
    .await
}

async fn commit_once<B: KvBackend + 'static>(
    store: &KvRawOrderedJobStore<B>,
    transaction: StoreTransaction,
    canonical: Vec<u8>,
    remaining: std::time::Duration,
) -> Result<CommitOutcome, RawOrderedJobStoreError> {
    let backend = std::sync::Arc::clone(&store.backend);
    run_native_job_attempt(
        remaining,
        store.admission.get().cloned(),
        move |slot| async move {
            backend
                .with_attempt(AttemptMode::ReadWrite, move |attempt| {
                    Box::pin(async move {
                        let mut commit = KvJobCommit { attempt };
                        slot.preserve(execute_commit(&mut commit, &transaction, &canonical).await)
                    })
                })
                .await
        },
    )
    .await
}

struct KvJobCommit<'a, A> {
    attempt: &'a mut A,
}

impl<A: KvAttempt> AtomicCommit for KvJobCommit<'_, A> {
    type Error = JobCommitError;
    const MAX_SCHEDULED_JOB_VALUE_BYTES: usize = SCHEDULED_JOB_VALUE_BYTES;

    async fn load_established_commit(
        &mut self,
        operation_id: CommitOperationId,
    ) -> Result<Option<EstablishedCommit>, Self::Error> {
        receipt::load(self.attempt, operation_id)
            .await
            .map_err(Into::into)
    }

    async fn load_transaction_check_observations(
        &mut self,
        checks: &[Check],
    ) -> Result<TransactionCheckObservations, Self::Error> {
        let keys = data_keys(checks.iter().map(|check| check.key().as_bytes()))?;
        let values = self
            .attempt
            .get_batch_admitted(
                keys,
                ReadProtection::Conflict,
                point_allowance(checks.len())?,
            )
            .await?;
        TransactionCheckObservations::from_values(checks, values).map_err(Into::into)
    }

    async fn save_committed_transaction(
        &mut self,
        transaction: CommittedTransaction<'_>,
    ) -> Result<(), Self::Error> {
        let mut changes = receipt::encode(
            transaction.operation_id,
            transaction.transaction_content,
            EstablishedCommitOutcome::Committed,
        )?;
        let count = transaction
            .mutations
            .puts
            .len()
            .checked_add(transaction.mutations.deletes.len())
            .ok_or_else(|| {
                storage_types::StorageError::from(
                    storage_types::InfrastructureResultKind::NativeCapacityRejected,
                )
            })?;
        changes.try_reserve_exact(count).map_err(|_| {
            storage_types::StorageError::from(
                storage_types::InfrastructureResultKind::NativeCapacityRejected,
            )
        })?;
        for put in transaction.mutations.puts {
            let mut value = Vec::new();
            value.try_reserve_exact(put.value.len()).map_err(|_| {
                storage_types::StorageError::from(
                    storage_types::InfrastructureResultKind::NativeCapacityRejected,
                )
            })?;
            value.extend_from_slice(put.value);
            changes.push(KvMutation::Put {
                key: data_key(put.key.as_bytes())?,
                value,
            });
        }
        for key in transaction.mutations.deletes {
            changes.push(KvMutation::Delete {
                key: data_key(key.as_bytes())?,
            });
        }
        self.attempt.stage(changes)?;
        Ok(())
    }

    async fn save_conflicted_transaction(
        &mut self,
        transaction: ConflictedTransaction<'_>,
    ) -> Result<(), Self::Error> {
        let changes = receipt::encode(
            transaction.operation_id,
            transaction.transaction_content,
            EstablishedCommitOutcome::Conflict,
        )?;
        self.attempt.stage(changes)?;
        Ok(())
    }
}
