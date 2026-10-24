use scheduled_bg_jobs::store::{Check, CommitOperationId, CommitOutcome, StoreTransaction};
use storage_provider::{
    RawOrderedJobStoreError,
    job_store::{
        AtomicCommit, CommittedTransaction, ConflictedTransaction, EstablishedCommit,
        EstablishedCommitOutcome, JobCommitError, TransactionCheckObservations, encode_transaction,
        execute_commit, run_native_job_attempt, settle_native_commit,
    },
};
use storage_types::{AttemptMode, StorageError};

use super::{SqlRawOrderedJobStore, receipt, store::decode_many};
use crate::{
    ReadProtection, SqlAttempt, SqlBackend, SqlEngine, SqlStatement,
    constants::{SCHEDULED_JOB_POINT_READ_BYTES as POINT_READ_BYTES, SCHEDULED_JOB_VALUE_BYTES},
};

pub(super) async fn commit<B: SqlBackend + 'static>(
    store: &SqlRawOrderedJobStore<B>,
    transaction: StoreTransaction,
) -> Result<CommitOutcome, RawOrderedJobStoreError> {
    let canonical = encode_transaction(&transaction)?;
    settle_native_commit(store.capabilities.max_transaction_duration(), |remaining| {
        commit_once(store, transaction.clone(), canonical.clone(), remaining)
    })
    .await
}

async fn commit_once<B: SqlBackend + 'static>(
    store: &SqlRawOrderedJobStore<B>,
    transaction: StoreTransaction,
    canonical: Vec<u8>,
    remaining: std::time::Duration,
) -> Result<CommitOutcome, RawOrderedJobStoreError> {
    let engine = store.engine;
    let backend = std::sync::Arc::clone(&store.backend);
    run_native_job_attempt(
        remaining,
        store.admission.get().cloned(),
        move |slot| async move {
            backend
                .with_attempt(AttemptMode::ReadWrite, move |attempt| {
                    Box::pin(async move {
                        let mut commit = SqlJobCommit { attempt, engine };
                        slot.preserve(execute_commit(&mut commit, &transaction, &canonical).await)
                    })
                })
                .await
        },
    )
    .await
}

struct SqlJobCommit<'a, A> {
    attempt: &'a mut A,
    engine: SqlEngine,
}

impl<A: SqlAttempt> AtomicCommit for SqlJobCommit<'_, A> {
    type Error = JobCommitError;
    const MAX_SCHEDULED_JOB_VALUE_BYTES: usize = SCHEDULED_JOB_VALUE_BYTES;

    async fn load_established_commit(
        &mut self,
        operation_id: CommitOperationId,
    ) -> Result<Option<EstablishedCommit>, Self::Error> {
        receipt::load(self.attempt, self.engine, operation_id).await
    }

    async fn load_transaction_check_observations(
        &mut self,
        checks: &[Check],
    ) -> Result<TransactionCheckObservations, Self::Error> {
        let mut keys = Vec::new();
        keys.try_reserve_exact(checks.len()).map_err(|_| {
            StorageError::from(storage_types::InfrastructureResultKind::NativeCapacityRejected)
        })?;
        keys.extend(checks.iter().map(|check| check.key().clone()));
        let max_bytes = POINT_READ_BYTES
            .get()
            .checked_mul(keys.len())
            .and_then(std::num::NonZeroUsize::new)
            .ok_or_else(|| StorageError::internal("scheduled job check byte limit overflowed"))?;
        let page = self
            .attempt
            .query(
                super::sql_statements::points(self.engine, &keys)?,
                ReadProtection::Conflict,
                max_bytes,
            )
            .await?;
        let values = decode_many(page, &keys, |value: &Vec<u8>| {
            super::sql_statements::copy_blob(value)
        })?;
        TransactionCheckObservations::from_values(checks, values).map_err(Into::into)
    }

    async fn save_committed_transaction(
        &mut self,
        transaction: CommittedTransaction<'_>,
    ) -> Result<(), Self::Error> {
        let count = transaction
            .mutations
            .puts
            .len()
            .checked_add(transaction.mutations.deletes.len())
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| {
                StorageError::from(storage_types::InfrastructureResultKind::NativeCapacityRejected)
            })?;
        let mut statements = Vec::new();
        statements.try_reserve_exact(count).map_err(|_| {
            StorageError::from(storage_types::InfrastructureResultKind::NativeCapacityRejected)
        })?;
        for put in transaction.mutations.puts {
            statements.push(super::sql_statements::put(
                self.engine,
                put.key.as_bytes(),
                put.value,
            )?);
        }
        for key in transaction.mutations.deletes {
            statements.push(super::sql_statements::delete(self.engine, key.as_bytes())?);
        }
        statements.push(receipt::save(
            self.engine,
            transaction.operation_id,
            transaction.transaction_content,
            EstablishedCommitOutcome::Committed,
        )?);
        self.execute_transaction(statements).await
    }

    async fn save_conflicted_transaction(
        &mut self,
        transaction: ConflictedTransaction<'_>,
    ) -> Result<(), Self::Error> {
        let statement = receipt::save(
            self.engine,
            transaction.operation_id,
            transaction.transaction_content,
            EstablishedCommitOutcome::Conflict,
        )?;
        let mut statements = Vec::new();
        statements.try_reserve_exact(1).map_err(|_| {
            StorageError::from(storage_types::InfrastructureResultKind::NativeCapacityRejected)
        })?;
        statements.push(statement);
        self.execute_transaction(statements).await
    }
}

impl<A: SqlAttempt> SqlJobCommit<'_, A> {
    async fn execute_transaction(
        &mut self,
        statements: Vec<SqlStatement>,
    ) -> Result<(), JobCommitError> {
        let changed = self.attempt.execute_batch(statements).await?;
        // The receipt insert is the last statement. Exactly one affected row
        // proves that this attempt stored the receipt for replay; zero rows
        // would leave the preceding mutations without that durable proof.
        if changed.last() != Some(&1) {
            return Err(
                StorageError::internal("scheduled-job receipt insert affected no row").into(),
            );
        }
        Ok(())
    }
}
