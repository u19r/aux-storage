use scheduled_bg_jobs::store::{Check, CommitOutcome, Mutation, StoreTransaction};
use storage_provider::{
    RawOrderedJobStoreError,
    job_store::{
        AtomicCommit, JobCommitError, execute_commit, run_native_job_attempt, settle_native_commit,
    },
};
use storage_types::{AttemptMode, StorageError};

use super::{SqlRawOrderedJobStore, store::decode_many};
use crate::{
    ReadProtection, SqlAttempt, SqlBackend, SqlEngine,
    constants::SCHEDULED_JOB_POINT_READ_BYTES as POINT_READ_BYTES,
};

pub(super) async fn commit<B: SqlBackend + 'static>(
    store: &SqlRawOrderedJobStore<B>,
    transaction: StoreTransaction,
) -> Result<CommitOutcome, RawOrderedJobStoreError> {
    settle_native_commit(store.capabilities.max_transaction_duration(), |remaining| {
        commit_once(store, transaction.clone(), remaining)
    })
    .await
}

async fn commit_once<B: SqlBackend + 'static>(
    store: &SqlRawOrderedJobStore<B>,
    transaction: StoreTransaction,
    remaining: std::time::Duration,
) -> Result<CommitOutcome, RawOrderedJobStoreError> {
    let engine = store.engine;
    let backend = std::sync::Arc::clone(&store.backend);
    let max_value_bytes = store.capabilities.max_value_bytes();
    run_native_job_attempt(
        remaining,
        store.admission.get().cloned(),
        move |slot| async move {
            backend
                .with_attempt(AttemptMode::ReadWrite, move |attempt| {
                    Box::pin(async move {
                        let mut commit = SqlJobCommit { attempt, engine };
                        slot.preserve(
                            execute_commit(&mut commit, &transaction, max_value_bytes).await,
                        )
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

    async fn load_check_values(
        &mut self,
        checks: &[Check],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
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
        Ok(values)
    }

    async fn store_mutations(&mut self, mutations: &[Mutation]) -> Result<(), Self::Error> {
        let mut statements = Vec::new();
        statements
            .try_reserve_exact(mutations.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for mutation in mutations {
            statements.push(match mutation {
                Mutation::Put(key, value) => {
                    super::sql_statements::put(self.engine, key.as_bytes(), value)?
                }
                Mutation::Delete(key) => {
                    super::sql_statements::delete(self.engine, key.as_bytes())?
                }
            });
        }
        if !statements.is_empty() {
            self.attempt.execute_batch(statements).await?;
        }
        Ok(())
    }
}
