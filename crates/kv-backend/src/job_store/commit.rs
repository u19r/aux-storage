use scheduled_bg_jobs::store::{Check, CommitOutcome, Mutation, StoreTransaction};
use storage_provider::{
    RawOrderedJobStoreError,
    job_store::{
        AtomicCommit, JobCommitError, execute_commit, run_native_job_attempt, settle_native_commit,
    },
};
use storage_types::AttemptMode;

use super::{KvRawOrderedJobStore, data_key, data_keys, point_allowance};
use crate::{KvAttempt, KvBackend, KvMutation, ReadProtection};

pub(super) async fn commit<B: KvBackend + 'static>(
    store: &KvRawOrderedJobStore<B>,
    transaction: StoreTransaction,
) -> Result<CommitOutcome, RawOrderedJobStoreError> {
    settle_native_commit(store.capabilities.max_transaction_duration(), |remaining| {
        commit_once(store, transaction.clone(), remaining)
    })
    .await
}

async fn commit_once<B: KvBackend + 'static>(
    store: &KvRawOrderedJobStore<B>,
    transaction: StoreTransaction,
    remaining: std::time::Duration,
) -> Result<CommitOutcome, RawOrderedJobStoreError> {
    let backend = std::sync::Arc::clone(&store.backend);
    let max_value_bytes = store.capabilities.max_value_bytes();
    run_native_job_attempt(
        remaining,
        store.admission.get().cloned(),
        move |slot| async move {
            backend
                .with_attempt(AttemptMode::ReadWrite, move |attempt| {
                    Box::pin(async move {
                        let mut commit = KvJobCommit { attempt };
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

struct KvJobCommit<'a, A> {
    attempt: &'a mut A,
}

impl<A: KvAttempt> AtomicCommit for KvJobCommit<'_, A> {
    type Error = JobCommitError;

    async fn load_check_values(
        &mut self,
        checks: &[Check],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let keys = data_keys(checks.iter().map(|check| check.key().as_bytes()))?;
        let values = self
            .attempt
            .get_batch_admitted(
                keys,
                ReadProtection::Conflict,
                point_allowance(checks.len())?,
            )
            .await?;
        Ok(values)
    }

    async fn store_mutations(&mut self, mutations: &[Mutation]) -> Result<(), Self::Error> {
        let mut changes = Vec::new();
        changes
            .try_reserve_exact(mutations.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for mutation in mutations {
            changes.push(match mutation {
                Mutation::Put(key, bytes) => {
                    let mut value = Vec::new();
                    value.try_reserve_exact(bytes.len()).map_err(|_| {
                        storage_types::InfrastructureResultKind::NativeCapacityRejected
                    })?;
                    value.extend_from_slice(bytes);
                    KvMutation::Put {
                        key: data_key(key.as_bytes())?,
                        value,
                    }
                }
                Mutation::Delete(key) => KvMutation::Delete {
                    key: data_key(key.as_bytes())?,
                },
            });
        }
        if !changes.is_empty() {
            self.attempt.stage(changes)?;
        }
        Ok(())
    }
}
