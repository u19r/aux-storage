use scheduled_bg_jobs::store::{Check, CommitOutcome, StoreTransaction};

use super::{
    AtomicCommit, CheckObservation, CommitExecution, CommittedTransaction, ConflictedTransaction,
    EstablishedCommitOutcome,
};
use crate::RawOrderedJobStoreError;

pub async fn execute_commit<P>(
    provider: &mut P,
    transaction: &StoreTransaction,
    canonical: &[u8],
) -> Result<CommitExecution, P::Error>
where
    P: AtomicCommit,
{
    let operation_id = transaction.operation_id();
    if let Some(established) = provider.load_established_commit(operation_id).await? {
        if established.transaction_content != canonical {
            return Err(P::Error::from(
                RawOrderedJobStoreError::OperationReuseMismatch,
            ));
        }
        return Ok(CommitExecution::Replay(match established.outcome {
            EstablishedCommitOutcome::Committed => CommitOutcome::Committed,
            EstablishedCommitOutcome::Conflict => CommitOutcome::Conflict,
        }));
    }

    let observations = if transaction.checks().is_empty() {
        Vec::new()
    } else {
        provider
            .load_transaction_check_observations(transaction.checks())
            .await?
            .observations
    };
    if observations.len() != transaction.checks().len() {
        return Err(P::Error::from(corrupt_observations()));
    }

    let mut checks_match = true;
    for (check, observation) in transaction.checks().iter().zip(observations) {
        if let CheckObservation::Value(value) = &observation
            && value.len() > P::MAX_SCHEDULED_JOB_VALUE_BYTES
        {
            return Err(P::Error::from(RawOrderedJobStoreError::Corrupt(
                "scheduled job check value",
            )));
        }
        checks_match &= check_matches(check, observation).map_err(P::Error::from)?;
    }

    if checks_match {
        provider
            .save_committed_transaction(CommittedTransaction {
                operation_id,
                transaction_content: canonical,
                mutations: transaction.mutations().try_into().map_err(P::Error::from)?,
            })
            .await?;
        Ok(CommitExecution::Write(CommitOutcome::Committed))
    } else {
        provider
            .save_conflicted_transaction(ConflictedTransaction {
                operation_id,
                transaction_content: canonical,
            })
            .await?;
        Ok(CommitExecution::Write(CommitOutcome::Conflict))
    }
}

fn check_matches(
    check: &Check,
    observation: CheckObservation,
) -> Result<bool, RawOrderedJobStoreError> {
    match (check, observation) {
        (Check::Absent(_), CheckObservation::Missing) => Ok(true),
        (Check::Absent(_), CheckObservation::Present | CheckObservation::Value(_)) => Ok(false),
        (Check::Value(_, _), CheckObservation::Present) => Err(corrupt_observations()),
        (Check::Value(_, _), CheckObservation::Missing) => Ok(false),
        (Check::Value(_, expected), CheckObservation::Value(value)) => {
            Ok(value.as_slice() == expected.as_ref())
        }
    }
}

fn corrupt_observations() -> RawOrderedJobStoreError {
    RawOrderedJobStoreError::Corrupt("scheduled job check observations")
}
