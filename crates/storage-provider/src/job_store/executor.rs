use scheduled_bg_jobs::store::{Check, CommitOutcome, StoreTransaction};

use super::AtomicCommit;
use crate::RawOrderedJobStoreError;

pub async fn execute_commit<P>(
    provider: &mut P,
    transaction: &StoreTransaction,
    max_value_bytes: std::num::NonZeroUsize,
) -> Result<CommitOutcome, P::Error>
where
    P: AtomicCommit,
{
    let observations = if transaction.checks().is_empty() {
        Vec::new()
    } else {
        provider.load_check_values(transaction.checks()).await?
    };
    if observations.len() != transaction.checks().len() {
        return Err(P::Error::from(RawOrderedJobStoreError::Corrupt(
            "scheduled job check observations",
        )));
    }

    let mut checks_match = true;
    for (check, observation) in transaction.checks().iter().zip(observations) {
        if matches!(check, Check::Value(_, _))
            && observation
                .as_ref()
                .is_some_and(|value| value.len() > max_value_bytes.get())
        {
            return Err(P::Error::from(RawOrderedJobStoreError::Corrupt(
                "scheduled job check value",
            )));
        }
        checks_match &= match check {
            Check::Absent(_) => observation.is_none(),
            Check::Value(_, expected) => observation.as_deref() == Some(expected.as_ref()),
        };
    }

    if !checks_match {
        return Ok(CommitOutcome::Conflict);
    }
    provider.store_mutations(transaction.mutations()).await?;
    Ok(CommitOutcome::Committed)
}
