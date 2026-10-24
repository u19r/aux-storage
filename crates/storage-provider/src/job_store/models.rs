use bytes::Bytes;
use scheduled_bg_jobs::store::{Check, CommitOperationId, CommitOutcome, Mutation, StoreKey};
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum EstablishedCommitOutcome {
    Committed,
    Conflict,
}

#[derive(Eq, PartialEq)]
pub struct EstablishedCommit {
    pub transaction_content: Vec<u8>,
    pub outcome: EstablishedCommitOutcome,
}

#[derive(Eq, PartialEq)]
pub enum CheckObservation {
    Missing,
    Present,
    Value(Vec<u8>),
}

#[derive(Eq, PartialEq)]
pub struct TransactionCheckObservations {
    /// One entry per requested check, in request order.
    pub observations: Vec<CheckObservation>,
}

impl TransactionCheckObservations {
    pub fn from_values(
        checks: &[Check],
        values: Vec<Option<Vec<u8>>>,
    ) -> Result<Self, crate::RawOrderedJobStoreError> {
        if values.len() != checks.len() {
            return Err(crate::RawOrderedJobStoreError::Corrupt(
                "scheduled job check observations",
            ));
        }
        let mut observations = Vec::new();
        observations
            .try_reserve_exact(checks.len())
            .map_err(|_| capacity_rejected())?;
        for (check, value) in checks.iter().zip(values) {
            observations.push(match (check, value) {
                (_, None) => CheckObservation::Missing,
                (Check::Absent(_), Some(_)) => CheckObservation::Present,
                (Check::Value(_, _), Some(value)) => CheckObservation::Value(value),
            });
        }
        Ok(Self { observations })
    }
}

pub struct ScheduledJobPut<'a> {
    pub key: &'a StoreKey,
    pub value: &'a Bytes,
}

pub struct ScheduledJobMutations<'a> {
    pub puts: Vec<ScheduledJobPut<'a>>,
    pub deletes: Vec<&'a StoreKey>,
}

impl<'a> TryFrom<&'a [Mutation]> for ScheduledJobMutations<'a> {
    type Error = crate::RawOrderedJobStoreError;

    fn try_from(mutations: &'a [Mutation]) -> Result<Self, Self::Error> {
        let put_count = mutations
            .iter()
            .filter(|mutation| matches!(mutation, Mutation::Put(..)))
            .count();
        let mut puts = Vec::new();
        let mut deletes = Vec::new();
        puts.try_reserve_exact(put_count)
            .map_err(|_| capacity_rejected())?;
        deletes
            .try_reserve_exact(mutations.len() - put_count)
            .map_err(|_| capacity_rejected())?;
        for mutation in mutations {
            match mutation {
                Mutation::Put(key, value) => puts.push(ScheduledJobPut { key, value }),
                Mutation::Delete(key) => deletes.push(key),
            }
        }
        Ok(Self { puts, deletes })
    }
}

pub struct CommittedTransaction<'a> {
    pub operation_id: CommitOperationId,
    pub transaction_content: &'a [u8],
    pub mutations: ScheduledJobMutations<'a>,
}

pub struct ConflictedTransaction<'a> {
    pub operation_id: CommitOperationId,
    pub transaction_content: &'a [u8],
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CommitExecution {
    Replay(CommitOutcome),
    Write(CommitOutcome),
}

fn capacity_rejected() -> crate::RawOrderedJobStoreError {
    crate::RawOrderedJobStoreError::Backend(
        storage_types::InfrastructureResultKind::NativeCapacityRejected.into(),
    )
}
