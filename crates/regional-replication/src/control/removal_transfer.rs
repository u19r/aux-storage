//! Deterministic bounded seal-transfer steps within one removal operation.
use replication_types::{configuration::proposal::ControlProposal, identity::replica::RegionId};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use crate::{lifecycle::SealRequest, native_record::corrupt_record};

pub(super) enum RemovalTransferChange {
    Observe,
    Advance(RemovalTransferPosition),
}

pub(super) async fn execute_removal_transfer<P: storage_provider::DynamoPort>(
    port: &P,
    journal: std::sync::Arc<crate::journal::JournalStore>,
    proposal: std::sync::Arc<ControlProposal>,
    change: RemovalTransferChange,
) -> StorageResult<RemovalTransferPosition>
where
    for<'native> P::Attempt<'native>:
        infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
{
    let change = std::sync::Arc::new(change);
    storage_common::retry::retry_native_conflicts(
        port,
        storage_provider::DYNAMO_WRITE_RETRY,
        || {
            let journal = std::sync::Arc::clone(&journal);
            let proposal = std::sync::Arc::clone(&proposal);
            let change = std::sync::Arc::clone(&change);
            async move {
                std::sync::Arc::clone(&journal)
                    .with_admitted_attempt(
                        (
                            port,
                            storage_provider::native_admission::NativeWork::Recovery,
                        ),
                        crate::journal::NativeAttempt::Items(storage_types::AttemptMode::ReadWrite),
                        move |attempt, _| {
                            Box::pin(async move {
                                match *change {
                                    RemovalTransferChange::Observe => {
                                        journal
                                            .load_removal_transfer_position(attempt, &proposal)
                                            .await
                                    }
                                    RemovalTransferChange::Advance(completed) => {
                                        journal
                                            .advance_removal_transfer(attempt, &proposal, completed)
                                            .await
                                    }
                                }
                            })
                        },
                    )
                    .await
            }
        },
    )
    .await
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct RemovalTransferPosition(u32);

impl RemovalTransferPosition {
    pub(crate) fn first() -> Self {
        Self(0)
    }

    pub(crate) fn checked_next(self) -> StorageResult<Self> {
        self.0.checked_add(1).map(Self).ok_or_else(corrupt_record)
    }
}

pub(crate) struct RemovalTransferStep {
    pub(crate) request: SealRequest,
    pub(crate) survivor: RegionId,
}

pub(crate) struct RemovalTransferPlan<'a> {
    proposal: &'a ControlProposal,
    retiring: Vec<RegionId>,
    survivors: Vec<RegionId>,
    count: u32,
}

impl<'a> RemovalTransferPlan<'a> {
    pub(crate) fn try_new(proposal: &'a ControlProposal) -> StorageResult<Self> {
        let retiring = replication_provider::control::retiring_regions(proposal)
            .map_err(|_| corrupt_record())?;
        let first = retiring.first().ok_or_else(corrupt_record)?;
        let survivors = SealRequest::try_new(proposal.clone(), first.clone())?
            .survivors()?
            .to_vec();
        if survivors.is_empty() {
            return Err(corrupt_record());
        }
        let count = retiring
            .len()
            .checked_mul(survivors.len())
            .and_then(|count| u32::try_from(count).ok())
            .ok_or_else(corrupt_record)?;
        Ok(Self {
            proposal,
            retiring,
            survivors,
            count,
        })
    }

    pub(crate) fn require_position(&self, position: RemovalTransferPosition) -> StorageResult<()> {
        if position.0 > self.count {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(crate) fn select_step(
        &self,
        position: RemovalTransferPosition,
    ) -> StorageResult<Option<RemovalTransferStep>> {
        self.require_position(position)?;
        if position.0 == self.count {
            return Ok(None);
        }
        let index = usize::try_from(position.0).map_err(|_| corrupt_record())?;
        let region = self
            .retiring
            .get(index / self.survivors.len())
            .ok_or_else(corrupt_record)?;
        let survivor = self
            .survivors
            .get(index % self.survivors.len())
            .ok_or_else(corrupt_record)?
            .clone();
        Ok(Some(RemovalTransferStep {
            request: SealRequest::try_new(self.proposal.clone(), region.clone())?,
            survivor,
        }))
    }
}
