//! Primary preparation settlement survives restart without repeated peer
//! exchanges.
use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecord};
use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_types::{Revision, StorageResult};

use super::JournalStore;
use crate::{
    constants::PUBLIC_TRIM_PREPARATION_SUFFIX,
    native_record::corrupt_record,
    transport::RemoteTrimReceipt,
    trim::{
        proposal::PublicTrimProposal,
        record::{TrimDecision, TrimObservation},
    },
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedTrimParticipants {
    proposal: CanonicalDigest,
}

impl JournalStore {
    pub(super) fn require_public_trim_preparation(
        &self,
        row: &DurableRecord,
        proposal: &PublicTrimProposal,
    ) -> StorageResult<()> {
        let record: PreparedTrimParticipants = self.records.decode(row)?;
        if self.region != proposal.primary
            || row.revision != Revision::INITIAL
            || row.key != self.identity_key(PUBLIC_TRIM_PREPARATION_SUFFIX, &proposal.operation)?
            || record.proposal != self.records.content_digest(proposal)?
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(super) async fn record_public_trim_preparation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&TrimObservation, &[RemoteTrimReceipt]),
        observed: Option<Revision>,
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (record, receipts) = input;
            let proposal = &record.proposal;
            if self.region != proposal.primary {
                return Err(corrupt_record());
            }
            match (record.decision, observed) {
                (TrimDecision::Cancelled | TrimDecision::Completed(_), None) => return Ok(false),
                (
                    TrimDecision::Preparing
                    | TrimDecision::Granted(_)
                    | TrimDecision::Committed(_)
                    | TrimDecision::Installed(_),
                    Some(_),
                ) => return Ok(true),
                (TrimDecision::Preparing, None) => {}
                _ => return Err(corrupt_record()),
            }
            if receipts.len() != proposal.participants.len() - 1 {
                return Err(corrupt_record());
            }
            let regions = proposal
                .participants
                .iter()
                .filter(|participant| participant.region != self.region);
            for (participant, receipt) in regions.zip(receipts) {
                receipt.require_decision(proposal, &participant.region, TrimDecision::Preparing)?;
            }
            let key = self.identity_key(PUBLIC_TRIM_PREPARATION_SUFFIX, &proposal.operation)?;
            let capacity = self.records.observe_discretionary_capacity(attempt).await?;
            let prepared = PreparedTrimParticipants {
                proposal: self.records.content_digest(proposal)?,
            };
            self.records
                .store(attempt, &[self.records.prepare(key, None, &prepared)?])
                .await?;
            self.records
                .require_discretionary_capacity(attempt, capacity)
                .await?;
            Ok(true)
        }
        .await;
        settlement.complete(result)
    }
}
