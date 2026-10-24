//! Immutable proposals bind compact phase records across native recovery paths.
use infrastructure_provider::record::{DurableRecord, RecordChange};
use replication_types::{identity::resource::ControlOperationId, protocol::CanonicalDigest};
use serde::{Deserialize, Serialize};
use storage_types::{Revision, StorageResult};

use super::JournalStore;
use crate::{
    constants::{PUBLIC_TRIM_OPERATION_SUFFIX, PUBLIC_TRIM_PROPOSAL_SUFFIX},
    native_record::corrupt_record,
    trim::{
        proposal::PublicTrimProposal,
        record::{TrimDecision, TrimObservation},
    },
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeTrimOperation {
    operation: ControlOperationId,
    content: CanonicalDigest,
    decision: TrimDecision,
    released_grant: Option<CanonicalDigest>,
}

impl JournalStore {
    pub(in crate::journal) fn decode_public_trim_record(
        &self,
        row: &DurableRecord,
        proposal_row: &DurableRecord,
    ) -> StorageResult<TrimObservation> {
        let marker: NativeTrimOperation = self.records.decode(row)?;
        let proposal: PublicTrimProposal = self.records.decode(proposal_row)?;
        proposal.validate().map_err(|_| corrupt_record())?;
        if row.key != self.identity_key(PUBLIC_TRIM_OPERATION_SUFFIX, &proposal.operation)?
            || proposal_row.key
                != self.identity_key(PUBLIC_TRIM_PROPOSAL_SUFFIX, &proposal.operation)?
            || proposal_row.revision != Revision::INITIAL
            || marker.operation != proposal.operation
            || marker.content != self.records.content_digest(&proposal)?
        {
            return Err(corrupt_record());
        }
        let record = TrimObservation {
            proposal,
            decision: marker.decision,
            released_grant: marker.released_grant,
        };
        self.require_public_trim_phase(&record, row.revision)?;
        Ok(record)
    }

    pub(super) fn prepare_public_trim_record(
        &self,
        record: &TrimObservation,
        expected: Option<Revision>,
    ) -> StorageResult<RecordChange> {
        let revision = expected.map_or(Ok(Revision::INITIAL), Revision::checked_next)?;
        self.require_public_trim_phase(record, revision)?;
        let marker = NativeTrimOperation {
            operation: record.proposal.operation,
            content: self.records.content_digest(&record.proposal)?,
            decision: record.decision,
            released_grant: record.released_grant,
        };
        self.records.prepare(
            self.identity_key(PUBLIC_TRIM_OPERATION_SUFFIX, &marker.operation)?,
            expected,
            &marker,
        )
    }

    pub(super) fn prepare_new_public_trim_record(
        &self,
        record: &TrimObservation,
    ) -> StorageResult<[RecordChange; 2]> {
        Ok([
            self.records.prepare(
                self.identity_key(PUBLIC_TRIM_PROPOSAL_SUFFIX, &record.proposal.operation)?,
                None,
                &record.proposal,
            )?,
            self.prepare_public_trim_record(record, None)?,
        ])
    }

    fn require_public_trim_phase(
        &self,
        record: &TrimObservation,
        revision: Revision,
    ) -> StorageResult<()> {
        let granted = Revision::INITIAL.checked_next()?;
        let committed = granted.checked_next()?;
        let installed = if self.region == record.proposal.primary {
            committed.checked_next()?
        } else {
            committed
        };
        let valid = match record.decision {
            TrimDecision::Preparing => revision == Revision::INITIAL,
            TrimDecision::Granted(_) => revision == granted,
            TrimDecision::Committed(_) => {
                self.region == record.proposal.primary && revision == committed
            }
            TrimDecision::Installed(_) => revision == installed,
            TrimDecision::Completed(_) => revision == installed.checked_next()?,
            TrimDecision::Cancelled if record.released_grant.is_some() => revision == committed,
            TrimDecision::Cancelled => revision == Revision::INITIAL || revision == granted,
        };
        if !valid || (record.decision != TrimDecision::Cancelled && record.released_grant.is_some())
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}
