//! Durable primary authorization precedes fresh native grant qualification.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey},
};
use storage_types::{ReadProtection, Revision, StorageResult};

use super::JournalStore;
use crate::{
    native_record::corrupt_record,
    trim::{
        grant_turn::{PrimaryTrimGrantTurn, TrimGrantTurn},
        proposal::PublicTrimProposal,
        record::{TrimDecision, TrimObservation},
    },
};

impl JournalStore {
    pub(super) fn public_trim_grant_turn_key(
        &self,
        proposal: &PublicTrimProposal,
    ) -> StorageResult<DurableRecordKey> {
        self.identity_key(
            crate::constants::PUBLIC_TRIM_GRANT_TURN_SUFFIX,
            &proposal.operation,
        )
    }

    pub(super) fn decode_public_trim_grant_turn(
        &self,
        row: &DurableRecord,
        proposal: &PublicTrimProposal,
    ) -> StorageResult<TrimGrantTurn> {
        let turn: TrimGrantTurn = self.records.decode(row)?;
        turn.validate()?;
        if row.revision != Revision::INITIAL
            || row.key != self.public_trim_grant_turn_key(proposal)?
            || turn.proposal != *proposal
            || turn.target()? != &self.region
        {
            return Err(corrupt_record());
        }
        Ok(turn)
    }

    pub(super) async fn load_public_trim_grant_turn<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &TrimObservation,
        protection: ReadProtection,
    ) -> StorageResult<Option<TrimGrantTurn>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.public_trim_grant_turn_key(&record.proposal)?;
            let row = self
                .records
                .load(attempt, &[key], protection)
                .await?
                .pop()
                .flatten();
            let turn = row
                .as_ref()
                .map(|row| self.decode_public_trim_grant_turn(row, &record.proposal))
                .transpose()?;
            match record.decision {
                TrimDecision::Preparing => {}
                TrimDecision::Cancelled | TrimDecision::Completed(_) if turn.is_none() => {}
                TrimDecision::Granted(_)
                | TrimDecision::Committed(_)
                | TrimDecision::Installed(_)
                    if turn.is_some() => {}
                _ => return Err(corrupt_record()),
            }
            Ok(turn)
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn authorize_public_trim_grant<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        authorization: &PrimaryTrimGrantTurn,
        record: &TrimObservation,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let turn = &authorization.turn;
            turn.validate()?;
            record.require_proposal(&turn.proposal)?;
            if turn.target()? != &self.region {
                return Err(corrupt_record());
            }
            if record.decision == TrimDecision::Cancelled {
                return Ok(());
            }
            if matches!(record.decision, TrimDecision::Completed(_)) {
                return self
                    .load_public_trim_commit(attempt, record, ReadProtection::Conflict)
                    .await?
                    .ok_or_else(corrupt_record)?
                    .into_certificate()
                    .require_grant_turn(turn, &self.region);
            }
            if let Some(existing) = self
                .load_public_trim_grant_turn(attempt, record, ReadProtection::Conflict)
                .await?
            {
                if existing != *turn {
                    return Err(corrupt_record());
                }
                return Ok(());
            }
            let capacity = self.records.observe_discretionary_capacity(attempt).await?;
            let key = self.public_trim_grant_turn_key(&record.proposal)?;
            self.records
                .store(attempt, &[self.records.prepare(key, None, turn)?])
                .await?;
            self.records
                .require_discretionary_capacity(attempt, capacity)
                .await
        }
        .await;
        settlement.complete(result)
    }
}
