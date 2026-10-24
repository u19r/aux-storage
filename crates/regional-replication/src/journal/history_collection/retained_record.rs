//! Self-describing retained declarations preserve their adopted collection
//! scope.
use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use crate::{bootstrap::ReenableHistoryCut, journal::JournalStore, native_record::corrupt_record};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct CollectedRetainedCut {
    pub(super) proposal: replication_types::configuration::proposal::ControlProposal,
    pub(super) previous: CanonicalDigest,
    pub(super) cut: ReenableHistoryCut,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CollectedRetainedDefinition {
    pub(super) proposal: replication_types::configuration::proposal::ControlProposal,
    pub(super) inventory: Option<crate::bootstrap::RetainedHistoryInventory>,
}

impl JournalStore {
    pub(in crate::journal) fn require_collected_retained_definition(
        &self,
        row: &infrastructure_provider::record::DurableRecord,
    ) -> StorageResult<()> {
        let fact: CollectedRetainedDefinition = self.records.decode(row)?;
        let request = crate::history_capture::HistoryCaptureRequest {
            proposal: fact.proposal.clone(),
            region: fact.proposal.operation().home_region.clone(),
        };
        self.require_collection_request(&request)?;
        if let Some(inventory) = fact.inventory {
            crate::bootstrap::ReenableHistoryCustody {
                definition: inventory,
                ordinal: 0,
            }
            .require_custodian(&fact.proposal)?;
        }
        if row.revision != storage_types::Revision::INITIAL
            || row.key
                != self.identity_key(
                    crate::constants::COLLECTED_RETAINED_DEFINITION_SUFFIX,
                    &fact.proposal.operation().id,
                )?
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(in crate::journal) fn decode_collected_retained_cut(
        &self,
        row: &infrastructure_provider::record::DurableRecord,
    ) -> StorageResult<CollectedRetainedCut> {
        let fact: CollectedRetainedCut = self.records.decode(row)?;
        let request = crate::history_capture::HistoryCaptureRequest {
            proposal: fact.proposal.clone(),
            region: fact.proposal.operation().home_region.clone(),
        };
        self.require_collection_request(&request)?;
        fact.cut.require_scope(&fact.proposal)?;
        let custody = fact.cut.custody.as_ref().ok_or_else(corrupt_record)?;
        if custody.ordinal != fact.cut.ordinal {
            return Err(corrupt_record());
        }
        if fact.cut.ordinal == 0 {
            let before = fact
                .proposal
                .participant(&fact.proposal.operation().home_region)
                .map_err(|_| corrupt_record())?
                .before
                .as_ref()
                .ok_or_else(corrupt_record)?;
            let seed =
                CanonicalDigest::of_json(&(fact.proposal.operation().content, before.physical))
                    .map_err(|_| corrupt_record())?;
            if fact.previous != seed {
                return Err(corrupt_record());
            }
        }
        if row.revision != storage_types::Revision::INITIAL
            || row.key
                != self.identity_key(
                    crate::constants::COLLECTED_RETAINED_PAGE_SUFFIX,
                    &(fact.proposal.operation().id, fact.cut.ordinal),
                )?
        {
            return Err(corrupt_record());
        }
        Ok(fact)
    }
}
