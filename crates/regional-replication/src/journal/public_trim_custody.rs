//! Ordered native nested inventory traversal retains original publication
//! scopes.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey},
};
use replication_types::{
    configuration::proposal::ControlProposal, identity::resource::ControlOperationId,
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::{
    JournalStore,
    public_trim_scan::{SourceScanPhase, SourceScanRecord},
    reader_hold::RetentionHoldReference,
};
use crate::{
    bootstrap::{ReenableHistoryCustody, RetainedHistoryInventory},
    history::source_retention::SourceRetentionRange,
    native_record::corrupt_record,
    trim::proposal::PublicTrimProposal,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum CustodyKind {
    Reenable(ControlOperationId),
    Adoption(ControlOperationId),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct CustodyScan {
    kind: CustodyKind,
    index: DurableRecordKey,
    next: Option<DurableRecordKey>,
    inventory: CanonicalDigest,
    cuts: u64,
    ordinal: u64,
    vector: CanonicalDigest,
    has_overlap: bool,
}

impl JournalStore {
    pub(in crate::journal) async fn prepare_public_trim_custody<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        row: &DurableRecord,
        continuation: (Option<DurableRecordKey>, &mut SourceScanRecord),
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let kind = match self.decode_retention_hold(row)? {
                RetentionHoldReference::Reenable(operation) => CustodyKind::Reenable(operation),
                RetentionHoldReference::Adoption(operation) => CustodyKind::Adoption(operation),
                _ => return Ok(false),
            };
            let (next, scan) = continuation;
            let inventory = self
                .load_public_trim_custody_inventory(attempt, &kind)
                .await?;
            scan.vector =
                self.records
                    .content_digest(&(scan.vector, &row.key, row.revision, &inventory))?;
            let Some((_, inventory)) = inventory else {
                scan.unresolved_holds = scan
                    .unresolved_holds
                    .checked_add(1)
                    .ok_or_else(corrupt_record)?;
                scan.phase = match next {
                    Some(after) => SourceScanPhase::Obligations { after: Some(after) },
                    None => SourceScanPhase::AwaitingQualification,
                };
                return Ok(true);
            };
            scan.phase = SourceScanPhase::Custody(CustodyScan {
                kind,
                index: row.key.clone(),
                next,
                inventory: self.records.content_digest(&inventory)?,
                cuts: inventory.cuts,
                ordinal: 0,
                vector: inventory.inventory_seed()?,
                has_overlap: false,
            });
            Ok(true)
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn scan_public_trim_custody<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &PublicTrimProposal,
        scan: &mut SourceScanRecord,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let SourceScanPhase::Custody(custody) = &mut scan.phase else {
                return Err(corrupt_record());
            };
            self.require_public_trim_custody_scan(custody)?;
            let (inventory, page) = self.load_public_trim_custody_page(attempt, custody).await?;
            if self.records.content_digest(&inventory)? != custody.inventory
                || inventory.cuts != custody.cuts
            {
                return Err(corrupt_record());
            }
            custody.vector = inventory.require_page(&page, (custody.ordinal, custody.vector))?;
            let held = &page.cut.cut;
            custody.has_overlap |= proposal
                .candidate
                .sources
                .iter()
                .any(|removed| SourceRetentionRange::Source(held).overlaps_cut(removed));
            scan.vector = self.records.content_digest(&(scan.vector, &page))?;
            if let Some(next) = page.next_ordinal {
                custody.ordinal = next;
                return Ok(());
            }
            if custody.has_overlap {
                scan.overlapping_holds = scan
                    .overlapping_holds
                    .checked_add(1)
                    .ok_or_else(corrupt_record)?;
            }
            scan.phase = match custody.next.take() {
                Some(after) => SourceScanPhase::Obligations { after: Some(after) },
                None => SourceScanPhase::AwaitingQualification,
            };
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) fn require_public_trim_custody_scan(
        &self,
        custody: &CustodyScan,
    ) -> StorageResult<()> {
        let reference = match custody.kind {
            CustodyKind::Reenable(operation) => RetentionHoldReference::Reenable(operation),
            CustodyKind::Adoption(operation) => RetentionHoldReference::Adoption(operation),
        };
        if custody.index != self.retention_hold_key(&reference)? || custody.ordinal >= custody.cuts
        {
            return Err(corrupt_record());
        }
        if let Some(next) = &custody.next {
            let first = self.record_key(crate::constants::READER_HOLD_SUFFIX.to_vec())?;
            if !self.record_prefix_range(&first)?.contains(next) || next < &custody.index {
                return Err(corrupt_record());
            }
        }
        Ok(())
    }

    async fn load_public_trim_custody_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        custody: &CustodyScan,
    ) -> StorageResult<(
        RetainedHistoryInventory,
        crate::bootstrap::ReenableHistoryPage,
    )> {
        match custody.kind {
            CustodyKind::Reenable(operation) => {
                let (definition, page) = self
                    .load_reenable_trim_export(attempt, operation, custody.ordinal)
                    .await?;
                Ok((RetainedHistoryInventory::try_from(&definition)?, page))
            }
            CustodyKind::Adoption(operation) => {
                let (owner, inventory) = self
                    .load_adoption_trim_inventory(attempt, operation)
                    .await?
                    .ok_or_else(corrupt_record)?;
                let reference = ReenableHistoryCustody {
                    definition: inventory.clone(),
                    ordinal: custody.ordinal,
                };
                let page = self
                    .load_retained_custody_page(
                        attempt,
                        &owner,
                        &reference,
                        storage_types::ReadProtection::Shared,
                    )
                    .await?;
                Ok((inventory, page))
            }
        }
    }

    async fn load_public_trim_custody_inventory<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        kind: &CustodyKind,
    ) -> StorageResult<Option<(ControlProposal, RetainedHistoryInventory)>> {
        match kind {
            CustodyKind::Reenable(operation) => {
                self.load_reenable_trim_inventory(attempt, *operation).await
            }
            CustodyKind::Adoption(operation) => {
                self.load_adoption_trim_inventory(attempt, *operation).await
            }
        }
    }
}
