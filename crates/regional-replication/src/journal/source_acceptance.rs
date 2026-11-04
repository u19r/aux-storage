//! Source publication retains compact proof and owned spans, never every full
//! event.

use std::collections::BTreeMap;

use replication_types::{
    event::{
        disposition::CanonicalAcceptanceReceipt, identity::CanonicalEventId,
        interval::CanonicalEventLink, publication::CanonicalPublication,
    },
    identity::origin::OriginId,
};
use storage_types::{StorageResult, TableHistoryId};

use super::{
    JournalStore, history_view::FirstSeenDeclaration, original_bytes::PreparedOriginalBatch,
};
use crate::{canonical_original::CanonicalOriginalRef, native_record::corrupt_record};

#[derive(Default)]
pub(in crate::journal) struct PreparedSourceAcceptance {
    originals: PreparedOriginalBatch,
    declarations: Vec<FirstSeenDeclaration>,
    receipts: Vec<CanonicalAcceptanceReceipt>,
    links: BTreeMap<CanonicalEventId, SourcePublicationEvidence>,
}

struct SourcePublicationEvidence {
    physical: TableHistoryId,
    binding: SourcePublicationBinding,
    original_bytes: usize,
}

enum SourcePublicationBinding {
    Bound(CanonicalEventLink),
    Deferred(CanonicalPublication<'static>),
}

impl PreparedSourceAcceptance {
    pub(in crate::journal) fn include(
        &mut self,
        journal: &JournalStore,
        input: (
            CanonicalOriginalRef<'_>,
            &CanonicalAcceptanceReceipt,
            TableHistoryId,
        ),
        publication: (&OriginId, &CanonicalEventLink),
    ) -> StorageResult<()> {
        input
            .0
            .publication()
            .require_link(publication.1, publication.0)
            .map_err(|_| corrupt_record())?;
        self.include_original(
            journal,
            input,
            SourcePublicationBinding::Bound(publication.1.clone()),
        )
    }

    pub(in crate::journal) fn include_unpublished(
        &mut self,
        journal: &JournalStore,
        input: (
            CanonicalOriginalRef<'_>,
            &CanonicalAcceptanceReceipt,
            TableHistoryId,
        ),
    ) -> StorageResult<()> {
        let evidence = SourcePublicationBinding::Deferred(input.0.publication().into_owned());
        self.include_original(journal, input, evidence)
    }

    fn include_original(
        &mut self,
        journal: &JournalStore,
        input: (
            CanonicalOriginalRef<'_>,
            &CanonicalAcceptanceReceipt,
            TableHistoryId,
        ),
        evidence: SourcePublicationBinding,
    ) -> StorageResult<()> {
        let (event, receipt, history) = input;
        if &receipt.event != event.identity()
            || receipt.digest != event.digest()
            || self.links.contains_key(event.identity())
        {
            return Err(corrupt_record());
        }
        let original_bytes = event.bytes().len();
        self.originals.include(journal, event, history)?;
        self.declarations
            .push(journal.declare_first_seen(event, history));
        self.receipts.push(receipt.clone());
        self.links.insert(
            event.identity().clone(),
            SourcePublicationEvidence {
                physical: history,
                binding: evidence,
                original_bytes,
            },
        );
        Ok(())
    }

    pub(in crate::journal) async fn require_unaccepted<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        journal: &JournalStore,
        attempt: &mut A,
    ) -> StorageResult<()> {
        let mut keys = Vec::with_capacity(self.receipts.len());
        for receipt in &self.receipts {
            let physical = self
                .links
                .get(&receipt.event)
                .ok_or_else(corrupt_record)?
                .physical;
            keys.push(journal.table_identity_key(
                crate::constants::ACCEPTANCE_SUFFIX,
                physical,
                &receipt.event,
            )?);
            keys.push(journal.table_identity_key(
                crate::constants::EVENT_SUFFIX,
                physical,
                &receipt.event,
            )?);
        }
        let rows = journal
            .load_immutable_waves(attempt, &keys, storage_types::ReadProtection::Conflict)
            .await?;
        if rows.iter().any(Option::is_some) {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(in crate::journal) fn bind_source_activities(
        &mut self,
        journal: &JournalStore,
        sources: &[super::SourcePosition],
    ) -> StorageResult<Vec<super::delivery_activity::DeliveryActivity>> {
        let mut activities = Vec::new();
        for source in sources {
            let mut original_bytes = 0usize;
            for link in &source.events {
                let evidence = self.links.remove(&link.event).ok_or_else(corrupt_record)?;
                activities.push(super::delivery_activity::DeliveryActivity {
                    table: link.table,
                    physical: evidence.physical,
                    source_table: link.table,
                    lineage: link.lineage,
                    origin: source.origin.clone(),
                    through: source.position,
                });
                original_bytes = original_bytes
                    .checked_add(evidence.original_bytes)
                    .ok_or_else(corrupt_record)?;
                match &evidence.binding {
                    SourcePublicationBinding::Bound(expected) if expected == link => {}
                    SourcePublicationBinding::Deferred(proof) => proof
                        .require_link(link, &source.origin)
                        .map_err(|_| corrupt_record())?,
                    SourcePublicationBinding::Bound(_) => return Err(corrupt_record()),
                }
            }
            journal.require_source_original_bytes(original_bytes)?;
        }
        if !self.links.is_empty() {
            return Err(corrupt_record());
        }
        Ok(activities)
    }

    pub(in crate::journal) async fn prepare<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        self,
        journal: &JournalStore,
        attempt: &mut A,
    ) -> StorageResult<Vec<infrastructure_provider::record::RecordChange>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let originals = self.originals.into_changes();
            let deliveries = journal
                .prepare_first_seen_declarations(attempt, &self.declarations)
                .await?;
            if originals.len() != self.receipts.len() || deliveries.len() != self.receipts.len() {
                return Err(corrupt_record());
            }
            let prepared = self
                .receipts
                .iter()
                .zip(originals)
                .zip(deliveries)
                .map(|((receipt, original), delivery)| {
                    let (accepted, pointer) = delivery.accept(receipt)?;
                    Ok((original, accepted, pointer))
                })
                .collect::<StorageResult<Vec<_>>>()?;
            journal.prepare_acceptance_records(prepared)
        }
        .await;
        settlement.complete(result)
    }
}
