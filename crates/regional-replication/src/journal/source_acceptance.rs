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
    originals: PreparedOriginalBatch<'static>,
    declarations: Vec<FirstSeenDeclaration>,
    receipts: Vec<CanonicalAcceptanceReceipt>,
    links: BTreeMap<CanonicalEventId, SourcePublicationEvidence>,
}

struct SourcePublicationEvidence {
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
            Option<TableHistoryId>,
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
            Option<TableHistoryId>,
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
            Option<TableHistoryId>,
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
        self.originals.include_owned(journal, event, history)?;
        self.declarations.push(journal.declare_first_seen(event));
        self.receipts.push(receipt.clone());
        self.links.insert(
            event.identity().clone(),
            SourcePublicationEvidence {
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
            keys.push(journal.identity_key(crate::constants::ACCEPTANCE_SUFFIX, &receipt.event)?);
            keys.push(journal.identity_key(crate::constants::EVENT_SUFFIX, &receipt.event)?);
        }
        let rows = journal
            .load_immutable_waves(attempt, &keys, storage_types::ReadProtection::Conflict)
            .await?;
        if rows.iter().any(Option::is_some) {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(in crate::journal) fn require_sources(
        &mut self,
        journal: &JournalStore,
        sources: &[super::SourcePosition],
    ) -> StorageResult<()> {
        for source in sources {
            let mut original_bytes = 0usize;
            for link in &source.events {
                let evidence = self.links.remove(&link.event).ok_or_else(corrupt_record)?;
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
        Ok(())
    }

    pub(in crate::journal) async fn prepare<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        self,
        journal: &JournalStore,
        attempt: &mut A,
    ) -> StorageResult<Vec<infrastructure_provider::record::RecordChange>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let originals = self.originals.prepare(journal, attempt).await?;
            drop(self.originals);
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
