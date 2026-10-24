use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, regional_application::observation::StoredAcceptance,
};
use replication_types::event::{
    content::CanonicalEvent, disposition::CanonicalAcceptanceReceipt, identity::CanonicalEventId,
};
use storage_types::{ReadProtection, StorageResult};

use super::store::JournalStore;
use crate::{
    constants::{ACCEPTANCE_SUFFIX, EVENT_SUFFIX},
    native_record::corrupt_record,
};

impl JournalStore {
    /// Imports retain the original bytes and advance local causality, including
    /// losing events. They never allocate a local source position.
    pub(crate) async fn accept_import_event<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            &CanonicalEvent,
            &CanonicalAcceptanceReceipt,
            storage_types::TimestampMillis,
        ),
        original: super::PreparedImportOriginal,
    ) -> StorageResult<StoredAcceptance> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (event, receipt, observed_at) = input;
            let prepared = original.into_acceptance(event, receipt)?;
            let candidate = event.as_ref();
            if let replication_types::event::content::CanonicalPayload::Item(item) =
                &candidate.payload
                && let replication_types::event::order::WinnerOrder::Mutation { clock, .. } =
                    &item.order
            {
                self.report_clock_skew(*clock, i128::from(observed_at.timestamp_millis()));
            }
            let changes = self.prepare_acceptance_records(vec![prepared])?;
            self.records.store(attempt, &changes).await?;
            Ok(StoredAcceptance {
                receipt: receipt.clone(),
                event: event.clone(),
            })
        }
        .await;
        settlement.complete(result)
    }

    pub async fn load_acceptance<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        identity: &CanonicalEventId,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Option<StoredAcceptance>> {
        self.load_acceptance_protected(attempt, identity, codec, ReadProtection::Snapshot)
            .await
    }

    pub(in crate::journal) async fn load_acceptance_protected<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        identity: &CanonicalEventId,
        codec: &CanonicalJsonCodec,
        protection: ReadProtection,
    ) -> StorageResult<Option<StoredAcceptance>> {
        Ok(self
            .load_acceptance_with_delivery(attempt, identity, codec, protection)
            .await?
            .map(|(accepted, _)| accepted))
    }

    pub(in crate::journal) async fn load_acceptance_with_delivery<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        identity: &CanonicalEventId,
        codec: &CanonicalJsonCodec,
        protection: ReadProtection,
    ) -> StorageResult<Option<(StoredAcceptance, crate::history::view::CanonicalViewPointer)>> {
        let identities = [identity];
        let selection = super::acceptance_batch::AcceptanceReadSelection::Identities(&identities);
        let mut accepted = self
            .load_acceptance_inventory(attempt, (&selection, protection), codec)
            .await
            .map_err(storage_types::StorageError::from)?
            .ok_or_else(corrupt_record)?
            .into_iter();
        let result = accepted.next().ok_or_else(corrupt_record)?;
        if accepted.next().is_some() {
            return Err(corrupt_record());
        }
        Ok(result)
    }

    pub async fn store_acceptance<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        input: (
            &CanonicalEvent,
            &CanonicalAcceptanceReceipt,
            Option<storage_types::TableHistoryId>,
        ),
    ) -> StorageResult<()> {
        self.store_acceptance_batch(attempt, &[input]).await
    }

    pub(in crate::journal) async fn store_acceptance_batch<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: &[(
            &CanonicalEvent,
            &CanonicalAcceptanceReceipt,
            Option<storage_types::TableHistoryId>,
        )],
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if input.is_empty() {
                return Ok(());
            }
            let acceptances = input
                .iter()
                .map(|(event, receipt, _)| (*event, *receipt))
                .collect::<Vec<_>>();
            require_acceptance_inputs(&acceptances)?;
            let declarations = input
                .iter()
                .map(|(event, _, history)| (*event, *history))
                .collect::<Vec<_>>();
            let originals = self.prepare_original_batch(attempt, &declarations).await?;
            let events = input.iter().map(|(event, _, _)| *event).collect::<Vec<_>>();
            let deliveries = self.prepare_first_seen_batch(attempt, &events).await?;
            if originals.len() != input.len() || deliveries.len() != input.len() {
                return Err(corrupt_record());
            }
            let prepared = acceptances
                .iter()
                .zip(originals)
                .zip(deliveries)
                .map(|(((_, receipt), original), delivery)| {
                    let (accepted, pointer) = delivery.accept(receipt)?;
                    Ok((original, accepted, pointer))
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let changes = self.prepare_acceptance_records(prepared)?;
            self.records.store(attempt, &changes).await
        }
        .await;
        settlement.complete(result)
    }
    pub(in crate::journal) fn prepare_acceptance_records(
        &self,
        input: Vec<(
            infrastructure_provider::record::RecordChange,
            super::history_view::NativeAcceptanceRecord,
            infrastructure_provider::record::RecordChange,
        )>,
    ) -> StorageResult<Vec<infrastructure_provider::record::RecordChange>> {
        let mut changes =
            Vec::with_capacity(input.len().checked_mul(3).ok_or_else(corrupt_record)?);
        for (original, accepted, pointer) in input {
            if original.key() != &self.identity_key(EVENT_SUFFIX, &accepted.receipt.event)? {
                return Err(corrupt_record());
            }
            changes.push(self.records.prepare_acceptance(
                self.identity_key(ACCEPTANCE_SUFFIX, &accepted.receipt.event)?,
                &accepted,
            )?);
            changes.push(original);
            changes.push(pointer);
        }
        Ok(changes)
    }

    pub(in crate::journal) fn require_acceptance_binding(
        &self,
        accepted: (
            &CanonicalAcceptanceReceipt,
            &crate::history::view::CanonicalViewPointer,
        ),
        input: (&CanonicalEventId, &CanonicalEvent),
    ) -> StorageResult<()> {
        let (receipt, delivery) = accepted;
        let (identity, event) = input;
        if receipt.event != *identity
            || event.as_ref().id != *identity
            || receipt.digest != event.as_ref().digest
            || delivery.lineage != event.lineage()
            || delivery.receiver.journal_partition != self.select_delivery_partition(event.into())
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

fn require_acceptance_inputs(
    input: &[(&CanonicalEvent, &CanonicalAcceptanceReceipt)],
) -> StorageResult<()> {
    let mut identities = std::collections::BTreeSet::new();
    for (event, receipt) in input {
        let candidate = event.as_ref();
        if receipt.event != candidate.id
            || receipt.digest != candidate.digest
            || !identities.insert(&candidate.id)
        {
            return Err(corrupt_record());
        }
    }
    Ok(())
}
