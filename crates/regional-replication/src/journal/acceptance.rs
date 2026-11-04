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
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
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

    pub async fn load_acceptance<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (storage_types::TableHistoryId, &CanonicalEventId),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Option<StoredAcceptance>> {
        self.load_acceptance_protected(attempt, input, codec, ReadProtection::Snapshot)
            .await
    }

    pub(in crate::journal) async fn load_acceptance_protected<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (storage_types::TableHistoryId, &CanonicalEventId),
        codec: &CanonicalJsonCodec,
        protection: ReadProtection,
    ) -> StorageResult<Option<StoredAcceptance>> {
        Ok(self
            .load_acceptance_with_delivery(attempt, input, codec, protection)
            .await?
            .map(|(accepted, _)| accepted))
    }

    pub(in crate::journal) async fn load_acceptance_with_delivery<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (storage_types::TableHistoryId, &CanonicalEventId),
        codec: &CanonicalJsonCodec,
        protection: ReadProtection,
    ) -> StorageResult<Option<(StoredAcceptance, crate::history::view::CanonicalViewPointer)>> {
        let identities = [(input.1, input.0)];
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
            if original.key()
                != &self.table_identity_key(
                    EVENT_SUFFIX,
                    accepted.delivery.physical,
                    &accepted.receipt.event,
                )?
            {
                return Err(corrupt_record());
            }
            changes.push(self.records.prepare_acceptance(
                self.table_identity_key(
                    ACCEPTANCE_SUFFIX,
                    accepted.delivery.physical,
                    &accepted.receipt.event,
                )?,
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
