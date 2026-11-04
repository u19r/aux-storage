//! Receiver positions select qualified originals without name resolution.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordAddress, RecordChange},
};
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::{
    event::content::CanonicalEvent, identity::journal_partition::JournalPartition,
};
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, StorageResult};

use super::JournalStore;
use crate::{
    constants::CANONICAL_VIEW_SUFFIX,
    history::view::{CanonicalViewPointer, DeliveryPosition},
    native_record::corrupt_record,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) enum ReceiverRecord {
    Event(Box<CanonicalViewPointer>),
    Removed {
        partition: JournalPartition,
        position: DeliveryPosition,
    },
}

impl JournalStore {
    pub(in crate::journal) async fn load_receiver_original<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        selection: (
            &replication_types::identity::origin::OriginId,
            DeliveryPosition,
        ),
        input: (&CanonicalJsonCodec, ReadProtection),
    ) -> StorageResult<Option<(CanonicalViewPointer, CanonicalEvent)>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(pointer) = self
                .load_receiver_pointer(attempt, selection, input.1)
                .await?
            else {
                return Ok(None);
            };
            let event = self.load_pointer_original(attempt, &pointer, input).await?;
            Ok(Some((pointer, event)))
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn load_receiver_pointer<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        selection: (
            &replication_types::identity::origin::OriginId,
            DeliveryPosition,
        ),
        protection: ReadProtection,
    ) -> StorageResult<Option<CanonicalViewPointer>> {
        let (receiver, position) = selection;
        self.require_receiver_scope(receiver, position)?;
        let key = self.canonical_view_key(receiver.journal_partition, position)?;
        let row = self
            .load_immutable_waves(attempt, std::slice::from_ref(&key), protection)
            .await?
            .pop()
            .flatten()
            .ok_or(storage_types::StorageEnum::HistoryUnavailable)?;
        let pointer = self.decode_receiver_pointer(&row)?;
        if pointer
            .as_ref()
            .is_some_and(|pointer| &pointer.receiver != receiver || pointer.position != position)
            || row.key != key
        {
            return Err(corrupt_record());
        }
        Ok(pointer)
    }

    pub(super) async fn load_pointer_original<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        pointer: &CanonicalViewPointer,
        input: (&CanonicalJsonCodec, ReadProtection),
    ) -> StorageResult<CanonicalEvent> {
        let (accepted, delivery) = self
            .load_acceptance_with_delivery(
                attempt,
                (pointer.physical, &pointer.event),
                input.0,
                input.1,
            )
            .await?
            .ok_or_else(corrupt_record)?;
        if &delivery != pointer || accepted.event.as_ref().digest != pointer.digest {
            return Err(corrupt_record());
        }
        Ok(accepted.event)
    }

    pub(in crate::journal) fn decode_receiver_pointer(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<Option<CanonicalViewPointer>> {
        match self.records.decode::<ReceiverRecord>(row)? {
            ReceiverRecord::Event(pointer) => {
                self.require_receiver_scope(&pointer.receiver, pointer.position)?;
                if row.revision != storage_types::Revision::INITIAL
                    || row.key
                        != self.canonical_view_key(
                            pointer.receiver.journal_partition,
                            pointer.position,
                        )?
                {
                    return Err(corrupt_record());
                }
                Ok(Some(*pointer))
            }
            ReceiverRecord::Removed {
                partition,
                position,
            } => {
                self.require_receiver_scope(&self.origin(partition)?, position)?;
                if row.revision != storage_types::Revision::INITIAL.checked_next()?
                    || row.key != self.canonical_view_key(partition, position)?
                {
                    return Err(corrupt_record());
                }
                Ok(None)
            }
        }
    }

    pub(super) async fn prepare_table_receiver_removals<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        history: storage_types::TableHistoryId,
        acceptances: &[DurableRecord],
    ) -> StorageResult<Vec<RecordChange>> {
        let receipts = acceptances
            .iter()
            .map(|row| self.decode_native_acceptance(row))
            .collect::<StorageResult<Vec<_>>>()?;
        if receipts
            .iter()
            .any(|receipt| receipt.delivery.physical != history)
        {
            return Err(corrupt_record());
        }
        let keys = receipts
            .iter()
            .map(|receipt| {
                self.canonical_view_key(
                    receipt.delivery.receiver.journal_partition,
                    receipt.delivery.position,
                )
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let rows = self
            .records
            .load(attempt, &keys, ReadProtection::Conflict)
            .await?;
        let mut changes = Vec::new();
        for (receipt, row) in receipts.iter().zip(rows) {
            let row = row.ok_or_else(corrupt_record)?;
            let Some(pointer) = self.decode_receiver_pointer(&row)? else {
                continue;
            };
            if pointer != receipt.delivery {
                return Err(corrupt_record());
            }
            let removed = ReceiverRecord::Removed {
                partition: pointer.receiver.journal_partition,
                position: pointer.position,
            };
            changes.push(
                self.records
                    .prepare(row.key, Some(row.revision), &removed)?,
            );
        }
        Ok(changes)
    }

    pub(super) fn require_receiver_scope(
        &self,
        receiver: &replication_types::identity::origin::OriginId,
        position: DeliveryPosition,
    ) -> StorageResult<()> {
        if position == DeliveryPosition::from(0)
            || *receiver != self.origin(receiver.journal_partition)?
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(super) fn canonical_view_key(
        &self,
        partition: JournalPartition,
        position: DeliveryPosition,
    ) -> StorageResult<DurableRecordKey> {
        let mut key: RecordAddress = self.identity_key(CANONICAL_VIEW_SUFFIX, &partition)?.into();
        key.suffix.extend_from_slice(&position.bytes());
        key.try_into()
    }
}
