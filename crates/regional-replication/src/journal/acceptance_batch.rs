//! One ordered acceptance/original reader for source and import proof.
use infrastructure_provider::record::DurableRecord;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, delivery::error::DeliveryError,
    regional_application::observation::StoredAcceptance,
};
use replication_types::event::{
    content::CanonicalEvent, identity::CanonicalEventId, interval::CanonicalEventLink,
};
use storage_types::{ReadProtection, StorageResult};

use super::{JournalStore, history_view::NativeAcceptanceRecord};
use crate::{
    constants::{ACCEPTANCE_SUFFIX, EVENT_SUFFIX},
    history::view::CanonicalViewPointer,
    native_record::corrupt_record,
};

pub(in crate::journal) enum AcceptanceReadSelection<'input> {
    Identities(&'input [&'input CanonicalEventId]),
    SourceProof {
        identities: &'input [&'input CanonicalEventId],
        maximum: usize,
    },
    Replays(&'input [&'input CanonicalEvent]),
    Frame {
        links: &'input [CanonicalEventLink],
        maximum: usize,
    },
}

impl AcceptanceReadSelection<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Identities(ids)
            | Self::SourceProof {
                identities: ids, ..
            } => ids.len(),
            Self::Replays(events) => events.len(),
            Self::Frame { links, .. } => links.len(),
        }
    }

    fn identity(&self, slot: usize) -> StorageResult<&CanonicalEventId> {
        match self {
            Self::Identities(ids)
            | Self::SourceProof {
                identities: ids, ..
            } => ids.get(slot).copied(),
            Self::Replays(events) => events.get(slot).map(|event| &event.as_ref().id),
            Self::Frame { links, .. } => links.get(slot).map(|link| &link.event),
        }
        .ok_or_else(corrupt_record)
    }

    fn require_receipt(&self, slot: usize, receipt: &NativeAcceptanceRecord) -> StorageResult<()> {
        if &receipt.receipt.event != self.identity(slot)? {
            return Err(corrupt_record());
        }
        if let Self::Frame { links, .. } = self {
            let link = links.get(slot).ok_or_else(corrupt_record)?;
            if receipt.receipt.digest != link.digest {
                return Err(corrupt_record());
            }
        }
        Ok(())
    }

    fn require_original_total(&self, total: usize) -> Result<bool, DeliveryError> {
        match self {
            Self::Frame { maximum, .. } if total > *maximum => Ok(false),
            Self::SourceProof { maximum, .. } if total > *maximum => {
                Err(storage_types::StorageError::validation(
                    "complete source proof exceeds its qualified original allowance",
                )
                .into())
            }
            _ => Ok(true),
        }
    }

    fn require_replay_metadata(
        &self,
        journal: &JournalStore,
        slot: usize,
        receipt: &NativeAcceptanceRecord,
        length: usize,
    ) -> StorageResult<()> {
        let Self::Replays(events) = self else {
            return Ok(());
        };
        let event = events.get(slot).ok_or_else(corrupt_record)?;
        journal.require_acceptance_binding(
            (&receipt.receipt, &receipt.delivery),
            (&event.as_ref().id, event),
        )?;
        if event.as_ref().bytes.as_ref().len() != length {
            return Err(corrupt_record());
        }
        Ok(())
    }

    fn verify_original(
        &self,
        journal: &JournalStore,
        slot: usize,
        original: (&DurableRecord, &[u8]),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<CanonicalEvent> {
        let (row, bytes) = original;
        let Self::Replays(events) = self else {
            return journal.decode_retained_event(row, bytes, codec);
        };
        let event = events.get(slot).ok_or_else(corrupt_record)?;
        if event.as_ref().bytes.as_ref() != bytes
            || row.revision != storage_types::Revision::INITIAL
            || row.key != journal.identity_key(EVENT_SUFFIX, &event.as_ref().id)?
        {
            return Err(corrupt_record());
        }
        Ok((*event).clone())
    }
}

impl JournalStore {
    pub(crate) async fn select_frame_acceptances<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        links: &[CanonicalEventLink],
        input: (&CanonicalJsonCodec, usize),
    ) -> Result<Option<Vec<StoredAcceptance>>, DeliveryError> {
        let selection = AcceptanceReadSelection::Frame {
            links,
            maximum: input.1,
        };
        let Some(accepted) = self
            .load_acceptance_inventory(attempt, (&selection, ReadProtection::Snapshot), input.0)
            .await?
        else {
            return Ok(None);
        };
        accepted
            .into_iter()
            .map(|accepted| {
                accepted
                    .map(|(accepted, _)| accepted)
                    .ok_or_else(corrupt_record)
            })
            .collect::<StorageResult<Vec<_>>>()
            .map(Some)
            .map_err(DeliveryError::from)
    }

    pub(in crate::journal) async fn load_acceptance_inventory<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&AcceptanceReadSelection<'_>, ReadProtection),
        codec: &CanonicalJsonCodec,
    ) -> Result<Option<Vec<Option<(StoredAcceptance, CanonicalViewPointer)>>>, DeliveryError> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let mut accepted = Vec::new();
            accepted.try_reserve_exact(input.0.len()).map_err(|_| {
                storage_types::StorageError::from(
                    storage_types::InfrastructureResultKind::NativeCapacityRejected,
                )
            })?;
            let Some(prepared) = self.load_acceptance_metadata(attempt, input, codec).await? else {
                return Ok(None);
            };
            let AcceptanceInventory {
                receipts,
                originals,
                manifests,
                maximum,
            } = prepared;
            let bytes = self
                .load_retained_manifests(attempt, &originals, (manifests, maximum))
                .await?;
            if bytes.len() != originals.len() {
                return Err(corrupt_record().into());
            }
            let mut bodies = originals.iter().zip(bytes);
            for (slot, receipt) in receipts.into_iter().enumerate() {
                let Some(native) = receipt else {
                    accepted.push(None);
                    continue;
                };
                let (row, bytes) = bodies.next().ok_or_else(corrupt_record)?;
                let event = input.0.verify_original(self, slot, (row, &bytes), codec)?;
                self.require_acceptance_binding(
                    (&native.receipt, &native.delivery),
                    (input.0.identity(slot)?, &event),
                )?;
                accepted.push(Some((
                    StoredAcceptance {
                        receipt: native.receipt,
                        event,
                    },
                    native.delivery,
                )));
            }
            if bodies.next().is_some() {
                return Err(corrupt_record().into());
            }
            Ok(Some(accepted))
        }
        .await;
        settlement.complete(result)
    }

    async fn load_acceptance_metadata<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&AcceptanceReadSelection<'_>, ReadProtection),
        codec: &CanonicalJsonCodec,
    ) -> Result<Option<AcceptanceInventory>, DeliveryError> {
        let mut keys = Vec::new();
        let count = input
            .0
            .len()
            .checked_mul(2)
            .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)
            .map_err(storage_types::StorageError::from)?;
        keys.try_reserve_exact(count).map_err(|_| {
            storage_types::StorageError::from(
                storage_types::InfrastructureResultKind::NativeCapacityRejected,
            )
        })?;
        let mut prepared = AcceptanceInventory::try_new(input.0.len())?;
        for slot in 0..input.0.len() {
            let identity = input.0.identity(slot)?;
            keys.push(self.identity_key(ACCEPTANCE_SUFFIX, identity)?);
            keys.push(self.identity_key(EVENT_SUFFIX, identity)?);
        }
        for wave in self.records.read_waves(&keys, |key| key)? {
            let rows = self
                .load_immutable_waves(attempt, &keys[wave.clone()], input.1)
                .await?;
            for (slot, row) in wave.zip(rows) {
                if !prepared.include_metadata(self, (slot, row), (input.0, codec))? {
                    return Ok(None);
                }
            }
        }
        if prepared.receipts.len() != input.0.len()
            || prepared.originals.len()
                != prepared
                    .receipts
                    .iter()
                    .filter(|receipt| receipt.is_some())
                    .count()
        {
            return Err(corrupt_record().into());
        }
        Ok(Some(prepared))
    }
}

struct AcceptanceInventory {
    receipts: Vec<Option<NativeAcceptanceRecord>>,
    originals: Vec<DurableRecord>,
    manifests: Vec<stream_provider::retained_image::RetainedImageManifest>,
    maximum: usize,
}

impl AcceptanceInventory {
    fn try_new(count: usize) -> StorageResult<Self> {
        let mut prepared = Self {
            receipts: Vec::new(),
            originals: Vec::new(),
            manifests: Vec::new(),
            maximum: 0,
        };
        prepared
            .receipts
            .try_reserve_exact(count)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        prepared
            .originals
            .try_reserve_exact(count)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        prepared
            .manifests
            .try_reserve_exact(count)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        Ok(prepared)
    }

    fn include_metadata(
        &mut self,
        journal: &JournalStore,
        row: (usize, Option<DurableRecord>),
        input: (&AcceptanceReadSelection<'_>, &CanonicalJsonCodec),
    ) -> Result<bool, DeliveryError> {
        let (slot, row) = row;
        if slot % 2 == 0 {
            let receipt = row
                .map(|row| journal.decode_native_acceptance(&row))
                .transpose()?;
            if let Some(receipt) = &receipt {
                input.0.require_receipt(slot / 2, receipt)?;
            }
            self.receipts.push(receipt);
            return Ok(true);
        }
        let receipt = self.receipts.last().ok_or_else(corrupt_record)?;
        match (receipt, row) {
            (None, None) => {
                if matches!(input.0, AcceptanceReadSelection::Frame { .. }) {
                    return Err(corrupt_record().into());
                }
            }
            (Some(receipt), Some(row)) => {
                let manifest = journal.decode_retained_manifest(&row, input.1)?;
                input.0.require_replay_metadata(
                    journal,
                    slot / 2,
                    receipt,
                    manifest.original_length(),
                )?;
                self.maximum = self
                    .maximum
                    .checked_add(manifest.original_length())
                    .ok_or_else(corrupt_record)?;
                if !input.0.require_original_total(self.maximum)? {
                    return Ok(false);
                }
                self.manifests.push(manifest);
                self.originals.push(row);
            }
            _ => return Err(corrupt_record().into()),
        }
        Ok(true)
    }
}
