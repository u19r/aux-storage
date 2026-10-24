//! One attempt's prepared originals and verified immutable acceptance
//! inventory.
use infrastructure_provider::record::RecordChange;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, regional_application::observation::StoredAcceptance,
};
use replication_types::{
    event::{content::CanonicalEvent, identity::CanonicalEventId},
    protocol::CanonicalDigest,
};
use storage_types::{InfrastructureResultKind, ReadProtection, StorageResult, TableHistoryId};

use super::{JournalStore, acceptance_batch::AcceptanceReadSelection};
use crate::native_record::corrupt_record;

#[derive(Default)]
pub(crate) struct PreparedImportEvents {
    events: Vec<(CanonicalEvent, PreparedImportEvent)>,
}

enum PreparedImportEvent {
    Pending(Box<PreparedImportOriginal>),
    Publishing(CanonicalDigest),
    Accepted(StoredAcceptance),
}

pub(crate) struct PreparedImportOriginal {
    event: CanonicalEvent,
    change: RecordChange,
    first_seen: super::history_view::PreparedFirstSeen,
}

impl PreparedImportEvents {
    pub(crate) fn begin_publication(
        &mut self,
        event: &CanonicalEvent,
    ) -> StorageResult<PreparedImportOriginal> {
        let slot = self
            .find_event(&event.as_ref().id)
            .ok_or_else(corrupt_record)?;
        let (prepared, entry) = &mut self.events[slot];
        if prepared.as_ref().bytes != event.as_ref().bytes {
            return Err(corrupt_record());
        }
        if !matches!(entry, PreparedImportEvent::Pending(_)) {
            return Err(corrupt_record());
        }
        let previous = std::mem::replace(
            entry,
            PreparedImportEvent::Publishing(event.as_ref().digest),
        );
        let PreparedImportEvent::Pending(original) = previous else {
            return Err(corrupt_record());
        };
        Ok(*original)
    }

    pub(crate) fn observed_acceptance(
        &self,
        identity: &CanonicalEventId,
    ) -> Option<StoredAcceptance> {
        let PreparedImportEvent::Accepted(accepted) = &self.events[self.find_event(identity)?].1
        else {
            return None;
        };
        Some(accepted.clone())
    }

    /// The preparation read protects both present and absent acceptances until
    /// commit; subsequent events use this inventory, including earlier writes.
    pub(crate) fn observe_event(
        &self,
        event: &CanonicalEvent,
    ) -> StorageResult<Option<StoredAcceptance>> {
        let (prepared, state) = &self.events[self
            .find_event(&event.as_ref().id)
            .ok_or_else(corrupt_record)?];
        if prepared.as_ref().bytes != event.as_ref().bytes {
            return Err(corrupt_record());
        }
        match state {
            PreparedImportEvent::Pending(_) => Ok(None),
            PreparedImportEvent::Accepted(accepted) => Ok(Some(accepted.clone())),
            PreparedImportEvent::Publishing(_) => Err(corrupt_record()),
        }
    }

    pub(crate) fn complete_publication(&mut self, accepted: StoredAcceptance) -> StorageResult<()> {
        let slot = self
            .find_event(&accepted.receipt.event)
            .ok_or_else(corrupt_record)?;
        let (event, entry) = &mut self.events[slot];
        if event.as_ref().bytes != accepted.event.as_ref().bytes
            || event.as_ref().id != accepted.event.as_ref().id
            || !matches!(entry, PreparedImportEvent::Publishing(digest) if *digest == accepted.receipt.digest)
        {
            return Err(corrupt_record());
        }
        *entry = PreparedImportEvent::Accepted(accepted);
        Ok(())
    }

    pub(crate) fn require_accepted_inventory(&self) -> StorageResult<()> {
        if self
            .events
            .iter()
            .any(|(_, event)| !matches!(event, PreparedImportEvent::Accepted(_)))
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    fn find_event(&self, identity: &CanonicalEventId) -> Option<usize> {
        self.events
            .binary_search_by(|(event, _)| event.as_ref().id.cmp(identity))
            .ok()
    }
}

impl PreparedImportOriginal {
    pub(super) fn into_acceptance(
        self,
        event: &CanonicalEvent,
        receipt: &replication_types::event::disposition::CanonicalAcceptanceReceipt,
    ) -> StorageResult<(
        RecordChange,
        super::history_view::NativeAcceptanceRecord,
        RecordChange,
    )> {
        if self.event.as_ref().id != event.as_ref().id
            || self.event.as_ref().bytes != event.as_ref().bytes
        {
            return Err(corrupt_record());
        }
        let (accepted, pointer) = self.first_seen.accept(receipt)?;
        Ok((self.change, accepted, pointer))
    }
}

impl JournalStore {
    pub(crate) async fn prepare_import_events<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: &[(&CanonicalEvent, Option<TableHistoryId>)],
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<PreparedImportEvents> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            let mut prepared = ImportOriginalPreparation::try_new(input)?;
            let selection = AcceptanceReadSelection::Replays(&prepared.replays);
            let accepted = self
                .load_acceptance_inventory(
                    settlement.as_mut(),
                    (&selection, ReadProtection::Conflict),
                    codec,
                )
                .await
                .map_err(storage_types::StorageError::from)?
                .ok_or_else(corrupt_record)?;
            prepared.select_originals(input, accepted)?;
            let changes = self
                .prepare_original_batch(settlement.as_mut(), &prepared.selected)
                .await?;
            if changes.len() != prepared.selected.len() {
                return Err(corrupt_record());
            }
            let deliveries = self
                .prepare_first_seen_batch(settlement.as_mut(), &prepared.selected_events)
                .await?;
            prepared.bind_originals(changes, deliveries)
        }
        .await;
        settlement.complete(result)
    }
}

/// Lookup order is independent of first-seen delivery order. All banks are
/// reserved before any replay read; completion cannot grow the publication
/// bank.
struct ImportOriginalPreparation<'input> {
    ordinals: Vec<usize>,
    replays: Vec<&'input CanonicalEvent>,
    selected: Vec<(&'input CanonicalEvent, Option<TableHistoryId>)>,
    selected_events: Vec<&'input CanonicalEvent>,
    inventory: PreparedImportEvents,
}

impl<'input> ImportOriginalPreparation<'input> {
    fn try_new(input: &[(&'input CanonicalEvent, Option<TableHistoryId>)]) -> StorageResult<Self> {
        let ordinals = Self::select_unique_ordinals(input)?;
        let mut prepared = Self {
            ordinals,
            replays: Vec::new(),
            selected: Vec::new(),
            selected_events: Vec::new(),
            inventory: PreparedImportEvents::default(),
        };
        let count = prepared.ordinals.len();
        prepared
            .replays
            .try_reserve_exact(count)
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        prepared
            .selected
            .try_reserve_exact(count)
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        prepared
            .selected_events
            .try_reserve_exact(count)
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        prepared
            .inventory
            .events
            .try_reserve_exact(count)
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        prepared
            .replays
            .extend(prepared.ordinals.iter().map(|ordinal| input[*ordinal].0));
        Ok(prepared)
    }

    fn select_unique_ordinals(
        input: &[(&CanonicalEvent, Option<TableHistoryId>)],
    ) -> StorageResult<Vec<usize>> {
        let mut ordinals = Vec::new();
        ordinals
            .try_reserve_exact(input.len())
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        ordinals.extend(0..input.len());
        ordinals.sort_unstable_by(|left, right| {
            input[*left]
                .0
                .as_ref()
                .id
                .cmp(&input[*right].0.as_ref().id)
                .then(left.cmp(right))
        });
        for adjacent in ordinals.windows(2) {
            let (left, left_history) = input[adjacent[0]];
            let (right, right_history) = input[adjacent[1]];
            if left.as_ref().id == right.as_ref().id
                && (left.as_ref().bytes != right.as_ref().bytes || left_history != right_history)
            {
                return Err(corrupt_record());
            }
        }
        ordinals.dedup_by(|right, left| input[*right].0.as_ref().id == input[*left].0.as_ref().id);
        ordinals.sort_unstable();
        Ok(ordinals)
    }

    fn select_originals(
        &mut self,
        input: &[(&'input CanonicalEvent, Option<TableHistoryId>)],
        accepted: Vec<Option<(StoredAcceptance, crate::history::view::CanonicalViewPointer)>>,
    ) -> StorageResult<()> {
        if accepted.len() != self.ordinals.len() {
            return Err(corrupt_record());
        }
        for (ordinal, accepted) in self.ordinals.iter().zip(accepted) {
            let (event, history) = input[*ordinal];
            match accepted {
                Some((accepted, _)) => self
                    .inventory
                    .events
                    .push((event.clone(), PreparedImportEvent::Accepted(accepted))),
                None => {
                    self.selected.push((event, history));
                    self.selected_events.push(event);
                }
            }
        }
        Ok(())
    }

    fn bind_originals(
        mut self,
        changes: Vec<RecordChange>,
        deliveries: Vec<super::history_view::PreparedFirstSeen>,
    ) -> StorageResult<PreparedImportEvents> {
        if changes.len() != self.selected.len() || deliveries.len() != self.selected.len() {
            return Err(corrupt_record());
        }
        for (((event, _), change), first_seen) in
            self.selected.into_iter().zip(changes).zip(deliveries)
        {
            let original = PreparedImportOriginal {
                event: event.clone(),
                change,
                first_seen,
            };
            self.inventory.events.push((
                event.clone(),
                PreparedImportEvent::Pending(Box::new(original)),
            ));
        }
        self.inventory
            .events
            .sort_unstable_by(|(left, _), (right, _)| left.as_ref().id.cmp(&right.as_ref().id));
        Ok(self.inventory)
    }
}
