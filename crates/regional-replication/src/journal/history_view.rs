//! First acceptance owns the immutable receiver-local delivery pointer.

use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordAddress, RecordChange},
};
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use replication_types::{
    event::{content::CanonicalEvent, disposition::CanonicalAcceptanceReceipt},
    identity::journal_partition::JournalPartition,
};
use serde::{Deserialize, Serialize};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageError, StorageResult};

use super::JournalStore;
use crate::{
    constants::{ACCEPTANCE_SUFFIX, CANONICAL_VIEW_SUFFIX},
    history::view::{
        CanonicalViewObservation, CanonicalViewPointer, CanonicalViewRecord, CanonicalViewRequest,
        DeliveryPosition,
    },
    native_record::corrupt_record,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct NativeAcceptanceRecord {
    pub receipt: CanonicalAcceptanceReceipt,
    pub delivery: CanonicalViewPointer,
}

pub(in crate::journal) struct QualifiedViewSelection {
    pub(in crate::journal) physical: storage_types::TableHistoryId,
    pub(in crate::journal) authority: super::public_trim_authority::PublicTrimAuthority,
}

pub(in crate::journal) struct FirstSeenDeclaration {
    partition: JournalPartition,
    clock: Option<replication_types::event::order::HybridLogicalClock>,
    lineage: replication_types::identity::resource::TableLineage,
    event: replication_types::event::identity::CanonicalEventId,
    digest: replication_types::protocol::CanonicalDigest,
}

pub(in crate::journal) struct PreparedFirstSeen {
    delivery: CanonicalViewPointer,
    pointer: RecordChange,
}

impl PreparedFirstSeen {
    pub(in crate::journal) fn accept(
        self,
        receipt: &CanonicalAcceptanceReceipt,
    ) -> StorageResult<(NativeAcceptanceRecord, RecordChange)> {
        if self.delivery.event != receipt.event || self.delivery.digest != receipt.digest {
            return Err(corrupt_record());
        }
        Ok((
            NativeAcceptanceRecord {
                receipt: receipt.clone(),
                delivery: self.delivery,
            },
            self.pointer,
        ))
    }
}

impl JournalStore {
    pub(in crate::journal) fn declare_first_seen(
        &self,
        event: crate::canonical_original::CanonicalOriginalRef<'_>,
    ) -> FirstSeenDeclaration {
        FirstSeenDeclaration {
            partition: self.select_delivery_partition(event),
            clock: event.item_clock(),
            lineage: event.lineage(),
            event: event.identity().clone(),
            digest: event.digest(),
        }
    }

    pub(in crate::journal) async fn prepare_first_seen_batch<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: &[&CanonicalEvent],
    ) -> StorageResult<Vec<PreparedFirstSeen>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            let mut declarations = Vec::new();
            declarations
                .try_reserve_exact(input.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            declarations.extend(
                input
                    .iter()
                    .map(|event| self.declare_first_seen((*event).into())),
            );
            self.prepare_first_seen_declarations(settlement.as_mut(), &declarations)
                .await
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn prepare_first_seen_declarations<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: &[FirstSeenDeclaration],
    ) -> StorageResult<Vec<PreparedFirstSeen>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let mut partitions = Vec::new();
            let mut prepared = Vec::new();
            partitions
                .try_reserve_exact(input.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            prepared
                .try_reserve_exact(input.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            partitions.extend(input.iter().map(|event| (event.partition, event.clock)));
            let positions = self
                .allocate_acceptance_positions(attempt, &partitions)
                .await?;
            if positions.len() != input.len() {
                return Err(corrupt_record());
            }
            for (event, position) in input.iter().zip(positions) {
                let delivery = CanonicalViewPointer {
                    receiver: self.origin(event.partition)?,
                    position,
                    lineage: event.lineage,
                    event: event.event.clone(),
                    digest: event.digest,
                };
                let pointer = self.records.prepare(
                    self.canonical_view_key(event.partition, position)?,
                    None,
                    &delivery,
                )?;
                prepared.push(PreparedFirstSeen { delivery, pointer });
            }
            Ok(prepared)
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) fn decode_native_acceptance(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<NativeAcceptanceRecord> {
        let accepted: NativeAcceptanceRecord = self.records.decode(row)?;
        let pointer = &accepted.delivery;
        self.require_receiver_scope(&pointer.receiver, pointer.position)?;
        if row.revision != storage_types::Revision::INITIAL
            || pointer.event != accepted.receipt.event
            || pointer.digest != accepted.receipt.digest
            || row.key != self.identity_key(ACCEPTANCE_SUFFIX, &accepted.receipt.event)?
        {
            return Err(corrupt_record());
        }
        Ok(accepted)
    }

    pub(in crate::journal) fn select_delivery_partition(
        &self,
        event: crate::canonical_original::CanonicalOriginalRef<'_>,
    ) -> JournalPartition {
        event.item_key().map_or(0u16.into(), |key| {
            self.partitions.select(event.lineage(), key)
        })
    }

    pub(crate) async fn load_canonical_view<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            &CanonicalViewRequest,
            (&WorkLimits, &CanonicalJsonCodec),
            storage_types::TimestampMillis,
        ),
    ) -> StorageResult<CanonicalViewObservation> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (request, encoding, observed_at) = input;
            self.require_view_receiver(request)?;
            let fixed = self
                .load_receiver_checkpoint(attempt, request, observed_at)
                .await?;
            let (scope, authority, fixed_high) = match fixed {
                Some(bounds) => (bounds.scope, bounds.authority, Some(bounds.high)),
                None => {
                    let selected = self
                        .qualify_view_selection(attempt, request, ReadProtection::Snapshot)
                        .await?;
                    (
                        request.continuation(selected.authority.revision(), 0u64.into()),
                        selected.authority,
                        None,
                    )
                }
            };
            let after = scope.require_continuation(request)?;
            let head = self
                .observe_delivery_position(
                    attempt,
                    request.receiver.journal_partition,
                    ReadProtection::Snapshot,
                )
                .await?;
            if fixed_high.is_some_and(|high| high > head) {
                return Err(corrupt_record());
            }
            let head = fixed_high.unwrap_or(head);
            if after > head {
                return Err(StorageError::validation(
                    "Canonical view cursor exceeds its observed head",
                ));
            }
            let through = if after == head {
                head
            } else {
                after.checked_next()?
            };
            let record = if through == after {
                None
            } else {
                self.load_view_record(attempt, (request, through, &authority), encoding)
                    .await?
            };
            let observation = CanonicalViewObservation {
                through: scope.at_position(through),
                observed_head: head,
                record,
            };
            crate::history::response_budget::encoded_length(
                &observation,
                encoding.0.frame_bytes.get(),
            )?;
            Ok(observation)
        }
        .await;
        settlement.complete(crate::history::retention::settle_expiry(result))?
    }

    pub(in crate::journal) async fn qualify_view_selection<
        A: CatalogAttempt + PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        request: &CanonicalViewRequest,
        protection: ReadProtection,
    ) -> StorageResult<QualifiedViewSelection> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let stamp = attempt
                .read_catalog_stamp(&request.table)
                .await?
                .ok_or_else(|| StorageError::table_not_found(&request.table))?;
            let physical = attempt.read_table_at_stamp(&request.table, stamp).await?;
            let observed = self
                .observe_history_table(attempt, &physical, protection)
                .await?;
            let state = observed.state();
            let authority = if state.configuration.lineage != request.lineage {
                self.require_installed_history_lineage(
                    attempt,
                    state,
                    (request.lineage, request.membership),
                    protection,
                )
                .await?;
                self.load_public_trim_head(attempt, (state, request.lineage), protection)
                    .await?
            } else {
                if state.membership != request.membership {
                    return Err(StorageError::validation(
                        "History table revision changed; restart discovery",
                    ));
                }
                observed.authority
            };
            Ok(QualifiedViewSelection {
                physical: physical.history_id,
                authority,
            })
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn qualify_history_incarnation<
        A: CatalogAttempt + PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        scope: (
            &storage_types::TableName,
            storage_types::TableHistoryId,
            replication_types::identity::resource::TableLineage,
        ),
    ) -> StorageResult<super::public_trim_authority::PublicTrimIncarnation> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let stamp = attempt
                .read_catalog_stamp(scope.0)
                .await?
                .ok_or_else(|| StorageError::table_not_found(scope.0))?;
            if stamp.history_id != scope.1 {
                return Err(storage_types::StorageEnum::HistoryUnavailable.into());
            }
            let physical = attempt.read_table_at_stamp(scope.0, stamp).await?;
            let state = self
                .observe_capture_state(attempt, &physical, ReadProtection::Snapshot)
                .await?
                .ok_or_else(corrupt_record)?;
            Ok((state.state(), scope.2).into())
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) fn require_view_receiver(
        &self,
        request: &CanonicalViewRequest,
    ) -> StorageResult<()> {
        self.partitions
            .require_partition(request.receiver.journal_partition)
            .map_err(|_| StorageError::validation("Invalid canonical view receiver partition"))?;
        if request.receiver != self.origin(request.receiver.journal_partition)? {
            return Err(StorageError::validation(
                "Canonical view receiver generation does not match",
            ));
        }
        if let crate::history::selection::CanonicalViewSelection::Item { key } = &request.selection
            && self.partitions.select(request.lineage, key) != request.receiver.journal_partition
        {
            return Err(StorageError::validation(
                "Canonical item view uses another receiver partition",
            ));
        }
        Ok(())
    }

    pub(in crate::journal) async fn load_view_record<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        selection: (
            &CanonicalViewRequest,
            DeliveryPosition,
            &super::public_trim_authority::PublicTrimAuthority,
        ),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<Option<CanonicalViewRecord>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (request, position, authority) = selection;
            authority.require_lineage(request.lineage)?;
            let (pointer, event) = self
                .load_receiver_original(
                    attempt,
                    (&request.receiver, position),
                    (encoding.1, ReadProtection::Snapshot),
                )
                .await?;
            if pointer.lineage != request.lineage {
                return Ok(None);
            }
            if !self
                .is_receiver_original_visible(attempt, authority, &event, encoding)
                .await?
                || !request.selection.includes(&event)
            {
                return Ok(None);
            }
            let original = String::from_utf8(event.as_ref().bytes.as_ref().to_vec())
                .map_err(|_| corrupt_record())?;
            Ok(Some(CanonicalViewRecord { pointer, original }))
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn load_receiver_original<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        selection: (
            &replication_types::identity::origin::OriginId,
            DeliveryPosition,
        ),
        input: (&CanonicalJsonCodec, ReadProtection),
    ) -> StorageResult<(CanonicalViewPointer, CanonicalEvent)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (receiver, position) = selection;
            let (codec, protection) = input;
            self.require_receiver_scope(receiver, position)?;
            let key = self.canonical_view_key(receiver.journal_partition, position)?;
            let row = self
                .load_immutable_waves(attempt, std::slice::from_ref(&key), protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let pointer = self.decode_receiver_pointer(&row)?;
            if &pointer.receiver != receiver || pointer.position != position || row.key != key {
                return Err(corrupt_record());
            }
            let (accepted, delivery) = self
                .load_acceptance_with_delivery(attempt, &pointer.event, codec, protection)
                .await?
                .ok_or_else(corrupt_record)?;
            if delivery != pointer || accepted.event.as_ref().digest != pointer.digest {
                return Err(corrupt_record());
            }
            Ok((pointer, accepted.event))
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) fn decode_receiver_pointer(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<CanonicalViewPointer> {
        let pointer: CanonicalViewPointer = self.records.decode(row)?;
        self.require_receiver_scope(&pointer.receiver, pointer.position)?;
        if row.revision != storage_types::Revision::INITIAL
            || row.key
                != self.canonical_view_key(pointer.receiver.journal_partition, pointer.position)?
        {
            return Err(corrupt_record());
        }
        Ok(pointer)
    }

    fn require_receiver_scope(
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

    fn canonical_view_key(
        &self,
        partition: JournalPartition,
        position: DeliveryPosition,
    ) -> StorageResult<DurableRecordKey> {
        let mut key: RecordAddress = self.identity_key(CANONICAL_VIEW_SUFFIX, &partition)?.into();
        key.suffix.extend_from_slice(&position.bytes());
        key.try_into()
    }
}
