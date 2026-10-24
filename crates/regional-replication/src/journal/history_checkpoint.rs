//! One immutable receiver cut and its durable retention state per operation.

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use serde::{Deserialize, Serialize};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageError, StorageResult};

use super::{
    JournalStore,
    reader_hold::{ReaderHoldKind, ReaderHoldReference},
};
use crate::{
    history::{
        checkpoint::{
            CanonicalCheckpointAction, CanonicalCheckpointId, CanonicalCheckpointObservation,
            CanonicalCheckpointRequest, CanonicalCheckpointState, CheckpointCreation,
        },
        selection::CanonicalViewCursor,
        view::{CanonicalViewRequest, DeliveryPosition},
    },
    native_record::corrupt_record,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointRecord {
    operation: CanonicalCheckpointId,
    creation: CheckpointCreation,
    scope: CanonicalViewCursor,
    physical: storage_types::TableHistoryId,
    visibility: super::public_trim_authority::CapturedPublicTrimAuthority,
    high: DeliveryPosition,
    #[serde(with = "super::checkpoint_state")]
    state: CanonicalCheckpointState,
    retention: crate::history::retention::CheckpointRetention,
}

pub(in crate::journal) struct ReceiverCheckpointBounds {
    pub(in crate::journal) scope: CanonicalViewCursor,
    pub(in crate::journal) high: DeliveryPosition,
    pub(in crate::journal) authority: super::public_trim_authority::PublicTrimAuthority,
}

impl CheckpointRecord {
    fn release_if_requested(&mut self, action: CanonicalCheckpointAction) -> bool {
        match (action, self.state) {
            (CanonicalCheckpointAction::Release, CanonicalCheckpointState::Held) => {
                self.state = CanonicalCheckpointState::Released;
                true
            }
            _ => false,
        }
    }
}

impl JournalStore {
    pub(in crate::journal) async fn observe_receiver_hold<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        reference: ReaderHoldReference,
        protection: ReadProtection,
    ) -> StorageResult<crate::history::hold_discovery::ReaderHoldObservation> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let key = self.identity_key(
                crate::constants::CANONICAL_CHECKPOINT_SUFFIX,
                &reference.operation,
            )?;
            let row = self
                .records
                .load(attempt, &[key], protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let (checkpoint, range) = self.decode_receiver_hold(&row, reference)?;
            if checkpoint.state != CanonicalCheckpointState::Held {
                return Err(corrupt_record());
            }
            self.load_receiver_authority(attempt, &checkpoint, protection)
                .await?;
            Ok(
                crate::history::hold_discovery::ReaderHoldObservation::Receiver {
                    operation: checkpoint.operation,
                    retention: checkpoint.retention,
                    range,
                },
            )
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn expire_receiver_hold<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        entry: super::reader_expiry::ReaderExpiryEntry,
        observed_at: storage_types::TimestampMillis,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let key = self.identity_key(
                crate::constants::CANONICAL_CHECKPOINT_SUFFIX,
                &entry.reference.operation,
            )?;
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let (mut checkpoint, _) = self.decode_receiver_hold(&row, entry.reference)?;
            self.load_receiver_authority(attempt, &checkpoint, ReadProtection::Conflict)
                .await?;
            if checkpoint.retention
                != crate::history::retention::CheckpointRetention::Until(entry.deadline)
            {
                return Err(corrupt_record());
            }
            if !checkpoint
                .retention
                .expire_state(&mut checkpoint.state, observed_at)
            {
                return Err(corrupt_record());
            }
            let change = self.records.prepare(key, Some(row.revision), &checkpoint)?;
            self.store_terminal_reader(attempt, (entry.reference, checkpoint.retention, change))
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    fn decode_receiver_hold(
        &self,
        row: &infrastructure_provider::record::DurableRecord,
        reference: ReaderHoldReference,
    ) -> StorageResult<(
        CheckpointRecord,
        crate::history::hold_discovery::ReceiverHoldRange,
    )> {
        if reference.kind != ReaderHoldKind::Receiver {
            return Err(corrupt_record());
        }
        let checkpoint = self.decode_receiver_checkpoint(row, reference.operation)?;
        let range = checkpoint.scope.retained_partition(checkpoint.high)?;
        if range.lineage != reference.lineage {
            return Err(corrupt_record());
        }
        Ok((checkpoint, range))
    }

    pub(crate) async fn change_canonical_checkpoint<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            &CanonicalCheckpointRequest,
            (
                &replication_provider::runtime_configuration::limits::WorkLimits,
                &CanonicalJsonCodec,
            ),
            storage_types::TimestampMillis,
        ),
    ) -> StorageResult<Option<CanonicalCheckpointObservation>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (request, encoding, observed_at) = input;
            self.require_view_receiver(&request.view)?;
            let key = self.identity_key(
                crate::constants::CANONICAL_CHECKPOINT_SUFFIX,
                &request.operation,
            )?;
            let protection = ReadProtection::Conflict;
            let row = self
                .records
                .load(attempt, std::slice::from_ref(&key), protection)
                .await?
                .pop()
                .flatten();
            let capacity = if row.is_none() {
                Some(self.records.observe_discretionary_capacity(attempt).await?)
            } else {
                None
            };
            let (mut checkpoint, revision) = match row {
                Some(row) => {
                    let checkpoint =
                        self.decode_canonical_checkpoint(&row, &request.view, request.operation)?;
                    if request
                        .action
                        .creation()
                        .is_some_and(|creation| creation != checkpoint.creation)
                    {
                        return Err(StorageError::validation(
                            "Canonical checkpoint creation input changed",
                        ));
                    }
                    if checkpoint.retention != request.retention {
                        return Err(StorageError::validation(
                            "Canonical checkpoint retention policy changed",
                        ));
                    }
                    if checkpoint.state == CanonicalCheckpointState::Held {
                        self.load_receiver_authority(attempt, &checkpoint, protection)
                            .await?;
                        self.require_reader_hold(
                            attempt,
                            ReaderHoldReference {
                                kind: ReaderHoldKind::Receiver,
                                lineage: request.view.lineage,
                                operation: request.operation,
                            },
                        )
                        .await?;
                    }
                    (checkpoint, Some(row.revision))
                }
                None if request.action.creation().is_some() => {
                    request.retention.require_creation(observed_at)?;
                    (
                        self.prepare_canonical_checkpoint(
                            attempt,
                            (request, encoding, observed_at),
                        )
                        .await?,
                        None,
                    )
                }
                None => return Ok(None),
            };
            let expired = checkpoint
                .retention
                .expire_state(&mut checkpoint.state, observed_at);
            let release = checkpoint.release_if_requested(request.action);
            if revision.is_none() || release || expired {
                self.store_receiver_checkpoint(attempt, (key, revision, &checkpoint))
                    .await?;
            }
            if let Some(capacity) = capacity {
                self.records
                    .require_discretionary_capacity(attempt, capacity)
                    .await?;
            }
            let observation = CanonicalCheckpointObservation {
                operation: checkpoint.operation,
                state: checkpoint.state,
                through: checkpoint
                    .scope
                    .checkpoint_continuation(checkpoint.high, checkpoint.operation),
                retention: checkpoint.retention,
            };
            Ok(Some(observation))
        }
        .await;
        let result = result.and_then(|observation| {
            crate::history::response_budget::encoded_length(
                &observation,
                input.1.0.frame_bytes.get(),
            )?;
            Ok(observation)
        });
        settlement.complete(crate::history::retention::settle_expiry(result))?
    }

    async fn prepare_canonical_checkpoint<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            &CanonicalCheckpointRequest,
            (
                &replication_provider::runtime_configuration::limits::WorkLimits,
                &CanonicalJsonCodec,
            ),
            storage_types::TimestampMillis,
        ),
    ) -> StorageResult<CheckpointRecord> {
        let (request, encoding, observed_at) = input;
        self.release_replaced_receiver(attempt, request, observed_at)
            .await?;
        let selected = self
            .qualify_view_selection(attempt, &request.view, ReadProtection::Conflict)
            .await?;
        let high = self
            .observe_delivery_position(
                attempt,
                request.view.receiver.journal_partition,
                ReadProtection::Conflict,
            )
            .await?;
        if high != DeliveryPosition::from(0) {
            self.load_view_record(
                attempt,
                (&request.view, high, &selected.authority),
                encoding,
            )
            .await?;
        }
        self.require_reservation_admission(
            attempt,
            super::trim_reservation::ReservationAdmission::Incomplete,
        )
        .await?;
        self.retain_reader_hold(
            attempt,
            ReaderHoldReference {
                kind: ReaderHoldKind::Receiver,
                lineage: request.view.lineage,
                operation: request.operation,
            },
            request.retention,
        )
        .await?;
        Ok(CheckpointRecord {
            operation: request.operation,
            creation: request.action.creation().ok_or_else(corrupt_record)?,
            scope: request.view.checkpoint_scope(selected.authority.revision()),
            physical: selected.physical,
            visibility: selected.authority.capture(),
            high,
            state: CanonicalCheckpointState::Held,
            retention: request.retention,
        })
    }

    pub(in crate::journal) async fn load_receiver_checkpoint<
        A: CatalogAttempt + PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        request: &CanonicalViewRequest,
        observed_at: storage_types::TimestampMillis,
    ) -> StorageResult<Option<ReceiverCheckpointBounds>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(operation) = request.checkpoint else {
                return Ok(None);
            };
            let key =
                self.identity_key(crate::constants::CANONICAL_CHECKPOINT_SUFFIX, &operation)?;
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten()
                .ok_or_else(|| {
                    StorageError::from(storage_types::StorageEnum::HistoryUnavailable)
                })?;
            let mut checkpoint = self.decode_canonical_checkpoint(&row, request, operation)?;
            let current = if checkpoint.state == CanonicalCheckpointState::Held {
                let current = self
                    .load_receiver_authority(attempt, &checkpoint, ReadProtection::Conflict)
                    .await?;
                self.require_reader_hold(
                    attempt,
                    ReaderHoldReference {
                        kind: ReaderHoldKind::Receiver,
                        lineage: request.lineage,
                        operation,
                    },
                )
                .await?;
                Some(current)
            } else {
                None
            };
            if checkpoint
                .retention
                .expire_state(&mut checkpoint.state, observed_at)
            {
                self.store_receiver_checkpoint(attempt, (key, Some(row.revision), &checkpoint))
                    .await?;
            }
            if checkpoint.state == CanonicalCheckpointState::Expired {
                return Err(storage_types::StorageEnum::HistoryExpired.into());
            }
            if checkpoint.state != CanonicalCheckpointState::Held {
                return Err(storage_types::StorageEnum::HistoryUnavailable.into());
            }
            let incarnation = self
                .qualify_history_incarnation(
                    attempt,
                    (&request.table, checkpoint.physical, request.lineage),
                )
                .await?;
            let current = current.ok_or_else(corrupt_record)?;
            current.require_incarnation(incarnation)?;
            Ok(Some(ReceiverCheckpointBounds {
                scope: checkpoint
                    .scope
                    .checkpoint_continuation(0u64.into(), operation),
                high: checkpoint.high,
                authority: current,
            }))
        }
        .await;
        settlement.complete(crate::history::retention::settle_expiry(result))?
    }

    async fn load_receiver_authority<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        checkpoint: &CheckpointRecord,
        protection: ReadProtection,
    ) -> StorageResult<super::public_trim_authority::PublicTrimAuthority> {
        let current = self
            .load_public_trim_head(attempt, &checkpoint.visibility, protection)
            .await?;
        self.qualify_captured_public_trim_authority(
            attempt,
            &checkpoint.visibility,
            current,
            protection,
        )
        .await
    }

    fn decode_canonical_checkpoint(
        &self,
        row: &infrastructure_provider::record::DurableRecord,
        request: &CanonicalViewRequest,
        operation: CanonicalCheckpointId,
    ) -> StorageResult<CheckpointRecord> {
        let checkpoint = self.decode_receiver_checkpoint(row, operation)?;
        checkpoint.scope.require_checkpoint_selection(request)?;
        Ok(checkpoint)
    }

    fn decode_receiver_checkpoint(
        &self,
        row: &infrastructure_provider::record::DurableRecord,
        operation: CanonicalCheckpointId,
    ) -> StorageResult<CheckpointRecord> {
        let checkpoint: CheckpointRecord = self.records.decode(row)?;
        if checkpoint.operation != operation
            || row.key
                != self.identity_key(crate::constants::CANONICAL_CHECKPOINT_SUFFIX, &operation)?
        {
            return Err(corrupt_record());
        }
        checkpoint
            .creation
            .require_distinct(operation)
            .map_err(|_| corrupt_record())?;
        let range = checkpoint.scope.retained_partition(checkpoint.high)?;
        checkpoint.visibility.require_checkpoint_binding(
            checkpoint.physical,
            range.lineage,
            range.trim,
        )?;
        if !self.is_local_origin(&range.receiver) {
            return Err(corrupt_record());
        }
        self.partitions
            .require_partition(range.receiver.journal_partition)
            .map_err(|_| corrupt_record())?;
        Ok(checkpoint)
    }

    async fn release_replaced_receiver<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &CanonicalCheckpointRequest,
        observed_at: storage_types::TimestampMillis,
    ) -> StorageResult<()> {
        let CanonicalCheckpointAction::Rollover { previous } = request.action else {
            return Ok(());
        };
        let key = self.identity_key(crate::constants::CANONICAL_CHECKPOINT_SUFFIX, &previous)?;
        let row = self
            .records
            .load(
                attempt,
                std::slice::from_ref(&key),
                ReadProtection::Conflict,
            )
            .await?
            .pop()
            .flatten()
            .ok_or_else(|| StorageError::from(storage_types::StorageEnum::HistoryUnavailable))?;
        let mut checkpoint = self.decode_receiver_checkpoint(&row, previous)?;
        checkpoint.scope.require_rollover_target(&request.view)?;
        let range = checkpoint.scope.retained_partition(checkpoint.high)?;
        if checkpoint.state == CanonicalCheckpointState::Held {
            self.load_receiver_authority(attempt, &checkpoint, ReadProtection::Conflict)
                .await?;
            self.require_reader_hold(
                attempt,
                ReaderHoldReference {
                    kind: ReaderHoldKind::Receiver,
                    lineage: range.lineage,
                    operation: previous,
                },
            )
            .await?;
        }
        if checkpoint
            .retention
            .expire_state(&mut checkpoint.state, observed_at)
        {
            self.store_receiver_checkpoint(attempt, (key.clone(), Some(row.revision), &checkpoint))
                .await?;
        }
        if checkpoint.state == CanonicalCheckpointState::Expired {
            return Err(storage_types::StorageEnum::HistoryExpired.into());
        }
        if checkpoint.state != CanonicalCheckpointState::Held {
            return Err(storage_types::StorageEnum::HistoryUnavailable.into());
        }
        checkpoint.state = CanonicalCheckpointState::Released;
        self.store_receiver_checkpoint(attempt, (key, Some(row.revision), &checkpoint))
            .await
    }

    async fn store_receiver_checkpoint<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (
            infrastructure_provider::record::DurableRecordKey,
            Option<storage_types::Revision>,
            &CheckpointRecord,
        ),
    ) -> StorageResult<()> {
        let (key, revision, checkpoint) = input;
        let change = self.records.prepare(key, revision, checkpoint)?;
        if checkpoint.state == CanonicalCheckpointState::Held {
            return self.records.store(attempt, &[change]).await;
        }
        let lineage = checkpoint
            .scope
            .retained_partition(checkpoint.high)?
            .lineage;
        let reference = ReaderHoldReference {
            kind: ReaderHoldKind::Receiver,
            lineage,
            operation: checkpoint.operation,
        };
        self.store_terminal_reader(attempt, (reference, checkpoint.retention, change))
            .await
    }
}
