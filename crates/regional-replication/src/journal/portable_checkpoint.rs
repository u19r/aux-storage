//! Immutable source-vector receipts and lineage-indexed native reader holds.
use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecord};
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageError, StorageResult, TableName};

use super::{
    JournalStore,
    reader_hold::{ReaderHoldKind, ReaderHoldReference},
};
use crate::{
    history::{
        checkpoint::{CanonicalCheckpointId, CanonicalCheckpointState, CheckpointCreation},
        portable_checkpoint::{PortableCheckpointAction, PortableCheckpointRequest},
        table_cut::{CanonicalTableCut, CanonicalTableCutAction, CanonicalTableCutRequest},
    },
    native_record::corrupt_record,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PortableRecord {
    operation: CanonicalCheckpointId,
    table: TableName,
    incarnation: super::public_trim_authority::PublicTrimIncarnation,
    creation: CheckpointCreation,
    input_digest: Option<CanonicalDigest>,
    #[serde(with = "super::checkpoint_state")]
    state: CanonicalCheckpointState,
    cut: CanonicalTableCut,
    retention: crate::history::retention::CheckpointRetention,
}

pub(in crate::journal) struct PortableReadScope<'a> {
    pub table: &'a TableName,
    pub lineage: replication_types::identity::resource::TableLineage,
    pub membership: replication_types::configuration::membership::MembershipRevision,
}

impl JournalStore {
    pub(in crate::journal) async fn observe_source_hold<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        reference: ReaderHoldReference,
        protection: ReadProtection,
    ) -> StorageResult<crate::history::hold_discovery::ReaderHoldObservation> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            if reference.kind != ReaderHoldKind::Source {
                return Err(corrupt_record());
            }
            let key = self.identity_key(
                crate::constants::PORTABLE_CHECKPOINT_SUFFIX,
                &reference.operation,
            )?;
            let row = self
                .records
                .load(attempt, &[key], protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let record = self.decode_portable_record(&row, reference.operation)?;
            self.load_portable_authority(attempt, &record, protection)
                .await?;
            if record.cut.lineage != reference.lineage {
                return Err(corrupt_record());
            }
            if record.state != CanonicalCheckpointState::Held {
                return Err(corrupt_record());
            }
            Ok(
                crate::history::hold_discovery::ReaderHoldObservation::Source {
                    operation: record.operation,
                    retention: record.retention,
                    cut: record.cut,
                },
            )
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn expire_source_hold<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        entry: super::reader_expiry::ReaderExpiryEntry,
        observed_at: storage_types::TimestampMillis,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            if entry.reference.kind != ReaderHoldKind::Source {
                return Err(corrupt_record());
            }
            let key = self.identity_key(
                crate::constants::PORTABLE_CHECKPOINT_SUFFIX,
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
            let mut record = self.decode_portable_record(&row, entry.reference.operation)?;
            self.load_portable_authority(attempt, &record, ReadProtection::Conflict)
                .await?;
            if record.cut.lineage != entry.reference.lineage
                || record.retention
                    != crate::history::retention::CheckpointRetention::Until(entry.deadline)
            {
                return Err(corrupt_record());
            }
            if !record
                .retention
                .expire_state(&mut record.state, observed_at)
            {
                return Err(corrupt_record());
            }
            let change = self.records.prepare(key, Some(row.revision), &record)?;
            self.store_terminal_reader(attempt, (entry.reference, record.retention, change))
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn change_portable_checkpoint<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&PortableCheckpointRequest, storage_types::TimestampMillis),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<Option<crate::history::portable_checkpoint::PortableCheckpointObservation>>
    {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (request, observed_at) = input;
            let key = self.identity_key(
                crate::constants::PORTABLE_CHECKPOINT_SUFFIX,
                &request.portable_checkpoint,
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
            let (mut record, revision) = match row {
                Some(row) => {
                    let record = self.decode_portable_record(&row, request.portable_checkpoint)?;
                    self.require_portable_request(&record, request)?;
                    if record.state == CanonicalCheckpointState::Held {
                        self.load_portable_authority(attempt, &record, ReadProtection::Conflict)
                            .await?;
                        self.require_reader_hold(
                            attempt,
                            ReaderHoldReference {
                                kind: ReaderHoldKind::Source,
                                lineage: record.cut.lineage,
                                operation: record.operation,
                            },
                        )
                        .await?;
                    }
                    (record, Some(row.revision))
                }
                None if request.action.creation().is_some() => {
                    request.retention.require_creation(observed_at)?;
                    (
                        self.prepare_portable_checkpoint(attempt, (request, observed_at), encoding)
                            .await?,
                        None,
                    )
                }
                None => return Ok(None),
            };
            let expired = record
                .retention
                .expire_state(&mut record.state, observed_at);
            let release = matches!(request.action, PortableCheckpointAction::Release)
                && record.state == CanonicalCheckpointState::Held;
            if release {
                record.state = CanonicalCheckpointState::Released;
            }
            if revision.is_none() || release || expired {
                self.store_source_checkpoint(attempt, (key, revision, &record))
                    .await?;
            }
            if let Some(capacity) = capacity {
                self.records
                    .require_discretionary_capacity(attempt, capacity)
                    .await?;
            }
            let observation = crate::history::portable_checkpoint::PortableCheckpointObservation {
                operation: record.operation,
                state: record.state,
                cut: record.cut,
                retention: record.retention,
            };
            Ok(Some(observation))
        }
        .await;
        let result = result.and_then(|observation| {
            crate::history::response_budget::encoded_length(
                &observation,
                encoding.0.frame_bytes.get(),
            )?;
            Ok(observation)
        });
        settlement.complete(crate::history::retention::settle_expiry(result))?
    }

    async fn prepare_portable_checkpoint<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&PortableCheckpointRequest, storage_types::TimestampMillis),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<PortableRecord> {
        let (request, observed_at) = input;
        self.release_replaced_source(attempt, request, observed_at)
            .await?;
        let stamp = attempt
            .read_catalog_stamp(&request.table)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&request.table))?;
        let physical = attempt.read_table_at_stamp(&request.table, stamp).await?;
        let observed = self
            .observe_history_table(attempt, &physical, ReadProtection::Conflict)
            .await?;
        if observed.state().configuration.lineage != request.lineage {
            self.require_installed_history_lineage(
                attempt,
                observed.state(),
                (request.lineage, request.membership),
                ReadProtection::Conflict,
            )
            .await?;
        } else if observed.state().membership != request.membership {
            return Err(StorageError::validation(
                "Portable checkpoint table revision changed",
            ));
        }
        let action = match &request.action {
            PortableCheckpointAction::CreateCapture
            | PortableCheckpointAction::RolloverCapture { .. } => {
                CanonicalTableCutAction::CaptureHistorical {
                    lineage: request.lineage,
                    membership: request.membership,
                }
            }
            PortableCheckpointAction::CreateImport(cut) => {
                CanonicalTableCutAction::Verify(cut.clone())
            }
            _ => return Err(corrupt_record()),
        };
        let vector_request = CanonicalTableCutRequest {
            serving_region: request.serving_region.clone(),
            serving_group: request.serving_group,
            table: request.table.clone(),
            table_cut: action,
        };
        let cut = self
            .load_table_source_vector(attempt, &vector_request, encoding, ReadProtection::Conflict)
            .await?;
        if cut.lineage != request.lineage || cut.membership != request.membership {
            return Err(StorageError::validation(
                "Portable checkpoint source vector does not match its selection",
            ));
        }
        let ranges = cut
            .sources
            .iter()
            .map(crate::history::source_retention::SourceRetentionRange::Source)
            .collect::<Vec<_>>();
        self.require_reservation_admission(
            attempt,
            super::trim_reservation::ReservationAdmission::Sources(&ranges),
        )
        .await?;
        self.retain_reader_hold(
            attempt,
            ReaderHoldReference {
                kind: ReaderHoldKind::Source,
                lineage: request.lineage,
                operation: request.portable_checkpoint,
            },
            request.retention,
        )
        .await?;
        Ok(PortableRecord {
            operation: request.portable_checkpoint,
            table: request.table.clone(),
            incarnation: (observed.state(), request.lineage).into(),
            creation: request.action.creation().ok_or_else(corrupt_record)?,
            input_digest: self.portable_creation_digest(&request.action)?,
            state: CanonicalCheckpointState::Held,
            cut,
            retention: request.retention,
        })
    }

    pub(in crate::journal) async fn load_portable_checkpoint_cut<
        A: CatalogAttempt + PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        selection: PortableReadScope<'_>,
        operation: CanonicalCheckpointId,
        observed_at: storage_types::TimestampMillis,
    ) -> StorageResult<CanonicalTableCut> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key =
                self.identity_key(crate::constants::PORTABLE_CHECKPOINT_SUFFIX, &operation)?;
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
            let mut record = self.decode_portable_record(&row, operation)?;
            if record.table != *selection.table
                || record.cut.lineage != selection.lineage
                || record.cut.membership != selection.membership
            {
                return Err(StorageError::validation(
                    "Portable checkpoint does not match the history selection",
                ));
            }
            let authority = if record.state == CanonicalCheckpointState::Held {
                let authority = self
                    .load_portable_authority(attempt, &record, ReadProtection::Conflict)
                    .await?;
                self.require_reader_hold(
                    attempt,
                    ReaderHoldReference {
                        kind: ReaderHoldKind::Source,
                        lineage: selection.lineage,
                        operation,
                    },
                )
                .await?;
                Some(authority)
            } else {
                None
            };
            if record
                .retention
                .expire_state(&mut record.state, observed_at)
            {
                self.store_source_checkpoint(attempt, (key, Some(row.revision), &record))
                    .await?;
            }
            if record.state == CanonicalCheckpointState::Expired {
                return Err(storage_types::StorageEnum::HistoryExpired.into());
            }
            if record.state != CanonicalCheckpointState::Held {
                return Err(storage_types::StorageEnum::HistoryUnavailable.into());
            }
            let current = self
                .qualify_history_incarnation(
                    attempt,
                    (
                        selection.table,
                        record.incarnation.physical,
                        selection.lineage,
                    ),
                )
                .await?;
            authority
                .ok_or_else(corrupt_record)?
                .require_incarnation(current)?;
            Ok(record.cut)
        }
        .await;
        settlement.complete(crate::history::retention::settle_expiry(result))?
    }

    async fn load_portable_authority<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &PortableRecord,
        protection: ReadProtection,
    ) -> StorageResult<super::public_trim_authority::PublicTrimAuthority> {
        let authority = self
            .load_public_trim_head(attempt, record.incarnation, protection)
            .await?;
        authority.require_captured_revision(record.cut.trim)?;
        Ok(authority)
    }

    fn decode_portable_record(
        &self,
        row: &DurableRecord,
        operation: CanonicalCheckpointId,
    ) -> StorageResult<PortableRecord> {
        let record: PortableRecord = self.records.decode(row)?;
        if record.operation != operation
            || row.key
                != self.identity_key(crate::constants::PORTABLE_CHECKPOINT_SUFFIX, &operation)?
        {
            return Err(corrupt_record());
        }
        record
            .creation
            .require_distinct(operation)
            .map_err(|_| corrupt_record())?;
        record.incarnation.require_cut(&record.cut)?;
        Ok(record)
    }

    fn require_portable_request(
        &self,
        record: &PortableRecord,
        request: &PortableCheckpointRequest,
    ) -> StorageResult<()> {
        if record.retention != request.retention {
            return Err(StorageError::validation(
                "Portable checkpoint retention policy changed",
            ));
        }
        if record.table != request.table
            || record.cut.lineage != request.lineage
            || record.cut.membership != request.membership
        {
            return Err(StorageError::validation(
                "Portable checkpoint operation does not match its selection",
            ));
        }
        if let Some(creation) = request.action.creation()
            && (creation != record.creation
                || record.input_digest != self.portable_creation_digest(&request.action)?)
        {
            return Err(StorageError::validation(
                "Portable checkpoint creation input changed",
            ));
        }
        Ok(())
    }

    fn portable_creation_digest(
        &self,
        action: &PortableCheckpointAction,
    ) -> StorageResult<Option<CanonicalDigest>> {
        match action {
            PortableCheckpointAction::CreateImport(cut) => {
                self.records.content_digest(cut).map(Some)
            }
            _ => Ok(None),
        }
    }

    async fn release_replaced_source<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &PortableCheckpointRequest,
        observed_at: storage_types::TimestampMillis,
    ) -> StorageResult<()> {
        let PortableCheckpointAction::RolloverCapture { previous } = request.action else {
            return Ok(());
        };
        let key = self.identity_key(crate::constants::PORTABLE_CHECKPOINT_SUFFIX, &previous)?;
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
        let mut record = self.decode_portable_record(&row, previous)?;
        if record.table != request.table {
            return Err(StorageError::validation(
                "Checkpoint rollover must preserve table",
            ));
        }
        if record.state == CanonicalCheckpointState::Held {
            self.load_portable_authority(attempt, &record, ReadProtection::Conflict)
                .await?;
            self.require_reader_hold(
                attempt,
                ReaderHoldReference {
                    kind: ReaderHoldKind::Source,
                    lineage: record.cut.lineage,
                    operation: previous,
                },
            )
            .await?;
        }
        if record
            .retention
            .expire_state(&mut record.state, observed_at)
        {
            self.store_source_checkpoint(attempt, (key.clone(), Some(row.revision), &record))
                .await?;
        }
        if record.state == CanonicalCheckpointState::Expired {
            return Err(storage_types::StorageEnum::HistoryExpired.into());
        }
        if record.state != CanonicalCheckpointState::Held {
            return Err(storage_types::StorageEnum::HistoryUnavailable.into());
        }
        record.state = CanonicalCheckpointState::Released;
        self.store_source_checkpoint(attempt, (key, Some(row.revision), &record))
            .await
    }

    async fn store_source_checkpoint<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (
            infrastructure_provider::record::DurableRecordKey,
            Option<storage_types::Revision>,
            &PortableRecord,
        ),
    ) -> StorageResult<()> {
        let (key, revision, record) = input;
        let change = self.records.prepare(key, revision, record)?;
        if record.state == CanonicalCheckpointState::Held {
            return self.records.store(attempt, &[change]).await;
        }
        let reference = ReaderHoldReference {
            kind: ReaderHoldKind::Source,
            lineage: record.cut.lineage,
            operation: record.operation,
        };
        self.store_terminal_reader(attempt, (reference, record.retention, change))
            .await
    }
}
