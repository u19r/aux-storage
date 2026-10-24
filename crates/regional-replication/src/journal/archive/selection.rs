use std::sync::Arc;

use infrastructure_provider::{
    page::RecordPageRequest,
    record::{DurableRecord, DurableRecordKey},
};
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::protocol::CanonicalDigest;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    archive::{
        ArchiveCursor, ArchiveHold, ArchiveKind, ArchivePlan, ArchivePointer, ArchiveState,
        ArchiveTurn,
    },
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn prepare_archive_turn<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        kind: ArchiveKind,
        encoding: (
            &replication_provider::runtime_configuration::limits::WorkLimits,
            &CanonicalJsonCodec,
        ),
    ) -> StorageResult<ArchiveTurn> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let codec = encoding.1;
            let family = self
                .load_archive_state(attempt, kind, ReadProtection::Conflict)
                .await?;
            match family.state {
                ArchiveState::Pending(hold) => self
                    .load_archive_plan(attempt, hold, codec)
                    .await
                    .map(|plan| ArchiveTurn::Export(Box::new(plan))),
                ArchiveState::Qualifying { hold, scan } => {
                    self.qualify_archive_selection(attempt, (family.row, hold, *scan), encoding)
                        .await
                }
                ArchiveState::Scanning(_) => {
                    self.scan_archive_family(
                        attempt,
                        (family.row, family.cursor_row, family.cursor),
                        encoding,
                    )
                    .await
                }
            }
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn scan_archive_family<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        selected: (DurableRecord, DurableRecord, ArchiveCursor),
        encoding: (
            &replication_provider::runtime_configuration::limits::WorkLimits,
            &CanonicalJsonCodec,
        ),
    ) -> StorageResult<ArchiveTurn> {
        let (root, scan_row, cursor) = selected;
        let kind = cursor.kind;
        let key = root.key.clone();
        let capacity = self.records.observe_discretionary_capacity(attempt).await?;
        let first = self.record_key(cursor.kind.prefix().to_vec())?;
        let range = self.record_prefix_range(&first)?;
        let (first, last, _) = range.bounds();
        let range = infrastructure_provider::page::BoundedRecordRange::try_new(
            first.clone(),
            last.clone(),
            std::num::NonZeroUsize::MIN,
        )?;
        let page = self
            .records
            .scan(
                attempt,
                RecordPageRequest {
                    range,
                    after: cursor.after.clone(),
                    max_bytes: self.records.limits.max_bytes(),
                    protection: ReadProtection::Conflict,
                },
            )
            .await?;
        let finished = page.records.is_empty() && cursor.kind.next() == ArchiveKind::Event;
        let mut changes = Vec::with_capacity(2);
        let next = match page.records.into_iter().next() {
            Some(record) => {
                let hold = self
                    .prepare_archive_selection(attempt, (cursor, record), encoding.1)
                    .await?;
                let scan = self.begin_archive_admission(attempt, &hold).await?;
                ArchiveState::Qualifying {
                    hold,
                    scan: Box::new(scan),
                }
            }
            None => {
                let reset = ArchiveCursor { kind, after: None };
                changes.push(self.records.prepare(
                    scan_row.key,
                    Some(scan_row.revision),
                    &reset,
                )?);
                ArchiveState::Scanning(kind)
            }
        };
        changes.push(self.records.prepare(key, Some(root.revision), &next)?);
        self.records.store(attempt, &changes).await?;
        self.records
            .require_discretionary_capacity(attempt, capacity)
            .await?;
        Ok(if finished {
            ArchiveTurn::Idle
        } else {
            ArchiveTurn::Advanced
        })
    }

    async fn qualify_archive_selection<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        selected: (
            DurableRecord,
            ArchiveHold,
            crate::archive::admission::ArchiveAdmissionScan,
        ),
        encoding: (
            &replication_provider::runtime_configuration::limits::WorkLimits,
            &CanonicalJsonCodec,
        ),
    ) -> StorageResult<ArchiveTurn> {
        let (root, hold, mut scan) = selected;
        let capacity = self.records.observe_discretionary_capacity(attempt).await?;
        let work = self
            .advance_archive_admission(attempt, (&hold, &mut scan), encoding)
            .await?;
        if work == crate::archive::admission::ArchiveAdmissionWork::Waiting {
            return Ok(ArchiveTurn::Idle);
        }
        if work == crate::archive::admission::ArchiveAdmissionWork::Advanced {
            let state = ArchiveState::Qualifying {
                hold,
                scan: Box::new(scan),
            };
            self.records
                .store(
                    attempt,
                    &[self
                        .records
                        .prepare(root.key, Some(root.revision), &state)?],
                )
                .await?;
            self.records
                .require_discretionary_capacity(attempt, capacity)
                .await?;
            return Ok(ArchiveTurn::Advanced);
        }
        let plan = self
            .load_archive_plan(attempt, hold.clone(), encoding.1)
            .await?;
        let state = ArchiveState::Pending(hold.clone());
        let changes = [
            self.records
                .prepare(root.key, Some(root.revision), &state)?,
            self.records.prepare(
                self.archive_hold_key(hold.cursor.kind)?,
                None,
                &super::super::reader_hold::RetentionHoldReference::Archive(hold),
            )?,
        ];
        self.retain_journal_epoch(attempt).await?;
        self.records.store(attempt, &changes).await?;
        self.records
            .require_discretionary_capacity(attempt, capacity)
            .await?;
        Ok(ArchiveTurn::Export(Box::new(plan)))
    }

    async fn prepare_archive_selection<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (ArchiveCursor, DurableRecord),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<ArchiveHold> {
        let (cursor, record) = input;
        if record.revision != storage_types::Revision::INITIAL {
            return Err(corrupt_record());
        }
        self.require_archive_metadata(&record, codec)?;
        if let Some(pointer) = self
            .load_archive_pointer(attempt, &record, ReadProtection::Conflict)
            .await?
        {
            let hold = ArchiveHold::new(cursor, pointer);
            self.require_archive_hold(&hold)?;
            return Ok(hold);
        }
        let metadata = Arc::clone(&record.bytes);
        let bytes: Arc<[u8]> = if cursor.kind.is_payload() {
            self.load_retained_bytes(attempt, &record, codec).await?
        } else {
            Arc::clone(&metadata)
        };
        if cursor.kind.is_payload() {
            self.require_archive_original_identity(&record, &bytes, codec)?;
        }
        let pointer = ArchivePointer {
            key: record.key.clone(),
            revision: record.revision,
            manifest: CanonicalDigest::of(&metadata),
            metadata_length: metadata.len(),
            content: CanonicalDigest::of(&bytes),
            length: bytes.len(),
        };
        let hold = ArchiveHold::new(cursor, pointer);
        self.require_archive_hold(&hold)?;
        Ok(hold)
    }

    async fn load_archive_plan<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        hold: ArchiveHold,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<ArchivePlan> {
        self.require_archive_hold(&hold)?;
        let record = match self
            .records
            .load(
                attempt,
                std::slice::from_ref(&hold.pointer.key),
                ReadProtection::Conflict,
            )
            .await?
            .pop()
            .flatten()
        {
            Some(record) => record,
            None => self.load_archive_record(&hold.pointer).await?,
        };
        self.prepare_archive_plan(attempt, hold, record, codec)
            .await
    }

    async fn prepare_archive_plan<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        hold: ArchiveHold,
        record: DurableRecord,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<ArchivePlan> {
        self.require_archive_hold(&hold)?;
        hold.pointer.require_record(&record)?;
        self.require_archive_metadata(&record, codec)?;
        if hold.cursor.kind.has_publication_coverage() {
            return self
                .prepare_archive_publication(attempt, hold, record, codec)
                .await;
        }
        let manifest = self.decode_retained_manifest(&record, codec)?;
        if hold.pointer.length != manifest.original_length() {
            return Err(corrupt_record());
        }
        let bytes = self.load_retained_bytes(attempt, &record, codec).await?;
        self.require_archive_original_identity(&record, &bytes, codec)?;
        ArchivePlan::try_new(hold, record, bytes)
    }

    fn require_archive_original_identity(
        &self,
        record: &DurableRecord,
        bytes: &Arc<[u8]>,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<()> {
        match ArchiveKind::from_key(&record.key) {
            Some(ArchiveKind::Event) => {
                self.decode_retained_event(record, bytes, codec)?;
            }
            Some(ArchiveKind::Baseline) => {
                self.decode_retained_baseline(record, Arc::clone(bytes), codec)?;
            }
            _ => return Err(corrupt_record()),
        }
        Ok(())
    }

    pub(super) fn require_archive_metadata(
        &self,
        record: &DurableRecord,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<()> {
        match ArchiveKind::from_key(&record.key) {
            Some(ArchiveKind::CollectionBaseline) => {
                self.require_collected_baseline_definition(record)?
            }
            Some(ArchiveKind::CollectionRetainedDefinition) => {
                self.require_collected_retained_definition(record)?
            }
            Some(ArchiveKind::CollectionRetainedCut) => {
                self.decode_collected_retained_cut(record)?;
            }
            Some(ArchiveKind::CollectionCatalog) => {
                self.decode_collected_catalog_page(record)?;
            }
            Some(ArchiveKind::CollectionRetiredPage) => {
                self.decode_collected_retired_index(record)?;
            }
            Some(ArchiveKind::CollectionDeclaration) => {
                self.decode_source_collection_declaration(record)?;
            }
            Some(ArchiveKind::CollectionDefinition) => {
                self.decode_collected_history_definition(record)?;
            }
            Some(ArchiveKind::CollectionPage) => {
                self.decode_collected_history_page(record)?;
            }
            Some(ArchiveKind::ImportManifest) => {
                self.decode_import_manifest_record(record, codec)?;
            }
            Some(ArchiveKind::ImportPage) => {
                self.decode_import_page_record(record)?;
            }
            Some(ArchiveKind::RetainedPosition) => {
                self.decode_retained_position_record(record)?;
            }
            _ => {}
        }
        Ok(())
    }

    pub(in crate::journal) fn require_archive_hold(&self, hold: &ArchiveHold) -> StorageResult<()> {
        hold.require_publication()?;
        self.require_archive_cursor(&hold.cursor)?;
        self.archive_record_size(&hold.pointer)?;
        let address = hold.pointer.key.as_ref();
        if !address.suffix.starts_with(hold.cursor.kind.prefix())
            || hold
                .cursor
                .after
                .as_ref()
                .is_some_and(|after| after >= &hold.pointer.key)
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(super) fn require_archive_cursor(&self, cursor: &ArchiveCursor) -> StorageResult<()> {
        if let Some(after) = &cursor.after
            && (self.record_key(after.as_ref().suffix.clone())? != *after
                || !after.as_ref().suffix.starts_with(cursor.kind.prefix()))
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(in crate::journal) fn archive_pointer_key(
        &self,
        key: &DurableRecordKey,
    ) -> StorageResult<DurableRecordKey> {
        if self.record_key(key.as_ref().suffix.clone())? != *key {
            return Err(corrupt_record());
        }
        let mut suffix = crate::constants::ARCHIVE_POINTER_SUFFIX.to_vec();
        suffix.extend_from_slice(&key.as_ref().suffix);
        self.record_key(suffix)
    }
}
