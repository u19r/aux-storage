//! Bounded native journal initialization joins its immutable database binding.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange},
};
use replication_types::identity::{journal_partition::JournalPartitionCount, resource::TenantId};
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, Revision, StorageResult};

use super::{JournalStore, native_binding::native_binding_key};
use crate::{
    constants::{ORIGIN_LAYOUT_SUFFIX, REGIONAL_LAYOUT_VERSION},
    native_record::corrupt_record,
};

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LayoutState {
    pub(super) version: u16,
    pub(super) tenant: TenantId,
    pub(super) partitions: JournalPartitionCount,
    pub(super) initialized: u32,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct PartitionQualificationCursor(u32);

struct InitializationPage {
    key: DurableRecordKey,
    revision: Option<Revision>,
    layout: LayoutState,
    binding: Option<RecordChange>,
    epoch: Option<RecordChange>,
}

impl JournalStore {
    pub(super) fn require_initialization_capacity(&self) -> StorageResult<()> {
        let binding = self
            .prepare_native_binding(None, [u8::MAX; 16])?
            .ok_or_else(corrupt_record)?;
        let archive = self.prepare_archive_binding(([u8::MAX; 16], [u8::MAX; 16]))?;
        let epoch = self.prepare_journal_epoch_genesis()?;
        let count = u32::from(self.partitions);
        let layout = self.records.prepare(
            self.record_key(ORIGIN_LAYOUT_SUFFIX.to_vec())?,
            None,
            &LayoutState {
                version: REGIONAL_LAYOUT_VERSION,
                tenant: self.tenant,
                partitions: self.partitions,
                initialized: count,
            },
        )?;
        let partition =
            self.prepare_initial_partition(count.checked_sub(1).ok_or_else(corrupt_record)?)?;
        for change in [&layout, &epoch, &binding, &archive, &partition] {
            self.records
                .limits
                .check_batch(1, self.initialization_change_bytes(change)?)?;
        }
        Ok(())
    }

    /// Reconcile the native binding and advance one bounded initialization
    /// page. Missing committed floors are never recreated after the page
    /// cursor passes them.
    pub(crate) async fn register_layout<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        physical_identity: [u8; 16],
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.record_key(ORIGIN_LAYOUT_SUFFIX.to_vec())?;
            let rows = self
                .records
                .load(
                    attempt,
                    &[
                        key.clone(),
                        native_binding_key()?,
                        self.journal_epoch_key()?,
                    ],
                    ReadProtection::Conflict,
                )
                .await?;
            if rows.len() != 3 {
                return Err(corrupt_record());
            }
            let (layout, revision) = self.decode_initialization_layout(rows[0].as_ref())?;
            let binding = self.prepare_native_binding(rows[1].as_ref(), physical_identity)?;
            let epoch = self.prepare_journal_epoch_registration(rows[2].as_ref(), revision)?;
            if binding.is_some()
                && attempt
                    .has_records_in_namespace(
                        crate::constants::REGIONAL_RECORD_NAMESPACE,
                        ReadProtection::Conflict,
                    )
                    .await?
            {
                return Err(corrupt_record());
            }
            let count = u32::from(self.partitions);
            if layout.initialized == count {
                let (_, count) = self
                    .load_journal_barrier(attempt, ReadProtection::Conflict)
                    .await?;
                self.require_trim_reservation_count(attempt, count, ReadProtection::Conflict)
                    .await?;
                self.records
                    .require_history_capacity_initialized(attempt)
                    .await?;
                return Ok(true);
            }
            let (changes, layout) = self.prepare_initialization_page(InitializationPage {
                key,
                revision,
                layout,
                binding,
                epoch,
            })?;
            self.records.store(attempt, &changes).await?;
            Ok(layout.initialized == count)
        }
        .await;
        settlement.complete(result)
    }

    /// Observe complete authority without resubmitting an uncertain
    /// initialization page.
    pub(crate) async fn observe_initialized_layout<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        physical_identity: [u8; 16],
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let keys = [
                self.record_key(ORIGIN_LAYOUT_SUFFIX.to_vec())?,
                native_binding_key()?,
                self.journal_epoch_key()?,
            ];
            let rows = self
                .records
                .load(attempt, &keys, ReadProtection::Snapshot)
                .await?;
            if rows.len() != keys.len() {
                return Err(corrupt_record());
            }
            if rows.iter().all(Option::is_none) {
                return Ok(false);
            }
            let row = rows[0].as_ref().ok_or_else(corrupt_record)?;
            let (layout, revision) = self.decode_initialization_layout(Some(row))?;
            let binding = rows[1].as_ref().ok_or_else(corrupt_record)?;
            self.prepare_native_binding(Some(binding), physical_identity)?;
            self.prepare_journal_epoch_registration(rows[2].as_ref(), revision)?;
            if layout.initialized != u32::from(self.partitions) {
                return Ok(false);
            }
            let (_, count) = self
                .load_journal_barrier(attempt, ReadProtection::Snapshot)
                .await?;
            self.require_trim_reservation_count(attempt, count, ReadProtection::Snapshot)
                .await?;
            self.records
                .require_history_capacity_initialized(attempt)
                .await?;
            Ok(true)
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn observe_initialized_partition_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        after: PartitionQualificationCursor,
    ) -> StorageResult<Option<PartitionQualificationCursor>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let after = after.0;
            let count = u32::from(self.partitions);
            if after >= count {
                return Err(corrupt_record());
            }
            let limits = self.records.limits;
            let row_bytes = limits
                .max_key_bytes()
                .get()
                .checked_add(limits.max_record_bytes().get())
                .ok_or_else(corrupt_record)?;
            let quantum = limits
                .max_records()
                .get()
                .min(limits.max_bytes().get() / row_bytes)
                .min(usize::try_from(count - after).map_err(|_| corrupt_record())?);
            if quantum == 0 {
                return Err(corrupt_record());
            }
            let through = after
                .checked_add(u32::try_from(quantum).map_err(|_| corrupt_record())?)
                .ok_or_else(corrupt_record)?;
            let partitions = (after..through)
                .map(|partition| {
                    u16::try_from(partition)
                        .map(Into::into)
                        .map_err(|_| corrupt_record())
                })
                .collect::<StorageResult<Vec<_>>>()?;
            self.observe_partitions(attempt, &partitions, ReadProtection::Snapshot)
                .await?;
            Ok((through < count).then_some(PartitionQualificationCursor(through)))
        }
        .await;
        settlement.complete(result)
    }

    fn decode_initialization_layout(
        &self,
        row: Option<&DurableRecord>,
    ) -> StorageResult<(LayoutState, Option<Revision>)> {
        let (layout, revision) = match row {
            Some(record) => (
                self.records.decode::<LayoutState>(record)?,
                Some(record.revision),
            ),
            None => (
                LayoutState {
                    version: REGIONAL_LAYOUT_VERSION,
                    tenant: self.tenant,
                    partitions: self.partitions,
                    initialized: 0,
                },
                None,
            ),
        };
        if layout.version != REGIONAL_LAYOUT_VERSION
            || layout.tenant != self.tenant
            || layout.partitions != self.partitions
            || layout.initialized > u32::from(self.partitions)
        {
            return Err(corrupt_record());
        }
        Ok((layout, revision))
    }

    fn prepare_initialization_page(
        &self,
        input: InitializationPage,
    ) -> StorageResult<(Vec<RecordChange>, LayoutState)> {
        let InitializationPage {
            key,
            revision,
            mut layout,
            binding,
            epoch,
        } = input;
        let count = u32::from(self.partitions);
        let limits = self.records.limits;
        let mut changes: Vec<_> = binding.into_iter().chain(epoch).collect();
        let reservation = self.records.prepare(
            key.clone(),
            revision,
            &LayoutState {
                initialized: count,
                ..layout
            },
        )?;
        let mut bytes = self.initialization_change_bytes(&reservation)?;
        for change in &changes {
            bytes = bytes
                .checked_add(self.initialization_change_bytes(change)?)
                .ok_or_else(corrupt_record)?;
        }
        while layout.initialized < count
            && (changes.is_empty() || changes.len() + 1 < limits.max_records().get())
        {
            let change = self.prepare_initial_partition(layout.initialized)?;
            let next_bytes = bytes
                .checked_add(self.initialization_change_bytes(&change)?)
                .ok_or_else(corrupt_record)?;
            if next_bytes > limits.max_bytes().get() && !changes.is_empty() {
                break;
            }
            changes.push(change);
            bytes = next_bytes;
            layout.initialized += 1;
        }
        changes.push(self.records.prepare(key, revision, &layout)?);
        Ok((changes, layout))
    }

    fn initialization_change_bytes(&self, change: &RecordChange) -> StorageResult<usize> {
        let RecordChange::Store { record, .. } = change else {
            return Err(corrupt_record());
        };
        self.records.limits.validate_record(record)
    }
}
