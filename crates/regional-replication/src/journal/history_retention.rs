//! Native conflict boundary for reader obligation changes and future
//! reclamation.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange},
};
use replication_types::identity::resource::TableLineage;
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, Revision, StorageResult};

use super::JournalStore;
use crate::{history::retention::RetentionRevision, native_record::corrupt_record};

#[derive(PartialEq, Eq, Serialize, Deserialize)]
enum RetentionEpochScope {
    Lineage(TableLineage),
    Journal(replication_types::identity::origin::JournalGeneration),
}

/// The generation epoch is also the counted reader barrier: one revision owns
/// hold mutations and reservation transitions, rather than parallel roots.
/// Logical reservation revision pins bounded admission independently of
/// ordinary epoch churn.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalRetentionBarrier {
    generation: replication_types::identity::origin::JournalGeneration,
    reservations: u64,
    reservation_revision: crate::trim::reservation::ReservationRevision,
}

/// Actual root observation; no serialized description can issue count mutation
/// custody.
pub(super) struct NativeReservationBarrier {
    key: DurableRecordKey,
    revision: Revision,
    value: JournalRetentionBarrier,
}

impl NativeReservationBarrier {
    pub(super) fn reservation_state(&self) -> (crate::trim::reservation::ReservationRevision, u64) {
        (self.value.reservation_revision, self.value.reservations)
    }
}

struct RetentionEpochObservation {
    revision: Option<Revision>,
    value: RetentionEpochValue,
}

enum RetentionEpochValue {
    Lineage(TableLineage),
    Journal(JournalRetentionBarrier),
}

impl JournalStore {
    pub(in crate::journal) async fn observe_retention_revision<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        lineage: TableLineage,
        protection: ReadProtection,
    ) -> StorageResult<RetentionRevision> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let epochs = self.history_epoch_addresses(lineage)?;
            let keys = [epochs[0].0.clone(), epochs[1].0.clone()];
            let rows = self.records.load(attempt, &keys, protection).await?;
            let lineage = self
                .decode_epoch_observation(rows[0].as_ref(), &epochs[0].0, &epochs[0].1)?
                .revision;
            let observed =
                self.decode_epoch_observation(rows[1].as_ref(), &epochs[1].0, &epochs[1].1)?;
            let journal = observed.revision.ok_or_else(corrupt_record)?;
            if let RetentionEpochValue::Journal(barrier) = observed.value
                && barrier.reservations != 0
            {
                self.require_trim_reservation_count(attempt, barrier.reservations, protection)
                    .await?;
            }
            Ok(RetentionRevision { lineage, journal })
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn retain_history_epoch<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        lineage: TableLineage,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let epochs = self.history_epoch_addresses(lineage)?;
            self.retain_epoch_changes(attempt, &epochs).await
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn retain_history_epochs<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        lineages: &std::collections::BTreeSet<TableLineage>,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let mut epochs = std::collections::BTreeMap::new();
            for lineage in lineages {
                for (key, scope) in self.history_epoch_addresses(*lineage)? {
                    epochs.insert(key, scope);
                }
            }
            self.retain_epoch_changes(attempt, &epochs.into_iter().collect::<Vec<_>>())
                .await
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn retain_journal_epoch<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let epochs = [(
                self.journal_epoch_key()?,
                RetentionEpochScope::Journal(self.generation),
            )];
            self.retain_epoch_changes(attempt, &epochs).await
        }
        .await;
        settlement.complete(result)
    }

    pub(super) fn journal_epoch_key(&self) -> StorageResult<DurableRecordKey> {
        self.record_key(crate::constants::READER_RETENTION_EPOCH_SUFFIX.to_vec())
    }

    pub(super) fn prepare_journal_epoch_genesis(&self) -> StorageResult<RecordChange> {
        self.records.prepare(
            self.journal_epoch_key()?,
            None,
            &JournalRetentionBarrier {
                generation: self.generation,
                reservations: 0,
                reservation_revision: crate::trim::reservation::ReservationRevision::GENESIS,
            },
        )
    }

    pub(super) fn prepare_journal_epoch_registration(
        &self,
        row: Option<&DurableRecord>,
        layout: Option<Revision>,
    ) -> StorageResult<Option<RecordChange>> {
        if layout.is_none() {
            if row.is_some() {
                return Err(corrupt_record());
            }
            return self.prepare_journal_epoch_genesis().map(Some);
        }
        self.decode_epoch_observation(
            row,
            &self.journal_epoch_key()?,
            &RetentionEpochScope::Journal(self.generation),
        )?
        .revision
        .ok_or_else(corrupt_record)?;
        Ok(None)
    }

    async fn retain_epoch_changes<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        epochs: &[(
            infrastructure_provider::record::DurableRecordKey,
            RetentionEpochScope,
        )],
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if epochs.is_empty() {
                return Ok(());
            }
            let keys = epochs
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            let waves = self.records.read_waves(&keys, |key| key)?;
            let mut changes = Vec::with_capacity(epochs.len());
            for wave in waves {
                let rows = self
                    .records
                    .load(attempt, &keys[wave.clone()], ReadProtection::Conflict)
                    .await?;
                for (epoch, row) in epochs[wave].iter().zip(rows) {
                    changes.push(
                        self.prepare_retention_epoch_change(attempt, epoch, row)
                            .await?,
                    );
                }
            }
            self.records.store(attempt, &changes).await
        }
        .await;
        settlement.complete(result)
    }

    async fn prepare_retention_epoch_change<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        epoch: &(DurableRecordKey, RetentionEpochScope),
        row: Option<DurableRecord>,
    ) -> StorageResult<RecordChange> {
        let (key, scope) = epoch;
        let observed = self.decode_epoch_observation(row.as_ref(), key, scope)?;
        match observed.value {
            RetentionEpochValue::Lineage(lineage) => self.records.prepare(
                key.clone(),
                observed.revision,
                &RetentionEpochScope::Lineage(lineage),
            ),
            RetentionEpochValue::Journal(barrier) => {
                if barrier.reservations != 0 {
                    self.require_trim_reservation_count(
                        attempt,
                        barrier.reservations,
                        ReadProtection::Conflict,
                    )
                    .await?;
                }
                self.records
                    .prepare(key.clone(), observed.revision, &barrier)
            }
        }
    }

    pub(super) async fn load_reservation_barrier<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        protection: ReadProtection,
    ) -> StorageResult<NativeReservationBarrier> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.journal_epoch_key()?;
            let row = self
                .records
                .load(attempt, std::slice::from_ref(&key), protection)
                .await?
                .pop()
                .flatten();
            let observed = self.decode_epoch_observation(
                row.as_ref(),
                &key,
                &RetentionEpochScope::Journal(self.generation),
            )?;
            let RetentionEpochValue::Journal(value) = observed.value else {
                return Err(corrupt_record());
            };
            Ok(NativeReservationBarrier {
                key,
                revision: observed.revision.ok_or_else(corrupt_record)?,
                value,
            })
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn load_journal_barrier<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        protection: ReadProtection,
    ) -> StorageResult<(Revision, u64)> {
        let observed = self.load_reservation_barrier(attempt, protection).await?;
        Ok((observed.revision, observed.value.reservations))
    }

    pub(super) fn prepare_journal_barrier_count(
        &self,
        observed: &NativeReservationBarrier,
        reservations: u64,
    ) -> StorageResult<RecordChange> {
        if reservations > crate::constants::MAX_ACTIVE_TRIM_RESERVATIONS
            || observed.key != self.journal_epoch_key()?
            || observed.value.generation != self.generation
            || observed.value.reservations.abs_diff(reservations) != 1
        {
            return Err(corrupt_record());
        }
        let next = JournalRetentionBarrier {
            generation: self.generation,
            reservations,
            reservation_revision: observed.value.reservation_revision.checked_next()?,
        };
        self.records
            .prepare(observed.key.clone(), Some(observed.revision), &next)
    }

    fn history_epoch_addresses(
        &self,
        lineage: TableLineage,
    ) -> StorageResult<[(DurableRecordKey, RetentionEpochScope); 2]> {
        Ok([
            (
                self.identity_key(crate::constants::HISTORY_RETENTION_EPOCH_SUFFIX, &lineage)?,
                RetentionEpochScope::Lineage(lineage),
            ),
            (
                self.journal_epoch_key()?,
                RetentionEpochScope::Journal(self.generation),
            ),
        ])
    }

    fn decode_epoch_observation(
        &self,
        row: Option<&DurableRecord>,
        key: &DurableRecordKey,
        scope: &RetentionEpochScope,
    ) -> StorageResult<RetentionEpochObservation> {
        let Some(row) = row else {
            return match scope {
                RetentionEpochScope::Lineage(lineage) => Ok(RetentionEpochObservation {
                    revision: None,
                    value: RetentionEpochValue::Lineage(*lineage),
                }),
                RetentionEpochScope::Journal(_) => Err(corrupt_record()),
            };
        };
        if row.key != *key {
            return Err(corrupt_record());
        }
        let value = match scope {
            RetentionEpochScope::Lineage(lineage) => {
                if self.records.decode::<RetentionEpochScope>(row)? != *scope {
                    return Err(corrupt_record());
                }
                RetentionEpochValue::Lineage(*lineage)
            }
            RetentionEpochScope::Journal(generation) => {
                let barrier: JournalRetentionBarrier = self.records.decode(row)?;
                if barrier.generation != *generation
                    || barrier.reservations > crate::constants::MAX_ACTIVE_TRIM_RESERVATIONS
                {
                    return Err(corrupt_record());
                }
                barrier
                    .reservation_revision
                    .require_barrier(row.revision, barrier.reservations)?;
                RetentionEpochValue::Journal(barrier)
            }
        };
        Ok(RetentionEpochObservation {
            revision: Some(row.revision),
            value,
        })
    }
}
