//! Region/group protected-history charges shared across retained generations.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange, RecordExpectation},
};
use replication_provider::runtime_configuration::limits::BacklogBudgets;
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, StorageResult};

use crate::native_record::{NativeRecords, corrupt_record};

#[derive(Clone)]
pub(crate) struct HistoryCapacity {
    key: DurableRecordKey,
    marker: DurableRecordKey,
    tenant: replication_types::identity::resource::TenantId,
    hot_bytes: u64,
    acceptance_bytes: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryCapacityMarker {
    tenant: replication_types::identity::resource::TenantId,
    version: u16,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoryByteAccount {
    key: DurableRecordKey,
    pub(crate) bytes: u64,
}

impl HistoryByteAccount {
    fn adjust(&self, delta: i128) -> StorageResult<Self> {
        let bytes = i128::from(self.bytes)
            .checked_add(delta)
            .ok_or_else(corrupt_record)?;
        Ok(Self {
            key: self.key.clone(),
            bytes: u64::try_from(bytes).map_err(|_| corrupt_record())?,
        })
    }
}

pub(crate) struct HistoryCapacityObservation {
    key: DurableRecordKey,
    bytes: u64,
}

struct PreparedHistoryCharge<'a> {
    initial_delta: i128,
    reads: Vec<HistoryChargeRead<'a>>,
    waves: Vec<std::ops::Range<usize>>,
}

struct HistoryChargeRead<'a> {
    key: &'a DurableRecordKey,
    revision: storage_types::Revision,
    next_bytes: usize,
}

impl HistoryCapacity {
    #[cfg(test)]
    pub(crate) fn root_key(&self) -> &DurableRecordKey {
        &self.key
    }

    pub(crate) fn try_new(
        scope: DurableRecordKey,
        tenant: replication_types::identity::resource::TenantId,
        budgets: BacklogBudgets,
    ) -> StorageResult<Self> {
        let acceptance_bytes = budgets
            .acceptance_bytes
            .get()
            .checked_sub(budgets.recovery_reserve_bytes.get())
            .filter(|bytes| *bytes > 0)
            .ok_or_else(corrupt_record)?;
        if budgets.acceptance_bytes > budgets.hot_history_bytes {
            return Err(corrupt_record());
        }
        let record_generation = scope.as_ref().generation;
        if record_generation == crate::constants::HISTORY_CAPACITY_ROOT_GENERATION
            || scope.as_ref().suffix != crate::constants::HISTORY_CAPACITY_SUFFIX
        {
            return Err(corrupt_record());
        }
        let mut root: infrastructure_provider::record::RecordAddress = scope.into();
        root.generation = crate::constants::HISTORY_CAPACITY_ROOT_GENERATION;
        let key: DurableRecordKey = root.try_into()?;
        let mut root: infrastructure_provider::record::RecordAddress =
            DurableRecordKey::try_from(&key)?.into();
        root.suffix.clear();
        root.suffix
            .try_reserve_exact(crate::constants::HISTORY_CAPACITY_MARKER_SUFFIX.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        root.suffix
            .extend_from_slice(crate::constants::HISTORY_CAPACITY_MARKER_SUFFIX);
        Ok(Self {
            key,
            marker: root.try_into()?,
            tenant,
            hot_bytes: budgets.hot_history_bytes.get(),
            acceptance_bytes,
        })
    }

    pub(crate) fn require_policy(&self, budgets: &BacklogBudgets) -> StorageResult<()> {
        if self.hot_bytes != budgets.hot_history_bytes.get()
            || Some(self.acceptance_bytes)
                != budgets
                    .acceptance_bytes
                    .get()
                    .checked_sub(budgets.recovery_reserve_bytes.get())
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    fn require_address(&self, key: &DurableRecordKey) -> StorageResult<()> {
        if crate::journal::native_binding::is_native_binding_key(key)
            || crate::journal::native_binding::is_archive_binding_key(key)
        {
            return Ok(());
        }
        let address = key.as_ref();
        let scope = self.key.as_ref();
        if key == &self.key
            || address.namespace != scope.namespace
            || address.resource != scope.resource
            || address.generation == crate::constants::HISTORY_CAPACITY_ROOT_GENERATION
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

impl NativeRecords {
    pub(crate) fn require_history_account_capacity(&self) -> StorageResult<usize> {
        let account = HistoryByteAccount {
            key: self.capacity.key.clone(),
            bytes: u64::MAX,
        };
        let change = self.prepare(self.capacity.key.clone(), None, &account)?;
        let marker = self.prepare_history_marker()?;
        let marker_bytes = marker.validate(self.limits)?;
        let account_bytes = change.validate(self.limits)?;
        self.limits.check_batch(1, account_bytes)?;
        account_bytes
            .checked_add(marker_bytes)
            .ok_or_else(corrupt_record)
    }

    fn prepare_history_marker(&self) -> StorageResult<RecordChange> {
        self.prepare(
            (&self.capacity.marker).try_into()?,
            None,
            &HistoryCapacityMarker {
                tenant: self.capacity.tenant,
                version: crate::constants::REGIONAL_LAYOUT_VERSION,
            },
        )
    }

    pub(crate) async fn observe_discretionary_capacity<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
    ) -> StorageResult<HistoryCapacityObservation> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (_, account) = self
                .load_history_capacity(attempt, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            Ok(HistoryCapacityObservation {
                key: (&self.capacity.key).try_into()?,
                bytes: account.bytes,
            })
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn require_discretionary_capacity<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        previous: HistoryCapacityObservation,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            if previous.key != self.capacity.key {
                return Err(corrupt_record());
            }
            let (_, account) = self
                .load_history_capacity(attempt, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            if account.bytes > previous.bytes && account.bytes > self.capacity.acceptance_bytes {
                return Err(exhausted_capacity());
            }
            Ok(())
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn require_acceptance_capacity<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (_, account) = self
                .load_history_capacity(attempt, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            if account.bytes > self.capacity.acceptance_bytes {
                return Err(exhausted_capacity());
            }
            Ok(())
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn require_history_capacity_initialized<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            self.load_history_capacity(attempt, ReadProtection::Snapshot)
                .await?
                .ok_or_else(corrupt_record)?;
            Ok(())
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn load_history_capacity<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        protection: ReadProtection,
    ) -> StorageResult<Option<(DurableRecord, HistoryByteAccount)>> {
        let keys = [
            (&self.capacity.key).try_into()?,
            (&self.capacity.marker).try_into()?,
        ];
        let rows = self.load(attempt, &keys, protection).await?;
        if rows.len() != keys.len() {
            return Err(corrupt_record());
        }
        let mut rows = rows.into_iter();
        match (rows.next().flatten(), rows.next().flatten()) {
            (None, None) => Ok(None),
            (Some(row), Some(marker)) => {
                let marker_value: HistoryCapacityMarker = self.decode(&marker)?;
                if marker.revision != storage_types::Revision::INITIAL
                    || marker_value.tenant != self.capacity.tenant
                    || marker_value.version != crate::constants::REGIONAL_LAYOUT_VERSION
                {
                    return Err(corrupt_record());
                }
                let account: HistoryByteAccount =
                    self.decode(&row).map_err(preserve_account_failure)?;
                if account.key != self.capacity.key {
                    return Err(corrupt_record());
                }
                Ok(Some((row, account)))
            }
            _ => Err(corrupt_record()),
        }
    }

    pub(crate) async fn prepare_history_charge<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        changes: &[RecordChange],
    ) -> StorageResult<Vec<RecordChange>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let prepared = self.prepare_charge_reads(changes)?;
            let mut charges = Vec::new();
            charges
                .try_reserve_exact(2)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            let current = self
                .load_history_capacity(attempt, ReadProtection::Conflict)
                .await?;
            if current.is_none() && !is_fresh_layout(changes) {
                return Err(corrupt_record());
            }
            let previous = current.as_ref().map_or(0, |(_, account)| account.bytes);
            let mut delta = 0i128;
            if current.is_none() {
                let marker = self.prepare_history_marker()?;
                delta = delta
                    .checked_add(marker.validate(self.limits)? as i128)
                    .ok_or_else(corrupt_record)?;
                charges.push(marker);
            }
            delta = delta
                .checked_add(self.load_charge_deltas(attempt, &prepared).await?)
                .ok_or_else(corrupt_record)?;
            let account = match current.as_ref() {
                Some((_, account)) => account.adjust(delta).map_err(preserve_account_failure)?,
                None => HistoryByteAccount {
                    key: self.capacity.key.clone(),
                    bytes: u64::try_from(delta).map_err(|_| corrupt_record())?,
                },
            };
            if account.bytes > self.capacity.hot_bytes && account.bytes > previous {
                return Err(exhausted_capacity());
            }
            charges.push(
                self.prepare(
                    account.key.clone(),
                    current.map(|(row, _)| row.revision),
                    &account,
                )
                .map_err(preserve_account_failure)?,
            );
            Ok(charges)
        }
        .await;
        settlement_guard.complete(result)
    }

    fn prepare_charge_reads<'a>(
        &self,
        changes: &'a [RecordChange],
    ) -> StorageResult<PreparedHistoryCharge<'a>> {
        let mut delta = 0i128;
        let mut reads = Vec::new();
        let mut keys = std::collections::HashSet::new();
        reads
            .try_reserve_exact(changes.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        keys.try_reserve(changes.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for change in changes {
            self.capacity.require_address(change.key())?;
            let bytes = change.validate(self.limits)?;
            if !keys.insert(change.key()) {
                return Err(corrupt_record());
            }
            let (expected, next_bytes) = match change {
                RecordChange::Store { expected, .. } => (*expected, bytes),
                RecordChange::Delete { expected, .. } => {
                    (RecordExpectation::Revision(*expected), 0)
                }
            };
            match expected {
                RecordExpectation::Absent => {
                    delta = delta
                        .checked_add(next_bytes as i128)
                        .ok_or_else(corrupt_record)?
                }
                RecordExpectation::Revision(revision) => reads.push(HistoryChargeRead {
                    key: change.key(),
                    revision,
                    next_bytes,
                }),
            }
        }
        let waves = self.read_waves(&reads, |read| read.key)?;
        Ok(PreparedHistoryCharge {
            initial_delta: delta,
            reads,
            waves,
        })
    }

    async fn load_charge_deltas<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        prepared: &PreparedHistoryCharge<'_>,
    ) -> StorageResult<i128> {
        let mut delta = prepared.initial_delta;
        for range in &prepared.waves {
            let wave = &prepared.reads[range.clone()];
            let mut keys = Vec::new();
            keys.try_reserve_exact(wave.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for read in wave {
                keys.push(read.key.try_into()?);
            }
            let rows = self
                .load_batch(attempt, &keys, ReadProtection::Conflict, self.limits)
                .await?;
            for (read, previous) in wave.iter().zip(rows) {
                delta = delta
                    .checked_add(read.charge_delta(previous, self.limits)?)
                    .ok_or_else(corrupt_record)?;
            }
        }
        Ok(delta)
    }
}

impl HistoryChargeRead<'_> {
    fn charge_delta(
        &self,
        previous: Option<DurableRecord>,
        limits: infrastructure_provider::limits::InfrastructureLimits,
    ) -> StorageResult<i128> {
        let Some(previous) = previous.filter(|record| record.revision == self.revision) else {
            return Err(storage_types::StorageEnum::TransactionConflict {
                message: "Regional history charge observation changed".into(),
            }
            .into());
        };
        Ok(self.next_bytes as i128 - limits.validate_record(&previous)? as i128)
    }
}

fn is_fresh_layout(changes: &[RecordChange]) -> bool {
    changes.iter().all(|change| {
        matches!(
            change,
            RecordChange::Store {
                expected: RecordExpectation::Absent,
                ..
            }
        )
    }) && changes
        .iter()
        .any(|change| change.key().as_ref().suffix == crate::constants::ORIGIN_LAYOUT_SUFFIX)
}

fn exhausted_capacity() -> storage_types::StorageError {
    storage_types::StorageEnum::LimitExceeded {
        message: "Regional retained history storage capacity is exhausted".into(),
    }
    .into()
}

fn preserve_account_failure(error: storage_types::StorageError) -> storage_types::StorageError {
    if error.is_commit_unknown()
        || error.native_attempt_rejection() == Some(storage_types::NativeAttemptRejection::Capacity)
    {
        return error;
    }
    corrupt_record()
}
