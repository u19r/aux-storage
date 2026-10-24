use std::collections::HashSet;

use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageError, StorageResult};

use crate::{
    limits::InfrastructureLimits,
    record::{DurableRecordKey, RecordChange, RecordExpectation},
};

/// Borrowed CAS effects validated for exactly one capability's budgets.
pub struct RecordBatch<'a> {
    changes: &'a [RecordChange],
    limits: InfrastructureLimits,
}

impl<'a> RecordBatch<'a> {
    pub fn try_new(
        changes: &'a [RecordChange],
        limits: InfrastructureLimits,
    ) -> StorageResult<Self> {
        limits.check_batch(changes.len(), 0)?;
        let mut keys = HashSet::new();
        keys.try_reserve(changes.len())
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        let mut bytes = 0usize;
        for change in changes {
            if !keys.insert(change.key()) {
                return Err(InfrastructureResultKind::Encoding {
                    context: InfrastructureContext::RecordBatch,
                }
                .into());
            }
            bytes = bytes.checked_add(change.validate(limits)?).ok_or_else(|| {
                StorageError::from(InfrastructureResultKind::InvalidBound {
                    context: InfrastructureContext::RecordBatch,
                })
            })?;
        }
        limits.check_batch(changes.len(), bytes)?;
        Ok(Self { changes, limits })
    }

    /// Qualify all ordered phases before dispatch, including cross-phase key
    /// uniqueness.
    pub fn plan_waves<const N: usize>(
        groups: [&'a [RecordChange]; N],
        limits: InfrastructureLimits,
    ) -> StorageResult<[Vec<Self>; N]> {
        let count = groups
            .iter()
            .try_fold(0usize, |count, group| count.checked_add(group.len()))
            .ok_or(InfrastructureResultKind::NativeCapacityRejected)?;
        let mut keys = HashSet::new();
        keys.try_reserve(count)
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        let mut waves: [Vec<Self>; N] = std::array::from_fn(|_| Vec::new());
        for (changes, waves) in groups.into_iter().zip(&mut waves) {
            Self::prepare_group_waves(changes, limits, &mut keys, waves)?;
        }
        Ok(waves)
    }

    fn prepare_group_waves(
        changes: &'a [RecordChange],
        limits: InfrastructureLimits,
        keys: &mut HashSet<&'a DurableRecordKey>,
        waves: &mut Vec<Self>,
    ) -> StorageResult<()> {
        let mut start = 0;
        let mut bytes = 0usize;
        for (position, change) in changes.iter().enumerate() {
            if !keys.insert(change.key()) {
                return Err(InfrastructureResultKind::Encoding {
                    context: InfrastructureContext::RecordBatch,
                }
                .into());
            }
            let size = change.validate(limits)?;
            let next = bytes.checked_add(size).ok_or_else(|| {
                StorageError::from(InfrastructureResultKind::InvalidBound {
                    context: InfrastructureContext::RecordBatch,
                })
            })?;
            if position - start == limits.max_records().get() || next > limits.max_bytes().get() {
                waves
                    .try_reserve(1)
                    .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
                waves.push(Self {
                    changes: &changes[start..position],
                    limits,
                });
                start = position;
                bytes = size;
            } else {
                bytes = next;
            }
        }
        if start < changes.len() {
            waves
                .try_reserve(1)
                .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
            waves.push(Self {
                changes: &changes[start..],
                limits,
            });
        }
        Ok(())
    }

    pub fn limits(&self) -> InfrastructureLimits {
        self.limits
    }
}

impl AsRef<[RecordChange]> for RecordBatch<'_> {
    fn as_ref(&self) -> &[RecordChange] {
        self.changes
    }
}

impl RecordChange {
    pub fn key(&self) -> &DurableRecordKey {
        match self {
            Self::Store { record, .. } => &record.key,
            Self::Delete { key, .. } => key,
        }
    }

    /// Admit one native logical mutation, including its expected/new revision
    /// relationship.
    pub fn validate(&self, limits: InfrastructureLimits) -> StorageResult<usize> {
        let bytes = match self {
            Self::Delete { key, .. } => limits.validate_key(key)?,
            Self::Store { expected, record } => {
                let revision = match expected {
                    RecordExpectation::Absent => 0.into(),
                    RecordExpectation::Revision(previous) => previous.checked_next()?,
                };
                if record.revision != revision {
                    return Err(InfrastructureResultKind::Encoding {
                        context: InfrastructureContext::Revision,
                    }
                    .into());
                }
                limits.validate_record(record)?
            }
        };
        limits.check_batch(1, bytes)?;
        Ok(bytes)
    }
}
