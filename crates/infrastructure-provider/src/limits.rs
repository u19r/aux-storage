use std::num::NonZeroUsize;

use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageError, StorageResult};

use crate::record::{DurableRecord, DurableRecordKey};

/// Physical budgets, qualified independently of public operation limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "InfrastructureLimitInput")]
pub struct InfrastructureLimits {
    key_bytes: NonZeroUsize,
    record_bytes: NonZeroUsize,
    bytes: NonZeroUsize,
    records: NonZeroUsize,
}

impl InfrastructureLimits {
    pub const MAX_KEY_BYTES: usize = crate::constants::INFRASTRUCTURE_KEY_BYTES;
    pub fn try_new(
        max_key_bytes: NonZeroUsize,
        max_record_bytes: NonZeroUsize,
        max_bytes: NonZeroUsize,
        max_records: NonZeroUsize,
    ) -> StorageResult<Self> {
        if max_key_bytes.get() > crate::constants::INFRASTRUCTURE_KEY_BYTES
            || max_key_bytes
                .get()
                .checked_add(max_record_bytes.get())
                .is_none_or(|bytes| bytes > max_bytes.get())
            || max_records
                .get()
                .checked_add(1)
                .and_then(|count| i64::try_from(count).ok())
                .is_none()
        {
            return Err(InfrastructureResultKind::InvalidBound {
                context: InfrastructureContext::Bounds,
            }
            .into());
        }
        Ok(Self {
            key_bytes: max_key_bytes,
            record_bytes: max_record_bytes,
            bytes: max_bytes,
            records: max_records,
        })
    }

    pub fn validate_key(&self, key: &DurableRecordKey) -> StorageResult<usize> {
        let address = key.as_ref();
        let size = address
            .namespace
            .len()
            .checked_add(address.resource.len())
            .and_then(|size| size.checked_add(address.suffix.len()))
            .and_then(|size| size.checked_add(16))
            .ok_or_else(|| {
                StorageError::from(InfrastructureResultKind::InvalidBound {
                    context: InfrastructureContext::RecordAddress,
                })
            })?;
        if size > self.key_bytes.get() {
            return Err(InfrastructureResultKind::InvalidBound {
                context: InfrastructureContext::RecordAddress,
            }
            .into());
        }
        Ok(size)
    }

    pub fn validate_record(&self, record: &DurableRecord) -> StorageResult<usize> {
        let key_bytes = self.validate_key(&record.key)?;
        if record.bytes.len() > self.record_bytes.get() {
            return Err(InfrastructureResultKind::InvalidBound {
                context: InfrastructureContext::Bounds,
            }
            .into());
        }
        key_bytes.checked_add(record.bytes.len()).ok_or_else(|| {
            StorageError::from(InfrastructureResultKind::InvalidBound {
                context: InfrastructureContext::Bounds,
            })
        })
    }

    pub fn validate_keys(&self, keys: &[DurableRecordKey]) -> StorageResult<usize> {
        self.check_batch(keys.len(), 0)?;
        let bytes = keys.iter().try_fold(0usize, |total, key| {
            total.checked_add(self.validate_key(key)?).ok_or_else(|| {
                StorageError::from(InfrastructureResultKind::InvalidBound {
                    context: InfrastructureContext::RecordBatch,
                })
            })
        })?;
        self.check_batch(keys.len(), bytes)?;
        Ok(bytes)
    }

    /// Reserve every requested key and its maximum payload before dispatch.
    pub fn project_read_keys(&self, keys: &[DurableRecordKey]) -> StorageResult<Self> {
        if keys.is_empty() {
            return Ok(*self);
        }
        let key_bytes = self.validate_keys(keys)?;
        let bytes = keys
            .len()
            .checked_mul(self.record_bytes.get())
            .and_then(|bytes| bytes.checked_add(key_bytes))
            .ok_or(InfrastructureResultKind::InvalidBound {
                context: InfrastructureContext::RecordBatch,
            })?;
        self.check_batch(keys.len(), bytes)?;
        let key_max = keys.iter().try_fold(0usize, |maximum, key| {
            self.validate_key(key).map(|bytes| maximum.max(bytes))
        })?;
        Self::try_new(
            NonZeroUsize::new(key_max).ok_or(InfrastructureResultKind::InvalidBound {
                context: InfrastructureContext::RecordAddress,
            })?,
            self.record_bytes,
            NonZeroUsize::new(bytes).ok_or(InfrastructureResultKind::InvalidBound {
                context: InfrastructureContext::RecordBatch,
            })?,
            NonZeroUsize::new(keys.len()).ok_or(InfrastructureResultKind::InvalidBound {
                context: InfrastructureContext::RecordBatch,
            })?,
        )
    }

    /// Missing rows still consume requested-key bytes; duplicate occurrences
    /// count separately.
    pub fn validate_observations(
        &self,
        keys: &[DurableRecordKey],
        rows: &[Option<DurableRecord>],
    ) -> StorageResult<()> {
        if rows.len() != keys.len() {
            return Err(InfrastructureResultKind::Encoding {
                context: InfrastructureContext::Observation,
            }
            .into());
        }
        let mut bytes = self.validate_keys(keys)?;
        for (key, row) in keys.iter().zip(rows) {
            let Some(row) = row else {
                continue;
            };
            if &row.key != key {
                return Err(InfrastructureResultKind::Encoding {
                    context: InfrastructureContext::Observation,
                }
                .into());
            }
            self.validate_record(row)?;
            bytes = bytes.checked_add(row.bytes.len()).ok_or(
                InfrastructureResultKind::InvalidBound {
                    context: InfrastructureContext::RecordBatch,
                },
            )?;
        }
        self.check_batch(keys.len(), bytes)
    }

    pub fn check_batch(&self, count: usize, bytes: usize) -> StorageResult<()> {
        if count > self.records.get() || bytes > self.bytes.get() {
            return Err(InfrastructureResultKind::InvalidBound {
                context: InfrastructureContext::RecordBatch,
            }
            .into());
        }
        Ok(())
    }

    pub fn max_record_bytes(&self) -> NonZeroUsize {
        self.record_bytes
    }
    pub fn max_key_bytes(&self) -> NonZeroUsize {
        self.key_bytes
    }
    pub fn max_bytes(&self) -> NonZeroUsize {
        self.bytes
    }
    pub fn max_records(&self) -> NonZeroUsize {
        self.records
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct InfrastructureLimitInput {
    key_bytes: NonZeroUsize,
    record_bytes: NonZeroUsize,
    bytes: NonZeroUsize,
    records: NonZeroUsize,
}

impl TryFrom<InfrastructureLimitInput> for InfrastructureLimits {
    type Error = StorageError;

    fn try_from(input: InfrastructureLimitInput) -> StorageResult<Self> {
        Self::try_new(
            input.key_bytes,
            input.record_bytes,
            input.bytes,
            input.records,
        )
    }
}
