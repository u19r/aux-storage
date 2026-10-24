//! Durable exact-incarnation selection and cursor for index retirement.

use std::num::NonZeroUsize;

use infrastructure_provider::{
    limits::InfrastructureLimits,
    record::{DurableRecord, DurableRecordKey, RecordAddress, RecordChange, RecordExpectation},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage_backfill::GsiBuildTarget;
use storage_types::{Revision, StorageError, StorageResult, TableHistoryId};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GsiReclaimRecord {
    format: u8,
    pub(crate) history: TableHistoryId,
    pub(crate) target: GsiBuildTarget,
    pub(crate) phase: GsiReclaimPhase,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum GsiReclaimPhase {
    OwnerReceipts { after: Option<DurableRecordKey> },
    PhysicalRows,
}

impl GsiReclaimRecord {
    pub(crate) fn try_new(history: TableHistoryId, target: GsiBuildTarget) -> StorageResult<Self> {
        let record = Self {
            format: 1,
            history,
            target,
            phase: GsiReclaimPhase::OwnerReceipts { after: None },
        };
        record.require_identity()?;
        Ok(record)
    }

    pub(crate) fn decode(row: &DurableRecord) -> StorageResult<Self> {
        Self::limits()?.validate_record(row)?;
        let record: Self = serde_json::from_slice(&row.bytes).map_err(|_| invalid_marker())?;
        record.require_identity()?;
        if record.key()? != row.key {
            return Err(invalid_marker());
        }
        Ok(record)
    }

    fn require_identity(&self) -> StorageResult<()> {
        if self.format != 1
            || self.history.as_ref() == &[0; 16]
            || self.target.table_name.as_ref().is_empty()
            || self.target.index_name.as_ref().is_empty()
            || self.target.index_name.as_ref().len() > storage_types::MAX_INDEX_NAME_BYTES
        {
            return Err(invalid_marker());
        }
        Ok(())
    }

    pub(crate) fn key(&self) -> StorageResult<DurableRecordKey> {
        let mut hash = Sha256::new();
        hash.update(self.history.as_ref());
        hash.update(serde_json::to_vec(&self.target).map_err(|_| invalid_marker())?);
        let mut suffix = vec![1];
        suffix.extend_from_slice(&hash.finalize());
        Self::scope_key(suffix)
    }

    pub(crate) fn require_marker_key(key: &DurableRecordKey) -> StorageResult<()> {
        let address = key.as_ref();
        if address.namespace != crate::constants::GSI_RECLAIM_NAMESPACE
            || address.resource != [1]
            || address.generation != 0
            || address.suffix.len() != 33
            || address.suffix[0] != 1
        {
            return Err(invalid_marker());
        }
        Ok(())
    }

    pub(crate) fn scope_key(suffix: Vec<u8>) -> StorageResult<DurableRecordKey> {
        RecordAddress {
            namespace: crate::constants::GSI_RECLAIM_NAMESPACE.into(),
            resource: vec![1],
            generation: 0,
            suffix,
        }
        .try_into()
    }

    pub(crate) fn prepare_store(&self, expected: RecordExpectation) -> StorageResult<RecordChange> {
        let revision = match expected {
            RecordExpectation::Absent => Revision::INITIAL,
            RecordExpectation::Revision(previous) => previous.checked_next()?,
        };
        let record = DurableRecord {
            key: self.key()?,
            revision,
            bytes: serde_json::to_vec(self)
                .map_err(|_| invalid_marker())?
                .into(),
        };
        Self::limits()?.validate_record(&record)?;
        Ok(RecordChange::Store { expected, record })
    }

    pub(crate) fn change_limits() -> StorageResult<InfrastructureLimits> {
        let limits = Self::limits()?;
        let bytes = limits
            .max_bytes()
            .get()
            .checked_mul(2)
            .and_then(NonZeroUsize::new)
            .ok_or_else(invalid_marker)?;
        InfrastructureLimits::try_new(
            limits.max_key_bytes(),
            limits.max_record_bytes(),
            bytes,
            NonZeroUsize::new(2).ok_or_else(invalid_marker)?,
        )
    }

    pub(crate) fn limits() -> StorageResult<InfrastructureLimits> {
        let key_bytes = crate::constants::GSI_RECLAIM_NAMESPACE.len() + 1 + 16 + 33;
        let count = NonZeroUsize::MIN;
        let bytes = NonZeroUsize::new(key_bytes + crate::CATALOG_READ_BYTES.get())
            .ok_or_else(invalid_marker)?;
        InfrastructureLimits::try_new(
            NonZeroUsize::new(key_bytes).ok_or_else(invalid_marker)?,
            crate::CATALOG_READ_BYTES,
            bytes,
            count,
        )
    }
}

pub(crate) fn invalid_marker() -> StorageError {
    StorageError::internal("Invalid index retirement authority")
}
