//! Only the unsigned CAS prefix belongs here; payload bytes remain opaque.

use std::sync::Arc;

use infrastructure_provider::{
    limits::InfrastructureLimits,
    record::{DurableRecord, DurableRecordKey},
};
use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageResult};

use super::key_layout;
use crate::{KvEntry, constants::INFRASTRUCTURE_RECORD_HEADER_BYTES};

pub(super) fn encode_record(record: &DurableRecord) -> StorageResult<Vec<u8>> {
    let size = INFRASTRUCTURE_RECORD_HEADER_BYTES
        .checked_add(record.bytes.len())
        .ok_or(InfrastructureResultKind::NativeCapacityRejected)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
    bytes.extend_from_slice(&u64::from(record.revision).to_be_bytes());
    bytes.extend_from_slice(&record.bytes);
    Ok(bytes)
}

pub(super) fn decode_record(key: DurableRecordKey, bytes: Vec<u8>) -> StorageResult<DurableRecord> {
    let revision = bytes.get(..INFRASTRUCTURE_RECORD_HEADER_BYTES).ok_or(
        InfrastructureResultKind::Encoding {
            context: InfrastructureContext::Revision,
        },
    )?;
    let revision = u64::from_be_bytes(revision.try_into().map_err(|_| {
        InfrastructureResultKind::Encoding {
            context: InfrastructureContext::Revision,
        }
    })?)
    .into();
    Ok(DurableRecord {
        key,
        revision,
        bytes: Arc::from(&bytes[INFRASTRUCTURE_RECORD_HEADER_BYTES..]),
    })
}

pub(super) fn decode_entry(entry: KvEntry) -> StorageResult<DurableRecord> {
    decode_record(key_layout::decode_key(&entry.key)?, entry.value)
}

pub(super) fn decode_records<'a>(
    keys: impl ExactSizeIterator<Item = &'a DurableRecordKey>,
    values: Vec<Option<Vec<u8>>>,
    limits: InfrastructureLimits,
) -> StorageResult<Vec<Option<DurableRecord>>> {
    let count = keys.len();
    if values.len() != count {
        return Err(InfrastructureResultKind::Encoding {
            context: InfrastructureContext::Observation,
        }
        .into());
    }
    limits.check_batch(count, 0)?;
    let mut used_bytes = 0usize;
    let mut records = Vec::new();
    records
        .try_reserve_exact(count)
        .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
    for (key, bytes) in keys.zip(values) {
        let record = bytes
            .map(|bytes| decode_record(DurableRecordKey::try_from(key)?, bytes))
            .transpose()?;
        let size = match &record {
            Some(record) => limits.validate_record(record)?,
            None => limits.validate_key(key)?,
        };
        used_bytes =
            used_bytes
                .checked_add(size)
                .ok_or(InfrastructureResultKind::InvalidBound {
                    context: InfrastructureContext::RecordBatch,
                })?;
        limits.check_batch(count, used_bytes)?;
        records.push(record);
    }
    Ok(records)
}

pub(super) fn decode_revisions(
    values: Vec<Option<crate::KvValuePrefix>>,
    limits: InfrastructureLimits,
) -> StorageResult<Vec<Option<storage_types::Revision>>> {
    limits.check_batch(values.len(), 0)?;
    let mut revisions = Vec::new();
    revisions
        .try_reserve_exact(values.len())
        .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
    for value in values {
        revisions.push(
            value
                .map(|value| decode_revision(value, limits))
                .transpose()?,
        );
    }
    Ok(revisions)
}

fn decode_revision(
    value: crate::KvValuePrefix,
    limits: InfrastructureLimits,
) -> StorageResult<storage_types::Revision> {
    let payload = value
        .length
        .checked_sub(INFRASTRUCTURE_RECORD_HEADER_BYTES)
        .ok_or_else(invalid_revision)?;
    if payload > limits.max_record_bytes().get() {
        return Err(invalid_revision());
    }
    let bytes = value
        .bytes
        .as_slice()
        .try_into()
        .map_err(|_| invalid_revision())?;
    Ok(u64::from_be_bytes(bytes).into())
}

fn invalid_revision() -> storage_types::StorageError {
    InfrastructureResultKind::Encoding {
        context: InfrastructureContext::Revision,
    }
    .into()
}
