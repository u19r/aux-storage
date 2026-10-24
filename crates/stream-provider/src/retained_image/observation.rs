//! Exact physical body observations, shared by protected writes and hydration.

use infrastructure_provider::{
    limits::InfrastructureLimits,
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey},
};
use storage_types::{ImmutableImageId, ReadProtection, Revision, StorageResult};

use super::{
    error::{capacity_rejected, invalid_image, invalid_observation},
    payload::RetainedPayload,
};

/// A declared immutable address and content bound for one native observation.
pub(super) struct BodyReadRequest {
    pub key: DurableRecordKey,
    pub identity: ImmutableImageId,
    pub length: usize,
    pub encoded_bytes: Option<usize>,
}

impl BodyReadRequest {
    pub(super) fn bound_encoded_bytes(&self, maximum: usize) -> StorageResult<usize> {
        let bound = RetainedPayload::encoded_length_bound(self.length)?;
        match self.encoded_bytes {
            Some(bytes) if bytes == 0 || bytes > bound => Err(invalid_image()),
            Some(bytes) if bytes > maximum => Err(capacity_rejected()),
            Some(bytes) => Ok(bytes),
            None => Ok(bound.min(maximum)),
        }
    }
}

pub(super) async fn load_body_wave<A: PhysicalRecordAttempt>(
    attempt: &mut A,
    images: &[BodyReadRequest],
    limits: InfrastructureLimits,
    protection: ReadProtection,
) -> StorageResult<Vec<Option<DurableRecord>>> {
    let limits = project_wave_limits(images, limits)?;
    let mut keys = Vec::new();
    keys.try_reserve_exact(images.len())
        .map_err(|_| capacity_rejected())?;
    for image in images {
        keys.push(DurableRecordKey::try_from(&image.key)?);
    }
    let rows = attempt.load_records(&keys, protection, limits).await?;
    limits.validate_observations(&keys, &rows)?;
    require_body_observations(images, &rows, limits)?;
    Ok(rows)
}

fn require_body_observations(
    images: &[BodyReadRequest],
    rows: &[Option<DurableRecord>],
    limits: InfrastructureLimits,
) -> StorageResult<()> {
    for (image, row) in images.iter().zip(rows) {
        let Some(row) = row else {
            continue;
        };
        if row.revision != Revision::INITIAL
            || RetainedPayload::decoded_length(&row.bytes)? != image.length
            || image
                .encoded_bytes
                .is_some_and(|expected| row.bytes.len() != expected)
            || row.bytes.len() > image.bound_encoded_bytes(limits.max_record_bytes().get())?
        {
            return Err(invalid_observation());
        }
    }
    Ok(())
}

fn project_wave_limits(
    images: &[BodyReadRequest],
    limits: InfrastructureLimits,
) -> StorageResult<InfrastructureLimits> {
    use std::num::NonZeroUsize;
    if images.is_empty() {
        return Ok(limits);
    }
    let (key_max, record_max, total) = images.iter().try_fold(
        (0usize, 0usize, 0usize),
        |(key_max, record_max, total), image| -> StorageResult<_> {
            let encoded = image.bound_encoded_bytes(limits.max_record_bytes().get())?;
            let key = limits.validate_key(&image.key)?;
            let total = total
                .checked_add(key)
                .and_then(|bytes| bytes.checked_add(encoded))
                .ok_or_else(invalid_image)?;
            Ok((key_max.max(key), record_max.max(encoded), total))
        },
    )?;
    limits.check_batch(images.len(), total)?;
    InfrastructureLimits::try_new(
        NonZeroUsize::new(key_max).ok_or_else(invalid_image)?,
        NonZeroUsize::new(record_max).ok_or_else(invalid_image)?,
        NonZeroUsize::new(total).ok_or_else(invalid_image)?,
        NonZeroUsize::new(images.len()).ok_or_else(invalid_image)?,
    )
}
