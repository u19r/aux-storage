use std::ops::Range;

use infrastructure_provider::{
    limits::InfrastructureLimits, physical_attempt::PhysicalRecordAttempt,
};
use storage_types::{ReadProtection, StorageResult};

use super::{
    error::{capacity_rejected, invalid_image, invalid_observation},
    observation::{BodyReadRequest, load_body_wave},
};

/// Partition before dispatch: returned native bytes cannot exceed a wave bound.
pub(super) fn prepare_waves(
    images: &[BodyReadRequest],
    limits: InfrastructureLimits,
) -> StorageResult<Vec<Range<usize>>> {
    let mut waves = Vec::new();
    waves
        .try_reserve_exact(images.len())
        .map_err(|_| capacity_rejected())?;
    let mut after = 0usize;
    let mut bytes = 0usize;
    for (index, image) in images.iter().enumerate() {
        let encoded = image.bound_encoded_bytes(limits.max_record_bytes().get())?;
        let size = limits
            .validate_key(&image.key)?
            .checked_add(encoded)
            .ok_or_else(invalid_image)?;
        limits.check_batch(1, size)?;
        let next = bytes.checked_add(size).ok_or_else(invalid_image)?;
        if index - after == limits.max_records().get() || next > limits.max_bytes().get() {
            waves.push(after..index);
            after = index;
            bytes = 0;
        }
        bytes = bytes.checked_add(size).ok_or_else(invalid_image)?;
    }
    if after != images.len() {
        waves.push(after..images.len());
    }
    Ok(waves)
}

pub(super) async fn load_wave<A: PhysicalRecordAttempt>(
    attempt: &mut A,
    images: &[BodyReadRequest],
    limits: InfrastructureLimits,
) -> StorageResult<Vec<infrastructure_provider::record::DurableRecord>> {
    let mut records = Vec::new();
    records
        .try_reserve_exact(images.len())
        .map_err(|_| capacity_rejected())?;
    let rows = load_body_wave(attempt, images, limits, ReadProtection::Snapshot).await?;
    for row in rows {
        records.push(row.ok_or_else(invalid_observation)?);
    }
    Ok(records)
}
