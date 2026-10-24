//! Bounds for the manifest and independently encoded immutable bodies.

use storage_types::StorageResult;

use super::{error::invalid_image, manifest::RetainedImageManifest, payload::RetainedPayload};
use crate::constants::MAX_RETAINED_MANIFEST_BODIES;

impl RetainedImageManifest {
    /// Raw format metadata, including every segment descriptor and image
    /// identity. Image bytes move into bodies, so this also bounds total
    /// raw stored bytes.
    pub fn metadata_length_bound(original_length: usize) -> StorageResult<usize> {
        let header = crate::constants::RETAINED_IMAGE_MANIFEST_HEADER_BYTES;
        let images = MAX_RETAINED_MANIFEST_BODIES;
        let descriptors = images
            .checked_mul(1 + 8 + 32)
            .and_then(|bytes| bytes.checked_add(images.checked_add(1)?.checked_mul(1 + 8)?))
            .and_then(|bytes| bytes.checked_add(16))
            .and_then(|bytes| {
                bytes.checked_add(
                    crate::constants::MAX_CAUSAL_BODY_ALIASES
                        .checked_mul(crate::constants::CAUSAL_SLICE_METADATA_BYTES + 1 + 8)?,
                )
            })
            .ok_or_else(invalid_image)?;
        original_length
            .checked_add(header)
            .and_then(|bytes| bytes.checked_add(descriptors))
            .ok_or_else(invalid_image)
    }

    /// Combined manifest and body value bytes, without native keys or receipts.
    /// The variable compression overhead is superadditive. Each additional
    /// encoded payload needs its own fixed overhead even for incompressible
    /// data.
    pub fn encoded_records_length_bound(original_length: usize) -> StorageResult<usize> {
        RetainedPayload::encoded_length_bound(Self::metadata_length_bound(original_length)?)?
            .checked_add(
                RetainedPayload::encoded_length_bound(0)?
                    .checked_mul(MAX_RETAINED_MANIFEST_BODIES)
                    .ok_or_else(invalid_image)?,
            )
            .ok_or_else(invalid_image)
    }

    /// Bound this invocation's actual declared output, not a provider capacity
    /// report. Immutable image addresses have one fixed namespace,
    /// incarnation and digest.
    pub fn image_work_limits(
        maximum: usize,
    ) -> storage_types::StorageResult<infrastructure_provider::limits::InfrastructureLimits> {
        use std::num::NonZeroUsize;
        let key = 16 + crate::constants::RETAINED_IMAGE_NAMESPACE.len() + 16 + 32;
        let record = super::payload::RetainedPayload::encoded_length_bound(
            super::manifest::RetainedImageManifest::metadata_length_bound(maximum)?,
        )?;
        let bytes = record
            .checked_add(key)
            .and_then(|bytes| bytes.checked_mul(MAX_RETAINED_MANIFEST_BODIES))
            .ok_or_else(super::error::invalid_image)?;
        infrastructure_provider::limits::InfrastructureLimits::try_new(
            NonZeroUsize::new(key).ok_or_else(super::error::invalid_image)?,
            NonZeroUsize::new(record).ok_or_else(super::error::invalid_image)?,
            NonZeroUsize::new(bytes).ok_or_else(super::error::invalid_image)?,
            NonZeroUsize::new(MAX_RETAINED_MANIFEST_BODIES)
                .ok_or_else(super::error::invalid_image)?,
        )
    }
}
