use infrastructure_provider::{limits::InfrastructureLimits, record::DurableRecord};
use storage_types::{ImageAddress, ImmutableImageId, ReadProtection, Revision, StorageResult};

use super::{
    address::image_record_key,
    error::{capacity_rejected, invalid_image},
    hydration_wave::prepare_waves,
    manifest::{ImageSegment, RetainedImageManifest},
    observation::{BodyReadRequest, load_body_wave},
    payload::RetainedPayload,
};

/// Immutable content effects; the caller stores and charges them before
/// publishing references.
pub(super) struct RetainedImageBatch<'a> {
    images: Vec<(BodyReadRequest, &'a [u8])>,
    addresses: Vec<usize>,
}

impl<'a> RetainedImageBatch<'a> {
    pub(super) fn try_new(
        manifest: &RetainedImageManifest,
        images: &[&'a [u8]],
    ) -> StorageResult<Self> {
        let mut batch = Self::empty();
        batch.include_manifest(manifest, images)?;
        Ok(batch)
    }

    pub(super) fn empty() -> Self {
        Self {
            images: Vec::new(),
            addresses: Vec::new(),
        }
    }

    pub(super) fn prepare_custody(
        &self,
        limits: InfrastructureLimits,
    ) -> StorageResult<super::custody_preparation::PreparedCustodyBodies> {
        super::custody_preparation::PreparedCustodyBodies::try_new(
            self.images.iter().map(|(request, _)| request),
            limits,
        )
    }

    pub(super) fn include_manifest(
        &mut self,
        manifest: &RetainedImageManifest,
        images: &[&'a [u8]],
    ) -> StorageResult<Vec<usize>> {
        let mut declared = Vec::new();
        declared
            .try_reserve(manifest.segments.len())
            .map_err(|_| capacity_rejected())?;
        for segment in &manifest.segments {
            if matches!(segment, ImageSegment::CausalSlice { .. }) {
                continue;
            }
            declared.extend(segment.body_reference(manifest.history)?);
        }
        if images.len() != declared.len() {
            return Err(invalid_image());
        }
        let mut slots = Vec::new();
        slots
            .try_reserve(images.len())
            .map_err(|_| capacity_rejected())?;
        self.images
            .try_reserve(images.len())
            .map_err(|_| capacity_rejected())?;
        self.addresses
            .try_reserve(images.len())
            .map_err(|_| capacity_rejected())?;
        for &image in images {
            let identity = ImmutableImageId::from_content(image);
            let position = declared
                .iter()
                .position(|(address, length)| {
                    address.identity() == identity && *length == image.len()
                })
                .ok_or_else(invalid_image)?;
            let (address, _) = declared.swap_remove(position);
            let key = image_record_key(address)?;
            match self
                .addresses
                .binary_search_by(|slot| self.images[*slot].0.key.cmp(&key))
            {
                Ok(index) => {
                    let slot = self.addresses[index];
                    if self.images[slot].1 != image {
                        return Err(invalid_image());
                    }
                    slots.push(slot);
                }
                Err(index) => {
                    let slot = self.images.len();
                    self.images.push((
                        BodyReadRequest {
                            key,
                            identity,
                            length: image.len(),
                            encoded_bytes: None,
                        },
                        image,
                    ));
                    self.addresses.insert(index, slot);
                    slots.push(slot);
                }
            }
        }
        Ok(slots)
    }

    pub(super) async fn prepare<A: super::RetainedOwnershipAttempt>(
        self,
        attempt: &mut A,
        limits: InfrastructureLimits,
    ) -> StorageResult<super::prepared_bodies::PreparedImageBodies> {
        let mut images = Vec::new();
        let mut originals = Vec::new();
        let mut changes = Vec::new();
        let mut bodies = Vec::new();
        images
            .try_reserve(self.images.len())
            .map_err(|_| capacity_rejected())?;
        originals
            .try_reserve(self.images.len())
            .map_err(|_| capacity_rejected())?;
        changes
            .try_reserve(self.images.len())
            .map_err(|_| capacity_rejected())?;
        bodies
            .try_reserve(self.images.len())
            .map_err(|_| capacity_rejected())?;
        for (image, original) in self {
            images.push(image);
            originals.push(original);
        }
        let waves = prepare_waves(&images, limits)?;
        attempt.ownership_changes().reserve_bodies(images.len())?;
        for wave in waves {
            let declarations = &images[wave.clone()];
            let rows =
                load_body_wave(attempt, declarations, limits, ReadProtection::Conflict).await?;
            for ((declaration, image), row) in declarations.iter().zip(&originals[wave]).zip(rows) {
                let created = row.is_none();
                let (change, body) = Self::prepare_observed_body(declaration, image, row, limits)?;
                let history: [u8; 16] = declaration
                    .key
                    .as_ref()
                    .resource
                    .as_slice()
                    .try_into()
                    .map_err(|_| invalid_image())?;
                attempt.ownership_changes().stage_body(
                    super::ownership_changes::PreparedBodyObservation {
                        address: ImageAddress::try_new(history.into(), declaration.identity)?,
                        raw_bytes: declaration.length,
                        encoded_bytes: body.encoded_bytes,
                        created,
                        charge: None,
                    },
                )?;
                changes.extend(change);
                bodies.push(body);
            }
        }
        Ok(super::prepared_bodies::PreparedImageBodies::new(
            changes, bodies, limits,
        ))
    }

    fn prepare_observed_body(
        declaration: &BodyReadRequest,
        image: &[u8],
        previous: Option<DurableRecord>,
        limits: InfrastructureLimits,
    ) -> StorageResult<(
        Option<DurableRecord>,
        super::preparation::RetainedImageBodyWork,
    )> {
        let (change, encoded_bytes) = match previous {
            Some(row) => {
                RetainedPayload::require_content(&row.bytes, image)?;
                (None, row.bytes.len())
            }
            None => {
                let record = Self::prepare_missing_body(declaration, image, limits)?;
                let length = record.bytes.len();
                (Some(record), length)
            }
        };
        Ok((
            change,
            super::preparation::RetainedImageBodyWork {
                identity: declaration.identity,
                encoded_bytes,
            },
        ))
    }

    fn prepare_missing_body(
        declaration: &BodyReadRequest,
        image: &[u8],
        limits: InfrastructureLimits,
    ) -> StorageResult<DurableRecord> {
        Ok(DurableRecord {
            key: (&declaration.key).try_into()?,
            revision: Revision::INITIAL,
            bytes: RetainedPayload::encode(image, limits.max_record_bytes().get())?.into(),
        })
    }
}

impl<'a> IntoIterator for RetainedImageBatch<'a> {
    type Item = (BodyReadRequest, &'a [u8]);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.images.into_iter()
    }
}
