use std::io::Read;

use infrastructure_provider::{
    limits::InfrastructureLimits, physical_attempt::PhysicalRecordAttempt,
};
use storage_types::StorageResult;

use super::{
    error::{capacity_rejected, invalid_image, invalid_observation},
    manifest::RetainedImageManifest,
    observation::BodyReadRequest,
};

/// Complete output admission and immutable body verification precede results.
#[derive(Default)]
pub struct ImageHydrationBatch {
    manifests: Vec<(RetainedImageManifest, Vec<usize>)>,
    images: Vec<BodyReadRequest>,
    addresses: Vec<usize>,
}

impl ImageHydrationBatch {
    pub fn try_new(input: Vec<RetainedImageManifest>, maximum: usize) -> StorageResult<Self> {
        let mut batch = Self::default();
        let mut remaining = maximum;
        batch
            .manifests
            .try_reserve_exact(input.len())
            .map_err(|_| capacity_rejected())?;
        for manifest in input {
            remaining = remaining
                .checked_sub(manifest.length)
                .ok_or_else(capacity_rejected)?;
            let slots = batch.include_manifest(&manifest, None)?;
            batch.manifests.push((manifest, slots));
        }
        Ok(batch)
    }

    pub(super) fn try_projected(
        input: Vec<(
            RetainedImageManifest,
            Vec<super::preparation::RetainedImageBodyWork>,
        )>,
    ) -> StorageResult<Self> {
        let mut batch = Self::default();
        batch
            .manifests
            .try_reserve_exact(input.len())
            .map_err(|_| capacity_rejected())?;
        for (manifest, work) in input {
            batch.include_projected(manifest, &work)?;
        }
        Ok(batch)
    }

    pub(super) fn include_projected(
        &mut self,
        manifest: RetainedImageManifest,
        work: &[super::preparation::RetainedImageBodyWork],
    ) -> StorageResult<()> {
        self.manifests
            .try_reserve(1)
            .map_err(|_| capacity_rejected())?;
        let slots = self.include_manifest(&manifest, Some(work))?;
        self.manifests.push((manifest, slots));
        Ok(())
    }

    pub async fn load<A: PhysicalRecordAttempt>(
        self,
        attempt: &mut A,
        limits: InfrastructureLimits,
    ) -> StorageResult<Vec<Vec<u8>>> {
        self.load_projected(attempt, limits, |reader, _| Self::load_original(reader))
            .await
    }

    /// Projection results remain private until every original and body
    /// verifies.
    pub(super) async fn load_projected<A: PhysicalRecordAttempt, T: Send>(
        self,
        attempt: &mut A,
        limits: InfrastructureLimits,
        mut project: impl FnMut(&mut dyn Read, [u8; 32]) -> StorageResult<T> + Send,
    ) -> StorageResult<Vec<T>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let waves = super::hydration_wave::prepare_waves(&self.images, limits)?;
            let mut images = Vec::new();
            let mut results = Vec::new();
            images
                .try_reserve_exact(self.images.len())
                .map_err(|_| capacity_rejected())?;
            results
                .try_reserve_exact(self.manifests.len())
                .map_err(|_| capacity_rejected())?;
            for wave in waves {
                images.extend(
                    super::hydration_wave::load_wave(attempt, &self.images[wave], limits).await?,
                );
            }
            let causal =
                super::causal_hydration::VerifiedCausalBodies::prepare(&self.manifests, &images)?;
            for (manifest, slots) in self.manifests {
                let mut reader = super::manifest_read::RetainedManifestRead::try_new(
                    &manifest, &images, &slots, &causal,
                )?;
                let result = project(&mut reader, manifest.digest)?;
                reader.require_complete()?;
                results.push(result);
            }
            Ok(results)
        }
        .await;
        settlement_guard.complete(result)
    }

    fn load_original(reader: &mut dyn Read) -> StorageResult<Vec<u8>> {
        let mut bytes = Vec::new();
        let mut scratch = [0; crate::constants::RETAINED_PAYLOAD_DECODE_CHUNK_BYTES];
        loop {
            let length = reader.read(&mut scratch).map_err(|_| invalid_image())?;
            if length == 0 {
                return Ok(bytes);
            }
            bytes.try_reserve(length).map_err(|_| capacity_rejected())?;
            bytes.extend_from_slice(&scratch[..length]);
        }
    }

    fn include_manifest(
        &mut self,
        manifest: &RetainedImageManifest,
        work: Option<&[super::preparation::RetainedImageBodyWork]>,
    ) -> StorageResult<Vec<usize>> {
        let references = manifest.references()?;
        if let Some(work) = work {
            references.require_body_work(work)?;
        }
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(manifest.segments.len())
            .map_err(|_| capacity_rejected())?;
        for segment in &manifest.segments {
            if let Some((address, length)) = segment.body_reference(manifest.history)? {
                let identity = address.identity();
                let key = super::address::image_record_key(address)?;
                let encoded_bytes = work
                    .map(|work| {
                        work.iter()
                            .find(|body| body.identity == identity)
                            .map(|body| body.encoded_bytes)
                            .ok_or_else(invalid_image)
                    })
                    .transpose()?;
                slots.push(self.include_image(BodyReadRequest {
                    key,
                    identity,
                    length,
                    encoded_bytes,
                })?);
            }
        }
        Ok(slots)
    }

    fn include_image(&mut self, image: BodyReadRequest) -> StorageResult<usize> {
        let insertion = match self
            .addresses
            .binary_search_by(|slot| self.images[*slot].key.cmp(&image.key))
        {
            Ok(index) => {
                let slot = self.addresses[index];
                let previous = &self.images[slot];
                if (previous.identity, previous.length, previous.encoded_bytes)
                    != (image.identity, image.length, image.encoded_bytes)
                {
                    return Err(invalid_observation());
                }
                return Ok(slot);
            }
            Err(index) => index,
        };
        self.images
            .try_reserve(1)
            .map_err(|_| capacity_rejected())?;
        self.addresses
            .try_reserve(1)
            .map_err(|_| capacity_rejected())?;
        let slot = self.images.len();
        self.images.push(image);
        self.addresses.insert(insertion, slot);
        Ok(slot)
    }
}
