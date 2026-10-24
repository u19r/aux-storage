//! Validated original spans outlive the temporary canonical encoding buffer.

use std::borrow::Cow;

use infrastructure_provider::limits::InfrastructureLimits;
use storage_types::StorageResult;

use super::{
    batch::RetainedImageBatch,
    error::{capacity_rejected, invalid_image},
    manifest::{ImageSegment, RetainedImageManifest},
    ownership_inventory::OwnershipRecords,
    preparation::RetainedImageCustody,
};

#[derive(Default)]
pub struct RetainedOriginalBatch<'a> {
    manifests: Vec<RetainedImageManifest>,
    bodies: OwnershipRecords<Cow<'a, [u8]>>,
}

impl<'a> RetainedOriginalBatch<'a> {
    pub fn include(
        &mut self,
        manifest: RetainedImageManifest,
        images: &[&'a [u8]],
    ) -> StorageResult<()> {
        let bodies = RetainedImageBatch::try_new(&manifest, images)?;
        self.include_bodies(manifest, bodies, |bytes| Ok(Cow::Borrowed(bytes)))
    }

    fn include_bodies<'input>(
        &mut self,
        manifest: RetainedImageManifest,
        bodies: RetainedImageBatch<'input>,
        retain: impl Fn(&'input [u8]) -> StorageResult<Cow<'a, [u8]>>,
    ) -> StorageResult<()> {
        let bodies = bodies.into_iter();
        let mut pending = Vec::new();
        pending
            .try_reserve_exact(bodies.len())
            .map_err(|_| capacity_rejected())?;
        self.manifests
            .try_reserve(1)
            .map_err(|_| capacity_rejected())?;
        self.bodies.try_reserve(bodies.len())?;
        for (declaration, bytes) in bodies {
            if let Some(previous) = self.bodies.get(&declaration.key) {
                if previous.as_ref() != bytes {
                    return Err(invalid_image());
                }
            } else {
                pending.push((declaration.key, retain(bytes)?));
            }
        }
        for (key, bytes) in pending {
            self.bodies.try_insert(key, bytes)?;
        }
        self.manifests.push(manifest);
        Ok(())
    }

    pub async fn prepare_with_custody<A: super::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        limits: InfrastructureLimits,
        custody: &impl RetainedImageCustody<A>,
    ) -> StorageResult<Vec<Vec<u8>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            let mut images = Vec::new();
            let mut declarations = Vec::new();
            images
                .try_reserve_exact(self.manifests.len())
                .map_err(|_| capacity_rejected())?;
            declarations
                .try_reserve_exact(self.manifests.len())
                .map_err(|_| capacity_rejected())?;
            for manifest in &self.manifests {
                images.push(self.borrow_images(manifest)?);
            }
            declarations.extend(
                self.manifests
                    .iter()
                    .zip(&images)
                    .map(|(manifest, images)| (manifest, images.as_slice())),
            );
            RetainedImageManifest::prepare_batch_with_custody(
                settlement.as_mut(),
                &declarations,
                limits,
                custody,
            )
            .await
        }
        .await;
        settlement.complete(result)
    }

    fn borrow_images<'borrow>(
        &'borrow self,
        manifest: &RetainedImageManifest,
    ) -> StorageResult<Vec<&'borrow [u8]>> {
        let mut images = Vec::new();
        images
            .try_reserve_exact(manifest.segments.len())
            .map_err(|_| capacity_rejected())?;
        for segment in &manifest.segments {
            if matches!(segment, ImageSegment::CausalSlice { .. }) {
                continue;
            }
            let Some((address, _)) = segment.body_reference(manifest.history)? else {
                continue;
            };
            let key = super::address::image_record_key(address)?;
            images.push(
                self.bodies
                    .get(&key)
                    .map(AsRef::as_ref)
                    .ok_or_else(invalid_image)?,
            );
        }
        Ok(images)
    }
}

impl AsRef<[RetainedImageManifest]> for RetainedOriginalBatch<'_> {
    fn as_ref(&self) -> &[RetainedImageManifest] {
        &self.manifests
    }
}

impl RetainedOriginalBatch<'static> {
    pub fn include_owned(
        &mut self,
        manifest: RetainedImageManifest,
        images: &[&[u8]],
    ) -> StorageResult<()> {
        let bodies = RetainedImageBatch::try_new(&manifest, images)?;
        self.include_bodies(manifest, bodies, |bytes| {
            let mut owned = Vec::new();
            owned
                .try_reserve_exact(bytes.len())
                .map_err(|_| capacity_rejected())?;
            owned.extend_from_slice(bytes);
            Ok(Cow::Owned(owned))
        })
    }
}
