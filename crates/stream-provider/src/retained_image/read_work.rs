//! Persisted projection and physical work are admitted independently of
//! original spelling.
use std::io::{self, Cursor};

use storage_types::{StorageResult, TableHistoryId};

use super::{
    RetainedImageManifest, RetainedPayload,
    error::{capacity_rejected, invalid_image},
    image_read_work::{ImageProjectionShape, ImageReadWork},
    json_projection::RetainedJsonProjection,
    preparation::{PreparedImageManifest, RetainedImageBodyWork},
};

pub(super) struct RetainedReadWork {
    pub(super) projection_bytes: usize,
    pub(super) metadata_bytes: usize,
    pub(super) bodies: Vec<RetainedImageBodyWork>,
}

impl RetainedReadWork {
    /// Primary and projection tuples retain only history and punctuation
    /// inline.
    pub(super) fn require_two_body_manifest(&self, manifest: &[u8]) -> StorageResult<()> {
        if self.metadata_bytes > crate::constants::RETAINED_TWO_BODY_METADATA_BYTES
            || !(1..=2).contains(&self.bodies.len())
            || manifest.is_empty()
            || manifest.len()
                > RetainedPayload::encoded_length_bound(
                    crate::constants::RETAINED_TWO_BODY_METADATA_BYTES,
                )?
            || self.bodies.iter().enumerate().any(|(index, body)| {
                self.bodies[..index]
                    .iter()
                    .any(|previous| previous.identity == body.identity)
            })
        {
            return Err(invalid_image());
        }
        self.admit_item(usize::MAX)?;
        Ok(())
    }

    pub(super) fn prepare(
        bytes: &[u8],
        prepared: &PreparedImageManifest,
        bodies: Vec<RetainedImageBodyWork>,
    ) -> StorageResult<Self> {
        prepared.references.require_original(bytes)?;
        prepared.references.require_body_work(&bodies)?;
        // String escapes only shrink. Compact numbers can add a leading zero;
        // twice the already accepted tuple length covers that expansion.
        let mut maximum = bytes.len().checked_mul(2).ok_or_else(invalid_image)?;
        let mut projection = RetainedJsonProjection::new(Cursor::new(bytes), &mut maximum);
        let projection_bytes = usize::try_from(
            io::copy(&mut projection, &mut io::sink()).map_err(|_| invalid_image())?,
        )
        .map_err(|_| invalid_image())?;
        Ok(Self {
            projection_bytes,
            metadata_bytes: prepared.metadata_bytes,
            bodies,
        })
    }

    pub(super) fn copy_body_work(&self) -> StorageResult<Vec<RetainedImageBodyWork>> {
        let mut bodies = Vec::new();
        bodies
            .try_reserve_exact(self.bodies.len())
            .map_err(|_| capacity_rejected())?;
        bodies.extend_from_slice(&self.bodies);
        Ok(bodies)
    }

    pub(super) fn decode_manifest(
        &self,
        encoded: &[u8],
        history: TableHistoryId,
        maximum: usize,
    ) -> StorageResult<RetainedImageManifest> {
        self.admit(maximum)?;
        let metadata = RetainedPayload::decode(encoded, self.metadata_bytes)?;
        if metadata.len() != self.metadata_bytes {
            return Err(invalid_image());
        }
        // Raw lengths describe streaming verification, never an allocation
        // budget.
        let manifest = RetainedImageManifest::decode(&metadata, usize::MAX)?;
        let references = manifest.references()?;
        references.require_history(history)?;
        references.require_body_work(&self.bodies)?;
        Ok(manifest)
    }

    pub(super) fn admit(&self, maximum: usize) -> StorageResult<usize> {
        let charge = self.calculate_physical_bytes()?.max(self.projection_bytes);
        if charge > maximum {
            return Err(capacity_rejected());
        }
        Ok(charge)
    }

    pub(super) fn admit_item(&self, physical_maximum: usize) -> StorageResult<usize> {
        Ok(self
            .qualify_images(physical_maximum, ImageProjectionShape::Item)?
            .page_bytes())
    }

    pub(super) fn admit_item_batch<'work>(
        works: impl Iterator<Item = &'work Self>,
        physical_maximum: usize,
    ) -> StorageResult<usize> {
        Ok(ImageReadWork::admit_batch(
            works.map(|work| work.qualify_images(physical_maximum, ImageProjectionShape::Item)),
            physical_maximum,
        )?
        .page_bytes())
    }

    pub(super) fn qualify_images(
        &self,
        physical_maximum: usize,
        shape: ImageProjectionShape,
    ) -> StorageResult<ImageReadWork> {
        let physical = self.calculate_physical_bytes()?;
        if self.projection_bytes > shape.bound_projection_bytes(self.metadata_bytes)? {
            return Err(invalid_image());
        }
        if physical > physical_maximum {
            return Err(capacity_rejected());
        }
        Ok(ImageReadWork::new(physical, self.projection_bytes))
    }

    fn calculate_physical_bytes(&self) -> StorageResult<usize> {
        if self.projection_bytes == 0
            || self.metadata_bytes == 0
            || self.bodies.len() > crate::constants::MAX_RETAINED_MANIFEST_BODIES
        {
            return Err(invalid_image());
        }
        let key_bytes = 16 + crate::constants::RETAINED_IMAGE_NAMESPACE.len() + 16 + 32;
        self.bodies
            .iter()
            .try_fold(self.metadata_bytes, |bytes, body| {
                if body.encoded_bytes == 0 {
                    return Err(invalid_image());
                }
                bytes
                    .checked_add(key_bytes)
                    .and_then(|bytes| bytes.checked_add(body.encoded_bytes))
                    .ok_or_else(invalid_image)
            })
    }
}
