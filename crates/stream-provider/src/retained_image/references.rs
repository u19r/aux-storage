//! A complete manifest has one canonical, validated view of body dependencies.

use sha2::{Digest as _, Sha256};
use storage_types::{ImageAddress, StorageResult, TableHistoryId};

use super::{
    error::{capacity_rejected, invalid_image},
    manifest::{ImageSegment, RetainedImageManifest},
    payload::RetainedPayload,
    preparation::RetainedImageBodyWork,
};

pub(super) struct RetainedReferences {
    length: usize,
    digest: [u8; 32],
    history: Option<TableHistoryId>,
    causal: Option<super::causal_body::CausalBodyScope>,
    bodies: Vec<BodyReference>,
}

struct BodyReference {
    address: ImageAddress,
    raw_bytes: usize,
}

impl TryFrom<&RetainedImageManifest> for RetainedReferences {
    type Error = storage_types::StorageError;

    fn try_from(manifest: &RetainedImageManifest) -> StorageResult<Self> {
        let mut references = Self {
            length: manifest.length,
            digest: manifest.digest,
            history: manifest.history,
            causal: manifest.require_causal_layout()?,
            bodies: Vec::new(),
        };
        for (index, segment) in manifest.segments.iter().enumerate() {
            if matches!(
                segment,
                ImageSegment::Causal { .. } | ImageSegment::CausalSlice { .. }
            ) && index % 2 != 1
            {
                return Err(invalid_image());
            }
            if let Some((address, raw_bytes)) = segment.body_reference(manifest.history)? {
                references.include_body(BodyReference { address, raw_bytes })?;
            }
        }
        super::manifest::require_image_scope(references.history, references.bodies.len())?;
        references.bodies.sort_unstable_by(|left, right| {
            left.address
                .identity()
                .as_ref()
                .cmp(right.address.identity().as_ref())
        });
        Ok(references)
    }
}

impl RetainedReferences {
    pub(super) fn require_original(&self, bytes: &[u8]) -> StorageResult<()> {
        if bytes.len() != self.length || <[u8; 32]>::from(Sha256::digest(bytes)) != self.digest {
            return Err(invalid_image());
        }
        Ok(())
    }

    pub(super) fn require_history(&self, history: TableHistoryId) -> StorageResult<()> {
        if self.causal.is_some()
            || history.as_ref() == &[0; 16]
            || self.history.is_some_and(|observed| observed != history)
        {
            return Err(invalid_image());
        }
        Ok(())
    }

    pub(super) fn require_body_work(&self, work: &[RetainedImageBodyWork]) -> StorageResult<()> {
        if work.len() != self.bodies.len() {
            return Err(invalid_image());
        }
        for (index, body) in work.iter().enumerate() {
            if body.encoded_bytes == 0
                || work[..index]
                    .iter()
                    .any(|previous| previous.identity == body.identity)
            {
                return Err(invalid_image());
            }
            let reference = self
                .bodies
                .iter()
                .find(|reference| reference.address.identity() == body.identity)
                .ok_or_else(invalid_image)?;
            if body.encoded_bytes > RetainedPayload::encoded_length_bound(reference.raw_bytes)? {
                return Err(invalid_image());
            }
        }
        Ok(())
    }

    pub(super) fn prepare_owner(
        &self,
        kind: super::owner_record::RetainedOwnerKind,
        locator: infrastructure_provider::record::DurableRecordKey,
        descriptor: &[u8],
    ) -> StorageResult<Option<super::owner_record::RetainedOwnerRecord>> {
        if let Some(scope) = self.causal
            && (kind != super::owner_record::RetainedOwnerKind::Event
                || scope != super::causal_body::CausalBodyScope::try_from(&locator)?)
        {
            return Err(invalid_image());
        }
        if self.bodies.is_empty() {
            return Ok(None);
        }
        let mut bodies = Vec::new();
        bodies
            .try_reserve_exact(self.bodies.len())
            .map_err(|_| capacity_rejected())?;
        bodies.extend(
            self.bodies
                .iter()
                .map(|body| (body.address, body.raw_bytes)),
        );
        let owner = super::owner_record::RetainedOwnerRecord {
            kind,
            locator,
            history: self.history.ok_or_else(invalid_image)?,
            descriptor: Sha256::digest(descriptor).into(),
            bodies,
        };
        owner.require_body_references()?;
        Ok(Some(owner))
    }

    fn include_body(&mut self, body: BodyReference) -> StorageResult<()> {
        if body.raw_bytes == 0 {
            return Err(invalid_image());
        }
        if let Some(previous) = self
            .bodies
            .iter()
            .find(|previous| previous.address == body.address)
        {
            if previous.raw_bytes != body.raw_bytes {
                return Err(invalid_image());
            }
            return Ok(());
        }
        if self
            .bodies
            .iter()
            .any(|previous| previous.address.identity() == body.address.identity())
        {
            return Err(invalid_image());
        }
        if self.bodies.len() == crate::constants::MAX_RETAINED_MANIFEST_BODIES {
            return Err(invalid_image());
        }
        self.bodies
            .try_reserve(1)
            .map_err(|_| capacity_rejected())?;
        self.bodies.push(body);
        Ok(())
    }
}
