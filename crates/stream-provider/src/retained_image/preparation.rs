//! Exact manifests and immutable body changes are prepared by one owner.

use infrastructure_provider::{
    limits::InfrastructureLimits, physical_attempt::PhysicalRecordAttempt,
};
use storage_types::{ImageAddress, StorageResult};

use super::{
    batch::RetainedImageBatch, custody_preparation::PreparedCustodyManifests,
    manifest::RetainedImageManifest,
};

#[derive(Clone)]
pub(super) struct RetainedImageBodyWork {
    pub(super) identity: storage_types::ImmutableImageId,
    pub(super) encoded_bytes: usize,
}

pub(super) struct PreparedImageManifest {
    pub(super) encoded: Vec<u8>,
    pub(super) metadata_bytes: usize,
    pub(super) references: super::references::RetainedReferences,
}

pub(super) struct RetainedImagePreparation {
    pub(super) manifest: PreparedImageManifest,
    pub(super) bodies: super::prepared_bodies::PreparedImageBodies,
}

impl RetainedImagePreparation {
    pub(super) async fn publish<A: PhysicalRecordAttempt>(
        self,
        attempt: &mut A,
    ) -> StorageResult<Vec<u8>> {
        self.bodies.store(attempt).await?;
        Ok(self.manifest.encoded)
    }
}

/// Protected body work consumed inside preparation, before manifest
/// publication.
pub struct PreparedImageCustody {
    pub(super) address: ImageAddress,
    pub(super) physical_bytes: usize,
}

impl PreparedImageCustody {
    pub fn address(&self) -> ImageAddress {
        self.address
    }
    pub fn physical_bytes(&self) -> usize {
        self.physical_bytes
    }
}

/// Custody joins the preparation owner's native attempt; descriptors cannot
/// supply it.
pub trait RetainedImageCustody<A: PhysicalRecordAttempt>: Sync {
    fn retain(
        &self,
        attempt: &mut A,
        bodies: &[PreparedImageCustody],
    ) -> impl std::future::Future<Output = StorageResult<()>> + Send;
}

impl RetainedImageManifest {
    pub async fn prepare_with_custody<A: super::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        images: &[&[u8]],
        limits: InfrastructureLimits,
        custody: &impl RetainedImageCustody<A>,
    ) -> StorageResult<Vec<u8>> {
        let encoded =
            Self::prepare_batch_with_custody(attempt, &[(self, images)], limits, custody).await?;
        encoded
            .into_iter()
            .next()
            .ok_or_else(super::error::invalid_image)
    }

    /// Prepare scoped immutable bodies once for every manifest in the caller's
    /// attempt.
    pub async fn prepare_batch_with_custody<A: super::RetainedOwnershipAttempt>(
        attempt: &mut A,
        input: &[(&Self, &[&[u8]])],
        limits: InfrastructureLimits,
        custody: &impl RetainedImageCustody<A>,
    ) -> StorageResult<Vec<Vec<u8>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let mut batch = RetainedImageBatch::empty();
            let mut manifests = PreparedCustodyManifests::try_new(input.len())?;
            for (manifest, images) in input {
                manifests.include((manifest, images), &mut batch, limits)?;
            }
            let observations = batch.prepare_custody(limits)?;
            let bodies = batch.prepare(attempt, limits).await?;
            let manifests = manifests.bind(&bodies.work)?;
            let observations = observations.bind(&bodies.work)?;
            bodies.store(attempt).await?;
            if !observations.is_empty() {
                custody.retain(attempt, &observations).await?;
            }
            Ok(manifests)
        }
        .await;
        settlement.complete(result)
    }

    /// Verify the complete image declaration before any native lookup. The
    /// caller charges and stores body changes with the encoded, compressed
    /// manifest in its one native attempt; preparation alone does not
    /// certify durable custody.
    pub(super) async fn prepare<A: super::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        images: &[&[u8]],
        limits: InfrastructureLimits,
    ) -> StorageResult<RetainedImagePreparation> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let references = self.references()?;
            let bodies = RetainedImageBatch::try_new(self, images)?;
            let (encoded, metadata_bytes) = self.encode_payload(limits.max_record_bytes().get())?;
            limits.check_batch(
                1,
                encoded
                    .len()
                    .checked_add(limits.max_key_bytes().get())
                    .ok_or_else(super::error::invalid_image)?,
            )?;
            let bodies = bodies.prepare(attempt, limits).await?;
            references.require_body_work(&bodies.work)?;
            let manifest = PreparedImageManifest {
                encoded,
                metadata_bytes,
                references,
            };
            Ok(RetainedImagePreparation { manifest, bodies })
        }
        .await;
        settlement_guard.complete(result)
    }
}
