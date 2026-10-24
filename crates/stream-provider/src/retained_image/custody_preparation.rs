//! Complete manifest and unique custody inventories precede native body
//! observations.

use infrastructure_provider::limits::InfrastructureLimits;
use storage_types::{ImageAddress, StorageResult};

use super::{
    batch::RetainedImageBatch,
    error::{capacity_rejected, invalid_image},
    manifest::RetainedImageManifest,
    observation::BodyReadRequest,
    preparation::{PreparedImageCustody, RetainedImageBodyWork},
    references::RetainedReferences,
};

pub(super) struct PreparedCustodyManifests {
    manifests: Vec<(Vec<u8>, RetainedReferences, Vec<usize>)>,
    outputs: Vec<Vec<u8>>,
    selected_work: Vec<RetainedImageBodyWork>,
}

impl PreparedCustodyManifests {
    pub(super) fn try_new(count: usize) -> StorageResult<Self> {
        let mut manifests = Vec::new();
        let mut outputs = Vec::new();
        manifests
            .try_reserve_exact(count)
            .map_err(|_| capacity_rejected())?;
        outputs
            .try_reserve_exact(count)
            .map_err(|_| capacity_rejected())?;
        Ok(Self {
            manifests,
            outputs,
            selected_work: Vec::new(),
        })
    }

    pub(super) fn include<'body>(
        &mut self,
        input: (&RetainedImageManifest, &[&'body [u8]]),
        batch: &mut RetainedImageBatch<'body>,
        limits: InfrastructureLimits,
    ) -> StorageResult<()> {
        let (manifest, images) = input;
        let references = manifest.references()?;
        let mut slots = batch.include_manifest(manifest, images)?;
        slots.sort_unstable();
        slots.dedup();
        self.selected_work
            .try_reserve(slots.len())
            .map_err(|_| capacity_rejected())?;
        let (encoded, _) = manifest.encode_payload(limits.max_record_bytes().get())?;
        limits.check_batch(
            1,
            encoded
                .len()
                .checked_add(limits.max_key_bytes().get())
                .ok_or_else(invalid_image)?,
        )?;
        self.manifests.push((encoded, references, slots));
        Ok(())
    }

    pub(super) fn bind(mut self, work: &[RetainedImageBodyWork]) -> StorageResult<Vec<Vec<u8>>> {
        for (encoded, references, slots) in self.manifests {
            self.selected_work.clear();
            for slot in slots {
                self.selected_work
                    .push(work.get(slot).cloned().ok_or_else(invalid_image)?);
            }
            references.require_body_work(&self.selected_work)?;
            self.outputs.push(encoded);
        }
        Ok(self.outputs)
    }
}

pub(super) struct PreparedCustodyBodies {
    addresses: Vec<(ImageAddress, usize)>,
    observations: Vec<PreparedImageCustody>,
}

impl PreparedCustodyBodies {
    pub(super) fn try_new<'request>(
        requests: impl ExactSizeIterator<Item = &'request BodyReadRequest>,
        limits: InfrastructureLimits,
    ) -> StorageResult<Self> {
        let mut addresses = Vec::new();
        let mut observations = Vec::new();
        addresses
            .try_reserve_exact(requests.len())
            .map_err(|_| capacity_rejected())?;
        observations
            .try_reserve_exact(requests.len())
            .map_err(|_| capacity_rejected())?;
        for request in requests {
            let history: [u8; 16] = request
                .key
                .as_ref()
                .resource
                .as_slice()
                .try_into()
                .map_err(|_| invalid_image())?;
            let address = ImageAddress::try_new(history.into(), request.identity)?;
            addresses.push((address, limits.validate_key(&request.key)?));
        }
        Ok(Self {
            addresses,
            observations,
        })
    }

    pub(super) fn bind(
        mut self,
        work: &[RetainedImageBodyWork],
    ) -> StorageResult<Vec<PreparedImageCustody>> {
        if work.len() != self.addresses.len() {
            return Err(invalid_image());
        }
        for ((address, key_bytes), body) in self.addresses.into_iter().zip(work) {
            if address.identity() != body.identity || body.encoded_bytes == 0 {
                return Err(invalid_image());
            }
            let physical_bytes = key_bytes
                .checked_add(body.encoded_bytes)
                .ok_or_else(invalid_image)?;
            self.observations.push(PreparedImageCustody {
                address,
                physical_bytes,
            });
        }
        self.observations.sort_unstable_by(|left, right| {
            left.address
                .history()
                .as_ref()
                .cmp(right.address.history().as_ref())
                .then_with(|| {
                    left.address
                        .identity()
                        .as_ref()
                        .cmp(right.address.identity().as_ref())
                })
        });
        Ok(self.observations)
    }
}
