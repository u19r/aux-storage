//! Decoded causal sources live only in their admitted reconstruction batch.

use std::collections::BTreeMap;

use infrastructure_provider::record::DurableRecord;
use sha2::{Digest as _, Sha256};
use storage_types::{ImmutableImageId, StorageResult};

use super::{
    error::invalid_image,
    manifest::{ImageSegment, RetainedImageManifest},
    payload::RetainedPayload,
};

pub(super) struct VerifiedCausalBodies {
    bodies: BTreeMap<usize, (ImmutableImageId, Vec<u8>)>,
}

impl VerifiedCausalBodies {
    pub(super) fn prepare(
        manifests: &[(RetainedImageManifest, Vec<usize>)],
        rows: &[DurableRecord],
    ) -> StorageResult<Self> {
        let mut verified = Self {
            bodies: BTreeMap::new(),
        };
        for (manifest, slots) in manifests {
            let mut slots = slots.iter();
            for segment in &manifest.segments {
                if segment.body_reference(manifest.history)?.is_none() {
                    continue;
                }
                let slot = *slots.next().ok_or_else(invalid_image)?;
                if let ImageSegment::Causal {
                    identity, length, ..
                } = segment
                {
                    verified.include_source(
                        slot,
                        (*identity, *length),
                        rows.get(slot).ok_or_else(invalid_image)?,
                    )?;
                }
            }
            if slots.next().is_some() {
                return Err(invalid_image());
            }
        }
        Ok(verified)
    }

    fn include_source(
        &mut self,
        slot: usize,
        declaration: (ImmutableImageId, usize),
        row: &DurableRecord,
    ) -> StorageResult<()> {
        if let Some((identity, bytes)) = self.bodies.get(&slot) {
            if (*identity, bytes.len()) != declaration {
                return Err(invalid_image());
            }
            return Ok(());
        }
        let bytes = RetainedPayload::decode(&row.bytes, declaration.1)?;
        if bytes.len() != declaration.1
            || <[u8; 32]>::from(Sha256::digest(&bytes)) != *declaration.0.as_ref()
        {
            return Err(invalid_image());
        }
        self.bodies.insert(slot, (declaration.0, bytes));
        Ok(())
    }

    pub(super) fn selection(&self, slot: usize, selection: (usize, usize)) -> StorageResult<&[u8]> {
        let (_, bytes) = self.bodies.get(&slot).ok_or_else(invalid_image)?;
        let end = selection
            .0
            .checked_add(selection.1)
            .ok_or_else(invalid_image)?;
        bytes.get(selection.0..end).ok_or_else(invalid_image)
    }
}
