//! Native descriptors store manifests and body work without JSON byte arrays.
use serde::{Serialize, de::DeserializeOwned};
use storage_types::{ImmutableImageId, StorageResult, TableHistoryId};

use super::{
    error::invalid_image, preparation::RetainedImageBodyWork, read_work::RetainedReadWork,
};

const MAGIC: &[u8; 4] = b"ARD1";
const HEADER_BYTES: usize = 4 + 1 + 16 + 4 * 8 + 1;

#[derive(Clone, Copy, PartialEq)]
pub(super) enum DescriptorKind {
    Primary = 1,
    ItemHistory = 2,
    Pointer = 3,
    SystemRecord = 4,
}

impl DescriptorKind {
    fn select_image_shape(self) -> super::image_read_work::ImageProjectionShape {
        use super::image_read_work::ImageProjectionShape;
        match self {
            Self::Primary => ImageProjectionShape::Item,
            Self::ItemHistory => ImageProjectionShape::History,
            Self::Pointer => ImageProjectionShape::Pointer,
            Self::SystemRecord => ImageProjectionShape::System,
        }
    }
}

pub(super) struct DescriptorInput<'a, I> {
    pub kind: DescriptorKind,
    pub history: TableHistoryId,
    pub identity: &'a I,
    pub manifest: &'a [u8],
    pub work: &'a RetainedReadWork,
}

pub(super) struct DecodedDescriptor<I> {
    pub history: TableHistoryId,
    pub identity: I,
    pub manifest: Vec<u8>,
    pub work: RetainedReadWork,
}

impl<I: Serialize> DescriptorInput<'_, I> {
    pub(super) fn encode(&self, maximum: usize) -> StorageResult<Vec<u8>> {
        require_descriptor(self.history, self.manifest, self.work)?;
        self.work
            .qualify_images(usize::MAX, self.kind.select_image_shape())?;
        let identity = super::json_output::RetainedJsonOutput::encode(self.identity)?;
        let capacity =
            encoded_length_bound(identity.len(), self.work.bodies.len(), self.manifest.len())?;
        if capacity > maximum {
            return Err(super::error::capacity_rejected());
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(|_| super::error::capacity_rejected())?;
        bytes.extend_from_slice(MAGIC);
        bytes.push(self.kind as u8);
        bytes.extend_from_slice(self.history.as_ref());
        for length in [
            identity.len(),
            self.work.projection_bytes,
            self.work.metadata_bytes,
            self.manifest.len(),
        ] {
            append_length(&mut bytes, length)?;
        }
        bytes.push(u8::try_from(self.work.bodies.len()).map_err(|_| invalid_image())?);
        bytes.extend_from_slice(&identity);
        for body in &self.work.bodies {
            bytes.extend_from_slice(body.identity.as_ref());
            append_length(&mut bytes, body.encoded_bytes)?;
        }
        bytes.extend_from_slice(self.manifest);
        Ok(bytes)
    }
}

pub(super) struct DescriptorDecoder<'a> {
    bytes: &'a [u8],
}

impl<'a> DescriptorDecoder<'a> {
    pub(super) fn decode<I: DeserializeOwned>(
        bytes: &'a [u8],
        kind: DescriptorKind,
        maximum: usize,
    ) -> StorageResult<DecodedDescriptor<I>> {
        if bytes.len() > maximum {
            return Err(invalid_image());
        }
        let mut decoder = Self { bytes };
        if decoder.take(4)? != MAGIC || decoder.take(1)?[0] != kind as u8 {
            return Err(invalid_image());
        }
        let history = TableHistoryId::from(
            <[u8; 16]>::try_from(decoder.take(16)?).map_err(|_| invalid_image())?,
        );
        let identity_length = decoder.decode_length()?;
        let projection_bytes = decoder.decode_length()?;
        let metadata_bytes = decoder.decode_length()?;
        let manifest_length = decoder.decode_length()?;
        let count = usize::from(decoder.take(1)?[0]);
        if count > crate::constants::MAX_RETAINED_MANIFEST_BODIES {
            return Err(invalid_image());
        }
        let identity = decoder.take(identity_length)?;
        let bodies = decoder.decode_body_work(count)?;
        let manifest = decoder.take(manifest_length)?;
        if !decoder.bytes.is_empty() {
            return Err(invalid_image());
        }
        let work = RetainedReadWork {
            projection_bytes,
            metadata_bytes,
            bodies,
        };
        require_descriptor(history, manifest, &work)?;
        work.qualify_images(maximum, kind.select_image_shape())?;
        Ok(DecodedDescriptor {
            history,
            identity: serde_json::from_slice(identity)?,
            manifest: {
                let mut owned = Vec::new();
                owned
                    .try_reserve_exact(manifest.len())
                    .map_err(|_| super::error::capacity_rejected())?;
                owned.extend_from_slice(manifest);
                owned
            },
            work,
        })
    }

    fn decode_body_work(&mut self, count: usize) -> StorageResult<Vec<RetainedImageBodyWork>> {
        let mut bodies = Vec::new();
        bodies
            .try_reserve_exact(count)
            .map_err(|_| super::error::capacity_rejected())?;
        for _ in 0..count {
            let digest = <[u8; 32]>::try_from(self.take(32)?).map_err(|_| invalid_image())?;
            bodies.push(RetainedImageBodyWork {
                identity: ImmutableImageId::from(digest),
                encoded_bytes: self.decode_length()?,
            });
        }
        Ok(bodies)
    }

    fn take(&mut self, length: usize) -> StorageResult<&'a [u8]> {
        let (value, remaining) = self
            .bytes
            .split_at_checked(length)
            .ok_or_else(invalid_image)?;
        self.bytes = remaining;
        Ok(value)
    }

    fn decode_length(&mut self) -> StorageResult<usize> {
        usize::try_from(u64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| invalid_image())?,
        ))
        .map_err(|_| invalid_image())
    }
}

fn require_descriptor(
    history: TableHistoryId,
    manifest: &[u8],
    work: &RetainedReadWork,
) -> StorageResult<()> {
    if history.as_ref() == &[0; 16]
        || manifest.is_empty()
        || work.bodies.len() > crate::constants::MAX_RETAINED_MANIFEST_BODIES
        || work.bodies.iter().enumerate().any(|(index, body)| {
            body.encoded_bytes == 0
                || work.bodies[..index]
                    .iter()
                    .any(|previous| previous.identity == body.identity)
        })
    {
        return Err(invalid_image());
    }
    Ok(())
}

fn append_length(bytes: &mut Vec<u8>, length: usize) -> StorageResult<()> {
    bytes.extend_from_slice(
        &u64::try_from(length)
            .map_err(|_| invalid_image())?
            .to_be_bytes(),
    );
    Ok(())
}

pub(super) fn encoded_length_bound(
    identity_bytes: usize,
    bodies: usize,
    manifest_bytes: usize,
) -> StorageResult<usize> {
    HEADER_BYTES
        .checked_add(identity_bytes)
        .and_then(|bytes| {
            bytes.checked_add(
                bodies.checked_mul(crate::constants::RETAINED_DESCRIPTOR_BODY_WORK_BYTES)?,
            )
        })
        .and_then(|bytes| bytes.checked_add(manifest_bytes))
        .ok_or_else(invalid_image)
}
