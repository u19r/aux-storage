//! Fixed binary projection metadata has no JSON byte-array amplification.

use storage_types::{ImmutableImageId, StorageResult, TableHistoryId};

use super::{
    error::invalid_image, preparation::RetainedImageBodyWork, projection::RetainedProjectionRecord,
    read_work::RetainedReadWork,
};

impl RetainedProjectionRecord {
    pub(super) fn encode_binary(&self) -> StorageResult<Vec<u8>> {
        let capacity = crate::constants::RETAINED_PROJECTION_HEADER_BYTES
            .checked_add(
                self.work.bodies.len() * crate::constants::RETAINED_DESCRIPTOR_BODY_WORK_BYTES,
            )
            .and_then(|bytes| bytes.checked_add(self.manifest.len()))
            .ok_or_else(invalid_image)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(|_| super::error::capacity_rejected())?;
        bytes.push(crate::constants::RETAINED_PROJECTION_RECORD_FORMAT);
        bytes.extend_from_slice(self.history.as_ref());
        for length in [
            self.work.projection_bytes,
            self.work.metadata_bytes,
            self.manifest.len(),
        ] {
            append_length(&mut bytes, length)?;
        }
        bytes.push(u8::try_from(self.work.bodies.len()).map_err(|_| invalid_image())?);
        for body in &self.work.bodies {
            bytes.extend_from_slice(body.identity.as_ref());
            append_length(&mut bytes, body.encoded_bytes)?;
        }
        bytes.extend_from_slice(&self.manifest);
        Ok(bytes)
    }
}

pub(super) struct ProjectionDecoder<'a> {
    bytes: &'a [u8],
}

impl<'a> ProjectionDecoder<'a> {
    pub(super) fn decode(
        bytes: &'a [u8],
        history: TableHistoryId,
        maximum: usize,
    ) -> StorageResult<RetainedProjectionRecord> {
        let mut decoder = Self { bytes };
        if decoder.take(1)?[0] != crate::constants::RETAINED_PROJECTION_RECORD_FORMAT
            || decoder.take(16)? != history.as_ref()
        {
            return Err(invalid_image());
        }
        let projection_bytes = decoder.decode_length()?;
        let metadata_bytes = decoder.decode_length()?;
        let manifest_length = decoder.decode_length()?;
        let count = usize::from(decoder.take(1)?[0]);
        if !(1..=2).contains(&count) {
            return Err(invalid_image());
        }
        let mut bodies = Vec::new();
        bodies
            .try_reserve_exact(count)
            .map_err(|_| super::error::capacity_rejected())?;
        for _ in 0..count {
            let digest: [u8; 32] = decoder.take(32)?.try_into().map_err(|_| invalid_image())?;
            let identity = ImmutableImageId::from(digest);
            bodies.push(RetainedImageBodyWork {
                identity,
                encoded_bytes: decoder.decode_length()?,
            });
        }
        let borrowed = decoder.take(manifest_length)?;
        let mut manifest = Vec::new();
        manifest
            .try_reserve_exact(borrowed.len())
            .map_err(|_| super::error::capacity_rejected())?;
        manifest.extend_from_slice(borrowed);
        if !decoder.bytes.is_empty() {
            return Err(invalid_image());
        }
        RetainedProjectionRecord::try_new(
            history,
            manifest,
            RetainedReadWork {
                projection_bytes,
                metadata_bytes,
                bodies,
            },
            maximum,
        )
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

fn append_length(bytes: &mut Vec<u8>, length: usize) -> StorageResult<()> {
    bytes.extend_from_slice(
        &u64::try_from(length)
            .map_err(|_| invalid_image())?
            .to_be_bytes(),
    );
    Ok(())
}
