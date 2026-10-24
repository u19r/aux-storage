use std::io::{Read, Write};

use sha2::{Digest, Sha256};
use storage_types::StorageResult;

use super::error::{capacity_rejected, invalid_image};
use crate::constants::{
    RETAINED_PAYLOAD_FORMAT, RETAINED_PAYLOAD_HEADER_BYTES, RETAINED_RAW_PAYLOAD_CHECKSUM_BYTES,
    RETAINED_RAW_PAYLOAD_FORMAT, RETAINED_RAW_PAYLOAD_MAX_BYTES,
};

/// One versioned bounded encoding shared by manifests and immutable bodies.
pub struct RetainedPayload;

impl RetainedPayload {
    pub(super) fn encode(bytes: &[u8], maximum: usize) -> StorageResult<Vec<u8>> {
        Self::encode_with(bytes.len(), maximum, |encoder| {
            encoder.write_all(bytes).map_err(|_| invalid_image())
        })
    }

    pub(super) fn encode_with(
        length: usize,
        maximum: usize,
        emit: impl FnOnce(&mut dyn Write) -> StorageResult<()>,
    ) -> StorageResult<Vec<u8>> {
        let maximum = maximum.min(Self::encoded_length_bound(length)?);
        if length <= RETAINED_RAW_PAYLOAD_MAX_BYTES
            && length
                .saturating_add(RETAINED_PAYLOAD_HEADER_BYTES + RETAINED_RAW_PAYLOAD_CHECKSUM_BYTES)
                <= maximum
        {
            return Self::encode_raw(length, maximum, emit);
        }
        let output = super::payload_write::RetainedPayloadWrite::try_new(length, maximum)?;
        let mut encoder = flate2::write::ZlibEncoder::new(output, flate2::Compression::default());
        let emitted = emit(&mut encoder);
        encoder.get_ref().require_capacity()?;
        emitted?;
        encoder
            .finish()
            .map(Into::into)
            .map_err(|error| match error.kind() {
                std::io::ErrorKind::FileTooLarge | std::io::ErrorKind::OutOfMemory => {
                    capacity_rejected()
                }
                _ => invalid_image(),
            })
    }

    fn encode_raw(
        length: usize,
        maximum: usize,
        emit: impl FnOnce(&mut dyn Write) -> StorageResult<()>,
    ) -> StorageResult<Vec<u8>> {
        let mut output = super::payload_write::RetainedPayloadWrite::try_new(
            length,
            maximum - RETAINED_RAW_PAYLOAD_CHECKSUM_BYTES,
        )?;
        let emitted = emit(&mut output);
        output.require_capacity()?;
        emitted?;
        let mut bytes: Vec<u8> = output.into();
        if bytes.len() != length + RETAINED_PAYLOAD_HEADER_BYTES {
            return Err(invalid_image());
        }
        bytes[0] = RETAINED_RAW_PAYLOAD_FORMAT;
        let checksum = Sha256::digest(&bytes[RETAINED_PAYLOAD_HEADER_BYTES..]);
        bytes
            .try_reserve_exact(RETAINED_RAW_PAYLOAD_CHECKSUM_BYTES)
            .map_err(|_| capacity_rejected())?;
        bytes.extend_from_slice(&checksum);
        Ok(bytes)
    }

    pub(super) fn raw_bytes(bytes: &[u8], length: usize) -> StorageResult<&[u8]> {
        let end = RETAINED_PAYLOAD_HEADER_BYTES
            .checked_add(length)
            .ok_or_else(invalid_image)?;
        if bytes.len()
            != end
                .checked_add(RETAINED_RAW_PAYLOAD_CHECKSUM_BYTES)
                .ok_or_else(invalid_image)?
        {
            return Err(invalid_image());
        }
        let payload = &bytes[RETAINED_PAYLOAD_HEADER_BYTES..end];
        if Sha256::digest(payload).as_slice() != &bytes[end..] {
            return Err(invalid_image());
        }
        Ok(payload)
    }

    pub fn decode(bytes: &[u8], maximum: usize) -> StorageResult<Vec<u8>> {
        let mut output = Vec::new();
        Self::visit_decoded(bytes, maximum, |chunk| {
            output
                .try_reserve(chunk.len())
                .map_err(|_| capacity_rejected())?;
            output.extend_from_slice(chunk);
            Ok(())
        })?;
        Ok(output)
    }

    /// Compare an immutable body without allocating its claimed decoded length.
    /// No successful result escapes before the entire encoded stream is
    /// verified.
    pub fn require_content(bytes: &[u8], expected: &[u8]) -> StorageResult<()> {
        if Self::decoded_length(bytes)? != expected.len() {
            return Err(invalid_image());
        }
        let mut after = 0usize;
        Self::visit_decoded(bytes, expected.len(), |chunk| {
            let end = after.checked_add(chunk.len()).ok_or_else(invalid_image)?;
            if expected.get(after..end) != Some(chunk) {
                return Err(invalid_image());
            }
            after = end;
            Ok(())
        })?;
        if after != expected.len() {
            return Err(invalid_image());
        }
        Ok(())
    }

    pub(super) fn visit_decoded(
        bytes: &[u8],
        maximum: usize,
        mut consume: impl FnMut(&[u8]) -> StorageResult<()>,
    ) -> StorageResult<()> {
        let mut reader = super::payload_read::RetainedPayloadRead::try_new(bytes, maximum)?;
        let mut scratch = [0; crate::constants::RETAINED_PAYLOAD_DECODE_CHUNK_BYTES];
        loop {
            let emitted = reader.read(&mut scratch).map_err(|_| invalid_image())?;
            if emitted == 0 {
                break;
            }
            consume(scratch.get(..emitted).ok_or_else(invalid_image)?)?;
        }
        Ok(())
    }

    pub fn decoded_length(bytes: &[u8]) -> StorageResult<usize> {
        if !matches!(
            bytes.first(),
            Some(&RETAINED_PAYLOAD_FORMAT) | Some(&RETAINED_RAW_PAYLOAD_FORMAT)
        ) {
            return Err(invalid_image());
        }
        let length = bytes
            .get(1..RETAINED_PAYLOAD_HEADER_BYTES)
            .ok_or_else(invalid_image)?;
        usize::try_from(u64::from_be_bytes(
            length.try_into().map_err(|_| invalid_image())?,
        ))
        .map_err(|_| invalid_image())
    }

    pub fn encoded_length_bound(length: usize) -> StorageResult<usize> {
        length
            .checked_add(length / 4096)
            .and_then(|bytes| bytes.checked_add(length / 16384))
            .and_then(|bytes| bytes.checked_add(length / 33554432))
            .and_then(|bytes| bytes.checked_add(64))
            .ok_or_else(invalid_image)
    }
}
