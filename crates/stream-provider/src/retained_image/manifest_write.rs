//! Manifest fields stream into bounded compression using their exact decoded
//! length.
use std::io::Write;

use storage_types::StorageResult;

use super::{
    error::invalid_image,
    manifest::{ImageSegment, RetainedImageManifest},
    payload::RetainedPayload,
};
use crate::constants::{RETAINED_IMAGE_MANIFEST_FORMAT, RETAINED_IMAGE_MANIFEST_HEADER_BYTES};

impl RetainedImageManifest {
    pub(super) fn encode_payload(&self, maximum: usize) -> StorageResult<(Vec<u8>, usize)> {
        let length = self.segments.iter().try_fold(
            RETAINED_IMAGE_MANIFEST_HEADER_BYTES,
            |total, segment| {
                total
                    .checked_add(segment.metadata_length()?)
                    .ok_or_else(invalid_image)
            },
        )?;
        let bytes =
            RetainedPayload::encode_with(length, maximum, |output| self.write_metadata(output))?;
        Ok((bytes, length))
    }

    fn write_metadata(&self, output: &mut dyn Write) -> StorageResult<()> {
        let length = u64::try_from(self.length).map_err(|_| invalid_image())?;
        let count = u8::try_from(self.segments.len()).map_err(|_| invalid_image())?;
        let history = self
            .history
            .map(|history| *history.as_ref())
            .unwrap_or([0; 16]);
        output
            .write_all(&length.to_be_bytes())
            .map_err(|_| invalid_image())?;
        output
            .write_all(&self.digest)
            .map_err(|_| invalid_image())?;
        output
            .write_all(&[
                RETAINED_IMAGE_MANIFEST_FORMAT,
                u8::from(self.history.is_some()),
            ])
            .map_err(|_| invalid_image())?;
        output.write_all(&history).map_err(|_| invalid_image())?;
        output.write_all(&[count]).map_err(|_| invalid_image())?;
        for segment in &self.segments {
            segment.write_metadata(output)?;
        }
        Ok(())
    }
}

impl ImageSegment {
    fn metadata_length(&self) -> StorageResult<usize> {
        let length = match self {
            Self::Inline(data) => data.len(),
            Self::Image { identity, .. } => identity.as_ref().len(),
            Self::Causal { .. } => 16 + 32,
            Self::CausalSlice { .. } => return Ok(crate::constants::CAUSAL_SLICE_METADATA_BYTES),
        };
        length.checked_add(1 + 8).ok_or_else(invalid_image)
    }

    fn write_metadata(&self, output: &mut dyn Write) -> StorageResult<()> {
        match self {
            Self::Inline(data) => Self::write_segment(output, (0, data.len()), data),
            Self::Image { identity, length } => {
                Self::write_segment(output, (1, *length), identity.as_ref())
            }
            Self::Causal {
                scope,
                identity,
                length,
            } => {
                Self::write_segment(output, (2, *length), scope.as_ref())?;
                output
                    .write_all(identity.as_ref())
                    .map_err(|_| invalid_image())
            }
            Self::CausalSlice {
                scope,
                identity,
                raw_bytes,
                offset,
                length,
            } => {
                Self::write_segment(output, (3, *length), scope.as_ref())?;
                output
                    .write_all(identity.as_ref())
                    .map_err(|_| invalid_image())?;
                output
                    .write_all(
                        &u64::try_from(*raw_bytes)
                            .map_err(|_| invalid_image())?
                            .to_be_bytes(),
                    )
                    .map_err(|_| invalid_image())?;
                output
                    .write_all(
                        &u64::try_from(*offset)
                            .map_err(|_| invalid_image())?
                            .to_be_bytes(),
                    )
                    .map_err(|_| invalid_image())
            }
        }
    }

    fn write_segment(
        output: &mut dyn Write,
        header: (u8, usize),
        bytes: &[u8],
    ) -> StorageResult<()> {
        let length = u64::try_from(header.1).map_err(|_| invalid_image())?;
        output.write_all(&[header.0]).map_err(|_| invalid_image())?;
        output
            .write_all(&length.to_be_bytes())
            .map_err(|_| invalid_image())?;
        output.write_all(bytes).map_err(|_| invalid_image())
    }
}
