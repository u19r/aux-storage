use storage_types::{ImmutableImageId, StorageResult, TableHistoryId};

use super::{
    error::invalid_image,
    manifest::{ImageSegment, RetainedImageManifest},
};
use crate::constants::MAX_RETAINED_MANIFEST_SEGMENTS;

pub(super) struct ManifestDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ManifestDecoder<'a> {
    pub(super) fn decode(bytes: &'a [u8], maximum: usize) -> StorageResult<RetainedImageManifest> {
        let mut decoder = Self { bytes, offset: 0 };
        let length = decoder.decode_length()?;
        if length > maximum {
            return Err(invalid_image());
        }
        let digest = decoder.decode_digest()?;
        let history = decoder.decode_scope()?;
        let count = usize::from(decoder.take(1)?[0]);
        if count % 2 != 1 || count > MAX_RETAINED_MANIFEST_SEGMENTS {
            return Err(invalid_image());
        }
        let mut segments = Vec::with_capacity(count);
        let mut total = 0usize;
        for index in 0..count {
            segments.push(decoder.decode_segment(index, &mut total, length)?);
        }
        if total != length || decoder.offset != bytes.len() {
            return Err(invalid_image());
        }
        let manifest = RetainedImageManifest {
            history,
            length,
            digest,
            segments,
        };
        manifest.references()?;
        Ok(manifest)
    }

    fn decode_scope(&mut self) -> StorageResult<Option<TableHistoryId>> {
        if self.take(1)?[0] != crate::constants::RETAINED_IMAGE_MANIFEST_FORMAT {
            return Err(invalid_image());
        }
        let kind = self.take(1)?[0];
        let bytes: [u8; 16] = self.take(16)?.try_into().map_err(|_| invalid_image())?;
        match kind {
            0 if bytes == [0; 16] => Ok(None),
            1 if bytes != [0; 16] => Ok(Some(TableHistoryId::from(bytes))),
            _ => Err(invalid_image()),
        }
    }

    fn decode_segment(
        &mut self,
        index: usize,
        total: &mut usize,
        maximum: usize,
    ) -> StorageResult<ImageSegment> {
        let kind = self.take(1)?[0];
        let size = self.decode_length()?;
        *total = total.checked_add(size).ok_or_else(invalid_image)?;
        if *total > maximum
            || (index.is_multiple_of(2) && kind != 0)
            || (!index.is_multiple_of(2) && !matches!(kind, 1..=3))
        {
            return Err(invalid_image());
        }
        match kind {
            0 => Ok(ImageSegment::Inline(self.take(size)?.to_vec())),
            1 if size > 0 => Ok(ImageSegment::Image {
                identity: ImmutableImageId::from(self.decode_digest()?),
                length: size,
            }),
            2 | 3 if size > 0 => self.decode_causal(kind, size),
            _ => Err(invalid_image()),
        }
    }

    fn decode_causal(&mut self, kind: u8, length: usize) -> StorageResult<ImageSegment> {
        let scope = super::causal_body::CausalBodyScope::try_from(
            <[u8; 16]>::try_from(self.take(16)?).map_err(|_| invalid_image())?,
        )?;
        let identity = ImmutableImageId::from(self.decode_digest()?);
        if kind == 2 {
            return Ok(ImageSegment::Causal {
                scope,
                identity,
                length,
            });
        }
        let raw_bytes = self.decode_length()?;
        let offset = self.decode_length()?;
        if offset.checked_add(length).is_none_or(|end| end > raw_bytes) {
            return Err(invalid_image());
        }
        Ok(ImageSegment::CausalSlice {
            scope,
            identity,
            raw_bytes,
            offset,
            length,
        })
    }

    fn take(&mut self, length: usize) -> StorageResult<&'a [u8]> {
        let end = self.offset.checked_add(length).ok_or_else(invalid_image)?;
        let slice = self.bytes.get(self.offset..end).ok_or_else(invalid_image)?;
        self.offset = end;
        Ok(slice)
    }

    fn decode_length(&mut self) -> StorageResult<usize> {
        usize::try_from(u64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| invalid_image())?,
        ))
        .map_err(|_| invalid_image())
    }

    fn decode_digest(&mut self) -> StorageResult<[u8; 32]> {
        self.take(32)?.try_into().map_err(|_| invalid_image())
    }
}
