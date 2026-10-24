//! Complete original spans share one exact, nonoverlapping construction
//! boundary.

use sha2::{Digest as _, Sha256};
use storage_types::{ImmutableImageId, StorageResult, TableHistoryId};

use super::{
    error::{capacity_rejected, invalid_image},
    manifest::{ImageSegment, RetainedImageManifest, require_image_scope},
};

pub(super) struct ManifestSpan<'a> {
    pub(super) bytes: &'a [u8],
    pub(super) segment: ImageSegment,
}

impl<'a> From<&'a [u8]> for ManifestSpan<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            segment: ImageSegment::Image {
                identity: ImmutableImageId::from_content(bytes),
                length: bytes.len(),
            },
        }
    }
}

impl RetainedImageManifest {
    pub fn split(
        bytes: &[u8],
        images: &[&[u8]],
        history: Option<TableHistoryId>,
    ) -> StorageResult<Self> {
        if images.len() > crate::constants::MAX_RETAINED_MANIFEST_BODIES {
            return Err(invalid_image());
        }
        let mut spans = Vec::new();
        spans
            .try_reserve_exact(images.len())
            .map_err(|_| capacity_rejected())?;
        spans.extend(images.iter().copied().map(ManifestSpan::from));
        Self::split_spans(bytes, spans, history)
    }

    pub(super) fn split_spans(
        bytes: &[u8],
        mut spans: Vec<ManifestSpan<'_>>,
        history: Option<TableHistoryId>,
    ) -> StorageResult<Self> {
        if spans.len()
            > crate::constants::MAX_RETAINED_MANIFEST_BODIES
                + crate::constants::MAX_CAUSAL_BODY_ALIASES
        {
            return Err(invalid_image());
        }
        require_image_scope(history, spans.len())?;
        spans.sort_unstable_by_key(|span| span.bytes.as_ptr() as usize);
        let count = spans
            .len()
            .checked_mul(2)
            .and_then(|count| count.checked_add(1))
            .ok_or_else(capacity_rejected)?;
        let mut segments = Vec::new();
        segments
            .try_reserve_exact(count)
            .map_err(|_| capacity_rejected())?;
        let mut after = 0usize;
        for span in spans {
            let start = (span.bytes.as_ptr() as usize)
                .checked_sub(bytes.as_ptr() as usize)
                .ok_or_else(invalid_image)?;
            let end = start
                .checked_add(span.bytes.len())
                .ok_or_else(invalid_image)?;
            if span.bytes.is_empty() || start < after || bytes.get(start..end) != Some(span.bytes) {
                return Err(invalid_image());
            }
            segments.push(ImageSegment::Inline(copy_inline_bytes(
                &bytes[after..start],
            )?));
            segments.push(span.segment);
            after = end;
        }
        segments.push(ImageSegment::Inline(copy_inline_bytes(&bytes[after..])?));
        let manifest = Self {
            history,
            length: bytes.len(),
            digest: Sha256::digest(bytes).into(),
            segments,
        };
        manifest.references()?;
        Ok(manifest)
    }
}

fn copy_inline_bytes(bytes: &[u8]) -> StorageResult<Vec<u8>> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(bytes.len())
        .map_err(|_| capacity_rejected())?;
    owned.extend_from_slice(bytes);
    Ok(owned)
}
