use storage_types::{ImageAddress, ImmutableImageId, StorageResult, TableHistoryId};

use super::{causal_body::CausalBodyScope, decoder::ManifestDecoder, error::invalid_image};

pub(super) enum ImageSegment {
    Inline(Vec<u8>),
    Image {
        identity: ImmutableImageId,
        length: usize,
    },
    Causal {
        scope: CausalBodyScope,
        identity: ImmutableImageId,
        length: usize,
    },
    CausalSlice {
        scope: CausalBodyScope,
        identity: ImmutableImageId,
        raw_bytes: usize,
        offset: usize,
        length: usize,
    },
}

/// Exact surrounding bytes plus bounded immutable image or declaration spans.
pub struct RetainedImageManifest {
    pub(super) length: usize,
    pub(super) history: Option<TableHistoryId>,
    pub(super) digest: [u8; 32],
    pub(super) segments: Vec<ImageSegment>,
}

impl RetainedImageManifest {
    pub fn original_length(&self) -> usize {
        self.length
    }

    /// A verified external copy retains the same exact original bytes.
    pub fn require_original(&self, bytes: &[u8]) -> StorageResult<()> {
        self.references()?.require_original(bytes)
    }

    pub(super) fn references(&self) -> StorageResult<super::references::RetainedReferences> {
        self.try_into()
    }

    /// Inspect untrusted metadata so the caller can qualify its content budget.
    pub fn declared_length(bytes: &[u8]) -> StorageResult<usize> {
        let length = bytes.get(..8).ok_or_else(invalid_image)?;
        usize::try_from(u64::from_be_bytes(
            length.try_into().map_err(|_| invalid_image())?,
        ))
        .map_err(|_| invalid_image())
    }

    pub fn decode(bytes: &[u8], maximum: usize) -> StorageResult<Self> {
        ManifestDecoder::decode(bytes, maximum)
    }

    pub fn require_causal_scope(
        &self,
        locator: &infrastructure_provider::record::DurableRecordKey,
    ) -> StorageResult<()> {
        let expected = CausalBodyScope::try_from(locator)?;
        if self.segments.iter().any(|segment| matches!(segment, ImageSegment::Causal { scope, .. } | ImageSegment::CausalSlice { scope, .. } if *scope != expected)) {
            return Err(invalid_image());
        }
        Ok(())
    }

    pub fn image_addresses(&self) -> impl Iterator<Item = StorageResult<ImageAddress>> + '_ {
        self.segments
            .iter()
            .filter_map(|segment| segment.body_reference(self.history).transpose())
            .map(|body| body.map(|(address, _)| address))
    }
}

impl ImageSegment {
    pub(super) fn body_reference(
        &self,
        history: Option<TableHistoryId>,
    ) -> StorageResult<Option<(ImageAddress, usize)>> {
        match self {
            Self::Inline(_) => Ok(None),
            Self::Image { identity, length } => Ok(Some((
                ImageAddress::try_new(history.ok_or_else(invalid_image)?, *identity)?,
                *length,
            ))),
            Self::Causal {
                scope,
                identity,
                length,
            } => Ok(Some((scope.address(*identity)?, *length))),
            Self::CausalSlice {
                scope,
                identity,
                raw_bytes,
                ..
            } => Ok(Some((scope.address(*identity)?, *raw_bytes))),
        }
    }
}

pub(super) fn require_image_scope(
    history: Option<TableHistoryId>,
    images: usize,
) -> StorageResult<()> {
    if history.is_some() != (images > 0)
        || history.is_some_and(|history| history.as_ref() == &[0; 16])
    {
        return Err(invalid_image());
    }
    Ok(())
}
