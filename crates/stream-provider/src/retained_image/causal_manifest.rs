//! Exact aliases select already retained causal bytes without changing their
//! spelling.

use infrastructure_provider::record::DurableRecordKey;
use storage_types::{ImmutableImageId, StorageResult, TableHistoryId};

use super::{
    causal_body::CausalBodyScope,
    error::{capacity_rejected, invalid_image},
    manifest::{ImageSegment, RetainedImageManifest},
    manifest_split::ManifestSpan,
};

pub struct RetainedCausalBody<'a> {
    bytes: &'a [u8],
    scope: CausalBodyScope,
    aliases: Vec<CausalBodyAlias<'a>>,
}

struct CausalBodyAlias<'a> {
    original: &'a [u8],
    offset: usize,
}

impl<'a> RetainedCausalBody<'a> {
    pub fn try_new(
        bytes: &'a [u8],
        locator: &DurableRecordKey,
        aliases: Vec<(&'a [u8], &'a [u8])>,
    ) -> StorageResult<Self> {
        if bytes.is_empty() || aliases.len() > crate::constants::MAX_CAUSAL_BODY_ALIASES {
            return Err(invalid_image());
        }
        let scope = CausalBodyScope::try_from(locator)?;
        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(aliases.len())
            .map_err(|_| capacity_rejected())?;
        for (original, observed) in aliases {
            let offset = (observed.as_ptr() as usize)
                .checked_sub(bytes.as_ptr() as usize)
                .ok_or_else(invalid_image)?;
            let end = offset
                .checked_add(observed.len())
                .ok_or_else(invalid_image)?;
            if original.is_empty()
                || original != observed
                || bytes.get(offset..end) != Some(observed)
            {
                return Err(invalid_image());
            }
            prepared.push(CausalBodyAlias { original, offset });
        }
        Ok(Self {
            bytes,
            scope,
            aliases: prepared,
        })
    }

    fn include_spans(self, spans: &mut Vec<ManifestSpan<'a>>) -> StorageResult<()> {
        spans
            .try_reserve(self.aliases.len())
            .map_err(|_| capacity_rejected())?;
        let identity = ImmutableImageId::from_content(self.bytes);
        let span = spans
            .iter_mut()
            .find(|span| {
                span.bytes.as_ptr() == self.bytes.as_ptr() && span.bytes.len() == self.bytes.len()
            })
            .ok_or_else(invalid_image)?;
        span.segment = ImageSegment::Causal {
            scope: self.scope,
            identity,
            length: self.bytes.len(),
        };
        spans.extend(self.aliases.into_iter().map(|alias| ManifestSpan {
            bytes: alias.original,
            segment: ImageSegment::CausalSlice {
                scope: self.scope,
                identity,
                raw_bytes: self.bytes.len(),
                offset: alias.offset,
                length: alias.original.len(),
            },
        }));
        Ok(())
    }
}

impl RetainedImageManifest {
    pub fn split_event<'a>(
        bytes: &'a [u8],
        images: &[&'a [u8]],
        history: Option<TableHistoryId>,
        causal: Option<RetainedCausalBody<'a>>,
    ) -> StorageResult<Self> {
        if images.len() > crate::constants::MAX_RETAINED_MANIFEST_BODIES {
            return Err(invalid_image());
        }
        let mut spans = Vec::new();
        spans
            .try_reserve_exact(images.len())
            .map_err(|_| capacity_rejected())?;
        spans.extend(images.iter().copied().map(ManifestSpan::from));
        if let Some(causal) = causal {
            causal.include_spans(&mut spans)?;
        }
        Self::split_spans(bytes, spans, history)
    }

    pub(super) fn require_causal_layout(&self) -> StorageResult<Option<CausalBodyScope>> {
        let mut source = None;
        for segment in &self.segments {
            if let ImageSegment::Causal {
                scope,
                identity,
                length,
            } = segment
                && source.replace((*scope, *identity, *length)).is_some()
            {
                return Err(invalid_image());
            }
        }
        let mut aliases = 0usize;
        for segment in &self.segments {
            aliases += usize::from(segment.require_causal_alias(source)?);
        }
        if aliases > crate::constants::MAX_CAUSAL_BODY_ALIASES {
            return Err(invalid_image());
        }
        Ok(source.map(|(scope, _, _)| scope))
    }
}

impl ImageSegment {
    fn require_causal_alias(
        &self,
        source: Option<(CausalBodyScope, ImmutableImageId, usize)>,
    ) -> StorageResult<bool> {
        let Self::CausalSlice {
            scope,
            identity,
            raw_bytes,
            offset,
            length,
        } = self
        else {
            return Ok(false);
        };
        if source != Some((*scope, *identity, *raw_bytes))
            || *length == 0
            || offset
                .checked_add(*length)
                .is_none_or(|end| end > *raw_bytes)
        {
            return Err(invalid_image());
        }
        Ok(true)
    }
}
