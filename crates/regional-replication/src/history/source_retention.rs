//! Original publication scope survives configuration and membership changes.
use replication_types::{
    configuration::membership::RegionalTableBinding, event::interval::TablePageId,
};

use super::{
    hold_discovery::CaptureHoldRange, publication_scope::SourcePublicationScope,
    source_cut::CanonicalSourceCut,
};

/// Borrow only ranges resolved from their actual native custody owners.
pub(crate) enum SourceRetentionRange<'a> {
    Source(&'a CanonicalSourceCut),
    Capture(&'a CaptureHoldRange),
    Fragment {
        binding: &'a RegionalTableBinding,
        page: &'a TablePageId,
    },
}

impl SourceRetentionRange<'_> {
    pub(crate) fn overlaps_cut(&self, removed: &CanonicalSourceCut) -> bool {
        let (is_same_source, after, through) = match self {
            Self::Source(held) => (
                SourcePublicationScope::from(*held) == removed.into(),
                held.after,
                Some(held.through),
            ),
            Self::Capture(held) => (
                held.lineage == removed.lineage && held.origin == removed.origin,
                held.after,
                held.through,
            ),
            Self::Fragment { binding, page } => (
                SourcePublicationScope::new(binding.lineage, &page.origin, binding.source_table)
                    == removed.into(),
                page.range.after(),
                Some(page.range.through()),
            ),
        };
        if !is_same_source {
            return false;
        }
        let lower = after.max(removed.after);
        lower < removed.through && through.is_none_or(|upper| lower < upper)
    }
}
