use scheduled_bg_jobs::store::{RangeEntry, RangePage, RangeRequest};

use crate::RawOrderedJobStoreError;

/// Apply the portable ordered-page budget after a family decodes physical
/// rows. The first over-budget row is never exposed as a continuation key.
pub fn finish_range_page(
    request: &RangeRequest,
    rows: impl IntoIterator<Item = Result<RangeEntry, RawOrderedJobStoreError>>,
    mut more: bool,
) -> Result<RangePage, RawOrderedJobStoreError> {
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(request.item_limit().get() as usize)
        .map_err(|_| {
            RawOrderedJobStoreError::Backend(
                storage_types::InfrastructureResultKind::NativeCapacityRejected.into(),
            )
        })?;
    let mut used_bytes = 0usize;
    for row in rows {
        let entry = row?;
        let entry_bytes = entry.key.as_bytes().len().saturating_add(entry.value.len());
        if entries.len() == request.item_limit().get() as usize
            || used_bytes.saturating_add(entry_bytes) > request.byte_limit().get()
        {
            if entries.is_empty() {
                return Err(RawOrderedJobStoreError::Corrupt(
                    "scheduled job range byte limit",
                ));
            }
            more = true;
            break;
        }
        used_bytes += entry_bytes;
        entries.push(entry);
    }
    if more && entries.is_empty() {
        return Err(RawOrderedJobStoreError::Corrupt(
            "scheduled job range continuation",
        ));
    }
    let continuation = more
        .then(|| entries.last().map(|entry| entry.key.clone()))
        .flatten();
    RangePage::new(entries, continuation)
        .map_err(|_| RawOrderedJobStoreError::Corrupt("scheduled job range page"))
}
