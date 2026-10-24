use storage_types::{ItemKey, ItemRevision, WireItem};

/// A live native projection and its physical key/revision evidence.
/// Historical projection tombstones are not queryable live projections.
pub struct GsiProjectionRow {
    pub key: ItemKey,
    pub image: WireItem,
    pub revision: ItemRevision,
}

pub struct TtlProjectionPage {
    pub rows: Vec<crate::maintenance::ExpirationIndexLocator>,
    pub continuation: Option<storage_common::TtlMaintenanceCursor>,
}

impl super::TtlProjectionScan {
    pub fn finish_native(
        &self,
        rows: Vec<crate::maintenance::ExpirationIndexLocator>,
        more: bool,
    ) -> storage_types::StorageResult<TtlProjectionPage> {
        use crate::maintenance::ExpirationCandidateCursor;
        let read = self.read();
        if rows.len() > read.limit.get().saturating_add(1) {
            return Err(storage_types::StorageError::internal(
                "TTL scan exceeded its native page bound",
            ));
        }
        let more = more || rows.len() > read.limit.get();
        let rows = rows.into_iter().take(read.limit.get()).collect::<Vec<_>>();
        if more && rows.is_empty() {
            return Err(storage_types::StorageError::internal(
                "TTL scan cannot advance an empty page",
            ));
        }
        let mut previous = read
            .cursor
            .as_ref()
            .map(ExpirationCandidateCursor::try_from)
            .transpose()?;
        for row in &rows {
            let next = ExpirationCandidateCursor::try_from((
                row.indexed_expiration_seconds,
                row.ttl_item_key.clone(),
            ))?;
            if previous.as_ref().is_some_and(|old| {
                (old.expiration_seconds, &old.ttl_item_key)
                    >= (next.expiration_seconds, &next.ttl_item_key)
            }) {
                return Err(storage_types::StorageError::internal(
                    "TTL scan did not advance its native key",
                ));
            }
            previous = Some(next);
        }
        let continuation = if more {
            Some(
                previous
                    .ok_or_else(|| {
                        storage_types::StorageError::internal(
                            "TTL scan cannot advance an empty page",
                        )
                    })?
                    .into(),
            )
        } else {
            None
        };
        Ok(TtlProjectionPage { rows, continuation })
    }
}
