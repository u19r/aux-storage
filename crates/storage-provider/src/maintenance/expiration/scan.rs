use std::num::NonZeroUsize;

use storage_common::{TtlMaintenanceCursor, ttl::TtlIndexKeyToken};
use storage_types::{KeyAttributes, StorageError, StorageResult, StoredTableInfo};

use super::{
    ExpirationCandidateCursor, ExpirationIndexLocator, ExpirationPagePlan,
    ExpirationScannedCandidate,
};

impl ExpirationPagePlan {
    pub(super) fn try_new(
        rows: Vec<ExpirationIndexLocator>,
        more: bool,
        limit: NonZeroUsize,
        cutoff_seconds: i64,
        table_info: &StoredTableInfo,
    ) -> StorageResult<Self> {
        if rows.len() > limit.get().saturating_add(1) {
            return Err(StorageError::internal(
                "expiration scan exceeded its requested physical page bound",
            ));
        }
        let has_more = more || rows.len() > limit.get();
        let mut candidates = Vec::with_capacity(limit.get().min(rows.len()));
        let mut last_cursor = None;
        for row in rows.into_iter().take(limit.get()) {
            if row.indexed_expiration_seconds > cutoff_seconds {
                return Err(StorageError::internal(
                    "expiration scan row is newer than the requested cutoff",
                ));
            }
            let cursor = ExpirationCandidateCursor::try_from((
                row.indexed_expiration_seconds,
                row.ttl_item_key.clone(),
            ))?;
            last_cursor = Some(TtlMaintenanceCursor::from(cursor));
            let ttl_item_key = TtlIndexKeyToken::from(row.ttl_item_key.clone());
            let key = ttl_item_key.parse_key_map(table_info)?;
            candidates.push(ExpirationScannedCandidate {
                index_locator: row,
                key: KeyAttributes::from(key),
            });
        }
        let next_cursor = if has_more {
            Some(
                last_cursor
                    .ok_or_else(|| StorageError::internal("expiration scan cannot advance"))?,
            )
        } else {
            None
        };
        Ok(Self {
            candidates,
            next_cursor,
        })
    }
}
