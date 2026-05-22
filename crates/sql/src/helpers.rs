//! Helper constants & functions shared across modules.
//! Supports unified read path & pagination tests.

use storage_types::{IndexName, ItemKey, StorageError, StoredTableInfo};

pub use crate::constants::{
    DEFAULT_QUERY_LIMIT, DEFAULT_SCAN_LIMIT, MAX_QUERY_LIMIT, MAX_SCAN_LIMIT,
};

/// Decode exclusive start key token (next page token) into an `ItemKey` for
/// pagination.
#[expect(clippy::ref_option)] // Simpler call sites keep &Option<String>
pub fn decode_exclusive_start(
    token: &Option<String>,
    table_info: &StoredTableInfo,
    index_name: &Option<IndexName>,
) -> Result<Option<ItemKey>, StorageError> {
    if let Some(tok) = token {
        let key_opt = ItemKey::item_key_from_next_page_token(tok, table_info, index_name)
            .map_err(|e| StorageError::validation(format!("Invalid next page token: {e}")))?;
        Ok(key_opt)
    } else {
        Ok(None)
    }
}
