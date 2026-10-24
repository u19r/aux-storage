//! Embedded-image budget shared by KV and SQL stream writers.
//!
//! Pointer records embed the new (and old) images only when their combined
//! size fits the budget; larger images stay in the item stream. All backends
//! share the threshold and predicate so the embed/pointer decision cannot
//! drift.

use crate::types::{EmbeddedStreamItem, StreamDataType};

/// Maximum combined old/new image payload embedded in pointer records.
pub const STREAM_EMBEDDED_MAX_BYTES: usize = 1024;

/// Whether the new image alone can still be embedded. Callers skip old-image
/// serialization when this is false; those bytes would be discarded by
/// [`should_embed_stream_images`].
#[must_use]
pub fn can_embed_new_image(new_len: usize) -> bool {
    new_len <= STREAM_EMBEDDED_MAX_BYTES
}

/// Whether combined new+old image bytes fit the embed budget.
#[must_use]
pub fn should_embed_stream_images(new_len: usize, old_len: Option<usize>) -> bool {
    old_len.map_or(new_len, |old| old + new_len) <= STREAM_EMBEDDED_MAX_BYTES
}

/// Build the embedded image list for a pointer record.
#[must_use]
pub fn embedded_stream_items(
    data: Vec<u8>,
    old_data: Option<Vec<u8>>,
    data_type: StreamDataType,
) -> Vec<EmbeddedStreamItem> {
    let mut items = Vec::with_capacity(1 + usize::from(old_data.is_some()));
    items.push(EmbeddedStreamItem { data, data_type });
    if let Some(old_data) = old_data {
        items.push(EmbeddedStreamItem {
            data: old_data,
            data_type: StreamDataType::DynamoDbJson,
        });
    }
    items
}
