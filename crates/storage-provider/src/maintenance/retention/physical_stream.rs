use storage_types::{StreamItemId, TimestampMillis};
use stream_provider::{StoredStreamPointer, StreamItem};

use super::{DetachedItemStreamPolicy, DetachedItemStreamRange, PhysicalStreamPointer};

/// Provider-neutral physical rows and policy facts named by one SystemStream
/// pointer.
pub struct PhysicalStreamPointerFamily {
    pub pointer: PhysicalStreamPointer,
    pub table_stream_item_id: StreamItemId,
    pub detached_item_policy: Option<DetachedItemStreamPolicy>,
}

impl PhysicalStreamPointerFamily {
    pub fn detached_item_stream_range(&self) -> Option<DetachedItemStreamRange> {
        let policy = self.detached_item_policy.as_ref()?;
        Some(DetachedItemStreamRange {
            table_name: self.pointer.table_name.clone(),
            table_incarnation: policy.table_incarnation.clone(),
            item_stream: self.pointer.item_stream.clone(),
            max_item_id: self.pointer.item_stream_item_id,
        })
    }
}

/// Shared selection result for one ordered page of physical SystemStream rows.
pub struct PhysicalStreamTrimSelection {
    pub pointers: Vec<PhysicalStreamPointer>,
    pub stop_scan: bool,
    pub decode_failures: usize,
}

impl PhysicalStreamTrimSelection {
    pub fn from_items(
        items: Vec<StreamItem>,
        cutoff: TimestampMillis,
        protected_cursor_floor: Option<StreamItemId>,
    ) -> Self {
        let mut pointers = Vec::new();
        let mut decode_failures = 0usize;

        for item in items {
            if item.created_at >= cutoff
                || protected_cursor_floor.is_some_and(|cursor| item.id >= cursor)
            {
                return Self {
                    pointers,
                    stop_scan: true,
                    decode_failures,
                };
            }

            let stored_pointer: StoredStreamPointer =
                match storage_types::storage_serde::from_bytes(&item.data) {
                    Ok(pointer) => pointer,
                    Err(_) => {
                        decode_failures = decode_failures.saturating_add(1);
                        continue;
                    }
                };

            pointers.push(PhysicalStreamPointer {
                pointer_item_id: item.id,
                item_stream_item_id: StreamItemId::from(
                    stored_pointer.target_item_stream_version(),
                ),
                table_name: stored_pointer.table_name().clone(),
                item_stream: stored_pointer.stream_name().clone(),
            });
        }

        Self {
            pointers,
            stop_scan: false,
            decode_failures,
        }
    }
}
