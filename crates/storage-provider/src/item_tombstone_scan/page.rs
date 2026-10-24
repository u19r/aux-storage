use storage_types::{KeyAttributes, StorageError, StorageResult};

use super::{ItemTombstone, ItemTombstoneScan};

pub struct ItemTombstonePage {
    pub items: Vec<ItemTombstone>,
    /// Source-provider ordering is opaque; reuse this key with the same scan.
    pub continuation: Option<KeyAttributes>,
}

impl ItemTombstonePage {
    pub fn try_new(
        scan: &ItemTombstoneScan,
        mut items: Vec<ItemTombstone>,
        mut more: bool,
    ) -> StorageResult<Self> {
        if items.len() > scan.probe_limit().get() as usize {
            return Err(StorageError::internal(
                "tombstone scan exceeded its row bound",
            ));
        }
        if items.len() > scan.limit().get() as usize {
            items.pop();
            more = true;
        }
        let continuation = if more {
            Some(
                items
                    .last()
                    .ok_or_else(|| {
                        StorageError::internal("tombstone continuation lacks an admitted row")
                    })?
                    .key
                    .clone(),
            )
        } else {
            None
        };
        Ok(Self {
            items,
            continuation,
        })
    }
}
