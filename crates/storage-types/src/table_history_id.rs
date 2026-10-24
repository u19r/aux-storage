//! Durable item-history namespace. Reuse the catalog identity; do not allocate
//! another ID.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableHistoryId([u8; 16]);

impl From<[u8; 16]> for TableHistoryId {
    fn from(value: [u8; 16]) -> Self {
        Self(value)
    }
}

impl From<Uuid> for TableHistoryId {
    fn from(value: Uuid) -> Self {
        Self(*value.as_bytes())
    }
}

impl From<u32> for TableHistoryId {
    fn from(value: u32) -> Self {
        let mut bytes = [0; 16];
        bytes[12..].copy_from_slice(&value.to_be_bytes());
        Self(bytes)
    }
}

impl AsRef<[u8; 16]> for TableHistoryId {
    fn as_ref(&self) -> &[u8; 16] {
        &self.0
    }
}

impl TableHistoryId {
    /// Replace a catalog incarnation deterministically for one durable
    /// operation. The old incarnation is the namespace, so replay on
    /// another replica agrees and recreation cannot reuse an earlier
    /// import's history.
    pub fn for_replacement(self, operation: &[u8]) -> Self {
        Self::from(Uuid::new_v5(&Uuid::from_bytes(self.0), operation))
    }
}
