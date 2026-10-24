//! An item-history namespace remains valid after its logical table is replaced.

use serde::{Deserialize, Serialize};

use crate::{StreamName, TableHistoryId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemHistoryAddress {
    pub history_id: TableHistoryId,
    pub stream_name: StreamName,
}

impl ItemHistoryAddress {
    /// Stable row namespace for stores with a single stream-name column.
    pub fn physical_stream_name(&self) -> StreamName {
        let identity = uuid::Uuid::from_bytes(*self.history_id.as_ref());
        let mut bytes = format!("history/{}/", identity.simple()).into_bytes();
        bytes.extend_from_slice(self.stream_name.as_ref());
        StreamName::new(&bytes)
    }
}
