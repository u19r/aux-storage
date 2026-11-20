//! Physical relations are selected only by immutable table/GSI identity.

use storage_types::{BoundGsi, IndexName, TableHistoryId, maintenance::TargetIncarnation};

use crate::SqlEngine;

impl SqlEngine {
    #[must_use]
    pub fn item_table_name(self, history_id: TableHistoryId) -> String {
        format!(
            "table_{}",
            uuid::Uuid::from_bytes(*history_id.as_ref()).simple()
        )
    }

    /// The partial index of one GSI build on the item relation. Within the
    /// Postgres 63-byte identifier limit: the index ID is never reused in a
    /// table incarnation, and a rebuild gets a new build identity.
    #[must_use]
    pub fn gsi_index_name(self, history_id: TableHistoryId, gsi: &BoundGsi) -> String {
        let build = gsi.build.as_ref();
        format!(
            "gsi_{}_{}_{}",
            uuid::Uuid::from_bytes(*history_id.as_ref()).simple(),
            gsi.index_id,
            incarnation_hex(build.get(..8).unwrap_or(build))
        )
    }
}

pub(crate) fn initial_gsi_incarnation(
    history_id: TableHistoryId,
    index: &IndexName,
) -> TargetIncarnation {
    let namespace = uuid::Uuid::from_bytes(*history_id.as_ref());
    TargetIncarnation::from(*uuid::Uuid::new_v5(&namespace, index.as_ref().as_bytes()).as_bytes())
}

fn incarnation_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
