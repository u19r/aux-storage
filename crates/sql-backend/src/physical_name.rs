//! Physical relations are selected only by immutable table/GSI identity.

use storage_types::{IndexName, TableHistoryId, maintenance::TargetIncarnation};

use crate::SqlEngine;

impl SqlEngine {
    #[must_use]
    pub fn item_table_name(self, history_id: TableHistoryId) -> String {
        format!(
            "table_{}",
            uuid::Uuid::from_bytes(*history_id.as_ref()).simple()
        )
    }

    #[must_use]
    pub fn gsi_table_name(self, incarnation: &TargetIncarnation) -> String {
        format!("gsi_{}", incarnation_hex(incarnation.as_ref()))
    }

    #[must_use]
    pub fn ttl_table_name(self, history_id: TableHistoryId) -> String {
        format!(
            "ttl_index_{}",
            uuid::Uuid::from_bytes(*history_id.as_ref()).simple()
        )
    }

    #[must_use]
    pub fn gsi_read_index_name(self, incarnation: &TargetIncarnation) -> String {
        format!("live_{}", incarnation_hex(incarnation.as_ref()))
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
