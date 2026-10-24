//! Persisted item/TTL names use the table incarnation; GSI names use the
//! build incarnation. PostgreSQL retains the suffix within its 63-byte limit.

use storage_types::{IndexName, TableHistoryId, TableName, maintenance::TargetIncarnation};

use crate::SqlEngine;

const POSTGRES_IDENTIFIER_LIMIT: usize = 63;
const HASH_HEX_LEN: usize = 16;
const HASH_SEPARATOR_BYTES: usize = 2;

impl SqlEngine {
    /// Qualify an already rendered physical relation or index name. All SQL
    /// engines use the same bounded identifier; no engine-specific schema or
    /// query rewriting is involved. Ordinary rendering never calls this method.
    #[must_use]
    pub fn item_table_name(self, table: &TableName, history_id: TableHistoryId) -> String {
        self.incarnation_relation_name(
            "table",
            &table.sanitized_name(),
            &uuid::Uuid::from_bytes(*history_id.as_ref())
                .simple()
                .to_string(),
        )
    }

    #[must_use]
    pub fn gsi_table_name(
        self,
        table: &TableName,
        index: &IndexName,
        incarnation: &TargetIncarnation,
    ) -> String {
        self.incarnation_relation_name(
            "gsi",
            &format!("{}_{}", table.sanitized_name(), index.sanitized_name()),
            &incarnation_hex(incarnation.as_ref()),
        )
    }

    #[must_use]
    pub fn ttl_table_name(self, table: &TableName, history_id: TableHistoryId) -> String {
        self.incarnation_relation_name(
            "ttl_index",
            &table.sanitized_name(),
            &uuid::Uuid::from_bytes(*history_id.as_ref())
                .simple()
                .to_string(),
        )
    }

    #[must_use]
    pub fn gsi_read_index_name(
        self,
        table: &TableName,
        index: &IndexName,
        incarnation: &TargetIncarnation,
    ) -> String {
        self.incarnation_relation_name(
            "live",
            &format!("{}_{}", table.sanitized_name(), index.sanitized_name()),
            &incarnation_hex(incarnation.as_ref()),
        )
    }

    fn incarnation_relation_name(self, prefix: &str, name: &str, suffix: &str) -> String {
        let full = format!("{prefix}_{name}_{suffix}");
        if self != Self::Postgres || full.len() <= POSTGRES_IDENTIFIER_LIMIT {
            return full;
        }
        let head_len = POSTGRES_IDENTIFIER_LIMIT.saturating_sub(
            prefix.len() + 1 + HASH_SEPARATOR_BYTES + HASH_HEX_LEN + 1 + suffix.len(),
        );
        let head: String = name.chars().filter(char::is_ascii).take(head_len).collect();
        format!("{prefix}_{head}_{:016x}_{suffix}", fnv1a64(name.as_bytes()))
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

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
