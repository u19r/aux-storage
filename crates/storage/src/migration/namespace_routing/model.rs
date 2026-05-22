use std::collections::HashMap;

use serde::Deserialize;
use storage_types::{TableName, TableNamespace, TimestampMillis};

use crate::tables::Tables;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceStorageMode {
    Dedicated,
    SharedTable,
}

impl NamespaceStorageMode {
    pub(crate) fn from_code(code: u8) -> Self {
        match code {
            1 => Self::SharedTable,
            _ => Self::Dedicated,
        }
    }

    pub(crate) fn source_table_name(self, namespace: &TableNamespace, loc: u16) -> TableName {
        match self {
            Self::Dedicated => Tables::namespace(namespace),
            Self::SharedTable => Tables::shared_namespace(loc),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceSourceTable {
    pub table_name: TableName,
    pub is_shared_table: bool,
}

#[must_use]
pub fn namespace_source_table(
    namespace: &TableNamespace,
    storage_mode_code: u8,
    loc: u16,
) -> NamespaceSourceTable {
    let storage_mode = NamespaceStorageMode::from_code(storage_mode_code);
    NamespaceSourceTable {
        table_name: storage_mode.source_table_name(namespace, loc),
        is_shared_table: storage_mode == NamespaceStorageMode::SharedTable,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceStorageMigrationMode {
    Single,
    DualWrite {
        old_loc: u16,
        new_loc: u16,
        cutover_at_ms: TimestampMillis,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteTarget {
    pub connection_id: String,
    pub table_name: TableName,
    pub loc: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceRoute {
    pub namespace: TableNamespace,
    pub storage_mode: NamespaceStorageMode,
    pub read_target: RouteTarget,
    pub write_targets: Vec<RouteTarget>,
    pub writes_paused: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct NamespaceRouteRecord {
    pub(crate) storage_mode: NamespaceStorageMode,
    pub(crate) loc: u16,
    pub(crate) migration_mode: NamespaceStorageMigrationMode,
}

#[derive(Debug, Clone)]
pub struct CutoverEvent {
    pub namespace: TableNamespace,
    pub migration_id: String,
    pub old_loc: u16,
    pub new_loc: u16,
    pub effective_at_ms: TimestampMillis,
    pub status: CutoverEventStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CutoverEventStatus {
    Scheduled,
    Applied,
    Canceled,
    Failed,
}

#[derive(Debug, Clone)]
pub(crate) struct CutoverOverride {
    pub(crate) new_loc: u16,
    pub(crate) effective_at_ms: TimestampMillis,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub(crate) struct NamespaceRouteRecordSerde {
    #[serde(default)]
    pub(crate) st: u8,
    #[serde(default)]
    pub(crate) loc: u16,
    #[serde(default)]
    pub(crate) migration_mode: NamespaceRouteMigrationModeSerde,
    #[serde(default)]
    pub(crate) id: Option<String>,
    #[serde(default)]
    pub(crate) pk: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
#[derive(Default)]
pub(crate) enum NamespaceRouteMigrationModeSerde {
    #[default]
    Single,
    DualWrite {
        old_loc: u16,
        new_loc: u16,
        cutover_at_ms: TimestampMillis,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct LocationDescriptorSerde {
    pub(crate) connection_id: String,
    pub(crate) backend_kind: LocationBackendKindSerde,
    #[serde(default)]
    pub(crate) metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LocationBackendKindSerde {
    RemoteAws,
    Sqlite,
    Rocksdb,
    Foundationdb,
    Postgres,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CutoverEventStatusSerde {
    Scheduled,
    Applied,
    Canceled,
    Failed,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CutoverEventSerde {
    pub(crate) namespace: TableNamespace,
    pub(crate) migration_id: String,
    pub(crate) old_loc: u16,
    pub(crate) new_loc: u16,
    pub(crate) effective_at_ms: TimestampMillis,
    pub(crate) status: CutoverEventStatusSerde,
}

impl From<CutoverEventStatusSerde> for CutoverEventStatus {
    fn from(value: CutoverEventStatusSerde) -> Self {
        match value {
            CutoverEventStatusSerde::Scheduled => Self::Scheduled,
            CutoverEventStatusSerde::Applied => Self::Applied,
            CutoverEventStatusSerde::Canceled => Self::Canceled,
            CutoverEventStatusSerde::Failed => Self::Failed,
        }
    }
}
