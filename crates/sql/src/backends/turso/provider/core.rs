#[cfg(test)]
use std::time::Instant;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bg_jobs::JobManager;
use storage_common::GsiPropagationGovernor;
#[cfg(test)]
use storage_common::provider_perf;
use storage_condition::{Condition, evaluate_condition, parse_condition_expression};
use storage_provider::{StorageProvider as _, split_item_into_key_and_attributes_sync};
use storage_types::{
    AttributeDefinition, AttributeValue, DurablePointReadGuard, ItemKey, KeyAttributeType,
    KeyAttributes, KeySchemaElement, KeyType, ReplicationEventMetadata, SplitDynamoItem,
    StorageEnum, StorageError, StorageResult, StoredTableInfo, StreamItemId, StreamName,
    StreamRetentionDuration, TableName, TableStatus, TimestampMillis, WireItem,
    WireItemKeyAttributes, context::ErrorContext as _, normalize_attribute_map_numbers_for_write,
};
use stream_provider::{
    CursorName, CursorPosition, EmbeddedStreamItem, StoredStreamPointer, StreamDataType,
    StreamItem, StreamProvider,
};
use tracing::instrument;
use turso::{Builder, Connection as TursoConnection, Error as TursoError, Value as TursoValue};
use uuid::Uuid;

use super::stream_duration::TursoStreamPointerIndexEntry;
use crate::{
    GsiPhysicalName,
    backends::turso::sql_statements,
    change_index,
    constants::{BASE_BACKOFF_MS, MAX_PUT_ITEM_ATTEMPTS, MAX_TRANSACTION_ATTEMPTS},
    provider_core::gsi_write::{
        GsiAttributesBlobStyle, GsiSqlPlanOptions, GsiUpsertStyle, PlaceholderNumbering,
        TableKeyColumnStyle, plan_gsi_sql_statements,
    },
    sqlite_cache_config::sqlite_page_cache_size_kb,
    write_plan::WriteMaintenancePlan,
};

const TURSO_CONNECTION_POOL_SIZE: usize = 64;
const STREAM_EMBEDDED_MAX_BYTES: usize = 1024;

type TxFuture<'a, T> = Pin<Box<dyn Future<Output = StorageResult<T>> + Send + 'a>>;

#[derive(Clone, Copy)]
pub(crate) struct TursoWriteStreamEntriesInput<'a> {
    pub old_item: Option<&'a HashMap<String, AttributeValue>>,
    pub is_deleted: bool,
    pub item_stream_version: storage_types::ItemStreamVersion,
    pub replication: Option<&'a ReplicationEventMetadata>,
}

pub(crate) struct TursoDeleteItemInput<'a> {
    pub(crate) table_info: &'a StoredTableInfo,
    pub(crate) key: &'a KeyAttributes,
    pub(crate) condition: Option<&'a Condition>,
    pub(crate) return_old_on_condition_failure: bool,
    pub(crate) replication: Option<&'a ReplicationEventMetadata>,
    pub(crate) item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

pub(super) fn condition_item_ref(
    old_item: Option<&HashMap<String, AttributeValue>>,
) -> &HashMap<String, AttributeValue> {
    static EMPTY_ITEM: LazyLock<HashMap<String, AttributeValue>> = LazyLock::new(HashMap::new);
    old_item.unwrap_or(&EMPTY_ITEM)
}

#[cfg(test)]
static TURSO_QUERY_CALLS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static TURSO_EXECUTE_CALLS: AtomicUsize = AtomicUsize::new(0);

pub(crate) trait TursoSqlConnection {
    fn as_turso_connection(&self) -> &TursoConnection;
    fn retry_conflicts(&self) -> bool;
}

impl TursoSqlConnection for TursoConnection {
    fn as_turso_connection(&self) -> &TursoConnection {
        self
    }

    fn retry_conflicts(&self) -> bool {
        true
    }
}

impl TursoSqlConnection for tokio::sync::OwnedMutexGuard<TursoConnection> {
    fn as_turso_connection(&self) -> &TursoConnection {
        self
    }

    fn retry_conflicts(&self) -> bool {
        true
    }
}

impl TursoSqlConnection for tokio::sync::MutexGuard<'_, TursoConnection> {
    fn as_turso_connection(&self) -> &TursoConnection {
        self
    }

    fn retry_conflicts(&self) -> bool {
        true
    }
}

pub(crate) struct TursoTransactionConnection<'a> {
    connection: &'a TursoConnection,
}

impl<'a> TursoTransactionConnection<'a> {
    pub(crate) fn new(connection: &'a TursoConnection) -> Self {
        Self { connection }
    }
}

impl TursoSqlConnection for TursoTransactionConnection<'_> {
    fn as_turso_connection(&self) -> &TursoConnection {
        self.connection
    }

    fn retry_conflicts(&self) -> bool {
        false
    }
}

#[derive(Clone, Copy)]
enum TursoQueryShape {
    MappedRows,
    RowSet,
}

enum TursoQueryOutput {
    MappedRows(Vec<HashMap<String, TursoValue>>),
    RowSet(TursoRowSet),
}

pub(crate) struct TursoRowSet {
    columns: Vec<String>,
    rows: Vec<Vec<TursoValue>>,
}

impl TursoRowSet {
    pub(crate) fn from_parts(columns: Vec<String>, rows: Vec<Vec<TursoValue>>) -> Self {
        Self { columns, rows }
    }

    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = TursoRowView<'_>> {
        self.rows.iter().map(|values| TursoRowView {
            columns: &self.columns,
            values,
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) struct TursoRowView<'a> {
    columns: &'a [String],
    values: &'a [TursoValue],
}

impl TursoRowView<'_> {
    pub(crate) fn get(&self, column: &str) -> Option<&TursoValue> {
        self.columns
            .iter()
            .position(|name| name == column)
            .and_then(|index| self.values.get(index))
    }
}

#[derive(Clone)]
pub struct TursoStorageProvider {
    connection_pool: Arc<Vec<Arc<tokio::sync::Mutex<TursoConnection>>>>,
    next_connection: Arc<AtomicUsize>,
    job_manager: JobManager,
    table_info_cache: Arc<tokio::sync::RwLock<HashMap<TableName, Arc<StoredTableInfo>>>>,
    pub(crate) immediate_gsi_consistency: bool,
    pub(crate) gsi_propagation_governor: Arc<GsiPropagationGovernor>,
    pub(crate) ddl_lock: Arc<tokio::sync::Mutex<()>>,
}

mod connection;
#[cfg(test)]
mod connection_tests;
mod gsi;
mod item;
mod row_decode;
mod row_write;
mod values;

#[cfg(test)]
pub(crate) use row_decode::row_view_to_item_map_main;
pub(crate) use row_decode::{
    attribute_scalar_to_turso_value, build_key_where_clause, gsi_table_name, row_optional_text,
    row_required_text, row_to_item_map_main, row_to_table_info, row_view_to_gsi_wire_item,
    row_view_to_main_wire_item,
};
pub(crate) use values::{
    canonical_revision_key, classify_execute_sql, is_conflict_storage_error,
    is_constraint_storage_error, is_key_absence_condition, map_turso_error, option_string_to_value,
    plan_turso_gsi_sql_statements, read_pragma_text, revision_from_guard_bytes, row_required_blob,
    row_required_i64, sleep_backoff, value_to_i64, value_to_string,
};
#[cfg(test)]
pub(crate) use values::{
    classify_query_sql, reset_turso_statement_counters, turso_statement_counters,
};
