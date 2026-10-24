//! Bounded physical reads and stored-value limits for the SQL family.

use std::num::NonZeroUsize;

pub(crate) const INFRASTRUCTURE_LAYOUT_VERSION: i64 = 28;

// One bounded scan, savepoint/rollback/release and conflict-revision query.
#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
pub(crate) const INFRASTRUCTURE_BATCH_CONTROL_STATEMENTS: usize = 5;
// Lower, upper, continuation and conflict keys, independent of point keys.
#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
pub(crate) const INFRASTRUCTURE_BATCH_READ_KEYS: usize = 4;
// Format DDL/read, record DDL/occupancy, format insert and final format read.
#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
pub(crate) const INFRASTRUCTURE_LAYOUT_STATEMENTS: usize = 6;

/// Bound tuple point loads below the parameter budgets of all SQL engines.
pub(crate) const INFRASTRUCTURE_POINT_LOAD_KEYS: usize = 128;
pub(crate) const INFRASTRUCTURE_RECORD_COLUMNS: [&str; 6] = [
    "namespace",
    "resource",
    "generation",
    "suffix",
    "revision",
    "bytes",
];

#[cfg(any(feature = "sqlite", feature = "turso"))]
pub(crate) const LOCAL_SQL_MAX_CONNECTIONS: usize = 32;

/// Bound outstanding networked driver statements within one native attempt.
#[cfg(feature = "postgres")]
pub(crate) const POSTGRES_PIPELINE_STATEMENTS: usize = 256;

// postgres-protocol 0.6.12: signed 32-bit lengths, unsigned 16-bit counts.
// tokio-postgres names prepared statements s<AtomicUsize>; include its NUL.
#[cfg(feature = "postgres")]
const POSTGRES_STATEMENT_NAME_BYTES: usize = 3 + usize::MAX.ilog10() as usize;
#[cfg(feature = "postgres")]
pub(crate) const POSTGRES_PARSE_PARAMETER_BYTES: usize = 4;
#[cfg(feature = "postgres")]
pub(crate) const POSTGRES_BIND_PARAMETER_BYTES: usize = 2 + 4;
#[cfg(feature = "postgres")]
pub(crate) const POSTGRES_PARSE_FRAME_BYTES: usize = 4 + POSTGRES_STATEMENT_NAME_BYTES + 1 + 2;
#[cfg(feature = "postgres")]
pub(crate) const POSTGRES_BIND_FRAME_BYTES: usize =
    4 + 1 + POSTGRES_STATEMENT_NAME_BYTES + 3 * 2 + 2;
#[cfg(feature = "postgres")]
pub(crate) const POSTGRES_RECORD_ROW_FRAME_BYTES: usize = 4 + 2 + 6 * 4;
#[cfg(feature = "postgres")]
pub(crate) const POSTGRES_FORMAT_ROW_FRAME_BYTES: usize = 4 + 2 + INFRASTRUCTURE_FORMAT_COLUMNS * 4;

pub(crate) const GSI_HISTORY_CHUNK_ROWS: usize = 32;
pub(crate) const GSI_HISTORY_CHUNK_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(64 * 1024 * 1024 - 1);
pub(crate) const GSI_PROJECTION_STATE_READ_ROWS: usize = 64;
pub(crate) const GSI_STREAM_PAGE_ROWS: u32 = 32;
#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
pub(crate) const SCHEDULED_JOB_POINT_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(2 * 1024 * 1024 - 1);
#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
pub(crate) const SCHEDULED_JOB_VALUE_BYTES: usize = 1024 * 1024;
#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
pub(crate) const JOB_RECEIPT_MAX_CONTENT_BYTES: usize = SCHEDULED_JOB_VALUE_BYTES + 1024;
pub(crate) const QUEUE_CATALOG_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);
pub(crate) const PUBSUB_CATALOG_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);
pub(crate) const QUEUE_MESSAGE_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(10 * 1024 * 1024 - 1);
pub(crate) const QUEUE_CLEANUP_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);
pub(crate) const STREAM_CATALOG_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);
pub(crate) const STREAM_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);
pub(crate) const SYSTEM_STREAM_PAGE_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);
pub(crate) const RANGE_PAGE_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);
pub(crate) const RETENTION_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);
pub(crate) const RETENTION_DELETE_BATCH_ROWS: usize = 400;

pub(crate) const SQLITE_PLACEHOLDERS: [&str; 21] = [
    "?0", "?1", "?2", "?3", "?4", "?5", "?6", "?7", "?8", "?9", "?10", "?11", "?12", "?13", "?14",
    "?15", "?16", "?17", "?18", "?19", "?20",
];
pub(crate) const POSTGRES_PLACEHOLDERS: [&str; 21] = [
    "$0", "$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8", "$9", "$10", "$11", "$12", "$13", "$14",
    "$15", "$16", "$17", "$18", "$19", "$20",
];

// Table keys occupy slots 1–2; GSI plus table keys occupy slots 1–4.
pub(crate) const POSTGRES_NUMERIC_PLACEHOLDERS: [&str; 5] = [
    "CAST($0 AS TEXT)::NUMERIC",
    "CAST($1 AS TEXT)::NUMERIC",
    "CAST($2 AS TEXT)::NUMERIC",
    "CAST($3 AS TEXT)::NUMERIC",
    "CAST($4 AS TEXT)::NUMERIC",
];

pub(crate) const ORDINALS_PER_TRANSACTION: u64 = 65_536;

#[cfg(any(feature = "sqlite", feature = "turso"))]
pub(crate) const SQLITE_RECORD_HEADER_BOUND: usize = 7 * 9;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) const INFRASTRUCTURE_FORMAT_COLUMNS: usize = 3;
/// Singleton/version integers and the persisted native identity.
pub(crate) const INFRASTRUCTURE_FORMAT_PAYLOAD_BYTES: usize = 2 * std::mem::size_of::<i64>() + 16;
/// Header-length varint plus one serial-type varint per format field.
#[cfg(feature = "sqlite")]
pub(crate) const SQLITE_FORMAT_HEADER_BOUND: usize = (INFRASTRUCTURE_FORMAT_COLUMNS + 1) * 9;
