use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use storage_types::{
    AttemptMode, AttemptPlan, AttemptScope, DeadlineClass, NativeWork, WriteRetryPolicy,
};

pub(crate) const NAME_REUSE_DELAY_MILLIS: i64 = 30_000;
pub(crate) const REGIONAL_HOLD_RECHECK_MILLIS: i64 = 60_000;
pub(crate) const DYNAMODB_STREAM_ITERATOR_VERSION: &str = "aux-stream-v2";

/// One known-abort retry policy for local Dynamo operations.
/// The initial jitter must span multiple timer ticks to disperse hot-key
/// writers.
pub const DYNAMO_WRITE_RETRY: WriteRetryPolicy =
    WriteRetryPolicy::new(12, Duration::from_millis(5));

/// Table catalog change: CreateTable, DeleteTable, UpdateTable,
/// UpdateTimeToLive, table reclaim, regional binding and pair-state changes.
pub(crate) const SCHEMA_CHANGE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::SchemaChange)
        .retrying(DYNAMO_WRITE_RETRY);
/// Pair end (finish, force unpair, abandon): a catalog change whose cleanup
/// deletes every regional marker and progress row of the table. The attempt
/// takes the exclusive catalog lock first; statement snapshots (Postgres READ
/// COMMITTED) then see every row that a paired writer committed before the
/// lock. A SERIALIZABLE snapshot is taken before the lock wait and misses them.
pub(crate) const PAIR_END_CHANGE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::SchemaChange)
        .retrying(DYNAMO_WRITE_RETRY);
/// Catalog and item point reads of one retried read operation.
pub(crate) const POINT_READ: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead).retrying(DYNAMO_WRITE_RETRY);
/// Range reads of one retried read operation (lists, stream records, read
/// sequences).
pub(crate) const RANGE_READ: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::RangeRead)
        .retrying(DYNAMO_WRITE_RETRY)
        .work(NativeWork::RangeRead);
/// A maintenance page that writes items under their own protection (expiration,
/// GSI build, regional apply).
pub(crate) const BACKGROUND_ITEM_PAGE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::BackgroundPage)
        .retrying(DYNAMO_WRITE_RETRY);
/// A maintenance page that reads and writes with protected reads (receipts,
/// retention, partitions, regional progress).
pub(crate) const BACKGROUND_PAGE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::BackgroundPage)
        .retrying(DYNAMO_WRITE_RETRY);
/// A retried read-only maintenance page.
pub(crate) const BACKGROUND_READ_PAGE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::BackgroundPage)
        .retrying(DYNAMO_WRITE_RETRY)
        .work(NativeWork::RangeRead);
/// One snapshot read for maintenance (a job receipt, a TTL policy). One try:
/// `with_attempt`, never `run`.
pub(crate) const BACKGROUND_SNAPSHOT_READ: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::BackgroundPage);
/// A table history append (stream port).
pub(crate) const TABLE_APPEND: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).retrying(DYNAMO_WRITE_RETRY);
/// Reconciliation read of one table append. One try: `with_attempt`, never
/// `run`.
pub(crate) const TABLE_APPEND_RECONCILE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead);
/// One item write (PutItem, UpdateItem, DeleteItem): its staged item writes
/// are the attempt's last native calls.
pub(crate) const ITEM_WRITE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
        .retrying(DYNAMO_WRITE_RETRY)
        .scoped(AttemptScope::ItemWrite);
/// TransactWriteItems, a job effect, and every BatchWriteItem group (one
/// operation deadline for all groups).
pub(crate) const TRANSACTION_WRITE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Transaction)
        .retrying(DYNAMO_WRITE_RETRY)
        .scoped(AttemptScope::ItemWrite);

/// Bound physical catalog reads before decoding table metadata or names.
pub const CATALOG_READ_BYTES: NonZeroUsize = NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);

/// Page budget of a transaction-receipt read: one receipt, or one trim page of
/// receipts, each at most `TRANSACT_RECEIPT_WRITE_BYTES` plus its token.
pub const TRANSACT_RECEIPT_PAGE_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);

/// Bound one protected SystemStream authority snapshot on every backend.
pub const SYSTEM_STREAM_SNAPSHOT_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);
pub const SYSTEM_STREAM_SNAPSHOT_ROWS: NonZeroU32 = NonZeroU32::MIN
    .saturating_add(storage_types::SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES as u32 + 1 - 1);

/// BatchGetItem bounds returned item payloads across all requested tables.
pub(crate) const MAX_BATCH_GET_RESPONSE_ITEM_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const READ_SEQUENCE_DEFAULT_QUERY_ITEMS: u32 = 100;

pub(crate) const INTERNAL_TABLE_NAMES: [&str; 3] = ["sys", "ana", "rep"];

/// Query and Scan count logical source bytes before filtering and projection.
pub const MAX_RANGE_READ_EVALUATED_BYTES: u64 = 1024 * 1024;
pub const EXPIRATION_SCAN_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(4 * 1024 * 1024 - 1);
pub(crate) const SCHEDULED_JOB_MAX_TRANSACTION_BYTES: usize = 1024 * 1024;
pub(crate) const SCHEDULED_JOB_MAX_GET_MANY_KEYS: u32 = 500;
pub(crate) const SCHEDULED_JOB_MAX_TRANSACTION_KEYS: u32 = 100;
pub(crate) const SCHEDULED_JOB_MAX_RANGE_ITEMS: u32 = 500;

pub(crate) const UPDATE_SECTION_KEYWORDS: [(&[u8], &str); 4] = [
    (b" SET ", "SET "),
    (b" REMOVE ", "REMOVE "),
    (b" ADD ", "ADD "),
    (b" DELETE ", "DELETE "),
];

/// Global bounded discovery scope for exact retired index incarnations.
pub(crate) const GSI_RECLAIM_NAMESPACE: &str = "dynamo-gsi-reclaim";

/// Reserve protocol fields in addition to the measured frontier and token.
pub(crate) const TABLE_STREAM_RESPONSE_RESERVE_BYTES: usize = 64 * 1024;

/// DynamoDB GetRecords returns at most 1 MiB per page.
pub(crate) const DYNAMODB_STREAM_RESPONSE_BYTES: usize = 1024 * 1024;

/// Bound one cleanup transaction independently of marker discovery size.
pub(crate) const TABLE_CLEANUP_PAGE_ROWS: usize = 32;
/// Pending table work yields one discovery window before its next job turn.
pub(crate) const TABLE_CLEANUP_YIELD_MILLIS: u8 = 1;
