use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use storage_types::WriteRetryPolicy;

pub(crate) const NAME_REUSE_DELAY_MILLIS: i64 = 30_000;
pub(crate) const DYNAMODB_STREAM_ITERATOR_VERSION: &str = "aux-stream-v2";

/// One known-abort retry policy for every local Dynamo write family.
/// The initial jitter must span multiple timer ticks to disperse hot-key
/// writers.
pub const DYNAMO_WRITE_RETRY: WriteRetryPolicy =
    WriteRetryPolicy::new(12, Duration::from_millis(5));

/// Bound physical catalog reads before decoding table metadata or names.
pub const CATALOG_READ_BYTES: NonZeroUsize = NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);
pub(crate) const TABLE_STREAM_INITIALIZATION_KEY_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(255);

/// SQL page-progress budget for full-request receipts persisted by previous
/// writers. The first row may exceed it; this is not a hard receipt or native
/// memory bound. New receipts have a fixed write bound; retire this legacy
/// allowance after all previous writers and their ten-minute token windows have
/// expired.
pub const TRANSACT_RECEIPT_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(16 * 1024 * 1024 - 1);

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

/// DynamoDB rounds evaluated range-read bytes to this capacity boundary.
pub const READ_CAPACITY_UNIT_BYTES: u64 = 4 * 1024;

/// Private receipt domain outside the public transaction-token grammar.
pub(crate) const JOB_EFFECT_RECEIPT_PREFIX: &str = "job-effect:";

/// Global bounded discovery scope for exact retired index incarnations.
pub(crate) const GSI_RECLAIM_NAMESPACE: &str = "dynamo-gsi-reclaim";

/// Bound catch-up pointer admission before resolving history payloads.
pub(crate) const GSI_STREAM_PAGE_ROWS: u32 = 32;

/// Exact current/predecessor admission includes native stream envelopes.
pub(crate) const TABLE_HISTORY_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(8 * 1024 * 1024 - 1);
/// Reserve protocol fields in addition to the measured frontier and token.
pub(crate) const TABLE_STREAM_RESPONSE_RESERVE_BYTES: usize = 64 * 1024;

/// DynamoDB GetRecords returns at most 1 MiB per page.
pub(crate) const DYNAMODB_STREAM_RESPONSE_BYTES: usize = 1024 * 1024;

/// Bound one cleanup transaction independently of marker discovery size.
pub(crate) const TABLE_CLEANUP_PAGE_ROWS: usize = 32;
/// Pending table work yields one discovery window before its next job turn.
pub(crate) const TABLE_CLEANUP_YIELD_MILLIS: u8 = 1;
pub(crate) const TABLE_RECLAIM_SYSTEM_ROWS: NonZeroU32 = NonZeroU32::MIN.saturating_add(63);

pub(crate) const TABLE_STREAM_INITIALIZATION_NAMESPACE: &str =
    "aux-storage:stream-initialization:v1";
