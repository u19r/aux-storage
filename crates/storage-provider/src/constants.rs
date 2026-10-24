use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use storage_types::WriteRetryPolicy;

pub(crate) const NAME_REUSE_DELAY_MILLIS: i64 = 30_000;

/// One known-abort retry policy for every local Dynamo write family.
pub const DYNAMO_WRITE_RETRY: WriteRetryPolicy =
    WriteRetryPolicy::new(12, Duration::from_millis(1));

/// Bound physical catalog reads before decoding table metadata or names.
pub const CATALOG_READ_BYTES: NonZeroUsize = NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);

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
pub const STREAM_TRIM_DUE_BUCKET_MILLIS: i64 = 60 * 60 * 1000;
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

/// Projection custody identity; the digest never participates in index
/// ordering.
pub(crate) const GSI_PROJECTION_OWNER_IDENTITY_TAG: &[u8] = b"aux-storage-projection-owner-v1";
