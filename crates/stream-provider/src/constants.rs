use storage_types::{PaginationLimit, WriteRetryPolicy};

pub const DEFAULT_LIMIT: u32 = 100;
pub const MAX_LIMIT: u32 = 1000;
pub const STREAM_LIMITS: PaginationLimit = PaginationLimit::new(DEFAULT_LIMIT, MAX_LIMIT);
pub const MAX_STREAM_ITEM_DATA_BYTES: usize = 1_048_576;
/// Current single-writer lineage for ordinary Dynamo stream effects.
pub const DYNAMO_SYSTEM_SHARD_ID: &str = "default-shard";
pub const DYNAMO_SYSTEM_LINEAGE_ID: &str = "default-lineage";
pub(crate) const SYSTEM_STREAM_PAGE_ENVELOPE_RESERVE_BYTES: usize = 256;
pub const SYSTEM_STREAM_HISTORY_READ_BATCH: usize = 8;
pub(crate) const STREAM_WRITE_RETRY: WriteRetryPolicy =
    WriteRetryPolicy::new(5, std::time::Duration::from_millis(2));
/// Logical user-stream namespace shared by catalog creation and read
/// validation.
pub(crate) const USER_STREAM_PREFIX: &[u8] = b"\x7fu/";

/// Maximum number of logical SystemStream records one native trim may reclaim.
pub const MAX_SYSTEM_STREAM_TRIM_PAGE_LIMIT: usize = 1_000;

/// Maximum number of logical rows one native trim may inspect per page.
pub const MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS: usize = 4_096;

/// Maximum size of an opaque SystemStream trim continuation.
pub const MAX_SYSTEM_STREAM_TRIM_CONTINUATION_BYTES: usize = 4 * 1024;

/// Versioned binary envelope: magic, data type, timestamp, optional name
/// length, payload length.
pub(crate) const STREAM_ROW_PREFIX: &[u8; 4] = b"ASR1";
pub(crate) const STREAM_IMAGE_ROW_PREFIX: &[u8; 4] = b"ASI1";
pub(crate) const STREAM_ROW_HEADER_BYTES: usize = 21;

pub(crate) const STREAM_APPEND_RECEIPT_VERSION: u16 = 2;
/// Fixed public system-history window, independent of table policies and reader
/// holds.
pub(crate) const SYSTEM_HISTORY_WINDOW_MILLIS: i64 = 24 * 60 * 60 * 1000;
pub(crate) const SYSTEM_HISTORY_READ_BYTES: std::num::NonZeroUsize =
    std::num::NonZeroUsize::MIN.saturating_add(8 * 1024 * 1024 - 1);
