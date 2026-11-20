use storage_types::{
    AttemptMode, AttemptPlan, DeadlineClass, NativeWork, PaginationLimit, WriteRetryPolicy,
};

pub const DEFAULT_LIMIT: u32 = 100;
pub const MAX_LIMIT: u32 = 1000;
pub const STREAM_LIMITS: PaginationLimit = PaginationLimit::new(DEFAULT_LIMIT, MAX_LIMIT);
pub const MAX_STREAM_ITEM_DATA_BYTES: usize = 1_048_576;
/// Current single-writer lineage for ordinary Dynamo stream effects.
pub const DYNAMO_SYSTEM_SHARD_ID: &str = "default-shard";
pub const DYNAMO_SYSTEM_LINEAGE_ID: &str = "default-lineage";
pub(crate) const SYSTEM_STREAM_PAGE_ENVELOPE_RESERVE_BYTES: usize = 256;
pub const SYSTEM_STREAM_HISTORY_READ_BATCH: usize = 256;
pub(crate) const STREAM_WRITE_RETRY: WriteRetryPolicy =
    WriteRetryPolicy::new(5, std::time::Duration::from_millis(2));
/// Stream catalog change (CreateStream, DeleteStream, system-stream split).
pub(crate) const STREAM_CATALOG_WRITE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::SchemaChange)
        .retrying(STREAM_WRITE_RETRY);
/// A cursor change or a user-stream append.
pub(crate) const STREAM_WRITE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).retrying(STREAM_WRITE_RETRY);
/// A page read from a cursor that advances the cursor in the same attempt.
pub(crate) const CURSOR_PAGE_READ: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::RangeRead).retrying(STREAM_WRITE_RETRY);
/// One page of the system stream.
pub(crate) const SYSTEM_PAGE_READ: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::RangeRead)
        .retrying(STREAM_WRITE_RETRY)
        .work(NativeWork::RangeRead);
/// One catalog or cursor row. One try: used with `with_attempt`, never `run`.
pub(crate) const STREAM_POINT_READ: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead);
/// A page of stream rows or the system history snapshot. One try: used with
/// `with_attempt`, never `run`.
pub(crate) const STREAM_RANGE_READ: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::RangeRead).work(NativeWork::RangeRead);
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
pub(crate) const STREAM_ROW_HEADER_BYTES: usize = 21;

pub(crate) const STREAM_APPEND_RECEIPT_VERSION: u16 = 2;

/// Largest proof of a table append receipt. A proof holds identities only:
/// the one producer this repository had held a table history id, a DynamoDB
/// table name (at most 255 bytes), a table identity, an event identity and a
/// 32-byte digest.
pub const STREAM_APPEND_PROOF_BYTES: usize = 1024;

/// Widest JSON of a byte array of `count` bytes: brackets, three digits per
/// byte and the commas between them.
const fn json_byte_array_bytes(count: usize) -> usize {
    2 + 3 * count + count.saturating_sub(1)
}

/// Largest `storage_serde` encoding of a `StreamAppendReceipt`: the encoding
/// prefix and the JSON object with every field at its widest. `storage_serde`
/// keeps a compressed form only when it is smaller, so the prefix plus the
/// JSON bounds both forms.
pub const STREAM_APPEND_RECEIPT_ENCODED_BYTES: usize =
    storage_types::STORAGE_SERDE_ENCODED_PREFIX_LEN
        + br#"{"version":,"history_id":,"operation":,"request_digest":,"proof":,"response":}"#
            .len()
        + 5 // u16 version
        + json_byte_array_bytes(16) // TableHistoryId([u8; 16])
        + 38 // quoted hyphenated UUID of StreamAppendOperationId
        + json_byte_array_bytes(32) // request_digest
        + json_byte_array_bytes(STREAM_APPEND_PROOF_BYTES)
        // Widest response: {"AttemptAllocated":"<24 hex digits of a 12-byte StreamItemId>"};
        // {"Versionstamped":[255,255]} is shorter.
        + br#"{"AttemptAllocated":""}"#.len()
        + 24;
/// Fixed public system-history window, independent of table policies and reader
/// holds.
pub(crate) const SYSTEM_HISTORY_WINDOW_MILLIS: i64 = 24 * 60 * 60 * 1000;

/// Maximum user stream name length in bytes.
pub(crate) const MAX_STREAM_NAME_LEN: usize = 255;
/// Maximum cursor name length in bytes.
pub(crate) const MAX_CURSOR_NAME_LEN: usize = 64;
/// Stream and cursor names allow these separators between alphanumeric
/// boundary characters.
pub(crate) const STREAM_NAME_ALLOWED_MIDDLE_CHARS: &[char] = &['_', '-', '.'];
pub(crate) const CURSOR_NAME_ALLOWED_MIDDLE_CHARS: &[char] = &['_', '-'];
/// TTL accepts one second through one non-leap year.
pub(crate) const MAX_STREAM_TTL_SECONDS: u32 = 31_536_000;
