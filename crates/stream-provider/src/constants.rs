use storage_types::{PaginationLimit, WriteRetryPolicy};

pub const DEFAULT_LIMIT: u32 = 100;
pub const MAX_LIMIT: u32 = 1000;
pub const STREAM_LIMITS: PaginationLimit = PaginationLimit::new(DEFAULT_LIMIT, MAX_LIMIT);
pub const MAX_STREAM_ITEM_DATA_BYTES: usize = 1_048_576;
/// Current single-writer lineage for ordinary Dynamo stream effects.
pub const DYNAMO_SYSTEM_SHARD_ID: &str = "default-shard";
pub const DYNAMO_SYSTEM_LINEAGE_ID: &str = "default-lineage";
pub(crate) const BOOTSTRAP_CURSOR_PREFIX: &str = "__aux_bootstrap_cursor_v1__";
pub(crate) const MAX_CURSOR_NAME_BYTES: usize = 255;
pub const MAX_SYSTEM_STREAM_CURSOR_SCAN_ROWS: u32 = 4_096;
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
pub(crate) const STREAM_POINTER_ROW_PREFIX: &[u8; 4] = b"ASP1";
pub(crate) const STREAM_IMAGE_ROW_PREFIX: &[u8; 4] = b"ASI1";
pub(crate) const STREAM_ROW_HEADER_BYTES: usize = 21;

pub(crate) const STREAM_APPEND_RECEIPT_VERSION: u16 = 1;
pub(crate) const RETAINED_PAYLOAD_FORMAT: u8 = 1;
pub(crate) const RETAINED_RAW_PAYLOAD_FORMAT: u8 = 2;
pub(crate) const RETAINED_RAW_PAYLOAD_MAX_BYTES: usize = 2048;
pub(crate) const RETAINED_RAW_PAYLOAD_CHECKSUM_BYTES: usize = 32;
pub(crate) const RETAINED_PAYLOAD_HEADER_BYTES: usize = 9;
pub(crate) const IMAGE_CHARGE_RECORD_MAGIC: &[u8; 4] = b"AXIC";
pub(crate) const IMAGE_CUSTODY_RECORD_VERSION: u16 = 2;
pub(crate) const IMAGE_CUSTODY_RECORD_HEADER_BYTES: usize = 4 + 2 + 32;
/// Two key representations, four image/declaration spans and one causal body.
pub const MAX_RETAINED_MANIFEST_BODIES: usize = 7;

pub(crate) const RETAINED_IMAGE_NAMESPACE: &str = "dynamo-image";
pub(crate) const RETAINED_IMAGE_MANIFEST_FORMAT: u8 = 6;
pub(crate) const RETAINED_PROJECTION_RECORD_FORMAT: u8 = 5;
/// Two image spans leave only the history JSON array and tuple separators
/// inline.
pub(crate) const RETAINED_TWO_BODY_METADATA_BYTES: usize =
    RETAINED_IMAGE_MANIFEST_HEADER_BYTES + 2 * (1 + 8 + 32) + 3 * (1 + 8) + (16 * 4 + 1) + 4;
pub(crate) const RETAINED_IMAGE_MANIFEST_HEADER_BYTES: usize = 8 + 32 + 1 + 1 + 16 + 1;

/// Scratch space for retained decoding; declared raw length never sizes
/// scratch.
pub(crate) const RETAINED_PAYLOAD_DECODE_CHUNK_BYTES: usize = 4096;

/// Each attribute map/list layer adds at most two JSON containers, plus the
/// tuple and keys.
pub(crate) const RETAINED_JSON_MAX_CONTAINERS: usize =
    2 * storage_types::MAX_ATTRIBUTE_NESTING_DEPTH + 8;

pub(crate) const RETAINED_PROJECTION_HEADER_BYTES: usize = 1 + 16 + 3 * 8 + 1;
pub(crate) const RETAINED_DESCRIPTOR_BODY_WORK_BYTES: usize = 32 + 8;
/// The generic primary descriptor stores its unit identity as JSON null.
pub(crate) const RETAINED_PRIMARY_IDENTITY_BYTES: usize = 4;

pub(crate) const IMAGE_OWNER_RECORD_MAGIC: &[u8; 4] = b"AXIO";
pub(crate) const RETAINED_OWNER_SUFFIX_BYTES: usize = 1 + 32;
pub(crate) const IMAGE_EDGE_SUFFIX_BYTES: usize = 32 + 1 + 1 + 32;
pub(crate) const RETAINED_OWNER_NAMESPACE: &str = "dynamo-image-owner";
pub(crate) const PRIMARY_IMAGE_LOCATOR_NAMESPACE: &str = "dynamo-primary-owner";
/// Ordered primary suffix: a u16 partition length and the two admitted key
/// values.
pub(crate) const PRIMARY_IMAGE_LOCATOR_BYTES: usize = 16
    + 16
    + PRIMARY_IMAGE_LOCATOR_NAMESPACE.len()
    + 2
    + storage_types::MAX_PARTITION_KEY_BYTES
    + storage_types::MAX_SORT_KEY_BYTES;
pub(crate) const IMAGE_OWNER_RECLAIM_RECORDS: usize = 64;
pub const GSI_IMAGE_LOCATOR_NAMESPACE: &str = "dynamo-projection-owner";
/// Exact incarnation/name scope followed by the complete ordered-key identity
/// digest.
pub(crate) const GSI_IMAGE_LOCATOR_BYTES: usize = 16
    + 16
    + GSI_IMAGE_LOCATOR_NAMESPACE.len()
    + 2
    + storage_types::MAX_TARGET_INCARNATION_BYTES
    + 2
    + storage_types::MAX_INDEX_NAME_BYTES
    + 32;

/// Physical receipt hydration wave, independent of public item/page limits.
pub(crate) const IMAGE_OWNER_READ_RECORDS: usize = 64;

/// Stable stream receipt locator scope; role lives in the common owner key.
pub(crate) const STREAM_IMAGE_OWNER_NAMESPACE: &str = "dynamo-stream-owner";

pub(crate) const IMAGE_HEAD_RECORD_MAGIC: &[u8; 4] = b"AXIH";
pub(crate) const IMAGE_EDGE_RECORD_MAGIC: &[u8; 4] = b"AXIE";
pub(crate) const IMAGE_HEAD_NAMESPACE: &str = "dynamo-image-head";
pub(crate) const IMAGE_EDGE_NAMESPACE: &str = "dynamo-image-edge";

/// Three u32 span lengths and one u64 generation in a binary custody address.
pub(crate) const CUSTODY_KEY_HEADER_BYTES: usize = 3 * 4 + 8;
pub(crate) const CUSTODY_IMAGE_ADDRESS_BYTES: usize = 16 + 32;
/// Includes payload tag, owner kind, history, descriptor, count and key framing
/// delta.
pub(crate) const IMAGE_OWNER_PAYLOAD_FIXED_BYTES: usize =
    1 + 1 + 16 + 32 + 1 + CUSTODY_KEY_HEADER_BYTES - 16;
pub(crate) const IMAGE_OWNER_BODY_BYTES: usize = CUSTODY_IMAGE_ADDRESS_BYTES + 8;

/// Payload tag, graph format, image address, three u64 fields and charge flag.
pub(crate) const IMAGE_HEAD_BASE_PAYLOAD_BYTES: usize =
    1 + 2 + CUSTODY_IMAGE_ADDRESS_BYTES + 3 * 8 + 1;
/// Head plus two logical-key framing deltas, charge image address and charged
/// bytes.
pub(crate) const IMAGE_HEAD_PAYLOAD_FIXED_BYTES: usize = IMAGE_HEAD_BASE_PAYLOAD_BYTES
    + 2 * (CUSTODY_KEY_HEADER_BYTES - 16)
    + CUSTODY_IMAGE_ADDRESS_BYTES
    + 8;

pub(crate) const CAUSAL_BODY_SCOPE_DOMAIN: &[u8] = b"retained-journal-causal-body-v1";

pub(crate) const MAX_CAUSAL_BODY_ALIASES: usize = storage_types::MAX_TRANSACTION_MEMBERS;
pub(crate) const MAX_RETAINED_MANIFEST_SEGMENTS: usize =
    2 * (MAX_RETAINED_MANIFEST_BODIES + MAX_CAUSAL_BODY_ALIASES) + 1;
pub(crate) const CAUSAL_SLICE_METADATA_BYTES: usize = 1 + 8 + 16 + 32 + 8 + 8;

/// Domain separation for persisted lineage pointer slots; changing it changes
/// physical keys.
pub(crate) const SYSTEM_POINTER_SLOT_DOMAIN: &[u8] = b"aux-system-pointer-slot-v1\0";
