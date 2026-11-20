//! Physical KV keys shared by family lowering and native drivers.

use std::num::{NonZeroU64, NonZeroUsize};

/// An isolated one-byte global KV range belongs to exactly one physical
/// layout. Composite prefixes such as `aT` and `at` share a byte
/// intentionally. Table-owned families live under
/// `table_key_layout::TABLE_ROOT`.
#[repr(u8)]
enum KeyFamily {
    Lookup = b'M',
    TableStream = b'S',
    TrimDue = b'd',
    GsiRunnable = b'f',
}

pub(crate) const LOOKUP_FAMILY: u8 = KeyFamily::Lookup as u8;
pub(crate) const TABLE_STREAM_FAMILY: u8 = KeyFamily::TableStream as u8;
pub(crate) const PRIMARY_ITEM_KEY_PREFIX_BYTES: usize =
    crate::table_key_layout::TABLE_KEY_PREFIX_BYTES;
pub(crate) const GSI_PROJECTION_KEY_PREFIX_BYTES: usize =
    PRIMARY_ITEM_KEY_PREFIX_BYTES + size_of::<u16>();
pub(crate) const PRIMARY_ITEM_FORMAT: u8 = 3;
pub(crate) const PRIMARY_ITEM_HEADER_BYTES: std::num::NonZeroUsize =
    std::num::NonZeroUsize::MIN.saturating_add(13);
pub(crate) const KV_CHUNK_HEADER_BYTES: usize = 7;
pub(crate) const KV_VALUE_INLINE_TAG: u8 = 0;
pub(crate) const KV_VALUE_CHUNKED_TAG: u8 = 1;
/// Chunks of owners outside a table root and outside the queue message family.
pub(crate) const KV_CHUNK_KEY_FAMILY: &[u8] = b"\0chunk/";
pub(crate) const KV_CHUNK_BYTES: usize = 64 * 1024;
/// Chunk key bytes beyond the owner key, in the larger (global) chunk home.
pub(crate) const KV_CHUNK_KEY_OVERHEAD: usize = KV_CHUNK_KEY_FAMILY.len() + 4;
/// Representable u32 length with at most u16::MAX fixed-size chunks; not
/// admission.
pub(crate) const KV_MAX_VALUE_BYTES: usize = KV_CHUNK_BYTES * u16::MAX as usize;
/// Active readers without a family envelope retain their previous per-value
/// allowance.
pub(crate) const KV_UNQUALIFIED_VALUE_BYTES: usize = 10 * 1024 * 1024;
pub(crate) const KV_UNQUALIFIED_RANGE_HEAD_BYTES: usize =
    KV_UNQUALIFIED_VALUE_BYTES + KV_CHUNK_HEADER_BYTES;

pub(crate) const GSI_PROJECTION_FORMAT: u8 = 2;
// Partition digest, doubled sort, terminator and full length-prefixed primary
// tie.
pub(crate) const GSI_PROJECTION_SUFFIX_MAX_BYTES: usize =
    32 + 3 * storage_types::MAX_SORT_KEY_BYTES + storage_types::MAX_PARTITION_KEY_BYTES + 5;
pub(crate) const TABLE_RECLAIM_PREFIX: &[u8] = b"\x7fm/t/";
pub(crate) const TABLE_ID_ALLOCATOR_KEY: &[u8] = b"aT";
/// The last issued stream-enable stamp (big-endian millis).
pub(crate) const STREAM_LABEL_CLOCK_KEY: &[u8] = b"aL";
pub(crate) const INITIAL_REVISION: NonZeroU64 = NonZeroU64::MIN;
pub(crate) const QUEUE_NAME_PREFIX: &[u8] = b"\x7fq/n/";
pub(crate) const PUBSUB_TOPIC_PREFIX: &[u8] = b"\x7fp/t/";
pub(crate) const PUBSUB_SUBSCRIPTION_PREFIX: &[u8] = b"\x7fp/s/";
pub(crate) const PUBSUB_SUBSCRIPTION_TOPIC_PREFIX: &[u8] = b"\x7fp/x/";
pub(crate) const PUBSUB_SUBSCRIPTION_IDENTITY_PREFIX: &[u8] = b"\x7fp/d/";
pub(crate) const PUBSUB_SUBSCRIPTION_TOKEN_PREFIX: &[u8] = b"\x7fp/k/";
pub const PUBSUB_PUBLISH_INTENT_PREFIX: &[u8] = b"\x7fp/i/";
pub(crate) const PUBSUB_PUBLISH_CHUNK_PREFIX: &[u8] = b"\x7fp/c/";
pub(crate) const PUBSUB_PUBLISH_DUE_PREFIX: &[u8] = b"\x7fp/j/";
/// Delivery payloads, written once (W3S R1).
pub const PUBSUB_DELIVERY_PAYLOAD_PREFIX: &[u8] = b"\x7fp/v/";
/// Delivery states: destination id, due time, id (W3S R2).
pub const PUBSUB_DELIVERY_STATE_PREFIX: &[u8] = b"\x7fp/q/";
pub(crate) const CATALOG_BYTES: NonZeroUsize = NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);
/// Message bodies (W7): `MESSAGE_PREFIX | incarnation | message id`, written
/// once; chunks of large bodies are under `QUEUE_MESSAGE_CHUNK_PREFIX`.
pub(crate) const MESSAGE_PREFIX: &[u8] = b"\x7fq/m/";
/// Message states (W7): `QUEUE_STATE_PREFIX | incarnation | message id`,
/// bounded and never chunked.
pub(crate) const QUEUE_STATE_PREFIX: &[u8] = b"\x7fq/s/";
pub(crate) const DUE_PREFIX: &[u8] = b"\x7fq/d/";
pub(crate) const RECEIPT_PREFIX: &[u8] = b"\x7fq/r/";
pub(crate) const QUEUE_WAKE_PREFIX: &[u8] = b"\x7fq/w/";
/// Chunks of queue messages: `QUEUE_MESSAGE_CHUNK_PREFIX | incarnation | u16
/// length | message id | u16 n`.
pub(crate) const QUEUE_MESSAGE_CHUNK_PREFIX: &[u8] = b"\x7fq/k/";
pub(crate) const MESSAGE_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);
pub(crate) const STREAM_CATALOG_PREFIX: &[u8] = b"\x7fu/c/";
pub(crate) const STREAM_ID_PREFIX: &[u8] = b"\x7fu/i/";
pub(crate) const STREAM_CURSOR_PREFIX: &[u8] = b"\x7fu/k/";
pub(crate) const TTL_INDEX_FORMAT_KEY: &[u8] = b"\x7fm/e";
pub(crate) const TTL_INDEX_FORMAT_VERSION: u8 = 2;
/// Maximum concurrent native read calls of one attempt. One call submits all
/// of its point reads or range cursors at once under one permit; native work
/// budgets bound the keys of each call.
#[cfg(feature = "foundationdb")]
pub(crate) const FOUNDATIONDB_PIPELINE_REQUESTS: usize = 32;
#[cfg(feature = "foundationdb")]
pub(crate) const FOUNDATIONDB_MAX_KEY_BYTES: usize = 10_000;
// FDB 7.4.6 VectorRef::expectedSize includes sizeof each native entry.
// On its supported 64-bit client ABI, two StringRefs occupy 32 bytes;
// 64 conservatively bounds MutationRef including optional checksum fields.
// See fdbclient/{CommitTransaction.h,FDBTypes.h} and flow/Arena.h upstream.
#[cfg(feature = "foundationdb")]
pub(crate) const FOUNDATIONDB_MUTATION_METADATA_BYTES: usize = 64;
/// Locked RocksDB WriteBatch: fixed64 sequence and fixed32 count.
#[cfg(feature = "rocksdb")]
pub(crate) const ROCKSDB_BATCH_METADATA_BYTES: usize = 12;
/// Default-column-family tag plus two maximal varint32 lengths (put/range
/// clear).
#[cfg(feature = "rocksdb")]
pub(crate) const ROCKSDB_MUTATION_METADATA_BYTES: usize = 1 + 5 + 5;
#[cfg(feature = "foundationdb")]
pub(crate) const FOUNDATIONDB_CONFLICT_RANGE_METADATA_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(31);
#[cfg(feature = "foundationdb")]
pub(crate) const FOUNDATIONDB_TRANSACTION_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(9_999_999);
#[cfg(feature = "foundationdb")]
pub(crate) const FOUNDATIONDB_TRANSACTION_TOO_LARGE_CODE: i32 = 2101;
/// `transaction_cancelled` and `transaction_timed_out`: a commit submitted
/// before either can still apply.
#[cfg(feature = "foundationdb")]
pub(crate) const FOUNDATIONDB_TRANSACTION_CANCELLED_CODE: i32 = 1025;
#[cfg(feature = "foundationdb")]
pub(crate) const FOUNDATIONDB_TRANSACTION_TIMED_OUT_CODE: i32 = 1031;
#[cfg(any(test, feature = "foundationdb"))]
pub(crate) const FOUNDATIONDB_NATIVE_VALUE_BYTES: usize = 100_000;
// Shared logical versionstamp lowering is compiled for every native provider.
pub(crate) const KV_VERSIONSTAMP_PLACEHOLDER: [u8; 10] = [u8::MAX; 10];

pub(crate) const PAGE_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(storage_provider::MAX_RANGE_READ_EVALUATED_BYTES as usize - 1);

#[cfg(feature = "rocksdb")]
pub(crate) const BACKEND_HEALTH_KEY: &[u8] = b"\0backend-health";
pub(crate) const TRANSACT_RECEIPT_PREFIX: &[u8] = b"\x7ft/";
pub(crate) const GSI_RUNNABLE_FAMILY: u8 = KeyFamily::GsiRunnable as u8;
pub(crate) const TRIM_DUE_FAMILY: u8 = KeyFamily::TrimDue as u8;
pub(crate) const SYSTEM_STREAM_SCOPE_KEY: &[u8] = b"\x7fs/s";
pub(crate) const SYSTEM_STREAM_SCHEMA_KEY: &[u8] = b"\x7fs/v";
pub(crate) const SYSTEM_STREAM_CATALOG_EPOCH_KEY: &[u8] = b"\x7fs/c";
pub(crate) const SYSTEM_STREAM_LINEAGE_PREFIX: &[u8] = b"\x7fs/l/";
pub(crate) const SYSTEM_STREAM_ITEM_PREFIX: &[u8] = b"\x7fs/i/";
pub(crate) const SYSTEM_STREAM_RECORD_PREFIX: &[u8] = b"\x7fs/r/";
pub(crate) const SYSTEM_STREAM_TRIM_RECEIPT_PREFIX: &[u8] = b"\x7fs/t/";
pub(crate) const SYSTEM_STREAM_SPLIT_RECEIPT_PREFIX: &[u8] = b"\x7fs/sr/";
pub(crate) const SCHEDULED_JOB_VALUE_BYTES: usize = 1024 * 1024;
pub(crate) const JOB_DATA_PREFIX: &[u8] = b"\x7fj/d/";
pub(crate) const MAINTENANCE_IDENTITY_KEY: &[u8] = b"\x7fm/i";
pub(crate) const MAINTENANCE_PROGRESS_PREFIX: &[u8] = b"\x7fm/p";
pub(crate) const RETENTION_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);
pub(crate) const STREAM_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);
pub(crate) const SYSTEM_STREAM_PAGE_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);
pub(crate) const USER_STREAM_SEQUENCE_PREFIX: &[u8] = b"\x7fu/q/";
pub(crate) const USER_STREAM_SEQUENCE_BYTES: usize = std::mem::size_of::<u64>();
pub(crate) const STREAM_ROUTE_BYTES: usize = 2 * std::mem::size_of::<u16>();
pub(crate) const PARTITION_STATE_PREFIX: &[u8] = b"\x7fu/p/";
pub(crate) const PARTITION_ROW_PREFIX: &[u8] = b"\x7fu/d/";
pub(crate) const PARTITION_ITEM_INDEX_PREFIX: &[u8] = b"\x7fu/x/";

pub(crate) const PARTITION_SAMPLE_PREFIX: &[u8] = b"\x7fu/l/";

pub(crate) const SYSTEM_STREAM_RETIRED_PREFIX: &[u8] = b"\0system-retired/";

pub(crate) const INFRASTRUCTURE_PREFIX: &[u8] = b"\x7fi/";
pub(crate) const INFRASTRUCTURE_FORMAT_KEY: &[u8] = b"\x7fi/v";
pub(crate) const INFRASTRUCTURE_RECORD_PREFIX: &[u8] = b"\x7fi/r/";
pub(crate) const INFRASTRUCTURE_LAYOUT_VERSION: u8 = 47;
pub(crate) const INFRASTRUCTURE_RECORD_HEADER_BYTES: usize = 8;
// Generation bytes plus three tuple segment terminators.
pub(crate) const INFRASTRUCTURE_KEY_FRAMING_BYTES: usize = 14;

#[cfg(feature = "foundationdb")]
pub(crate) const FOUNDATIONDB_MIN_TRANSACTION_BYTES: usize = 32;

pub(crate) const TABLE_HISTORY_RECLAIM_ROWS: std::num::NonZeroU32 =
    std::num::NonZeroU32::MIN.saturating_add(63);

/// Raw FoundationDB system key; never apply the application tuple prefix.
#[cfg(any(test, feature = "foundationdb"))]
pub(crate) const FOUNDATIONDB_METADATA_VERSION_KEY: &[u8] = b"\xff/metadataVersion";
/// Durable RocksDB catalog counter in the application's native namespace.
#[cfg(any(feature = "rocksdb", test))]
pub(crate) const ROCKSDB_CATALOG_VERSION_KEY: &[u8] = b"\x7fm/catalog-version";
