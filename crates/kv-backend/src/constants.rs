//! Physical KV keys shared by family lowering and native drivers.

use std::num::{NonZeroU64, NonZeroUsize};

/// An isolated one-byte KV range belongs to exactly one physical layout.
/// Composite prefixes such as `aT` and `at` share a byte intentionally.
#[repr(u8)]
enum KeyFamily {
    GsiNameLock = b'B',
    Lookup = b'M',
    TableStream = b'S',
    GsiBuild = b'b',
    TtlConfig = b'c',
    TrimDue = b'd',
    TtlIndex = b'e',
    GsiRunnable = b'f',
    GsiProjection = b'g',
    Metadata = b'm',
    PrimaryItem = b'p',
    TrimState = b'r',
    GsiTombstone = b'x',
}

pub(crate) const LOOKUP_FAMILY: u8 = KeyFamily::Lookup as u8;
pub(crate) const TABLE_STREAM_FAMILY: u8 = KeyFamily::TableStream as u8;
pub(crate) const METADATA_FAMILY: u8 = KeyFamily::Metadata as u8;
pub(crate) const PRIMARY_ITEM_FAMILY: u8 = KeyFamily::PrimaryItem as u8;
pub(crate) const PRIMARY_ITEM_KEY_PREFIX_BYTES: usize = 1 + size_of::<u32>();
pub(crate) const GSI_PROJECTION_KEY_PREFIX_BYTES: usize =
    PRIMARY_ITEM_KEY_PREFIX_BYTES + size_of::<u16>();
pub(crate) const PRIMARY_ITEM_FORMAT: u8 = 2;
pub(crate) const PRIMARY_ITEM_HEADER_BYTES: std::num::NonZeroUsize =
    std::num::NonZeroUsize::MIN.saturating_add(21);
pub(crate) const KV_CHUNK_HEADER_BYTES: usize = 7;
pub(crate) const KV_VALUE_INLINE_TAG: u8 = 0;
pub(crate) const KV_VALUE_CHUNKED_TAG: u8 = 1;
pub(crate) const KV_CHUNK_KEY_FAMILY: &[u8] = b"\0chunk/";
pub(crate) const KV_CHUNK_BYTES: usize = 64 * 1024;
pub(crate) const KV_CHUNK_KEY_OVERHEAD: usize = KV_CHUNK_KEY_FAMILY.len() + 4;
/// Representable u32 length with at most u16::MAX fixed-size chunks; not
/// admission.
pub(crate) const KV_MAX_VALUE_BYTES: usize = KV_CHUNK_BYTES * u16::MAX as usize;
/// Active readers without a family envelope retain their previous per-value
/// allowance.
pub(crate) const KV_UNQUALIFIED_VALUE_BYTES: usize = 10 * 1024 * 1024;
/// Cleanup keeps only small prefixes between native calls. Inline providers
/// must still admit the complete head before copying its prefix.
pub(crate) const HISTORY_METADATA_HEAD_BYTES: usize = 512 * 1024 * 1024;
pub(crate) const KV_UNQUALIFIED_RANGE_HEAD_BYTES: usize =
    KV_UNQUALIFIED_VALUE_BYTES + KV_CHUNK_HEADER_BYTES;

pub(crate) const PRIMARY_TOMBSTONE_PREFIX: &[u8] = b"at";
pub(crate) const TTL_CONFIG_FAMILY: u8 = KeyFamily::TtlConfig as u8;
pub(crate) const GSI_PROJECTION_FORMAT: u8 = 2;
pub(crate) const GSI_PROJECTION_FAMILY: u8 = KeyFamily::GsiProjection as u8;
pub(crate) const GSI_TOMBSTONE_FAMILY: u8 = KeyFamily::GsiTombstone as u8;
pub(crate) const GSI_REVISION_BYTES: usize = 8;
// Partition digest, doubled sort, terminator and full length-prefixed primary
// tie.
pub(crate) const GSI_PROJECTION_SUFFIX_MAX_BYTES: usize =
    32 + 3 * storage_types::MAX_SORT_KEY_BYTES + storage_types::MAX_PARTITION_KEY_BYTES + 5;
pub(crate) const TABLE_RECLAIM_PREFIX: &[u8] = b"\x7fm/t/";
pub(crate) const TABLE_ID_ALLOCATOR_KEY: &[u8] = b"aT";
pub(crate) const INITIAL_REVISION: NonZeroU64 = NonZeroU64::MIN;
pub(crate) const QUEUE_NAME_PREFIX: &[u8] = b"\x7fq/n/";
pub(crate) const QUEUE_URL_PREFIX: &[u8] = b"\x7fq/u/";
pub(crate) const PUBSUB_TOPIC_PREFIX: &[u8] = b"\x7fp/t/";
pub(crate) const PUBSUB_SUBSCRIPTION_PREFIX: &[u8] = b"\x7fp/s/";
pub(crate) const PUBSUB_SUBSCRIPTION_TOPIC_PREFIX: &[u8] = b"\x7fp/x/";
pub(crate) const PUBSUB_SUBSCRIPTION_IDENTITY_PREFIX: &[u8] = b"\x7fp/d/";
pub(crate) const PUBSUB_SUBSCRIPTION_TOKEN_PREFIX: &[u8] = b"\x7fp/k/";
pub const PUBSUB_PUBLISH_INTENT_PREFIX: &[u8] = b"\x7fp/i/";
pub(crate) const PUBSUB_PUBLISH_CHUNK_PREFIX: &[u8] = b"\x7fp/c/";
pub(crate) const PUBSUB_PUBLISH_DUE_PREFIX: &[u8] = b"\x7fp/j/";
pub const PUBSUB_DELIVERY_PREFIX: &[u8] = b"\x7fp/v/";
pub(crate) const PUBSUB_DELIVERY_DUE_PREFIX: &[u8] = b"\x7fp/y/";
pub(crate) const CATALOG_BYTES: NonZeroUsize = NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);
pub(crate) const MESSAGE_PREFIX: &[u8] = b"\x7fq/m/";
pub(crate) const DUE_PREFIX: &[u8] = b"\x7fq/d/";
pub(crate) const RECEIPT_PREFIX: &[u8] = b"\x7fq/r/";
pub(crate) const MESSAGE_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);
pub(crate) const STREAM_CATALOG_PREFIX: &[u8] = b"\x7fu/c/";
pub(crate) const STREAM_ID_PREFIX: &[u8] = b"\x7fu/i/";
pub(crate) const STREAM_CURSOR_PREFIX: &[u8] = b"\x7fu/k/";
pub(crate) const TTL_INDEX_FAMILY: u8 = KeyFamily::TtlIndex as u8;
pub(crate) const TTL_INDEX_FORMAT_KEY: &[u8] = b"\x7fm/e";
pub(crate) const TTL_INDEX_FORMAT_VERSION: u8 = 2;
/// Maximum live native point reads or range cursors in one ordered window.
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
#[cfg(any(test, feature = "foundationdb"))]
pub(crate) const FOUNDATIONDB_NATIVE_VALUE_BYTES: usize = 100_000;
// Shared logical versionstamp lowering is compiled for every native provider.
pub(crate) const KV_VERSIONSTAMP_PLACEHOLDER: [u8; 10] = [u8::MAX; 10];

pub(crate) const PAGE_READ_BYTES: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(storage_provider::MAX_RANGE_READ_EVALUATED_BYTES as usize - 1);

#[cfg(feature = "rocksdb")]
pub(crate) const BACKEND_HEALTH_KEY: &[u8] = b"\0backend-health";
pub(crate) const TRANSACT_RECEIPT_PREFIX: &[u8] = b"\x7ft/";
pub(crate) const GSI_BUILD_FAMILY: u8 = KeyFamily::GsiBuild as u8;
pub(crate) const GSI_RUNNABLE_FAMILY: u8 = KeyFamily::GsiRunnable as u8;
pub(crate) const GSI_NAME_LOCK_FAMILY: u8 = KeyFamily::GsiNameLock as u8;
pub(crate) const TRIM_STATE_FAMILY: u8 = KeyFamily::TrimState as u8;
pub(crate) const TRIM_DUE_FAMILY: u8 = KeyFamily::TrimDue as u8;
pub(crate) const SYSTEM_STREAM_SCOPE_KEY: &[u8] = b"\x7fs/s";
pub(crate) const SYSTEM_STREAM_SCHEMA_KEY: &[u8] = b"\x7fs/v";
pub(crate) const SYSTEM_STREAM_CATALOG_EPOCH_KEY: &[u8] = b"\x7fs/c";
pub(crate) const SYSTEM_STREAM_LINEAGE_PREFIX: &[u8] = b"\x7fs/l/";
pub(crate) const SYSTEM_STREAM_SEQUENCE_PREFIX: &[u8] = b"\x7fs/q/";
pub(crate) const SYSTEM_STREAM_ITEM_PREFIX: &[u8] = b"\x7fs/i/";
pub(crate) const TABLE_HISTORY_ROW_PREFIX: &[u8] = b"\x7fs/h/";
pub(crate) const SYSTEM_STREAM_RECORD_PREFIX: &[u8] = b"\x7fs/r/";
pub(crate) const SYSTEM_STREAM_TABLE_PREFIX: &[u8] = b"\x7fs/rt/";
pub(crate) const SYSTEM_STREAM_CHECKPOINT_TOKEN_PREFIX: &[u8] = b"\x7fs/ct/";
pub(crate) const SYSTEM_STREAM_CHECKPOINT_OPERATION_PREFIX: &[u8] = b"\x7fs/co/";
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

pub(crate) const STREAM_APPEND_RECEIPT_PREFIX: &[u8] = b"\0stream-append-receipt/";
#[cfg(feature = "foundationdb")]
pub(crate) const FOUNDATIONDB_MIN_TRANSACTION_BYTES: usize = 32;

pub(crate) const TABLE_HISTORY_RECLAIM_ROWS: std::num::NonZeroU32 =
    std::num::NonZeroU32::MIN.saturating_add(63);
