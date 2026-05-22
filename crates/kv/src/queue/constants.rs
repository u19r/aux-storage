/// Number of partition-claim rounds a receive request may perform before it
/// returns the messages found so far.
pub(crate) const PARTITIONED_QUEUE_RECEIVE_COALESCE_CLAIM_ROUNDS: usize = 4;

/// Lower bound for ready-key scans. Single-message receives are common and
/// concurrency already spreads partition discovery, so keep this small to
/// avoid reading large visible ranges for one claim.
pub(crate) const RECEIVE_SCAN_MIN_LIMIT: u32 = 4;

/// Upper bound for one ready-key range scan before rotating to another
/// partition.
pub(crate) const RECEIVE_SCAN_MAX_LIMIT: u32 = 128;

/// Maximum send placement retries when a partitioned queue route is hot or
/// concurrently changing.
pub(crate) const PARTITIONED_QUEUE_SEND_MAX_ATTEMPTS: usize = 8;

/// Internal sentinel used to pre-create partition key ranges. Reconcile must
/// ignore this because it is not queue data and should not block drain retire.
pub(crate) const QUEUE_PREWARM_MESSAGE_ID: &str = "__prewarm__";

/// Number of partition routes sampled per receive pass before claim
/// candidates are rotated.
pub(crate) const PARTITIONED_QUEUE_RECEIVE_SCAN_SHARDS: usize = 16;

/// Long-poll receive cadence after an empty partition sample. SQS receive is
/// sample based, so an empty sample cannot wait only for new writes; other
/// partitions may already contain visible messages.
pub(crate) const PARTITIONED_QUEUE_EMPTY_RECEIVE_POLL_MS: u64 = 200;

/// Overfetch factor used when scanning ready keys because some candidates may
/// no longer have an active visibility record by claim time.
pub(crate) const PARTITIONED_QUEUE_RECEIVE_SCAN_OVERFETCH_MULTIPLIER: u32 = 8;

/// Field name in the transient visibility record that stores the active marker.
pub(crate) const VISIBILITY_RECORD_STATE_FIELD: &str = "state";

/// Field name in the transient visibility record that stores the receipt
/// handle.
pub(crate) const VISIBILITY_RECORD_RECEIPT_HANDLE_FIELD: &str = "receipt_handle";

/// Active marker value for messages that can still be claimed.
pub(crate) const VISIBILITY_RECORD_ACTIVE_STATE: &str = "active";

/// Conservative payload chunk size for queue bodies stored on key-value
/// backends with a 100KB value limit.
pub(crate) const QUEUE_PAYLOAD_CHUNK_BYTES: usize = 64 * 1024;
