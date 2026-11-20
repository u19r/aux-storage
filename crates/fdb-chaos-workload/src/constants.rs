pub(crate) const WORKLOAD_KV_SMOKE: &str = "kv_smoke";
pub(crate) const WORKLOAD_NOOP: &str = "noop";
pub(crate) const WORKLOAD_PUBSUB_DELIVERY: &str = "pubsub_delivery";
pub(crate) const WORKLOAD_QUEUE_VISIBILITY: &str = "queue_visibility";
pub(crate) const WORKLOAD_QUEUE_CLAIM: &str = "queue_claim";
pub(crate) const WORKLOAD_TABLE_RECREATE: &str = "table_recreate";
pub(crate) const OPTION_WRITER_COUNT: &str = "writerCount";
pub(crate) const OPTION_RECREATE_COUNT: &str = "recreateCount";
pub(crate) const TABLE_RECREATE_PAUSE_MILLIS: u64 = 50;
pub(crate) const WORKLOAD_QUEUE_WAKE: &str = "queue_wake";
pub(crate) const QUEUE_WAKE_PREFIX_BYTES: &[u8] = b"aux-storage/fdb-chaos/queue-wake/";
/// The slot key family below the store prefix (kv-backend `QUEUE_WAKE_PREFIX`).
pub(crate) const QUEUE_WAKE_FAMILY: &[u8] = b"\x7fq/w/";
pub(crate) const QUEUE_WAKE_QUEUE_COUNT: usize = 4;
pub(crate) const QUEUE_WAKE_READ_ATTEMPTS: u32 = 20;
pub(crate) const WORKLOAD_QUEUE_DEAD_LETTER: &str = "queue_dead_letter";
pub(crate) const QUEUE_DLQ_MAX_RECEIVE_COUNT: u64 = 2;
pub(crate) const QUEUE_DLQ_VISIBILITY_SECONDS: u32 = 1;
pub(crate) const QUEUE_DLQ_PUSH_POLLS: u32 = 60;
pub(crate) const OPTION_NODE_COUNT: &str = "nodeCount";
pub(crate) const OPTION_MESSAGE_COUNT: &str = "messageCount";
/// Short leases so that expiries and reclaims happen in a short run.
pub(crate) const QUEUE_CLAIM_VISIBILITY_SECONDS: u32 = 3;
pub(crate) const QUEUE_CLAIM_MAX_MESSAGES: i64 = 3;
pub(crate) const QUEUE_CLAIM_DRAIN_POLLS: u32 = 40;
/// Simulated pause after an empty short poll.
pub(crate) const QUEUE_CLAIM_EMPTY_PAUSE_MILLIS: u64 = 200;
pub(crate) const WORKLOAD_TABLE_ATOMICITY: &str = "table_atomicity";
pub(crate) const WORKLOAD_READ_SEQUENCE_DAG: &str = "read_sequence_dag";
pub(crate) const OPTION_PROFILE: &str = "profile";
pub(crate) const OPTION_OPERATION_COUNT: &str = "operationCount";
pub(crate) const OPTION_HISTORY_SAMPLE_LIMIT: &str = "historySampleLimit";
pub(crate) const OPTION_ARTIFACT_ROOT: &str = "artifactRoot";
pub(crate) const OPTION_KEY_COUNT: &str = "keyCount";
pub(crate) const OPTION_ACTIVE_CLIENT_COUNT: &str = "activeClientCount";
pub(crate) const OPTION_SHARED_KEY_COUNT: &str = "sharedKeyCount";
pub(crate) const OPTION_SHARED_OPERATION_PERCENT: &str = "sharedOperationPercent";
pub(crate) const OPTION_SEED: &str = "readSequenceSeed";
pub(crate) const OPTION_BUGGIFY: &str = "readSequenceBuggify";
pub(crate) const GSI_INDEX_NAME: &str = "by_category_score";
pub(crate) const GSI_CATEGORY_ATTR: &str = "category";
pub(crate) const GSI_SCORE_ATTR: &str = "score";
pub(crate) const TABLE_STREAM_DURATION_HOURS: u16 = 24;
pub(crate) const QUEUE_REDELIVERY_WAIT_SECONDS: u32 = 5;
/// Simulated time between two short polls for a redelivery.
pub(crate) const QUEUE_REDELIVERY_POLL_MILLIS: u32 = 100;
/// Extra send calls in one round for sends with an unknown outcome or a known
/// abort.
pub(crate) const QUEUE_MAX_AMBIGUOUS_SENDS: u64 = 3;
/// Receives that one round or one drain makes before it stops.
pub(crate) const QUEUE_COLLECT_POLLS: u32 = 10;
/// Receives with an unknown commit outcome that one client tolerates; each
/// may have claimed its messages until the visibility timeout ends.
pub(crate) const QUEUE_MAX_AMBIGUOUS_RECEIVES: u64 = 3;
pub(crate) const QUEUE_RECEIVE_VISIBILITY_SECONDS: u32 = 30;
/// Calls of one idempotent queue operation, or receives after a conflict.
pub(crate) const QUEUE_IDEMPOTENT_CALL_ATTEMPTS: u32 = 10;

pub(crate) const KV_SMOKE_NATIVE_ATTEMPTS: u32 = 5;
/// Table metadata cache TTL in simulation: longer than any run. The cache
/// measures TTL on the machine clock, so a shorter TTL makes a cache hit
/// depend on real elapsed time and the run non-deterministic.
pub(crate) const SIMULATION_METADATA_CACHE_TTL: std::time::Duration =
    std::time::Duration::from_secs(7 * 24 * 60 * 60);
