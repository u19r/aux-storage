//! Native ordered-key execution contract.
//!
//! Key layout and domain decisions belong to the KV family and Dynamo layer.
//! Implementors of [`KvBackend`] only own native reads, writes, isolation,
//! paging, commit, and error mechanics.

mod backend;
pub mod catalog;
mod constants;
pub mod create_table;
mod delete_table;
mod expiration;
mod fault_injection;
#[cfg(feature = "foundationdb")]
pub mod foundationdb;
pub mod gsi;
pub mod infrastructure;
mod item_codec;
mod item_read;
mod item_stream;
mod item_write;
mod job_store;
mod maintenance;
mod mutation;
mod mutation_preparation;
mod native_catalog;
#[cfg(all(
    test,
    feature = "fault-injection",
    any(feature = "rocksdb", feature = "foundationdb")
))]
mod native_catalog_tests;
#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
mod native_deadline;
mod native_layout;
mod pubsub;
pub mod pubsub_publication;
mod pubsub_schedule;
mod pubsub_subscription;
#[cfg(test)]
mod pubsub_subscription_tests;
mod pubsub_topic;
mod queue;
mod queue_catalog;
mod queue_message;
#[cfg(test)]
mod queue_message_tests;
mod queue_wake_layout;
mod range;
mod range_read;
mod recovery_source;
pub use recovery_source::{RecoverySource, RecoverySourcePage, RecoverySourceRow};
mod record_footprint;
mod regional;
#[cfg(test)]
mod regional_tests;
pub mod retention;
#[cfg(feature = "rocksdb")]
pub mod rocksdb;
mod store;
#[cfg(all(test, feature = "rocksdb", feature = "fault-injection"))]
mod store_clock_tests;
#[cfg(all(test, feature = "rocksdb", feature = "fault-injection"))]
mod store_format_retry_tests;

#[cfg(all(test, feature = "rocksdb"))]
mod pubsub_poison_tests;
#[cfg(all(test, feature = "rocksdb"))]
mod store_admission_tests;
#[cfg(all(test, feature = "rocksdb"))]
mod store_tests;
mod stream;
mod stream_catalog;
mod stream_cursor;
pub mod stream_history;
#[cfg(test)]
mod stream_history_tests;
mod stream_label_clock;
mod stream_partition;
mod stream_read;
mod stream_split;
mod stream_trim;
pub mod stream_write;
mod table_history_reclaim;
mod table_key_layout;
#[cfg(test)]
mod table_key_layout_tests;
mod table_reclaim;
mod table_update;
mod transact_write;
mod ttl_policy;
mod value_allowance;
mod value_encoding;
mod value_hydration;
mod value_layout;
#[cfg(test)]
mod value_layout_tests;
mod value_prefix;
mod versionstamped_value;
mod work_admission;

pub use backend::{AttemptMode, KvAttempt, KvBackend, KvTopology, ReadProtection};
pub use constants::{
    PUBSUB_DELIVERY_PAYLOAD_PREFIX, PUBSUB_DELIVERY_STATE_PREFIX, PUBSUB_PUBLISH_INTENT_PREFIX,
};
pub use fault_injection::{
    KvAccess, KvAccessKind, KvCommitBoundary, KvFaultCall, KvFaultOperation, KvFaultProbe,
    KvNativeEvent, KvWave, KvWaveKind, is_value_chunk_key, key_family,
};
pub use mutation::{KvMutation, KvVersionstampedMutation};
pub use native_layout::{KvNativeLayout, NativeConflictRangeLayout};
pub use range::{KvEntry, KvEntryBounds, KvRange, KvRangePage};
pub use store::KvStore;
pub use value_allowance::KvValueAllowance;
pub use value_prefix::KvValuePrefix;

mod write_preparation;
