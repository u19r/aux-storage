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
#[cfg(feature = "fault-injection")]
mod fault_injection;
#[cfg(feature = "foundationdb")]
pub mod foundationdb;
pub mod gsi;
mod gsi_reclaim;
pub mod infrastructure;
mod item_codec;
mod item_delete;
mod item_read;
mod item_stream;
mod item_tombstone;
mod item_write;
mod job_store;
mod maintenance;
mod mutation;
mod mutation_preparation;
mod native_layout;
mod pubsub;
mod pubsub_delivery;
pub mod pubsub_publication;
mod pubsub_subscription;
mod pubsub_topic;
mod queue;
mod queue_catalog;
mod queue_message;
mod range;
mod range_read;
mod record_footprint;
pub mod retention;
#[cfg(feature = "rocksdb")]
pub mod rocksdb;
mod store;
#[cfg(all(test, feature = "rocksdb"))]
mod store_tests;
mod stream;
mod stream_catalog;
mod stream_checkpoint;
mod stream_cursor;
pub mod stream_history;
mod stream_partition;
mod stream_read;
mod stream_split;
mod stream_trim;
pub mod stream_write;
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
pub use constants::{PUBSUB_DELIVERY_PREFIX, PUBSUB_PUBLISH_INTENT_PREFIX};
#[cfg(feature = "fault-injection")]
pub use fault_injection::{KvCommitBoundary, KvFaultCall, KvFaultOperation, KvFaultProbe};
pub use mutation::{KvMutation, KvVersionstampedMutation};
pub use native_layout::{KvNativeLayout, NativeConflictRangeLayout};
pub use range::{KvEntry, KvEntryBounds, KvRange, KvRangePage};
pub use store::KvStore;
pub use value_allowance::KvValueAllowance;
pub use value_prefix::KvValuePrefix;

mod write_preparation;
