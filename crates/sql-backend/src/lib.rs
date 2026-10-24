//! Native SQL execution contract and backend-neutral physical values.
//!
//! Domain operations own decisions. The SQL family owns statement rendering and
//! row layout. Implementors of [`SqlBackend`] only own native transaction,
//! binding, execution, and error mechanics.

mod backend;
pub mod catalog;
mod constants;
pub mod create_table;
mod data_schema;
mod delete_table;
mod expiration;
#[cfg(feature = "fault-injection")]
mod fault_injection;
#[cfg(all(
    test,
    feature = "fault-injection",
    any(feature = "sqlite", feature = "postgres", feature = "turso")
))]
mod fault_injection_tests;
pub mod gsi;
mod gsi_reclaim;
mod gsi_reclaim_sql_statements;
pub mod infrastructure;
mod item_delete;
mod item_lock;
mod item_read;
pub mod item_schema;
mod item_stream;
mod item_tombstone;
mod item_write;
#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
mod job_store;
mod maintenance;
mod physical_name;
#[cfg(feature = "postgres")]
pub mod postgres;
mod pubsub;
mod pubsub_delivery;
mod pubsub_publication;
mod pubsub_subscription;
mod pubsub_topic;
mod queue;
mod queue_catalog;
mod queue_message;
mod range_read;
mod relation_layout;
pub mod retention;
mod row_view;
mod sql_template;
#[cfg(test)]
mod sql_template_tests;
#[cfg(feature = "sqlite")]
pub mod sqlite;
mod statement;
mod store;
mod stream;
mod stream_allocation;
#[cfg(test)]
mod stream_allocation_tests;
mod stream_catalog;
mod stream_checkpoint;
mod stream_cursor;
pub mod stream_history;
mod stream_read;
mod stream_split;
mod stream_trim;
mod stream_write;
mod table_reclaim;
mod table_update;
mod transact_write;
mod transact_write_statements;
mod ttl_policy;
#[cfg(feature = "turso")]
pub mod turso;
mod value;

pub use backend::{AttemptMode, ReadProtection, SqlAttempt, SqlBackend, SqlDataAccess, SqlEngine};
#[cfg(feature = "fault-injection")]
pub use fault_injection::{SqlCommitBoundary, SqlFaultCall, SqlFaultOperation, SqlFaultProbe};
pub use relation_layout::SqlLayout;
pub use statement::SqlStatement;
pub use store::SqlStore;
pub use value::{ColumnIndex, SqlQueryPage, SqlRow, SqlValue};

#[cfg(test)]
mod backend_tests;

#[cfg(test)]
mod item_write_tests;

#[cfg(test)]
mod item_read_tests;

#[cfg(test)]
mod test_support;

mod write_preparation;

#[cfg(test)]
mod item_lock_tests;
#[cfg(test)]
mod item_schema_tests;
