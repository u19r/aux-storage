//! Native SQL execution contract and backend-neutral physical values.
//!
//! Domain operations own decisions. The SQL family owns statement rendering and
//! row layout. Implementors of [`SqlBackend`] only own native transaction,
//! binding, execution, and error mechanics.

mod backend;
pub mod catalog;
mod catalog_lock;
mod constants;
pub mod create_table;
mod data_schema;
#[cfg(test)]
mod data_schema_tests;
mod ddl;
mod delete_table;
mod expiration;
mod fault_injection;
#[cfg(all(
    test,
    feature = "fault-injection",
    any(feature = "sqlite", feature = "postgres", feature = "turso")
))]
mod fault_injection_tests;
pub mod gsi;
mod gsi_reclaim_sql_statements;
pub mod infrastructure;
mod item_lock;
mod item_observation;
mod item_read;
pub mod item_schema;
mod item_stream;
mod item_write;
mod job_store;
mod maintenance;
mod physical_name;
#[cfg(feature = "postgres")]
pub mod postgres;
mod pubsub;
mod pubsub_publication;
mod pubsub_schedule;
mod pubsub_subscription;
mod pubsub_topic;
mod queue;
mod queue_catalog;
mod queue_catalog_check;
mod queue_message;
mod queue_wake_publish;
mod range_read;
mod regional;
#[cfg(test)]
mod regional_tests;
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
mod stream_cursor;
pub mod stream_history;
mod stream_label_clock;
mod stream_label_clock_sql_statements;
mod stream_read;
mod stream_split;
mod stream_trim;
mod stream_write;
mod table_history_reclaim;
mod table_history_reclaim_sql_statements;
mod table_reclaim;
mod table_reclaim_sql_statements;
mod table_update;
mod transact_write;
mod transact_write_statements;
mod ttl_policy;
#[cfg(feature = "turso")]
pub mod turso;
mod value;

pub use backend::{
    AttemptMode, ReadProtection, SqlAttempt, SqlBackend, SqlEngine, SqlOpenReadBatchResult,
    SqlReadBatchFailure, SqlReadBatchResult, SqlWriteBatchFailure,
};
pub use ddl::SqlDdl;
pub use fault_injection::{
    SqlCommitBoundary, SqlFaultCall, SqlFaultOperation, SqlFaultProbe, SqlNativeEvent,
    SqlWaveStatement,
};
pub use relation_layout::SqlLayout;
pub use statement::{SqlCountedWrite, SqlStatement};
pub use store::SqlStore;
pub use value::{ColumnIndex, SqlQueryPage, SqlRow, SqlValue};

#[cfg(test)]
mod backend_tests;

#[cfg(test)]
mod item_write_tests;

#[cfg(test)]
mod item_read_tests;

#[cfg(test)]
mod statement_tests;

#[cfg(all(test, feature = "postgres"))]
mod regional_pair_end_tests;

#[cfg(test)]
mod test_support;

mod write_batch;
mod write_preparation;

#[cfg(all(test, feature = "sqlite", feature = "fault-injection"))]
mod expiration_identity_tests;

#[cfg(all(test, feature = "sqlite"))]
mod store_admission_tests;

#[cfg(test)]
mod item_lock_tests;
#[cfg(test)]
mod item_schema_tests;
#[cfg(all(test, feature = "sqlite"))]
mod pubsub_poison_tests;
#[cfg(test)]
mod pubsub_publication_tests;
#[cfg(all(test, any(feature = "sqlite", feature = "turso")))]
mod pubsub_schedule_tests;
#[cfg(all(test, any(feature = "sqlite", feature = "turso")))]
mod pubsub_subscription_tests;
#[cfg(test)]
mod queue_catalog_tests;

#[cfg(test)]
mod write_batch_tests;

#[cfg(all(test, feature = "sqlite"))]
mod store_tests;
