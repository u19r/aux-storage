pub mod dynamodb;
mod dynamodb_metrics;
#[cfg(any(test, feature = "internal-test-routes"))]
pub mod internal;
#[cfg(any(feature = "queue", feature = "pubsub"))]
mod messaging_admission;
pub mod pubsub;
pub mod queue;

#[cfg(test)]
pub mod query_tests;

#[cfg(test)]
pub mod scan_tests;

#[cfg(test)]
mod create_table_tests;

#[cfg(test)]
mod batch_get_item_tests;
#[cfg(test)]
mod batch_get_response_tests;

#[cfg(test)]
mod range_response_tests;
#[cfg(test)]
mod write_extraction_tests;
#[cfg(test)]
mod write_path_tests;

#[cfg(test)]
mod batch_get_item_perf_tests;

#[cfg(test)]
mod read_sequence_tests;

#[cfg(test)]
mod batch_write_item_tests;

#[cfg(test)]
mod put_item_tests;

#[cfg(test)]
mod get_item_tests;

#[cfg(test)]
mod delete_item_tests;

#[cfg(test)]
mod list_tables_tests;

#[cfg(test)]
mod transact_write_items_tests;

#[cfg(test)]
mod transact_get_items_tests;

#[cfg(test)]
mod update_item_tests;

#[cfg(test)]
mod update_table_tests;

#[cfg(test)]
pub mod routes_tests;

#[cfg(test)]
mod dynamodb_stage_tests;

#[cfg(test)]
mod internal_tests;

#[cfg(test)]
mod system_stream_scope_tests;
