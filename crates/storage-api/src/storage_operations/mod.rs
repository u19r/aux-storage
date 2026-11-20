#[cfg(any(test, feature = "internal-test-routes"))]
mod append_table_stream_record;
#[cfg(any(test, feature = "internal-test-routes"))]
mod clear_all_tables;
mod describe_system_stream;
mod dynamo_admitted;
mod get_stream_records;
#[cfg(any(test, feature = "internal-test-routes"))]
mod run_background_job;
#[cfg(any(test, feature = "internal-test-routes"))]
mod split_system_stream;
#[cfg(any(test, feature = "internal-test-routes"))]
mod system_stream_trim;

#[cfg(test)]
mod batch_get_item_perf_tests;
#[cfg(test)]
mod describe_table_tests;
#[cfg(test)]
mod get_stream_records_tests;
#[cfg(test)]
mod run_background_job_tests;

#[cfg(any(test, feature = "internal-test-routes"))]
pub(crate) use append_table_stream_record::{
    append_table_stream_record, reconcile_table_stream_record,
};
#[cfg(any(test, feature = "internal-test-routes"))]
pub(crate) use clear_all_tables::clear_all_tables;
pub use describe_system_stream::describe_system_stream;
pub use dynamo_admitted::dynamo_admitted;
pub use get_stream_records::get_stream_records;
#[cfg(any(test, feature = "internal-test-routes"))]
pub(crate) use run_background_job::run_background_job;
#[cfg(any(test, feature = "internal-test-routes"))]
pub(crate) use split_system_stream::split_system_stream;
#[cfg(any(test, feature = "internal-test-routes"))]
pub(crate) use system_stream_trim::run_system_stream_trim_page;
