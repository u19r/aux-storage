mod storage_api_manager;
mod storage_manager_impl_append_table_stream_record;
mod storage_manager_impl_change_index;
mod storage_manager_impl_clear_all_tables;
mod storage_manager_impl_describe_system_stream;
mod storage_manager_impl_get_stream_records;
mod storage_manager_impl_run_background_job;
#[cfg(test)]
mod storage_manager_impl_run_background_job_tests;
mod storage_manager_impl_split_system_stream;
mod storage_manager_impl_system_stream_trim;

#[allow(unused_imports)]
pub use storage_api_manager::{StorageApiManager, StorageApiManagerImpl};

#[cfg(test)]
mod storage_manager_impl_batch_get_item_perf_tests;
#[cfg(test)]
mod storage_manager_impl_describe_table_tests;
#[cfg(test)]
mod storage_manager_impl_get_stream_records_tests;
#[cfg(test)]
mod storage_manager_impl_reopen_perf_tests;

mod storage_manager_impl_canonical_history;
