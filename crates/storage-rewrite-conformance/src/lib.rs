//! Provider-independent contract tests for the replacement backend stack.
//!
//! SQLite in memory is the default. Select one other provider with
//! `--no-default-features --features <provider>`, or set `STORAGE_TEST_BACKEND`
//! when several provider features are compiled. Postgres requires
//! `TEST_POSTGRES_DSN`; FoundationDB requires `FDB_CLUSTER_FILE`.

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod batch_get_item_conformance_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod batch_write_item_conformance_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod batch_write_item_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod batch_write_retention_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod cdc_deleted_table_trim_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod cdc_stream_isolation_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod cdc_stream_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod cdc_system_checkpoint_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod cdc_system_page_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod cdc_system_trim_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod cdc_unread_trim_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod cdc_write_noop_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod create_table_conformance_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod create_table_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod delete_item_conformance_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod delete_item_retention_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod delete_item_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod delete_table_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod describe_table_conformance_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod dynamo_operation_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod dynamodb_stream_metadata_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod dynamodb_streams_validation_tests;
#[cfg(all(test, any(feature = "sqlite", feature = "postgres", feature = "turso")))]
mod fault_injection_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod get_item_conformance_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod get_item_lifecycle_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod get_item_tests;
#[cfg(test)]
mod item_write_concurrency_tests;
#[cfg(all(test, any(feature = "rocksdb", feature = "foundationdb")))]
mod kv_fault_tests;
#[cfg(all(test, any(feature = "rocksdb", feature = "foundationdb")))]
mod kv_value_layout_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod list_tables_tests;
#[cfg(test)]
mod native_diagnostic_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod operation_trace_tests;
#[cfg(all(test, feature = "postgres"))]
mod postgres_user_stream_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod put_item_concurrency_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod put_item_conformance_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod put_item_indexer_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod put_item_retention_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod put_item_roundtrip_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod put_item_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod query_budget_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod query_conformance_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod query_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod queue_clock_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod read_sequence_consistency_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod read_sequence_dependency_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod read_sequence_mapping_tests;
#[cfg(test)]
mod read_sequence_observability_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod read_sequence_token_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod scan_budget_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod scan_conformance_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod scan_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod scheduled_job_store_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod stream_clock_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod table_recreation_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod test_support;
#[cfg(all(test, any(feature = "postgres", feature = "foundationdb")))]
mod test_support_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod transact_get_items_conformance_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod transact_get_items_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod transact_get_observability_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod transact_write_indexer_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod transact_write_items_conformance_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod transact_write_items_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod transact_write_retention_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod ttl_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod update_item_conformance_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod update_item_indexer_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod update_item_indexer_validation_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod update_item_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod update_table_conformance_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod update_table_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod user_stream_paging_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod request_validation_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod transact_get_validation_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod queue_contract_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )
))]
mod transport_contract_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod pubsub_contract_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )
))]
mod dynamo_item_contract_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )
))]
mod query_contract_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )
))]
mod cursor_contract_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )
))]
mod batch_transaction_contract_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )
))]
mod gsi_contract_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )
))]
mod expiration_authority_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )
))]
mod durable_contract_tests;
#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )
))]
mod growth_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )
))]
mod gsi_workflow_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )
))]
mod retention_discovery_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )
))]
mod retention_contract_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )
))]
mod retention_transaction_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )
))]
mod image_retention_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )
))]
mod gsi_lifecycle_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )
))]
mod image_publication_tests;

#[cfg(all(
    test,
    any(
        feature = "sqlite",
        feature = "turso",
        feature = "postgres",
        feature = "rocksdb",
        feature = "foundationdb"
    )
))]
mod cdc_system_split_tests;

#[cfg(all(test, feature = "rocksdb"))]
mod rocksdb_attempt_tests;
