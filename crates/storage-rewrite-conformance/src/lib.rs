//! Provider-independent contract tests for the replacement backend stack.
//!
//! SQLite in memory is the default. Select one other provider with
//! `--no-default-features --features <provider>`, or set `STORAGE_TEST_BACKEND`
//! when several provider features are compiled. Postgres requires
//! `TEST_POSTGRES_DSN`; FoundationDB requires `TEST_FDB_CLUSTER_FILE`.

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
mod batch_native_capacity_tests;
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
mod cdc_system_snapshot_tests;
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
mod commit_mark_tests;
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
mod dynamo_wave_census_tests;
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
mod dynamodb_stream_history_tests;
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
mod dynamodb_stream_reenable_tests;
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
mod native_census_test_support;
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
mod pipelined_authority_tests;
#[cfg(all(test, feature = "postgres"))]
mod postgres_catalog_lock_tests;
#[cfg(all(test, feature = "postgres"))]
mod postgres_gsi_build_tests;
#[cfg(all(test, feature = "postgres"))]
mod postgres_history_publication_tests;
#[cfg(all(test, feature = "postgres"))]
mod postgres_stream_order_tests;
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
mod queue_probe_tests;
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
mod queue_wake_tests;
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
#[cfg(all(test, any(feature = "foundationdb", feature = "turso")))]
mod receipt_races_tests;
#[cfg(all(test, feature = "sqlite"))]
mod regional_off_tests;
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
mod regional_pairing_tests;
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
mod regional_receive_tests;
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
mod regional_send_tests;
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
mod regional_unpair_tests;
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
mod regional_write_tests;
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
mod table_history_policy_tests;
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
mod table_history_retention_fault_tests;
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
mod table_history_retention_tests;
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
#[cfg(all(test, any(feature = "sqlite", feature = "turso", feature = "postgres")))]
mod ttl_census_tests;
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
mod user_stream_address_census_tests;

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
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod queue_claim_tests;

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
mod queue_state_split_tests;

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
mod queue_cost_census_tests;

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
mod queue_redrive_tests;

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
mod queue_api_compat_tests;

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
        feature = "rocksdb",
        feature = "postgres",
        feature = "foundationdb"
    )
))]
mod pubsub_stale_subscription_tests;

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
mod pubsub_cost_tests;

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
mod pubsub_claim_contract_tests;

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
mod pubsub_delivery_contract_tests;

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
mod pubsub_runner_contract_tests;

#[cfg(all(test, any(feature = "sqlite", feature = "turso", feature = "postgres")))]
mod pubsub_delivery_safety_tests;

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
mod pubsub_local_batch_tests;

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
mod pubsub_schedule_contract_tests;

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

#[cfg(test)]
mod consumed_capacity_zero_tests;
#[cfg(test)]
mod gsi_large_key_tests;
#[cfg(test)]
mod gsi_partial_index_tests;
#[cfg(test)]
mod materialized_value_tests;

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
mod table_stream_eligibility_tests;

#[cfg(all(test, feature = "foundationdb"))]
mod fdb_catalog_conflict_tests;

#[cfg(all(test, feature = "foundationdb"))]
mod table_stream_process_tests;

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
mod table_stream_fault_tests;

#[cfg(all(test, feature = "sqlite"))]
mod scheduled_job_process_tests;

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
mod item_write_key_tests;

#[cfg(all(test, feature = "postgres"))]
mod sql_item_write_concurrency_tests;

#[cfg(all(test, feature = "postgres"))]
mod postgres_stream_metrics_tests;

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
mod read_retry_tests;

#[cfg(all(test, feature = "postgres"))]
mod public_get_schema_tests;

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
mod aws_behavioral_item_a_tests;
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
mod aws_behavioral_item_b_tests;
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
mod aws_behavioral_pubsub_tests;
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
mod aws_behavioral_queue_api_tests;
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
mod aws_behavioral_queue_flow_tests;
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
mod aws_behavioral_queue_misc_tests;
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
mod aws_behavioral_read_tests;
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
mod aws_behavioral_table_tests;
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
mod aws_behavioral_transact_tests;
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
mod cached_write_tests;
