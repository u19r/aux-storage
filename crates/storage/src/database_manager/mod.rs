mod cache_write_execution;
mod atomic_item_ops;
mod capped_entity_ops;
mod constants;
mod construction;
mod core;
mod expression_ops;
mod guarded_write_coordinator;
mod logical_backfill_ops;
mod operation_metrics;
mod query_ops;
mod read_ops;
mod read_sequence_executor;
#[cfg(test)]
mod read_sequence_executor_tests;
mod replication_ops;
mod routed_write_ops;
mod runtime_options;
mod sync_apply_ops;
mod sync_condition_ops;
mod sync_lifecycle_ops;
mod sync_replay_ops;
mod sync_resolver_ops;
mod sync_serialization;
mod sync_transaction_resolver_ops;
mod transact_get_ops;
mod transact_item_ops;
mod transaction_write_coordinator;
mod wire_item_ops;
mod write_bulk_ops;
mod write_ops;

pub use runtime_options::{DatabaseManagerRuntimeOptions, DatabaseManagerRuntimeOptionsBuilder};

#[cfg(test)]
pub(crate) use crate::database_manager::core::DatabaseManagerTestPauseHandle;
pub(crate) use crate::database_manager::{
    cache_write_execution::PreparedCacheWrite,
    constants::{
        CAPPED_ENTITY_COUNTER_CREATE_CONDITION, CAPPED_ENTITY_COUNTER_DELETE_CONDITION,
        CAPPED_ENTITY_COUNTER_UPDATE_EXPRESSION, CAPPED_ENTITY_COUNTER_VALUE_ATTR,
        ENTITY_ABSENT_CONDITION, ENTITY_EXISTS_CONDITION, ROUTED_DEFAULT_CONNECTION_ID,
    },
    core::{
        capped_entity_counter_expression_names, capped_entity_counter_expression_values,
        capped_entity_counter_key, is_conditional_failure,
        transaction_canceled_reason_is_conditional,
        update_item_return_values_rewritable_from_post_image,
    },
    expression_ops::{
        validate_transact_encode_item_expression_usage,
        validate_transact_write_item_expression_usage, validate_update_expression_usage,
    },
    operation_metrics::{record_storage_operation, record_storage_operation_for_target},
    routed_write_ops::{
        RoutedWriteTargetRole, WriteTargetSet, ensure_route_writes_not_paused,
        fan_out_route_write_payload,
    },
    transact_item_ops::{
        set_transact_encode_item_table_name, set_transact_item_table_name,
        transact_encode_item_table_name, transact_item_table_name,
    },
    wire_item_ops::{
        decode_wire_items_to_decoded, decode_wire_items_to_maps,
        normalize_wire_items_for_shared_table, refresh_existing_updated_at_on_put_payload,
        stamp_updated_at_on_put_payload,
    },
};
pub use crate::database_manager::{
    core::{
        CappedStorageError, CreateCappedEntityInput, DatabaseManager, DeleteCappedEntityInput,
        DeleteItemInput, PutItemEntityEncodeInput, PutItemInput, QueryIndexInput, QueryTableInput,
        ResolvedBatchGetPlan, ResolvedGetItem, ResolvedStorageOperation, ScanTableInput,
        UpdateItemInput,
    },
    read_sequence_executor::{
        InProcessReadSequence, InProcessReadSequenceLimits, InProcessReadSequenceStats,
    },
    replication_ops::ReplicationMutationApplyOutcome,
    wire_item_ops::PutItemPayload,
};

#[cfg(test)]
mod construction_tests;
#[cfg(test)]
mod read_ops_projection_perf_tests;
#[cfg(test)]
mod read_ops_tests;
#[cfg(test)]
mod replication_ops_tests;
#[cfg(test)]
mod routed_write_ops_tests;
#[cfg(test)]
mod sync_apply_manager_tests;
#[cfg(test)]
mod sync_resolver_ops_alloc_tests;
#[cfg(test)]
mod sync_resolver_ops_support_tests;
#[cfg(test)]
mod sync_resolver_ops_tests;
#[cfg(test)]
mod sync_single_node_public_tests;
#[cfg(test)]
mod sync_single_node_side_effect_tests;
#[cfg(test)]
mod sync_single_node_tests;
#[cfg(test)]
mod transact_get_ops_tests;
#[cfg(test)]
mod transact_item_ops_tests;
#[cfg(test)]
mod write_ops_tests;
