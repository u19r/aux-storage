//! Internal cache planning and model-checking primitives for aux-storage.
//!
//! This crate is not a supported downstream API. Public items exist for
//! workspace crate-to-crate integration and validation scenarios.
#![doc(hidden)]

pub mod batch_get;
pub mod batch_write;
pub mod cluster_model;
pub mod cluster_transition;
pub mod differential;
pub mod model;
pub mod plan;
pub mod query;
pub mod runtime_query_proof;
pub mod runtime_write_plan;
pub mod transaction;
pub mod transition;

pub mod cluster_metrics;
pub mod distributed_node;
pub mod raft_network;
pub mod raft_types;

pub use batch_get::{
    BatchGetCachePlan, BatchGetCachePlanOptions, PreparedBatchGetExecution,
    RuntimeBatchGetCacheOutcome, batch_get_keys_and_attributes_count_map, batch_request_has_items,
    finish_batch_get_request, merge_cached_batch_get_response, plan_batch_get_request,
    plan_batch_get_request_with_options,
};
pub use batch_write::{
    PhysicalToLogicalWriteTableMap, RoutedBatchWriteTarget,
    insert_routed_batch_write_encode_request, insert_routed_batch_write_request,
    merge_unprocessed_batch_write_items,
};
pub use cluster_model::{
    ClusterEpoch, ClusterRole, ClusterState, Message, MessageKind, NodeIndex, NodeState, ReadRoute,
    RingState, ShardIndex, ShardLocal, ShardRole,
};
pub use cluster_transition::ClusterTransition;
pub use differential::{DifferentialMismatch, ObservedRead, ReadRequest, compare_observed_read};
pub use model::{
    CacheState, Epoch, GsiOrderVersion, GsiSchemaVersion, ItemCacheState, LocalReplicaState,
    SchemaVersion, Slot,
};
pub use plan::{
    BatchGetDecision, BatchGetPlan, CacheReadOutcome, CacheRouteOutcome, QueryDecision, QueryPlan,
};
pub use query::{GsiQuerySpace, PartitionId, QueryDirection, QueryRequest, QueryTarget};
pub use runtime_query_proof::{
    RuntimeCoverageRange, RuntimeOwnedQueryBounds, RuntimePageCoverageContext, RuntimePageWitness,
    RuntimePageWitnessKey, RuntimeParsedQueryShape, RuntimePreparedQueryExecution,
    RuntimePreparedQueryProofPagePlan, RuntimePreparedQueryProofRead, RuntimePreparedQueryRead,
    RuntimePreparedQueryReadOutcome, RuntimePreparedQueryWindow, RuntimeQueryBounds,
    RuntimeQueryCoverageSemantics, RuntimeQueryProofFallbackReason,
    RuntimeQueryProofMaterializedPage, RuntimeQueryProofReadPlan, RuntimeSortCondition,
    blocked_runtime_query_proof_read, borrow_runtime_coverage_ranges,
    covering_current_schema_range_indexes_for_bounds, decode_query_start_key_sort_repr,
    derive_page_coverage_range, derive_runtime_page_coverage_range, hash_key_name,
    key_attribute_type_for_name, matching_order_key_positions_for_bounds,
    next_page_token_for_query_entry, parse_partition_key_condition, parse_runtime_sort_condition,
    plan_runtime_query_execution, prepare_runtime_query_proof_page_plan,
    prepare_runtime_query_read, prepare_runtime_query_shape, prepare_runtime_query_window,
    primary_key_from_schema, push_unique_runtime_coverage_range, query_sort_clause,
    query_space_key_schema, range_key_name, ranges_exhaust_request_for_bounds,
    runtime_page_witness, runtime_page_witness_key, runtime_query_coverage_semantics,
    runtime_query_proof_fallback_reason, runtime_query_proof_read_plan, scalar_order_repr_for_type,
    sort_key_order_repr_for_schema_value, stable_query_space_schema_fingerprint,
    witnessed_page_len,
};
pub use runtime_write_plan::{
    RuntimeBaseWrite, RuntimeIndexTransition, RuntimeIndexTransitionTarget,
    RuntimeIndexTransitionTargetKind, RuntimePendingIndexTransition,
    RuntimePendingIndexTransitionKind, RuntimePendingIndexTransitionTarget,
    RuntimePendingIndexTransitionTargetKind, RuntimePointReadMutation,
    RuntimePreparedIndexPrewrite, RuntimePreparedUpdateCacheWrite, RuntimeQueryProofMutation,
    RuntimeWriteEffects, build_delete_item_cache_effects, build_index_transition,
    build_pending_delete_index_transition, build_pending_put_index_transition,
    build_pending_update_index_transition, build_put_item_cache_effects,
    collect_base_writes_for_batch_write, collect_base_writes_for_batch_write_encode,
    collect_base_writes_for_transact_write_items,
    collect_base_writes_for_transact_write_items_encode,
    collect_pending_index_transition_update_lookups,
    collect_pending_query_proof_targets_for_transact_write_items,
    collect_pending_query_proof_targets_for_transact_write_items_encode,
    collect_point_read_mutations_for_batch_write,
    collect_point_read_mutations_for_batch_write_encode,
    collect_point_read_mutations_for_transact_write_items,
    collect_point_read_mutations_for_transact_write_items_encode,
    collect_query_proof_targets_for_batch_write,
    collect_query_proof_targets_for_batch_write_encode, collect_transact_write_encode_table_names,
    collect_transact_write_table_names, compose_delete_item_effects, compose_put_item_effects,
    compose_update_item_effects, compose_write_effects, extract_primary_key_from_item,
    finalize_pending_index_transitions, finalize_update_cache_effects, maybe_indexed_table_info,
    maybe_prepare_index_prewrite, point_read_delete, point_read_invalidate,
    point_read_put_from_item, point_read_put_from_wire_item, prepare_update_cache_write,
    table_requires_index_tracking,
};
pub use transaction::{TxnOutcome, TxnShardId, TxnShardState, TxnState, TxnTransition};
pub use transition::{Transition, TransitionRange};

#[cfg(test)]
mod batch_get_alloc_tests;
#[cfg(test)]
mod batch_get_tests;
#[cfg(test)]
mod batch_write_tests;
#[cfg(test)]
mod cluster_parity_tests;
#[cfg(test)]
mod cluster_prop_tests;
#[cfg(test)]
mod differential_tests;
#[cfg(test)]
mod distributed_cache_tests;
#[cfg(test)]
mod model_prop_tests;
#[cfg(test)]
mod model_tests;
#[cfg(test)]
mod proof_exhaustive_tests;
#[cfg(test)]
mod quint_parity_tests;
#[cfg(test)]
mod runtime_query_proof_tests;
#[cfg(test)]
mod runtime_write_plan_tests;
#[cfg(test)]
mod transaction_prop_tests;
#[cfg(test)]
mod transaction_tests;

#[cfg(test)]
mod cluster_metrics_tests;
