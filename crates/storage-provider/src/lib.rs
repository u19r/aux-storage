//! Stable storage provider traits, configuration, and shared provider types.
//!
//! Downstream libraries should depend on this crate for embedded storage
//! abstractions instead of backend implementation crates.

mod change_index;
mod config;
#[cfg(test)]
mod config_tests;
mod constants;
#[cfg(test)]
mod constants_tests;
mod dynamo_attempt;
mod dynamo_baseline;
mod dynamo_batch_get;
mod dynamo_batch_write;
mod dynamo_bound_write;
mod dynamo_catalog;
mod dynamo_create_table;
mod dynamo_delete_item;
mod dynamo_delete_table;
mod dynamo_describe_table;
mod dynamo_get_item;
mod dynamo_list_streams;
mod dynamo_list_tables;
mod dynamo_operation_port;
mod dynamo_operations;
mod dynamo_put_item;
mod dynamo_range_read;
mod dynamo_read_execution;
mod dynamo_read_sequence;
mod dynamo_read_sequence_step;
mod dynamo_stream_write;
mod dynamo_table_stream;
mod dynamo_transact_get;
mod dynamo_transact_write;
mod dynamo_transact_write_execution;
mod dynamo_ttl;
mod dynamo_update_item;
mod dynamo_update_ttl;
mod dynamo_write_execution;
pub mod native_admission;
pub mod queue_cleanup_admission;
pub use dynamo_baseline::{prepare_baseline_delete, prepare_baseline_upsert};
mod dynamo_write_preparation;
#[cfg(test)]
mod expiration_candidate_tests;
#[cfg(test)]
mod expiration_transaction_contract_tests;
mod gsi_projection_owner;
#[cfg(test)]
mod gsi_readiness_tests;
mod item_tombstone_scan;
#[doc(hidden)]
pub mod job_store;
mod job_terminal_projection;
#[cfg(test)]
mod job_terminal_projection_tests;
mod job_write_authority;
#[doc(hidden)]
pub mod maintenance;
mod name_reservation;
mod primary_state;
mod primary_state_scan;
mod projection_scan;
pub use gsi_projection_owner::GsiProjectionOwner;
mod gsi_reclaim;
mod gsi_reclaim_discovery;
mod gsi_reclaim_record;
mod provider;
#[cfg(test)]
mod quint_custom_stream_duration_tests;
#[cfg(test)]
mod quint_partition_tests;
mod raw_ordered_job_store;
mod read_sequence_context;
#[cfg(test)]
mod read_sequence_context_tests;
mod read_sequence_metrics;
#[cfg(test)]
mod read_sequence_metrics_tests;
#[cfg(feature = "test-support")]
pub mod retention_test_support;
#[cfg(test)]
mod stream_duration_planner_tests;
#[cfg(test)]
mod stream_duration_trim_tests;
#[cfg(test)]
mod stream_duration_worker_tests;
#[cfg(test)]
mod stream_retention_hold_contract_tests;
#[cfg(test)]
mod stream_retention_tests;
mod table_cache;
pub use gsi_reclaim::{GsiReclaimAttempt, register_index_retirement};
mod table_reclaim;
#[cfg(feature = "test-support")]
pub mod test_support_tests;
mod transact_idempotency;
#[cfg(test)]
mod transact_idempotency_tests;
mod transact_request_identity;
mod ttl_sweep_repository;
mod update_logic;
mod write_context;
#[cfg(test)]
mod write_context_tests;

pub use constants::{
    CATALOG_READ_BYTES, DYNAMO_WRITE_RETRY, MAX_RANGE_READ_EVALUATED_BYTES,
    SYSTEM_STREAM_SNAPSHOT_BYTES, SYSTEM_STREAM_SNAPSHOT_ROWS, TRANSACT_RECEIPT_READ_BYTES,
};
pub use dynamo_attempt::DynamoAttempt;
pub use dynamo_batch_get::{batch_get_item_from_tables, batch_get_item_in_attempt};
pub use dynamo_batch_write::{
    AcceptedBatchWrite, AcceptedBatchWriteMember, BoundBatchWrite, DynamoBatchWriteShape,
    batch_write_table_lookup_error,
};
pub use dynamo_bound_write::{
    AcceptedMutationRef, BoundItemWrite, DynamoItemWriteShape, ItemWriteCause, WriteTableBinding,
};
pub use dynamo_catalog::{
    CachePublication, CatalogAttempt, CatalogStamp, TableMetadataCache, TtlPolicyLookup,
    resolve_table_in_attempt, resolve_tables_batch_in_attempt,
};
pub use dynamo_create_table::{CreateTableAttempt, CreateTableLayout, create_table_in_attempt};
pub use dynamo_delete_item::{
    AcceptedDeleteItem, BoundDeleteItem, DeleteItemAttempt, DeleteItemShape,
};
pub use dynamo_delete_table::{DeleteTableAttempt, delete_table_in_attempt};
pub use dynamo_describe_table::{describe_table_from_table, describe_table_in_attempt};
pub use dynamo_get_item::{GetItemAttempt, get_item_from_table, get_item_in_attempt};
pub use dynamo_list_streams::{CatalogStreamPage, ListStreamsAttempt, list_streams_in_attempt};
pub use dynamo_list_tables::{
    CatalogNamePage, ListTablesAttempt, is_public_table_name, list_tables_in_attempt,
};
pub use dynamo_operation_port::DynamoPort;
pub use dynamo_operations::DynamoOperations;
pub use dynamo_put_item::{
    AcceptedUpsert, BoundPutItem, GsiWriteSource, ItemWriteObservation,
    ItemWriteObservationAttempt, ItemWriteObservationRead, PutItemShape, PutTtlEffects,
    UpsertAttempt, combine_write_observations, lifecycle_tables_for_writes,
};
pub use dynamo_range_read::{
    DynamoQueryPage, DynamoRangeReadAttempt, DynamoRangeReadShape, SourceQueryRequest,
    ValidatedDynamoRangeRequest, range_read_output_on_table,
};
pub use dynamo_read_sequence::{
    BoundReadSequenceReader, read_sequence_in_attempt, read_sequence_with_reader,
};
pub use dynamo_read_sequence_step::{DynamoReadSequenceStep, ReadSequenceStepReader};
pub use dynamo_stream_write::DynamoStreamWriteAttempt;
pub use dynamo_table_stream::{
    DynamoTableStreamAttempt, get_table_stream_records_from_table,
    latest_table_stream_pointer_from_table,
};
pub use dynamo_transact_get::{transact_get_items_from_tables, transact_get_items_in_attempt};
pub use dynamo_transact_write::{
    AcceptedTransactConditionCheck, AcceptedTransactWrite, AcceptedTransactWriteMember,
    BoundTransactWrite, DynamoTransactWriteAttempt, DynamoTransactWriteShape,
};
pub use dynamo_ttl::{
    DescribeTimeToLiveAttempt, describe_time_to_live_from_table, describe_time_to_live_in_attempt,
};
pub use dynamo_update_item::{BoundUpdateItem, UpdateItemShape, UpdateReturnShape};
pub use dynamo_update_ttl::{UpdateTimeToLiveAttempt, update_time_to_live_in_attempt};
pub use dynamo_write_execution::{prepare_accepted_delete, prepare_accepted_upsert};
pub use dynamo_write_preparation::PreparedWriteAttempt;
pub use item_tombstone_scan::{
    ItemTombstone, ItemTombstonePage, ItemTombstoneScan, ItemTombstoneScanAttempt,
};
pub use job_terminal_projection::JobTerminalProjection;
pub use name_reservation::{TableNameReservation, name_is_locked, name_locked_until};
pub use primary_state::{ItemPrimaryEffects, PrimaryStateRow, UpsertImage};
pub use primary_state_scan::{PrimaryStateScan, PrimaryStateScanAttempt};
pub use projection_scan::{
    GsiProjectionMetadataAttempt, GsiProjectionRow, GsiProjectionScan, GsiProjectionScanAttempt,
    GsiTombstonePage, GsiTombstoneRow, GsiTombstoneScan, NativeProjectionAddress,
    ProjectionMetadataCursor, TtlIndexRead, TtlProjectionPage, TtlProjectionScan,
    TtlProjectionScanAttempt,
};
#[doc(hidden)]
pub use raw_ordered_job_store::{RawOrderedJobStore, RawOrderedJobStoreError};
pub use read_sequence_context::{
    ReadSequenceBatchGetResponse, ReadSequenceReadContext, ReadSequenceReadLimits,
};
pub use read_sequence_metrics::{
    ReadSequenceProviderCall, read_sequence_provider_metrics_snapshot,
    record_read_sequence_database_await, record_read_sequence_provider_call,
};
pub use storage_common::{
    TtlConfigRecord, TtlSweepPageCommand, TtlSweepPageInput, TtlSweepPageResult,
};
pub use storage_types::{AttributeValue, SemanticDecision};
pub use table_cache::{
    TABLE_INFO_CACHE_CAPACITY, TABLE_INFO_CACHE_TTL_SECONDS, table_info_cache_config,
};
pub use table_reclaim::{
    TableReclaimAttempt, TableReclaimHandler, TableReclaimPhase, TableReclaimRepository,
    TableReclaimStep, TableReclaimTarget, reclaim_table_page,
};
pub use ttl_sweep_repository::TtlSweepRepository;

pub use crate::{
    change_index::*,
    config::*,
    maintenance::{
        DetachedItemStreamObservation, DetachedItemStreamPolicy, DetachedItemStreamRange,
        DetachedItemStreamRanges, DetachedItemStreamReclaim, ItemHistoryRetention,
        ItemStreamDurationInput, PhysicalStreamPointer, PhysicalStreamPointerFamily,
        PhysicalStreamTrimSelection, StreamDurationTrimConfig, StreamDurationTrimPageRequest,
        StreamDurationTrimPageResult, StreamDurationTrimWorker, StreamRetentionFinishRecord,
        StreamRetentionHold, StreamRetentionRepository, StreamTrimBoundary,
        StreamTrimDeletionBound, StreamTrimDueMarker, StreamTrimDueRecord, StreamTrimMarkerOutcome,
        StreamTrimPageLimit, StreamTrimRow, StreamTrimScope, StreamTrimScopeBoundaries,
        StreamTrimScopeKind, StreamTrimState, StreamTrimStateRecord, StreamTrimStateUpdate,
        StreamTrimStateWrite, SystemStreamTrimExecutor, SystemStreamTrimRepository,
        item_retention_scope_id, merge_table_stream_duration_request, plan_item_stream_duration,
        plan_table_stream_duration, stream_retention_kind, table_retention_scope_id,
    },
    provider::*,
    transact_idempotency::*,
    update_logic::{
        BoundUpdateOperation, UpdateOperation, apply_bound_update_operations,
        apply_update_operations, before_update_item, before_update_item_optional,
        parse_update_expression, return_values_need_old_item, return_values_need_updated_fields,
        update_item_response, update_item_response_fields,
    },
    write_context::*,
};

#[cfg(test)]
mod dynamo_range_read_tests;

#[cfg(test)]
mod dynamo_catalog_tests;
