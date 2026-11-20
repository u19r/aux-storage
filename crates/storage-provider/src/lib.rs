//! Stable storage provider traits, configuration, and shared provider types.
//!
//! Downstream libraries should depend on this crate for embedded storage
//! abstractions instead of backend implementation crates.

#[cfg(feature = "test-support")]
pub mod baseline_test_support;
mod config;
#[cfg(test)]
mod config_tests;
mod constants;
#[cfg(test)]
mod constants_tests;
mod dynamo_baseline;
mod dynamo_batch_get;
mod dynamo_batch_write;
mod dynamo_bound_write;
mod dynamo_catalog;
mod dynamo_create_table;
mod dynamo_pipelined_authority;
mod dynamo_table_bindings;
mod regional_catalog;
mod regional_operations;
mod regional_pairing;
mod regional_progress;
mod regional_receive;
#[cfg(test)]
mod regional_receive_tests;
mod regional_send;
#[cfg(test)]
mod regional_send_tests;
mod regional_unpair;
mod table_append;
pub use regional_operations::RegionalOperations;
pub use regional_send::{RegionalSendOperations, SendLimits, SendTurn};
pub use regional_unpair::RegionalUnpairOperations;
mod regional_write;
#[cfg(test)]
mod regional_write_tests;
pub use regional_catalog::{RegionalBindingOperations, RegionalCatalogAttempt};
pub use regional_pairing::{
    RegionalPairingAttempt, RegionalPairingOperations, ScanTurn, SweepTurn,
};
pub use regional_progress::{
    LineageCursor, MarkerPage, ProgressDirection, RegionalMarkerAttempt, RegionalProgressAttempt,
};
pub use regional_receive::{RegionalReceiveAttempt, RegionalReceiveOperations};
pub use regional_write::{DeleteCause, RegionalDelete, RegionalUpsert};
#[cfg(feature = "test-support")]
pub mod table_reclaim_test_support;
pub use table_append::{StorageStreamOperations, TableAppendOperations};

mod dynamo_delete_item;
mod dynamo_delete_table;
mod dynamo_describe_table;
mod dynamo_get_item;
mod dynamo_list_streams;
mod dynamo_list_tables;
mod dynamo_operation_port;
#[cfg(test)]
mod dynamo_operation_port_tests;
mod dynamo_operations;
mod dynamodb_streams;
pub(crate) use dynamodb_streams::{
    DescribeDynamoStream, GetDynamoShardIterator, GetDynamoStreamRecords,
};
mod dynamo_put_item;
pub use dynamo_put_item::{
    ItemWritePrerequisites, WriteObservationBatch, WriteObservationBatchResult,
    WriteObservationFailure, read_write_observations_default,
};
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
#[cfg(test)]
mod dynamo_update_ttl_tests;
mod dynamo_write_execution;
#[cfg(test)]
mod dynamo_write_image_tests;
mod table_history_page;
mod table_history_read;
pub use dynamo_baseline::{prepare_baseline_delete, prepare_baseline_upsert};
mod dynamo_write_preparation;
mod dynamo_write_session;
#[cfg(test)]
mod expiration_candidate_tests;
#[cfg(test)]
mod expiration_transaction_contract_tests;
mod gsi_readiness;
#[cfg(test)]
mod gsi_readiness_tests;
mod gsi_reclaim;
mod gsi_reclaim_discovery;
mod gsi_reclaim_record;
#[doc(hidden)]
pub mod job_store;
mod job_terminal_projection;
#[cfg(test)]
mod job_terminal_projection_tests;
mod job_write_authority;
#[doc(hidden)]
pub mod maintenance;
mod name_reservation;
mod point_read;
mod primary_state;
mod primary_state_scan;
mod projection_scan;
#[cfg(test)]
mod quint_partition_tests;
mod range_read;
mod raw_ordered_job_store;
#[cfg(any(test, feature = "test-support"))]
pub mod retention_test_support;
#[cfg(test)]
mod stream_duration_planner_tests;
#[cfg(test)]
mod stream_duration_trim_tests;
#[cfg(test)]
mod stream_duration_worker_tests;
mod stream_label_clock;
#[cfg(test)]
mod stream_retention_tests;
mod table_cache;
mod table_update_transaction;
#[cfg(test)]
mod table_update_transaction_tests;
mod transact_get;
pub use gsi_reclaim::{NativeGsiRetirement, register_index_retirement};
mod table_reclaim;
mod transact_idempotency;
#[cfg(test)]
mod transact_idempotency_tests;
mod transact_request_identity;
mod update_logic;

pub use constants::{
    CATALOG_READ_BYTES, DYNAMO_WRITE_RETRY, MAX_RANGE_READ_EVALUATED_BYTES,
    SYSTEM_STREAM_SNAPSHOT_BYTES, SYSTEM_STREAM_SNAPSHOT_ROWS, TRANSACT_RECEIPT_PAGE_BYTES,
};
pub(crate) use dynamo_batch_get::batch_get_item_in_attempt;
pub(crate) use dynamo_batch_write::DynamoBatchWriteShape;
pub use dynamo_bound_write::BoundItemWrite;
pub(crate) use dynamo_catalog::resolve_tables_batch_in_attempt;
pub use dynamo_catalog::{
    CachePublication, CatalogAttempt, CatalogStamp, TableMetadataCache,
    prepare_writer_routes_in_attempt, resolve_table_in_attempt,
};
pub(crate) use dynamo_create_table::create_table_in_attempt;
pub use dynamo_create_table::{CreateTableAttempt, CreateTableLayout};
pub use dynamo_delete_item::AcceptedDeleteItem;
pub(crate) use dynamo_delete_item::{BoundDeleteItem, DeleteItemShape};
pub use dynamo_delete_table::DeleteTableAttempt;
pub(crate) use dynamo_delete_table::delete_table_in_attempt;
pub(crate) use dynamo_describe_table::describe_table_in_attempt;
pub(crate) use dynamo_get_item::get_item_in_attempt;
pub use dynamo_get_item::{
    ItemReadAttempt, PrimaryReadBatch, ReadObservationResult, TableReadCandidate,
    finish_primary_read_default,
};
pub(crate) use dynamo_list_streams::list_streams_in_attempt;
pub use dynamo_list_streams::{CatalogStreamPage, ListingAttempt};
pub use dynamo_list_tables::CatalogNamePage;
pub(crate) use dynamo_list_tables::list_tables_in_attempt;
pub use dynamo_operation_port::DynamoPort;
pub use dynamo_operations::DynamoOperations;
pub use dynamo_put_item::{
    AcceptedUpsert, BoundPutItem, ItemWriteAttempt, ItemWriteObservation, ItemWriteObservationRead,
    PutItemShape, combine_write_observations, lifecycle_tables_for_writes,
};
pub(crate) use dynamo_range_read::DynamoRangeReadShape;
pub use dynamo_range_read::read_range_observation_default;
pub(crate) use dynamo_read_sequence::read_sequence_in_attempt;
pub use dynamo_read_sequence::read_sequence_with_reader;
pub use dynamo_read_sequence_step::ReadSequenceStepReader;
pub use dynamo_stream_write::DynamoStreamWriteAttempt;
pub use dynamo_table_bindings::RequestTableBindings;
pub(crate) use dynamo_transact_write::DynamoTransactWriteShape;
pub use dynamo_transact_write::{DynamoTransactWriteAttempt, ReceiptSlot};
pub use dynamo_ttl::TtlPolicyAttempt;
pub(crate) use dynamo_ttl::describe_time_to_live_in_attempt;
pub(crate) use dynamo_update_item::BoundUpdateItem;
pub use dynamo_update_item::UpdateItemShape;
pub use dynamo_update_ttl::TtlCheckpointChange;
pub(crate) use dynamo_update_ttl::update_time_to_live_in_attempt;
pub(crate) use dynamo_write_execution::prepare_accepted_delete;
pub use dynamo_write_execution::prepare_accepted_upsert;
pub use job_terminal_projection::JobTerminalProjection;
pub use maintenance::StreamRetentionPort;
pub(crate) use name_reservation::name_is_locked;
pub use name_reservation::name_locked_until;
pub use primary_state::{PrimaryStateRow, UpsertImage};
pub use stream_label_clock::StreamLabelClockAttempt;
pub(crate) use stream_label_clock::issue_stream_enable_stamp;
mod live_item_metadata;
pub use live_item_metadata::LiveItemMetadata;
pub use primary_state_scan::{PrimaryStateScan, PrimaryStateScanAttempt};
pub use projection_scan::{
    GsiProjectionRow, GsiProjectionScan, NativeProjectionAddress, ProjectionScanAttempt,
    TtlIndexRead, TtlProjectionPage, TtlProjectionScan, TtlProjectionScanAttempt,
};
#[doc(hidden)]
pub use raw_ordered_job_store::{RawOrderedJobStore, RawOrderedJobStoreError};
pub use storage_common::{
    TtlConfigRecord, TtlSweepPageCommand, TtlSweepPageInput, TtlSweepPageResult,
};
pub use storage_types::{AttributeValue, SemanticDecision};
pub(crate) use table_cache::table_info_cache_config;
pub use table_reclaim::{
    ReclaimAttempt, TableReclaimHandler, TableReclaimPhase, TableReclaimStep, TableReclaimTarget,
    reclaim_table_page,
};

pub(crate) use crate::maintenance::plan_table_stream_duration;
pub use crate::{
    config::*,
    gsi_readiness::{
        GsiIndexReadiness, GsiReadinessRecord, GsiReadinessSnapshot,
        gsi_build_targets_from_catalog, gsi_readiness_from_catalog,
    },
    maintenance::{
        MaintenanceOperations, StreamDurationTrimConfig, StreamDurationTrimWorker,
        StreamRetentionFinishRecord, StreamTrimDueMarker, StreamTrimDueRecord, StreamTrimPageLimit,
        StreamTrimScope, StreamTrimState, StreamTrimStateRecord, StreamTrimStateUpdate,
        StreamTrimStateWrite, SystemStreamTrimExecutor, TableHistoryTrimFrontier,
        TableHistoryTrimLineage, table_retention_scope_id,
    },
    point_read::{
        BatchGetKeyIdentity, BatchGetPlan, BatchGetTablePlan, BatchGetTableResult, PointReadTable,
    },
    range_read::{
        BoundRangeReadPage, BoundRangeReadPlan, RangeAdmission, RangeReadBudget, RangeReadItem,
        RangeReadPage, RangeReadPlan, RangeReadProgress, RangeReadTarget, RangeReadiness,
    },
    table_update_transaction::{
        IndexRetirement, TableUpdateChange, TableUpdateRetention, TableUpdateTime,
        TableUpdateTransaction, deleted_index_retirement,
    },
    transact_get::{PreparedTransactGet, TransactGetPlan},
    transact_idempotency::*,
    update_logic::{
        BoundUpdateOperation, UpdateOperation, apply_bound_update_operations,
        apply_update_operations, before_update_item, before_update_item_optional,
        parse_update_expression, return_values_need_old_item, return_values_need_updated_fields,
        update_item_response_fields,
    },
};

#[cfg(test)]
mod dynamo_range_read_tests;

#[cfg(test)]
mod dynamo_catalog_tests;

#[cfg(test)]
mod dynamo_update_item_tests;
