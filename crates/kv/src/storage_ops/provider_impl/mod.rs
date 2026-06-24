use std::{collections::HashMap, sync::Arc, time::Instant};

use async_trait::async_trait;
use bg_jobs::{BackgroundJob, BackgroundJobName, JobConfig};
use storage_backfill::{BackfillConfig, BackfillCoordinator};
#[cfg(test)]
use storage_common::provider_perf;
use storage_common::{
    GSI_BACKFILL_JOB, JobIntervalMillis, MAX_GENERIC_LIMIT, RegistersJobs, STREAM_TRIM_JOB,
    register_gsi_jobs,
};
use storage_condition::parse_condition_expression;
use storage_provider::{
    CHANGE_INDEX_MARKER_RETENTION_MS, ChangeIndexMarker, ListChangeIndexMarkersRequest,
    StorageProvider, StorageProviderReadContext, StreamTrimDueMarker, StreamTrimState,
    plan_table_stream_duration, return_values_need_old_item, update_item_response,
};
use storage_types::{
    AllOld, AttributeValue, BatchGetItemRequest, BatchGetWireItemResponse,
    BatchWriteItemEncodeRequest, BatchWriteItemRequest, BatchWriteItemResponse, CreateTableRequest,
    DeleteRequest, DescribeTimeToLiveResponse, DurablePointReadProof, DurablePointReadRequest,
    EncodeWriteRequest, IndexName, ItemKey, ItemVersionedWireItem, KeyAttributes, KeySchemaElement,
    KeyType, KeysAndAttributes, Projection, ProjectionType, PutItemResponse, PutRequest,
    QueryTableRequest, ReadSequenceConsistency, ReplicationMutation, ScanTableRequest, StorageEnum,
    StorageError, StorageResult, StorageValidationKind, StoredTableInfo, StreamName,
    StreamRetentionDuration, TTL_PARTITION_ATTRIBUTE, TableName, TableStatus,
    TimeToLiveDescription, TimeToLiveStatus, TimestampMillis, TransactWriteItem,
    TransactWriteItemsEncodeRequest, TransactWriteItemsRequest, TransactWriteItemsResponse,
    UpdateItemRequest, UpdateItemResponse, UpdateTimeToLiveRequest, UpdateTimeToLiveResponse,
    WireItem, WriteRequest, context::WrappedError as _, normalize_attribute_map_numbers_for_write,
};
use tracing::{Span, instrument, warn};

use crate::{
    sorted_kv_store::SortedKvReadContext,
    storage_ops::constants::{IDEMPOTENCY_TOKEN_TTL_MS, REPLICATION_APPLY_PARALLELISM_HINT},
    storage_provider::{GsiBackfillJob, GsiUpdateJob},
};

mod batch_write;
mod batch_write_encode;
mod batch_write_support;
mod conditional_errors;
mod direct_mutations;
mod gsi_helpers;
mod item_encoding;
mod jobs;
mod key_helpers;
mod metadata_cache;
mod metadata_store;
mod metrics;
mod provider_trait;
mod put_item;
mod read;
mod replication;
mod runtime_helpers;
mod table_management;
mod table_streams;
mod table_updates;
#[cfg(test)]
mod test_helpers;
mod transaction_bindings;
mod transactions;
mod transactions_encode;
mod ttl_indexing;
mod ttl_management;
mod update_delete;

pub(crate) use conditional_errors::normalize_conditional_transaction_error;
pub(super) use direct_mutations::{
    kv_mutation_to_direct_with_literal_templates, to_direct_write_operation,
};
pub(crate) use item_encoding::{
    decode_wire_item_from_storage_bytes, encode_requests_to_write_requests,
    encode_wire_item_storage_bytes, normalized_attribute_map_for_write,
    normalized_wire_item_for_write,
};
use key_helpers::key_attributes_for_item;
pub(super) use metadata_store::kv_table_scope_id;
pub(crate) use metrics::{
    compute_items_bytes, record_provider_stage, record_query_result, record_read, record_write,
};
use runtime_helpers::{
    apply_gsi_write_pressure, change_index_marker_created_at_ms, delete_range, next_stream_item_id,
    parse_change_index_key,
};
pub(crate) use runtime_helpers::{now_ms_u64, should_log_job};
pub(crate) use transaction_bindings::{
    TransactConditionBindingCacheEntry, TransactUpdateBindingCacheEntry,
    cached_transact_condition_binding, cached_transact_update_binding,
};
pub(crate) use ttl_indexing::{
    project_wire_item_table_key_and_ttl, ttl_index_direct_operations_for_wire_items,
    ttl_tracking_enabled, wire_item_key_token_from_item_key,
};

const CREATE_TABLE_CONFLICT_RETRY_ATTEMPTS: usize = 4;

use storage_common::ttl::TtlConfigRecord;

use crate::{
    SortedKvDbStorageProvider,
    backends::common::KvMutation,
    billing_metrics::{
        WriteCostTally, attr_map_payload_bytes, record_read_cost, record_write_cost,
        serializable_payload_bytes, wire_items_payload_bytes,
    },
    helpers::increment_bytes,
    keyspace::{
        compact::{self, TableStorageId},
        table_identity::{StoredTableMetadata, TABLE_ID_ALLOCATOR_KEY, TableIdentity},
        table_keys,
    },
    sorted_kv::{decode_table_storage_id, encode_table_storage_id},
    sorted_kv_store::{
        BatchItem, DirectWriteOperation, RawKey, TransactWriteOperation,
        TransactWriteTableOperation,
    },
    storage_ops::{
        change_index_slot_prefix, stream_duration::stream_trim_state_write_ops_for_identity,
    },
    ttl,
};
