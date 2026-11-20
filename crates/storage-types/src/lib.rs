//! Shared storage types and helpers for DynamoDB-compatible APIs.
extern crate self as storage;
extern crate self as storage_types;

pub mod types;

pub mod ambient;
mod attempt_deadline;
mod attempt_outcome;
#[cfg(test)]
mod attempt_outcome_tests;
mod cancellation_reason;
#[cfg(test)]
mod cancellation_reason_tests;
pub mod condition;
pub use attempt_outcome::{
    AttemptFailure, AttemptKind, CapacityKind, ConflictCause, ErrorClass, FreshCause,
    MissExpectation, NativeCallPhase, NativeCause, NativeCode, NativeEngine, PhysicalWriteMiss,
    UnknownCause,
};
pub use cancellation_reason::{CancellationCode, CancellationReason};
#[cfg(test)]
mod attempt_deadline_tests;
mod attribute_value;
mod native_attempt;
#[cfg(test)]
mod native_attempt_tests;
mod native_work;
pub use native_work::{BoundWorkLimits, NativeWorkLimits};
mod frozen_image_projection;
pub use attempt_deadline::{
    AttemptDeadline, AttemptDeadlineSource, AttemptDeadlines, AttemptDurability, AttemptPlan,
    AttemptRequest, AttemptScope, AttemptTry, DEFAULT_ATTEMPT_BUDGETS_MS, DEFAULT_COMMIT_GRACE_MS,
    DeadlineClass, NativeWork, ReadVersionPolicy, expired_in,
};
pub(crate) use attribute_value::attribute_value_from_json_value;
pub use attribute_value::{
    AttributeValue, ConversionError, canonical_dynamo_json, canonical_dynamo_map_json,
    from_hashmap, to_hashmap,
};
pub use frozen_image_projection::{FrozenImageProjection, FrozenProjectionFields};
pub use native_attempt::{
    AttemptMode, NativeAttemptCall, NativeAttemptCapacity, NativeAttemptSettlement, ReadProtection,
};
mod attribute_map;
mod wire_attribute_order;
#[doc(hidden)]
pub use wire_attribute_order::wire_attribute_priority;
#[cfg(test)]
mod attribute_map_tests;
#[cfg(test)]
mod attribute_serialization_tests;
pub use attribute_map::AttributeMap;
mod vec_map;
pub use vec_map::{VecMap, VecMapEntry};
mod bounded_vec;
#[cfg(test)]
mod bounded_vec_tests;
pub use bounded_vec::BoundedVec;
mod exact_batch;
#[cfg(test)]
mod exact_batch_tests;
pub use exact_batch::{Aligned, Exact};
mod claim_window;
#[cfg(test)]
mod claim_window_tests;
pub use claim_window::ClaimSlot;
mod messaging_identity;
pub use messaging_identity::MessagingIdentity;
mod non_empty_string;
#[cfg(test)]
mod non_empty_string_tests;
pub use non_empty_string::NonEmptyString;
mod return_consumed_capacity;
pub use return_consumed_capacity::ReturnConsumedCapacity;
mod non_zero_vec;
#[cfg(test)]
mod non_zero_vec_tests;
#[cfg(test)]
mod return_consumed_capacity_tests;
pub use non_zero_vec::NonZeroVec;
mod constants;
#[cfg(test)]
mod constants_tests;
pub use constants::{
    DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE, DYNAMODB_IDEMPOTENT_PARAMETER_MISMATCH_MESSAGE,
    DYNAMODB_INTERNAL_SERVER_ERROR_MESSAGE, DYNAMODB_MISSING_AUTH_TOKEN_MESSAGE,
    DYNAMODB_PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE, DYNAMODB_REQUEST_LIMIT_EXCEEDED_MESSAGE,
    DYNAMODB_RESOURCE_IN_USE_MESSAGE, DYNAMODB_RESOURCE_NOT_FOUND_MESSAGE,
    DYNAMODB_STREAM_RECORDS_LIMIT_MAX, DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE,
    DYNAMODB_STREAM_RECORDS_LIMIT_MIN, DYNAMODB_THROTTLING_MESSAGE,
    DYNAMODB_TRANSACTION_CANCELED_MESSAGE, DYNAMODB_TRANSACTION_CONFLICT_MESSAGE,
    FOUNDATIONDB_DEFAULT_CACHE_READ_VERSION_MS, FOUNDATIONDB_DEFAULT_REPLY_BYTE_LIMIT,
    FOUNDATIONDB_MAX_REPLY_BYTE_LIMIT, FOUNDATIONDB_MIN_REPLY_BYTE_LIMIT, HIDDEN_TTL_INDEX_PREFIX,
    MAX_ATTRIBUTE_NESTING_DEPTH, MAX_EXPRESSION_BYTES, MAX_INDEX_NAME_BYTES,
    MAX_INDEXER_DECLARATION_BYTES, MAX_INDEXERS_CAPACITY, MAX_ITEM_SIZE_BYTES,
    MAX_LIST_TABLES_LIMIT, MAX_PARTITION_KEY_BYTES, MAX_SORT_KEY_BYTES,
    MAX_TARGET_INCARNATION_BYTES, MAX_TRANSACTION_ITEM_BYTES, MAX_TRANSACTION_MEMBERS,
    MAX_TRANSACTION_REQUEST_BYTES, ONE_ONLINE_INDEX_MESSAGE,
    READ_SEQUENCE_DEFAULT_MAX_FANOUT_PER_STEP, READ_SEQUENCE_DEFAULT_MAX_INTERMEDIATE_ITEMS,
    READ_SEQUENCE_DEFAULT_MAX_RESPONSE_BYTES, READ_SEQUENCE_DEFAULT_MAX_ROOT_ITEMS,
    READ_SEQUENCE_DEFAULT_MAX_TOTAL_READ_ITEMS, READ_SEQUENCE_HARD_MAX_ROOT_ITEMS,
    READ_SEQUENCE_HARD_MAX_SEQUENCE_STEPS, READ_SEQUENCE_TOKEN_MAX_BYTES,
    READ_SEQUENCE_TOKEN_VERSION, STORAGE_SERDE_ENCODED_PREFIX_LEN,
    STORAGE_SERDE_RAW_JSON_LIMIT_BYTES, STREAM_RECORDS_MAX_ENCODED_BYTES,
    SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES, SYSTEM_STREAM_LINEAGE_ID_MAX_BYTES,
    SYSTEM_STREAM_RECORDS_LIMIT_MAX, SYSTEM_STREAM_SCOPE_VERSION, TTL_PARTITION_ATTRIBUTE,
    dynamodb_table_not_found_message, parse_dynamodb_table_not_found_message,
};
mod dynamodb_number;
pub use dynamodb_number::{
    DynamodbNumberError, canonical_dynamodb_number, canonical_sqs_number, compare_dynamodb_numbers,
    decode_dynamodb_number_key, encode_dynamodb_number_key, project_dynamodb_number_from_bytes,
};
mod item_json_projection;
mod logical_item_size;
#[cfg(test)]
mod logical_item_size_set_tests;
#[cfg(test)]
mod logical_item_size_tests;
pub use logical_item_size::LogicalItemSize;
mod system_stream_effect_digest;
pub use system_stream_effect_digest::SystemStreamEffectDigest;
mod dynamodb_limits;
#[cfg(test)]
mod dynamodb_limits_perf_tests;
#[cfg(test)]
mod dynamodb_limits_tests;
pub(crate) use dynamodb_limits::validate_attribute_value_for_write;
pub use dynamodb_limits::{
    attribute_map_numbers_need_write_normalization, batch_write_key_validation_error,
    normalize_attribute_map_numbers_for_write, normalize_dynamodb_number_for_write,
    normalized_attribute_map_for_write, read_key_validation_error, validate_index_name,
    validate_item_key_attributes_for_schema, validate_key_attribute_value_for_schema,
    validate_key_attributes_for_schema,
};
pub mod dynamodb_binary;
#[cfg(test)]
mod dynamodb_binary_tests;
mod key_attribute_type;
pub use key_attribute_type::KeyAttributeType;
mod key_attributes;
#[cfg(test)]
mod key_attributes_tests;
pub use key_attributes::{
    AttributeValueLookup, CanonicalKeyAttributes, KeyAttributes, MissingKeyAttribute,
    SchemaKeyValues, canonicalize_top_level_number_value, key_attribute_values_match,
    normalize_key_attribute_value_for_write, schema_key_values, verify_key_attributes,
};
mod key_schema;
#[cfg(test)]
mod key_schema_tests;
pub use key_schema::{KeySchemaElement, PlannedKeyColumn, planned_key_columns};
mod key_type;
pub use key_type::KeyType;
mod stream_record;
pub use stream_record::StreamRecord;
#[cfg(test)]
mod stream_record_tests;
mod system_stream_source;
pub use system_stream_source::SystemStreamSourceTableName;
mod system_stream_checkpoint;
pub use system_stream_checkpoint::*;
mod stream_duration;
#[cfg(test)]
mod stream_duration_tests;
#[cfg(test)]
mod system_stream_checkpoint_tests;
pub use stream_duration::*;
mod table_namespace;
#[cfg(test)]
mod table_namespace_tests;
pub use table_namespace::TableNamespace;
mod clock;
mod timestamp_millis;
pub use clock::{Clock, SystemClock};
#[cfg(test)]
mod timestamp_millis_tests;
pub use timestamp_millis::*;
mod timestamp_seconds;
pub use timestamp_seconds::*;
mod timestamp_seconds_fractional;
#[cfg(test)]
mod timestamp_seconds_tests;
pub use timestamp_seconds_fractional::*;
mod transaction_runtime;
#[cfg(test)]
mod transaction_runtime_tests;
pub use transaction_runtime::*;
mod duration_seconds;
#[cfg(test)]
mod duration_seconds_tests;
#[cfg(test)]
mod timestamp_seconds_fractional_tests;
pub use duration_seconds::*;
mod pagination_limit;
#[cfg(test)]
mod pagination_limit_tests;
pub use pagination_limit::PaginationLimit;
#[cfg(test)]
mod non_covering_lookup_model_tests;
#[cfg(test)]
mod non_covering_lookup_tests;
mod stream_item_id;
pub use stream_item_id::*;
mod stream_key;
#[cfg(test)]
mod stream_key_tests;
pub use stream_key::*;
mod stored_table_info;
#[cfg(test)]
mod stored_table_info_tests;
pub use stored_table_info::*;
mod stored_wire_item;
pub use stored_wire_item::StoredWireItem;
mod materialized_item_record;
#[cfg(test)]
mod materialized_item_record_tests;
pub use materialized_item_record::MaterializedItemRecord;
mod item_key;
mod item_key_ordered;
pub use item_key::{IndexKey, IndexKeyPrefix, ItemKey, ItemKeyError, TableKey};
mod indexer_declaration;
mod table_key_canonical;
pub use indexer_declaration::IndexerDeclaration;
mod indexed_wire_item;
pub use indexed_wire_item::{validate_logical_item, validate_logical_item_names};
mod index_name;
#[cfg(test)]
mod index_name_tests;
#[cfg(test)]
mod indexed_wire_item_tests;
#[cfg(test)]
mod read_sequence_planner_boundary_tests;
mod revision;
#[cfg(test)]
mod revision_tests;
pub use revision::Revision;
pub mod storage_serde;
#[cfg(test)]
mod storage_serde_tests;
pub use index_name::IndexName;
mod physical_name;
#[cfg(test)]
mod physical_name_tests;
pub use physical_name::AttributeName;
mod table_name;
pub use table_name::TableName;
mod max_indexers;
#[cfg(test)]
mod max_indexers_tests;
pub use max_indexers::MaxIndexers;
#[cfg(test)]
mod stream_item_id_perf_tests;
#[cfg(test)]
mod stream_item_id_tests;
mod stream_name;
#[cfg(test)]
mod table_name_tests;
pub use stream_name::*;

mod user_stream_name;
pub use user_stream_name::*;
pub mod captured_entity_condition;
// Compiled for every backend: the regional marker sweep decodes ordered key
// suffixes on SQL too.
mod item_key_rocksdb;
#[cfg(all(test, feature = "rocksdb"))]
mod item_key_rocksdb_tests;
#[cfg(test)]
mod item_key_tests;
mod key_validation;
pub mod numeric;
pub mod single_table_entity;
#[cfg(test)]
mod single_table_entity_tests;
pub use single_table_entity::{EntityIndexer, SingleTableEntity, WireEntity};
mod storage_entity_type;
#[cfg(test)]
mod storage_entity_type_tests;
pub use key_validation::*;
pub use storage_entity_type::StorageEntityType;
pub mod layout_registry;
pub use inventory;
pub use layout_registry::*;

mod serializes_to_key;
pub use serializes_to_key::SerializesToKey;
mod lightweight_refs;
pub use lightweight_refs::*;
pub mod maintenance;
mod wire_item;
pub use wire_item::{
    BatchGetWireItemResponse, TryFromWireItem, TryIntoWireItem, VerifiedWireItem,
    WireAttributeDecode, WireItem, WireScalar, decode_wire_bool, decode_wire_field,
    decode_wire_field_json, decode_wire_serde_string, dynamo_json, encode_wire_attribute,
};
mod validated_entity;
pub use validated_entity::{NoopValidatedEntity, StoredEntity, ValidatedEntity};
#[cfg(test)]
mod error_message_tests;
mod errors;
mod infrastructure_result;
pub use infrastructure_result::{
    AttemptPhase, InfrastructureContext, InfrastructureResultKind, NativeUnavailable,
};
#[cfg(test)]
mod errors_tests;
pub use errors::{StorageEnum, StorageError, StorageResult, StorageValidationKind};
mod semantic_decision;
pub use semantic_decision::SemanticDecision;
mod expression_usage;
pub use expression_usage::*;
pub mod context;
#[cfg(test)]
mod context_tests;
mod projection_expression;
#[cfg(test)]
mod projection_expression_tests;
mod request_expression_validation;
pub use projection_expression::{
    AttributeProjection, ProjectionPath, ProjectionSegment, parse_projection_path,
    validate_gsi_filter_attributes, validate_gsi_projection, validate_gsi_required_attributes,
    validate_gsi_select,
};
mod request_response;
pub use request_response::*;
mod read_sequence;
pub use read_sequence::*;
mod multi_region;
pub use multi_region::*;
mod write_wire_request;
pub use write_wire_request::*;
#[cfg(test)]
mod numeric_prop_tests;
mod serde_types;
pub use serde_types::{
    DynamoRequestValidate, undefined_index_keys_message, validate_client_request_token,
};
#[cfg(test)]
mod write_wire_request_tests;

#[cfg(test)]
mod numeric_tests;

#[cfg(test)]
mod expression_usage_tests;
#[cfg(test)]
mod key_validation_tests;
#[cfg(test)]
mod request_response_tests;
#[cfg(test)]
mod request_validation_tests;
#[cfg(test)]
mod system_stream_scope_tests;

#[cfg(test)]
mod attribute_value_tests;

#[cfg(test)]
mod wire_item_alloc_tests;
#[cfg(test)]
mod wire_item_key_tests;
#[cfg(test)]
mod wire_item_projection_tests;

#[cfg(test)]
mod serde_types_tests;

#[cfg(test)]
mod lightweight_refs_tests;

#[cfg(test)]
mod stream_name_tests;

#[cfg(test)]
mod batch_get_representation_tests;

mod table_history_id;
pub use table_history_id::TableHistoryId;
mod table_history_lane;
pub use table_history_lane::TableHistoryLane;
#[cfg(test)]
mod table_history_lane_tests;
mod table_stream_sequence;
pub use table_stream_sequence::TableStreamSequence;
#[cfg(test)]
mod table_stream_sequence_tests;

mod table_stream_checkpoint;
pub use table_stream_checkpoint::{TableStreamCheckpoint, TableStreamReadProgress};
mod table_binding;
#[cfg(test)]
mod table_binding_tests;
pub use table_binding::{
    BoundGsi, BoundTableInfo, CatalogVersion, GsiPhase, KeyColumn, PairState, RegionId,
    RegionalBinding, StoredGsiBinding, StreamBinding, TableBinding, TableHistorySchema,
    TableHistoryState, TableHistoryTarget, TableStreamIdentity, TtlBinding,
};

mod stream_user_identity;
pub use stream_user_identity::StreamUserIdentity;

#[cfg(test)]
mod transaction_error_tests;

mod leader_routing;
pub use leader_routing::{STORAGE_LEADER_HINT_HEADER, STORAGE_NOT_LEADER_ERROR_TYPE};

mod item_provenance;
pub use item_provenance::ItemProvenance;
mod regional;
#[cfg(test)]
mod regional_tests;
pub use regional::{
    PairId, RegionalMark, RegionalMarker, RegionalOrder, RegionalRole, regional_marker_item_key,
};

#[cfg(test)]
mod item_provenance_tests;

#[cfg(test)]
mod wire_alias_decode_tests;
#[cfg(test)]
mod wire_json_decoding_tests;

mod page_byte_budget;
pub use page_byte_budget::PageByteBudget;

mod initial_stream_lineages;
pub use initial_stream_lineages::InitialStreamLineages;
