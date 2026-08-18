//! Shared storage types and helpers for DynamoDB-compatible APIs.
extern crate self as storage;
extern crate self as storage_types;

pub mod types {
    pub use crate::*;
}

mod attribute_value;
pub use attribute_value::{
    AttributeValue, ConversionError, attribute_value_from_json_value, canonical_dynamo_json,
    canonical_dynamo_map_json, from_hashmap, to_hashmap,
};
mod attribute_map;
#[cfg(test)]
mod attribute_map_tests;
pub use attribute_map::{AttributeMap, AttributeMapEntry};
mod constants;
pub use constants::*;
mod dynamodb_limits;
#[cfg(test)]
mod dynamodb_limits_perf_tests;
#[cfg(test)]
mod dynamodb_limits_tests;
pub use dynamodb_limits::{
    MAX_ATTRIBUTE_NAME_BYTES, MAX_ATTRIBUTE_NESTING_DEPTH, MAX_EXPRESSION_BYTES,
    MAX_ITEM_SIZE_BYTES, MAX_LIST_TABLES_LIMIT, MAX_PARTITION_KEY_BYTES, MAX_PROJECTED_ATTRIBUTES,
    MAX_QUERY_SCAN_RESPONSE_BYTES, MAX_SORT_KEY_BYTES, MAX_TRANSACTION_REQUEST_BYTES,
    attribute_map_numbers_need_write_normalization, normalize_attribute_map_numbers_for_write,
    normalize_dynamodb_number_for_write, validate_attribute_value_for_write,
    validate_item_key_attributes_for_schema, validate_key_attribute_value_for_schema,
    validate_key_attributes_for_schema,
};
pub mod dynamodb_binary;
#[cfg(test)]
mod dynamodb_binary_tests;
mod key_attribute_type;
pub use key_attribute_type::KeyAttributeType;
mod key_attributes;
pub use key_attributes::{AttributeValueLookup, KeyAttribute, KeyAttributes};
mod key_schema;
pub use key_schema::KeySchemaElement;
mod key_type;
pub use key_type::KeyType;
mod stream_record;
pub use stream_record::StreamRecord;
mod stream_duration;
#[cfg(test)]
mod stream_duration_tests;
pub use stream_duration::*;
mod table_namespace;
#[cfg(test)]
mod table_namespace_tests;
pub use table_namespace::{SYSTEM_TABLE_NAMESPACE, TableNamespace, TableNamespaceParseError};
mod timestamp_millis;
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
mod durable_revision;
pub use durable_revision::{
    DurableAbsenceProof, DurableBatchPointReadProof, DurableBatchPointReadProofEntry,
    DurableBatchPointReadRequest, DurableItemRevision, DurablePointReadGuard,
    DurablePointReadProof, DurablePointReadRequest, DurableTransactWriteGuard,
    GuardedDeleteItemRequest, GuardedPutItemRequest, GuardedTransactWriteItemsRequest,
    GuardedUpdateItemRequest,
};
mod pagination_limit;
#[cfg(test)]
mod pagination_limit_tests;
pub use pagination_limit::{PaginationLimit, PaginationLimitError};
mod non_covering_lookup;
#[cfg(test)]
mod non_covering_lookup_tests;
pub use non_covering_lookup::{
    NonCoveringLookupAttachment, NonCoveringLookupCandidate, NonCoveringLookupError,
    NonCoveringLookupFetch, NonCoveringLookupJoinMode, NonCoveringLookupPlan,
    merge_non_covering_lookup_items, plan_non_covering_lookup,
};
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
mod item_key;
pub use item_key::{IndexKey, IndexKeyPrefix, ItemKey, ItemKeyError, TableKey};
mod indexed_wire_item;
#[cfg(test)]
mod indexed_wire_item_tests;
pub use indexed_wire_item::{
    DecodedIndexedWireItem, INDEXED_VALUE_FORMAT_VERSION, INDEXED_VALUE_LZ4_CODEC,
    INDEXED_VALUE_LZ4_HEADER, INDEXED_VALUE_RAW_CODEC, INDEXED_VALUE_RAW_HEADER,
    INDEXER_TUPLE_OFFSET, IndexedWireItem, IndexerDeclaration, indexer_tuple_index,
};
mod index_name;
#[cfg(test)]
mod index_name_tests;
mod item_stream_version;
#[cfg(test)]
mod item_stream_version_tests;
#[cfg(test)]
mod quint_item_versioned_stream_tests;
#[cfg(test)]
mod quint_read_sequence_planner_tests;
pub use item_stream_version::ItemStreamVersion;
pub mod storage_serde;
#[cfg(test)]
mod storage_serde_tests;
pub use index_name::IndexName;
mod table_name;
pub use table_name::TableName;
mod max_indexers;
#[cfg(test)]
mod max_indexers_tests;
pub use max_indexers::{MAX_INDEXERS_CAPACITY, MaxIndexers};
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
#[cfg(feature = "rocksdb")]
mod item_key_rocksdb;
#[cfg(all(test, feature = "rocksdb"))]
mod item_key_rocksdb_tests;
#[cfg(any(feature = "sqlite", feature = "postgres", not(feature = "rocksdb")))]
mod item_key_sqlite;
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
mod wire_item;
pub use wire_item::{
    BatchGetWireItemResponse, TryFromWireItem, TryIntoWireItem, WireAttributeDecode, WireItem,
    WireItemKeyAttributes, decode_wire_field, decode_wire_field_json, decode_wire_serde_string,
    encode_wire_attribute,
};
mod ttl;
pub use ttl::*;
mod cacheable;
pub use cacheable::Cacheable;
pub mod canonical_json;
mod validated_entity;
pub use validated_entity::{NoopValidatedEntity, StoredEntity, ValidatedEntity};
#[cfg(test)]
mod error_message_tests;
mod errors;
#[cfg(test)]
mod errors_tests;
pub use errors::{
    StorageEnum, StorageError, StorageResult, StorageValidationInput, StorageValidationKind,
};
mod expression_usage;
pub use expression_usage::*;
pub mod context;
mod projection_expression;
#[cfg(test)]
mod projection_expression_tests;
mod request_expression_validation;
#[cfg(test)]
mod request_expression_validation_perf_tests;
pub use projection_expression::{
    AttributeProjection, project_attribute_map, project_attribute_map_ref, project_wire_items,
    validate_gsi_projection, validate_gsi_required_attributes,
};
mod request_response;
pub use request_response::*;
mod read_sequence;
mod read_sequence_error;
mod read_sequence_graph;
#[cfg(test)]
mod read_sequence_graph_tests;
mod read_sequence_planner;
mod read_sequence_response;
mod read_sequence_selector;
#[cfg(test)]
mod read_sequence_selector_tests;
pub use read_sequence::*;
pub use read_sequence_error::ReadSequenceValidationError;
pub use read_sequence_graph::{
    ReadSequenceFromInput, ReadSequenceGraphPlan, ReadSequenceInputCardinality,
    ReadSequenceMappedKeySource, ReadSequenceNode, ReadSequenceNodeId, ReadSequenceNodeInput,
    ReadSequenceNodeOperation, ReadSequenceStringTemplateError, ReadSequenceStringTemplatePart,
    ReadSequenceStringTemplateParts, read_sequence_input_literal, read_sequence_input_literal_name,
    read_sequence_input_marker, read_sequence_input_marker_name,
    read_sequence_operation_contains_literal_escape, read_sequence_string_template,
    read_sequence_string_template_name,
};
pub use read_sequence_planner::{
    ReadSequencePlan, plan_read_sequence, plan_read_sequence_with_capabilities,
};
pub use read_sequence_response::*;
pub use read_sequence_selector::{
    ParsedReadSequenceSelector, ReadSequenceAttributeValueType, ReadSequenceSelectorSegment,
};
mod multi_region;
pub use multi_region::*;
mod write_wire_request;
pub use write_wire_request::*;
#[cfg(test)]
mod numeric_prop_tests;
mod serde_types;
pub use serde_types::DynamoRequestValidate;
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
mod response_attribute_map_perf_tests;

#[cfg(test)]
mod attribute_value_tests;

#[cfg(test)]
mod wire_item_alloc_tests;
#[cfg(test)]
mod wire_item_projection_tests;

#[cfg(test)]
mod serde_types_tests;

#[cfg(test)]
mod lightweight_refs_tests;

#[cfg(test)]
mod stream_name_tests;
