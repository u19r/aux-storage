mod read_sequence_execution;
#[cfg(test)]
mod read_sequence_execution_tests;
mod read_sequence_mapping;
mod read_sequence_sql;
mod storage_provider_trait;

use std::{collections::HashMap, sync::Arc};

use storage_types::{AttributeValue, KeyAttributes, StorageResult, TableName};

pub enum AtomicItemWriteDecision {
    NoWrite {
        output: Vec<u8>,
    },
    Write {
        item: HashMap<String, AttributeValue>,
        additional_items: Vec<HashMap<String, AttributeValue>>,
        output: Vec<u8>,
    },
}

pub type AtomicItemTransform = Arc<
    dyn Fn(Option<&HashMap<String, AttributeValue>>) -> StorageResult<AtomicItemWriteDecision>
        + Send
        + Sync,
>;

pub struct AtomicItemReadModifyWriteRequest {
    pub table_name: TableName,
    pub key: KeyAttributes,
    pub transform: AtomicItemTransform,
}

pub use read_sequence_execution::{
    ReadSequenceExecuted, ReadSequenceExecution, ReadSequenceExecutionBudget,
    ReadSequenceFlatResult, ReadSequenceFlatRow, ReadSequenceMappedEntry,
    ReadSequenceMappedKeyValue, ReadSequenceMappedRangePage, ReadSequenceMappedRangeRequest,
    ReadSequenceUnsupportedReason,
};
pub use read_sequence_mapping::{
    ReadSequenceMappedEdge, ReadSequenceMappedEdgeAssessment, ReadSequenceMappedOptions,
    ReadSequenceMappedRejectionReason, ReadSequenceMappedSelection, ReadSequencePhysicalDescriptor,
    ReadSequencePhysicalOperation, select_read_sequence_mapped_edges,
};
pub use read_sequence_sql::{
    ReadSequenceSqlCacheKey, ReadSequenceSqlCompileError, ReadSequenceSqlDecodedRow,
    ReadSequenceSqlDialect, ReadSequenceSqlEnvelopeRow, ReadSequenceSqlIdentifier,
    ReadSequenceSqlIr, ReadSequenceSqlIrNode, ReadSequenceSqlKeyType, ReadSequenceSqlMappedSource,
    ReadSequenceSqlMetadata, ReadSequenceSqlNodeMetadata, ReadSequenceSqlOperator,
    ReadSequenceSqlPredicate, ReadSequenceSqlRowKind, ReadSequenceSqlShape,
    ReadSequenceSqlStatement, build_read_sequence_sql_ir, decode_read_sequence_sql_rows,
    emit_postgresql_read_sequence_sql, emit_sqlite_read_sequence_sql,
    materialize_read_sequence_sql_mapped, merge_read_sequence_sql_metadata,
    read_sequence_sql_mapped_source,
};
pub use storage_provider_trait::{
    StorageProvider, StorageProviderReadContext, split_item_into_key_and_attributes_sync,
};

#[cfg(test)]
mod provider_tests;
