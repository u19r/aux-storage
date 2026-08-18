use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use storage_types::{
    AttributeValue, ReadSequenceNode, ReadSequenceNodeId, ReadSequenceNodeOperation,
    ReadSequencePlan,
};

mod decoder;
mod emitter;
pub use decoder::{
    ReadSequenceSqlDecodedRow, ReadSequenceSqlEnvelopeRow, ReadSequenceSqlRowKind,
    decode_read_sequence_sql_rows, materialize_read_sequence_sql_mapped,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReadSequenceSqlDialect {
    PostgreSql,
    Sqlite,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReadSequenceSqlIdentifier(String);

impl ReadSequenceSqlIdentifier {
    pub fn new(value: impl Into<String>) -> Result<Self, ReadSequenceSqlCompileError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value.bytes().all(|byte| {
                byte == b'_' || byte == b'.' || byte == b'-' || byte.is_ascii_alphanumeric()
            })
        {
            return Err(ReadSequenceSqlCompileError::UnsafeIdentifier);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadSequenceSqlOperator {
    Equal,
    Prefix,
    GreaterThan,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadSequenceSqlPredicate {
    pub column: ReadSequenceSqlIdentifier,
    pub operator: ReadSequenceSqlOperator,
    pub value: AttributeValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadSequenceSqlShape {
    Get,
    BatchGet,
    Query,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadSequenceSqlKeyType {
    String,
    Number,
    Binary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadSequenceSqlNodeMetadata {
    pub relation: ReadSequenceSqlIdentifier,
    pub shape: ReadSequenceSqlShape,
    /// Logical DynamoDB names parallel to the physical SQL key columns.
    pub key_attribute_names: Vec<String>,
    pub key_columns: Vec<ReadSequenceSqlIdentifier>,
    pub key_types: Vec<ReadSequenceSqlKeyType>,
    pub order_columns: Vec<ReadSequenceSqlIdentifier>,
    pub predicates: Vec<ReadSequenceSqlPredicate>,
    pub batch_keys: Vec<Vec<AttributeValue>>,
    pub limit: Option<u32>,
    pub max_indexers: storage_types::MaxIndexers,
    /// Attributes visible through the physical relation. `None` means all
    /// reconstructed attributes; GSI projections provide an explicit list.
    pub projected_attributes: Option<Vec<String>>,
    pub exclude_tombstones: bool,
    pub mapped_source: Option<ReadSequenceSqlMappedSource>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadSequenceSqlMappedSource {
    pub parent: ReadSequenceNodeId,
    pub input_name: String,
    pub attribute_name: String,
    pub indexer: u8,
    pub iterates: bool,
    pub keys: Vec<ReadSequenceSqlMappedKey>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadSequenceSqlMappedKey {
    pub target_attribute_name: String,
    pub source: ReadSequenceSqlMappedKeySource,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReadSequenceSqlMappedKeySource {
    Indexer,
    Constant(AttributeValue),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadSequenceSqlMetadata {
    pub schema_digest: String,
    pub max_parameters: usize,
    pub max_sql_bytes: usize,
    pub nodes: BTreeMap<ReadSequenceNodeId, ReadSequenceSqlNodeMetadata>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadSequenceSqlIr {
    pub schema_digest: String,
    pub nodes: Vec<ReadSequenceSqlIrNode>,
    pub parameter_count: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadSequenceSqlIrNode {
    pub node: ReadSequenceNodeId,
    pub metadata: ReadSequenceSqlNodeMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadSequenceSqlCompileError {
    UnsafeIdentifier,
    MissingMetadata,
    UnsupportedShape,
    InvalidKeyMetadata,
    ParameterLimit,
    StatementLimit,
    MalformedResult,
    UnsupportedResultRow,
    MappingMiss,
}

impl std::fmt::Display for ReadSequenceSqlCompileError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::UnsafeIdentifier => "unsafe SQL identifier",
            Self::MissingMetadata => "missing SQL physical metadata",
            Self::UnsupportedShape => "unsupported SQL graph shape",
            Self::InvalidKeyMetadata => "invalid SQL key metadata",
            Self::ParameterLimit => "SQL parameter limit exceeded",
            Self::StatementLimit => "SQL statement size limit exceeded",
            Self::MalformedResult => "SQL result envelope is malformed",
            Self::UnsupportedResultRow => "SQL result row kind is unsupported",
            Self::MappingMiss => "SQL mapped source is incompatible",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for ReadSequenceSqlCompileError {}

#[derive(Debug, Clone, PartialEq)]
pub struct ReadSequenceSqlStatement {
    pub sql: String,
    pub parameters: Vec<AttributeValue>,
    pub cache_key: ReadSequenceSqlCacheKey,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ReadSequenceSqlLimits {
    pub(crate) max_sql_bytes: usize,
    pub(crate) max_parameters: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReadSequenceSqlCacheKey {
    pub structural_digest: String,
    pub schema_digest: String,
    pub dialect: ReadSequenceSqlDialect,
    pub compiler_version: u16,
    pub max_parameters: usize,
}

/// Add one backend-resolved root node to a whole-plan SQL metadata envelope.
///
/// Backend metadata builders intentionally return a single node with ordinal
/// zero because they are also used by the single-node compiler tests.  A
/// multi-root lowering remaps that node to its plan ordinal here, keeping the
/// shared IR as the only place which knows how to assemble a complete plan.
/// The schema digest and effective limits are combined deterministically so
/// the resulting statement cache key covers every physical table involved.
pub fn merge_read_sequence_sql_metadata(
    aggregate: &mut Option<ReadSequenceSqlMetadata>,
    node: ReadSequenceNodeId,
    metadata: ReadSequenceSqlMetadata,
) -> Result<(), ReadSequenceSqlCompileError> {
    let ReadSequenceSqlMetadata {
        schema_digest,
        max_parameters,
        max_sql_bytes,
        mut nodes,
    } = metadata;
    let Some((_, node_metadata)) = nodes.pop_first() else {
        return Err(ReadSequenceSqlCompileError::MissingMetadata);
    };
    if !nodes.is_empty() {
        return Err(ReadSequenceSqlCompileError::UnsupportedShape);
    }

    if let Some(aggregate) = aggregate {
        aggregate.max_parameters = aggregate.max_parameters.min(max_parameters);
        aggregate.max_sql_bytes = aggregate.max_sql_bytes.min(max_sql_bytes);
        aggregate
            .schema_digest
            .push_str(&format!(";node{}={schema_digest}", node.index()));
        if aggregate.nodes.insert(node, node_metadata).is_some() {
            return Err(ReadSequenceSqlCompileError::UnsupportedShape);
        }
    } else {
        *aggregate = Some(ReadSequenceSqlMetadata {
            schema_digest: format!("node{}={schema_digest}", node.index()),
            max_parameters,
            max_sql_bytes,
            nodes: [(node, node_metadata)].into_iter().collect(),
        });
    }
    Ok(())
}

/// Build a typed IR from an already validated graph.  The caller supplies
/// physical names and key predicates obtained from the backend schema owner;
/// public expression strings are intentionally absent from this API.
pub fn build_read_sequence_sql_ir(
    plan: &ReadSequencePlan,
    metadata: &ReadSequenceSqlMetadata,
) -> Result<ReadSequenceSqlIr, ReadSequenceSqlCompileError> {
    let mut nodes = Vec::with_capacity(plan.nodes.len());
    let mut parameter_count = 0usize;
    for (index, node) in plan.nodes.iter().enumerate() {
        let node_id = ReadSequenceNodeId::from_index(index);
        let Some(node_metadata) = metadata.nodes.get(&node_id).cloned() else {
            return Err(ReadSequenceSqlCompileError::MissingMetadata);
        };
        parameter_count = parameter_count.saturating_add(validate_sql_node(node, &node_metadata)?);
        nodes.push(ReadSequenceSqlIrNode {
            node: node_id,
            metadata: node_metadata,
        });
    }
    if parameter_count > metadata.max_parameters {
        return Err(ReadSequenceSqlCompileError::ParameterLimit);
    }
    Ok(ReadSequenceSqlIr {
        schema_digest: metadata.schema_digest.clone(),
        nodes,
        parameter_count,
    })
}

fn validate_sql_node(
    node: &ReadSequenceNode,
    metadata: &ReadSequenceSqlNodeMetadata,
) -> Result<usize, ReadSequenceSqlCompileError> {
    validate_sql_key_metadata(metadata)?;
    let expected_shape = match node.operation {
        ReadSequenceNodeOperation::Get(_) => ReadSequenceSqlShape::Get,
        ReadSequenceNodeOperation::BatchGet(_) => ReadSequenceSqlShape::BatchGet,
        ReadSequenceNodeOperation::Query(_) => ReadSequenceSqlShape::Query,
    };
    if expected_shape != metadata.shape
        || (!node.inputs().is_empty() && metadata.mapped_source.is_none())
        || (node.inputs().is_empty() && metadata.mapped_source.is_some())
    {
        return Err(ReadSequenceSqlCompileError::UnsupportedShape);
    }
    let parameter_count = match metadata.shape {
        ReadSequenceSqlShape::BatchGet => validate_batch_metadata(metadata)?,
        ReadSequenceSqlShape::Get | ReadSequenceSqlShape::Query => metadata.predicates.len(),
    };
    let mapped_constants = metadata.mapped_source.as_ref().map_or(0, |source| {
        source
            .keys
            .iter()
            .filter(|key| matches!(key.source, ReadSequenceSqlMappedKeySource::Constant(_)))
            .count()
    });
    Ok(parameter_count + mapped_constants + usize::from(metadata.limit.is_some()))
}

pub fn read_sequence_sql_mapped_source(
    plan: &ReadSequencePlan,
) -> Option<(
    ReadSequenceNodeId,
    ReadSequenceNodeId,
    ReadSequenceSqlMappedSource,
)> {
    if plan.nodes.len() != 2 {
        return None;
    }
    let (child_index, child) = plan
        .nodes
        .iter()
        .enumerate()
        .find(|(_, node)| !node.inputs().is_empty())?;
    let child_id = ReadSequenceNodeId::from_index(child_index);
    let [parent_id] = plan.graph.dependencies.get(child_index)?.as_slice() else {
        return None;
    };
    let parent = plan.nodes.get(parent_id.index())?;
    if !matches!(parent.operation, ReadSequenceNodeOperation::Query(_))
        || !parent.inputs().is_empty()
        || child.inputs().is_empty()
    {
        return None;
    }
    let (input_name, input, source) = child.inputs().iter().find_map(|(name, input)| {
        input
            .mapped_key_source
            .as_ref()
            .map(|source| (name, input, source))
    })?;
    if child.inputs().len() != 1
        || child
            .inputs()
            .values()
            .filter(|input| input.mapped_key_source.is_some())
            .count()
            != 1
    {
        return None;
    }
    if input.from.node != parent.name {
        return None;
    }
    let iterates = match (
        child.iterate.as_deref(),
        input.cardinality,
        input.on_missing,
        input.from.select.0.as_str(),
    ) {
        (
            Some(iterate),
            storage_types::ReadSequenceInputCardinality::Many,
            storage_types::ReadSequenceOnMissing::Skip,
            selector,
        ) if iterate == input_name
            && selector == format!("$.Query.Items[*].{}", source.attribute_name()) =>
        {
            true
        }
        (
            None,
            storage_types::ReadSequenceInputCardinality::One,
            storage_types::ReadSequenceOnMissing::Error,
            selector,
        ) if selector == format!("$.Query.Items[0].{}", source.attribute_name()) => false,
        _ => return None,
    };
    let ReadSequenceNodeOperation::Get(get) = &child.operation else {
        return None;
    };
    if get.key.is_empty()
        || get.key.len() > 2
        || get.attributes_to_get.is_some()
        || get.projection_expression.is_some()
        || get.expression_attribute_names.is_some()
        || get.return_consumed_capacity.is_some()
        || get.consistent_read == Some(true)
    {
        return None;
    }
    let mut keys = Vec::with_capacity(get.key.len());
    for (target_attribute_name, value) in get.key.iter() {
        let key_source = if let Some(name) = storage_types::read_sequence_input_marker_name(value) {
            if name != input_name {
                return None;
            }
            ReadSequenceSqlMappedKeySource::Indexer
        } else if storage_types::read_sequence_string_template_name(value).is_some() {
            return None;
        } else {
            ReadSequenceSqlMappedKeySource::Constant(value.clone())
        };
        keys.push(ReadSequenceSqlMappedKey {
            target_attribute_name: target_attribute_name.to_string(),
            source: key_source,
        });
    }
    if !keys
        .iter()
        .any(|key| matches!(key.source, ReadSequenceSqlMappedKeySource::Indexer))
    {
        return None;
    }
    Some((
        *parent_id,
        child_id,
        ReadSequenceSqlMappedSource {
            parent: *parent_id,
            input_name: input_name.clone(),
            attribute_name: source.attribute_name().to_string(),
            indexer: source.indexer(),
            iterates,
            keys,
        },
    ))
}

fn validate_sql_key_metadata(
    metadata: &ReadSequenceSqlNodeMetadata,
) -> Result<(), ReadSequenceSqlCompileError> {
    if metadata.key_columns.is_empty()
        || metadata.key_columns.len() > 4
        || metadata.key_attribute_names.len() != metadata.key_columns.len()
        || metadata.order_columns.len() > 4
        || metadata.key_types.len() != metadata.key_columns.len()
    {
        return Err(ReadSequenceSqlCompileError::InvalidKeyMetadata);
    }
    let allowed_columns = metadata
        .key_columns
        .iter()
        .chain(metadata.order_columns.iter())
        .map(|column| column.as_str())
        .collect::<BTreeSet<_>>();
    if metadata
        .predicates
        .iter()
        .any(|predicate| !allowed_columns.contains(predicate.column.as_str()))
    {
        return Err(ReadSequenceSqlCompileError::InvalidKeyMetadata);
    }
    Ok(())
}

fn validate_batch_metadata(
    metadata: &ReadSequenceSqlNodeMetadata,
) -> Result<usize, ReadSequenceSqlCompileError> {
    if metadata.batch_keys.is_empty()
        || metadata
            .batch_keys
            .iter()
            .any(|key| key.len() != metadata.key_columns.len())
    {
        return Err(ReadSequenceSqlCompileError::InvalidKeyMetadata);
    }
    Ok(metadata
        .batch_keys
        .len()
        .saturating_mul(metadata.key_columns.len()))
}

/// PostgreSQL-specific lowering entrypoint.  Keeping this boundary separate
/// prevents a backend from accidentally treating SQLite placeholder or JSON
/// rules as PostgreSQL syntax.
pub fn emit_postgresql_read_sequence_sql(
    plan: &ReadSequencePlan,
    ir: &ReadSequenceSqlIr,
    max_sql_bytes: usize,
    max_parameters: usize,
) -> Result<ReadSequenceSqlStatement, ReadSequenceSqlCompileError> {
    emitter::emit_dialect_read_sequence_sql(
        plan,
        ir,
        ReadSequenceSqlDialect::PostgreSql,
        ReadSequenceSqlLimits {
            max_sql_bytes,
            max_parameters,
        },
    )
}

/// SQLite-specific lowering entrypoint.  SQLite uses numbered `?N`
/// parameters and the same structural CTE shape, but remains independently
/// gated so JSON/limit capability checks can diverge without changing the IR.
pub fn emit_sqlite_read_sequence_sql(
    plan: &ReadSequencePlan,
    ir: &ReadSequenceSqlIr,
    max_sql_bytes: usize,
    max_parameters: usize,
) -> Result<ReadSequenceSqlStatement, ReadSequenceSqlCompileError> {
    emitter::emit_dialect_read_sequence_sql(
        plan,
        ir,
        ReadSequenceSqlDialect::Sqlite,
        ReadSequenceSqlLimits {
            max_sql_bytes,
            max_parameters,
        },
    )
}

#[cfg(test)]
mod read_sequence_sql_tests;
