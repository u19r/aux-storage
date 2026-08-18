use storage_provider::{
    ReadSequenceExecution, ReadSequenceExecutionBudget, ReadSequenceFlatResult,
    ReadSequenceFlatRow, ReadSequenceSqlCompileError, ReadSequenceSqlEnvelopeRow,
    ReadSequenceSqlKeyType, ReadSequenceSqlMetadata, ReadSequenceSqlRowKind,
    ReadSequenceUnsupportedReason, StorageProvider as _, build_read_sequence_sql_ir,
    decode_read_sequence_sql_rows, materialize_read_sequence_sql_mapped,
    merge_read_sequence_sql_metadata,
};
use storage_types::{
    AttributeValue, KeyAttributes, ReadSequenceConsistency, ReadSequenceNodeOperation,
    ReadSequencePlan, StorageError, StorageResult, TableName,
};
use turso::Value as TursoValue;

use crate::{
    backends::{
        sqlite::storage_provider::{
            compile_sqlite_read_sequence_statement, decode_sql_query_continuation,
            encode_sql_query_continuation, sql_item_key_attributes,
            sqlite_batch_read_sequence_metadata, sqlite_get_read_sequence_metadata,
            sqlite_mapped_child_metadata, sqlite_query_read_sequence_metadata,
        },
        turso::provider::{
            TursoStorageProvider, attribute_scalar_to_turso_value, value_to_i64, value_to_string,
        },
    },
    constants::DEFAULT_QUERY_LIMIT,
};

impl TursoStorageProvider {
    pub(super) async fn execute_read_sequence_plan_operation(
        &self,
        plan: &ReadSequencePlan,
        consistency: ReadSequenceConsistency,
        continuation: Option<&str>,
    ) -> StorageResult<ReadSequenceExecution> {
        if consistency != ReadSequenceConsistency::Eventual {
            return Ok(ReadSequenceExecution::Unsupported(
                ReadSequenceUnsupportedReason::OperationShape,
            ));
        }
        if continuation.is_none()
            && storage_provider::read_sequence_sql_mapped_source(plan).is_some()
        {
            return self.execute_mapped_read_sequence(plan).await;
        }
        if plan.nodes.len() != 1 {
            return Ok(ReadSequenceExecution::Unsupported(
                ReadSequenceUnsupportedReason::OperationShape,
            ));
        }
        self.execute_single_read_sequence(plan, continuation).await
    }

    pub(super) async fn execute_read_sequence_plan_with_budget_operation(
        &self,
        plan: &ReadSequencePlan,
        consistency: ReadSequenceConsistency,
        continuation: Option<&str>,
        budget: ReadSequenceExecutionBudget,
    ) -> StorageResult<ReadSequenceExecution> {
        if budget.is_unbounded() {
            return self
                .execute_read_sequence_plan_operation(plan, consistency, continuation)
                .await;
        }
        let bounded = match budget.bounded_query_plan(plan, DEFAULT_QUERY_LIMIT) {
            Ok(plan) => plan,
            Err(reason) => return Ok(ReadSequenceExecution::Unsupported(reason)),
        };
        self.execute_read_sequence_plan_operation(&bounded, consistency, continuation)
            .await
    }

    async fn execute_mapped_read_sequence(
        &self,
        plan: &ReadSequencePlan,
    ) -> StorageResult<ReadSequenceExecution> {
        let Some((parent_id, child_id, source)) =
            storage_provider::read_sequence_sql_mapped_source(plan)
        else {
            return Ok(ReadSequenceExecution::Unsupported(
                ReadSequenceUnsupportedReason::OperationShape,
            ));
        };
        let ReadSequenceNodeOperation::Query(parent_query) =
            &plan.nodes[parent_id.index()].operation
        else {
            unreachable!("mapped SQL source validates its Query parent");
        };
        let parent_info = self.get_table_info(&parent_query.table_name).await?;
        let Some(mut parent_metadata) =
            sqlite_query_read_sequence_metadata(parent_query, &parent_info, None)?
        else {
            return Ok(ReadSequenceExecution::Unsupported(
                ReadSequenceUnsupportedReason::OperationShape,
            ));
        };
        parent_metadata
            .nodes
            .first_entry()
            .ok_or_else(|| StorageError::internal("missing mapped parent Turso metadata"))?
            .into_mut()
            .limit = None;
        let ReadSequenceNodeOperation::Get(child_get) = &plan.nodes[child_id.index()].operation
        else {
            unreachable!("mapped SQL source validates its Get child");
        };
        let child_info = self.get_table_info(&child_get.table_name).await?;
        let Some(child_metadata) = sqlite_mapped_child_metadata(child_get, &child_info, source)?
        else {
            return Ok(ReadSequenceExecution::Unsupported(
                ReadSequenceUnsupportedReason::PhysicalLayout,
            ));
        };
        let mut metadata = None;
        for (node, value) in [(parent_id, parent_metadata), (child_id, child_metadata)] {
            merge_read_sequence_sql_metadata(&mut metadata, node, value)
                .map_err(|error| StorageError::internal(&error.to_string()))?;
        }
        let metadata =
            metadata.ok_or_else(|| StorageError::internal("missing mapped Turso SQL metadata"))?;
        let Some(statement) = compile_sqlite_read_sequence_statement(plan, &metadata)? else {
            return Ok(ReadSequenceExecution::Unsupported(
                ReadSequenceUnsupportedReason::ParameterLimit,
            ));
        };
        let rows = self
            .execute_read_sequence_statement(statement, &metadata)
            .await?;
        let ir = build_read_sequence_sql_ir(plan, &metadata)
            .map_err(|error| StorageError::internal(&error.to_string()))?;
        let decoded = decode_read_sequence_sql_rows(plan, &ir, rows)
            .map_err(|error| StorageError::internal(&error.to_string()))?;
        let rows = match materialize_read_sequence_sql_mapped(plan, &ir, decoded) {
            Ok(rows) => rows,
            Err(ReadSequenceSqlCompileError::MappingMiss) => {
                return Ok(ReadSequenceExecution::Unsupported(
                    ReadSequenceUnsupportedReason::PhysicalLayout,
                ));
            }
            Err(error) => return Err(StorageError::internal(&error.to_string())),
        };
        metrics::counter!(
            "storage.read_sequence.sql.statements.total",
            "dialect" => "turso",
            "shape" => "mapped_indexer"
        )
        .increment(1);
        Ok(ReadSequenceExecution::Executed(
            storage_provider::ReadSequenceExecuted {
                rows,
                next_continuation: None,
            },
        ))
    }

    async fn execute_single_read_sequence(
        &self,
        plan: &ReadSequencePlan,
        continuation: Option<&str>,
    ) -> StorageResult<ReadSequenceExecution> {
        let node = &plan.nodes[0];
        if !node.inputs().is_empty() || node.iterate.is_some() || !node.after().is_empty() {
            return Ok(ReadSequenceExecution::Unsupported(
                ReadSequenceUnsupportedReason::OperationShape,
            ));
        }
        let cursor = match continuation {
            Some(token) if matches!(node.operation, ReadSequenceNodeOperation::Query(_)) => {
                let Ok(cursor) = decode_sql_query_continuation(token) else {
                    return Ok(ReadSequenceExecution::Unsupported(
                        ReadSequenceUnsupportedReason::Continuation,
                    ));
                };
                cursor
            }
            Some(_) => {
                return Ok(ReadSequenceExecution::Unsupported(
                    ReadSequenceUnsupportedReason::Continuation,
                ));
            }
            None => None,
        };
        let Some((metadata, table_name)) =
            self.read_sequence_metadata(node, cursor.as_ref()).await?
        else {
            return Ok(ReadSequenceExecution::Unsupported(
                ReadSequenceUnsupportedReason::OperationShape,
            ));
        };
        let Some(statement) = compile_sqlite_read_sequence_statement(plan, &metadata)? else {
            return Ok(ReadSequenceExecution::Unsupported(
                ReadSequenceUnsupportedReason::ParameterLimit,
            ));
        };
        let rows = self
            .execute_read_sequence_statement(statement, &metadata)
            .await?;
        let ir = build_read_sequence_sql_ir(plan, &metadata)
            .map_err(|error| StorageError::internal(&error.to_string()))?;
        let decoded = decode_read_sequence_sql_rows(plan, &ir, rows)
            .map_err(|error| StorageError::internal(&error.to_string()))?;
        let (row, next_continuation) = materialize_single(plan, &metadata, table_name, decoded)?;
        metrics::counter!(
            "storage.read_sequence.sql.statements.total",
            "dialect" => "turso",
            "shape" => match node.operation {
                ReadSequenceNodeOperation::Get(_) => "get",
                ReadSequenceNodeOperation::BatchGet(_) => "batch_get",
                ReadSequenceNodeOperation::Query(_) => "query",
            }
        )
        .increment(1);
        Ok(ReadSequenceExecution::Executed(
            storage_provider::ReadSequenceExecuted {
                rows: vec![row],
                next_continuation,
            },
        ))
    }

    async fn read_sequence_metadata(
        &self,
        node: &storage_types::ReadSequenceNode,
        cursor: Option<&KeyAttributes>,
    ) -> StorageResult<Option<(ReadSequenceSqlMetadata, TableName)>> {
        match &node.operation {
            ReadSequenceNodeOperation::Get(request) => {
                let info = self.get_table_info(&request.table_name).await?;
                Ok(sqlite_get_read_sequence_metadata(request, &info)?
                    .map(|metadata| (metadata, request.table_name.clone())))
            }
            ReadSequenceNodeOperation::BatchGet(request) => {
                let Some((table_name, keys)) = request.request_items.iter().next() else {
                    return Ok(None);
                };
                if request.request_items.len() != 1
                    || keys.keys.is_empty()
                    || request.return_consumed_capacity.is_some()
                    || keys.attributes_to_get.is_some()
                    || keys.projection_expression.is_some()
                    || keys.expression_attribute_names.is_some()
                    || keys.consistent_read == Some(true)
                {
                    return Ok(None);
                }
                let info = self.get_table_info(table_name).await?;
                Ok(
                    sqlite_batch_read_sequence_metadata(table_name, keys, &info)?
                        .map(|metadata| (metadata, table_name.clone())),
                )
            }
            ReadSequenceNodeOperation::Query(request) => {
                if request.index_name.is_some() {
                    return Ok(None);
                }
                let info = self.get_table_info(&request.table_name).await?;
                Ok(sqlite_query_read_sequence_metadata(request, &info, cursor)?
                    .map(|metadata| (metadata, request.table_name.clone())))
            }
        }
    }

    async fn execute_read_sequence_statement(
        &self,
        statement: storage_provider::ReadSequenceSqlStatement,
        metadata: &ReadSequenceSqlMetadata,
    ) -> StorageResult<Vec<ReadSequenceSqlEnvelopeRow>> {
        let parameters = statement
            .parameters
            .iter()
            .map(attribute_scalar_to_turso_value)
            .collect::<StorageResult<Vec<_>>>()?;
        let connection = self.connect().await?;
        let rows = self
            .query_row_set(&connection, &statement.sql, parameters)
            .await?;
        rows.iter()
            .map(|row| turso_sql_row_to_envelope(row, metadata))
            .collect()
    }
}

fn materialize_single(
    plan: &ReadSequencePlan,
    metadata: &ReadSequenceSqlMetadata,
    table_name: TableName,
    decoded: Vec<storage_provider::ReadSequenceSqlDecodedRow>,
) -> StorageResult<(ReadSequenceFlatRow, Option<String>)> {
    let node_id = storage_types::ReadSequenceNodeId::from_index(0);
    let (result, next) = match &plan.nodes[0].operation {
        ReadSequenceNodeOperation::Get(_) => (
            ReadSequenceFlatResult::Get {
                item: decoded.first().map(|row| row.item.clone()),
            },
            None,
        ),
        ReadSequenceNodeOperation::BatchGet(_) => (
            ReadSequenceFlatResult::BatchGet {
                responses: [(
                    table_name,
                    decoded.into_iter().map(|row| row.item).collect(),
                )]
                .into_iter()
                .collect(),
            },
            None,
        ),
        ReadSequenceNodeOperation::Query(_) => {
            let limit = metadata
                .nodes
                .get(&node_id)
                .and_then(|node| node.limit)
                .ok_or_else(|| StorageError::internal("compiled Query is missing a limit"))?
                as usize;
            let mut items = decoded.into_iter().map(|row| row.item).collect::<Vec<_>>();
            let has_more = items.len() > limit;
            let cursor = has_more
                .then(|| {
                    items
                        .get(limit.saturating_sub(1))
                        .map(|item| sql_item_key_attributes(item, metadata))
                })
                .flatten();
            items.truncate(limit);
            let next = cursor
                .as_ref()
                .map(encode_sql_query_continuation)
                .transpose()?;
            let count = u32::try_from(items.len())
                .map_err(|_| StorageError::internal("compiled Query count overflow"))?;
            (
                ReadSequenceFlatResult::Query {
                    items,
                    count,
                    scanned_count: count,
                    last_evaluated_key: cursor,
                },
                next,
            )
        }
    };
    Ok((
        ReadSequenceFlatRow {
            node: node_id,
            invocation_ordinal: 0,
            input_refs: Default::default(),
            result,
        },
        next,
    ))
}

fn turso_sql_row_to_envelope(
    row: crate::backends::turso::provider::core::TursoRowView<'_>,
    metadata: &ReadSequenceSqlMetadata,
) -> StorageResult<ReadSequenceSqlEnvelopeRow> {
    let node_ordinal = required_i64(row, "node_ordinal")?;
    let invocation_ordinal = required_i64(row, "invocation_ordinal")?;
    let item_ordinal = required_i64(row, "item_ordinal")?;
    let node = storage_types::ReadSequenceNodeId::from_index(
        usize::try_from(node_ordinal)
            .map_err(|_| StorageError::internal("negative Turso SQL node ordinal"))?,
    );
    let node_metadata = metadata
        .nodes
        .get(&node)
        .ok_or_else(|| StorageError::internal("compiled Turso SQL returned an unknown node"))?;
    let key_values = node_metadata
        .key_types
        .iter()
        .enumerate()
        .map(|(index, key_type)| {
            let value = required_string(row, &format!("key_{index}"))?;
            Ok(match key_type {
                ReadSequenceSqlKeyType::String => AttributeValue::S(value),
                ReadSequenceSqlKeyType::Number => AttributeValue::N(value),
                ReadSequenceSqlKeyType::Binary => AttributeValue::B(value),
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let row_kind = match required_string(row, "row_kind")?.as_str() {
        "item" => ReadSequenceSqlRowKind::Item,
        "input_ref" => ReadSequenceSqlRowKind::InputRef,
        "missing" => ReadSequenceSqlRowKind::Missing,
        "continuation" => ReadSequenceSqlRowKind::Continuation,
        _ => {
            return Err(StorageError::internal(
                "unknown compiled Turso SQL row kind",
            ));
        }
    };
    let item_json = match row.get("item_json") {
        None | Some(TursoValue::Null) => None,
        Some(value) => Some(value_to_string(value)?.into_bytes()),
    };
    let indexer_values =
        serde_json::from_str::<Vec<Option<String>>>(&required_string(row, "indexer_json")?)
            .map_err(|error| {
                StorageError::internal(&format!("parse Turso indexer JSON: {error}"))
            })?;
    Ok(ReadSequenceSqlEnvelopeRow {
        node_ordinal: u32::try_from(node_ordinal)
            .map_err(|_| StorageError::internal("Turso SQL node ordinal overflow"))?,
        invocation_ordinal: u32::try_from(invocation_ordinal)
            .map_err(|_| StorageError::internal("Turso SQL invocation ordinal overflow"))?,
        row_kind,
        item_ordinal: u32::try_from(item_ordinal)
            .map_err(|_| StorageError::internal("Turso SQL item ordinal overflow"))?,
        key_values,
        item_json,
        indexer_values,
    })
}

fn required_i64(
    row: crate::backends::turso::provider::core::TursoRowView<'_>,
    column: &str,
) -> StorageResult<i64> {
    row.get(column)
        .ok_or_else(|| StorageError::internal(&format!("missing Turso column '{column}'")))
        .and_then(value_to_i64)
}

fn required_string(
    row: crate::backends::turso::provider::core::TursoRowView<'_>,
    column: &str,
) -> StorageResult<String> {
    row.get(column)
        .ok_or_else(|| StorageError::internal(&format!("missing Turso column '{column}'")))
        .and_then(value_to_string)
}
