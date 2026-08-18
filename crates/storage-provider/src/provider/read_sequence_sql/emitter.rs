use storage_types::{AttributeValue, ReadSequencePlan};

use crate::provider::read_sequence_sql::{
    ReadSequenceSqlCacheKey, ReadSequenceSqlCompileError, ReadSequenceSqlDialect,
    ReadSequenceSqlIdentifier, ReadSequenceSqlIr, ReadSequenceSqlKeyType, ReadSequenceSqlLimits,
    ReadSequenceSqlMappedKeySource, ReadSequenceSqlOperator, ReadSequenceSqlShape,
    ReadSequenceSqlStatement,
};

pub(super) fn emit_dialect_read_sequence_sql(
    plan: &ReadSequencePlan,
    ir: &ReadSequenceSqlIr,
    dialect: ReadSequenceSqlDialect,
    limits: ReadSequenceSqlLimits,
) -> Result<ReadSequenceSqlStatement, ReadSequenceSqlCompileError> {
    ReadSequenceSqlEmitter::new(plan, ir, dialect, limits).emit()
}

struct ReadSequenceSqlEmitter<'a> {
    plan: &'a ReadSequencePlan,
    ir: &'a ReadSequenceSqlIr,
    dialect: ReadSequenceSqlDialect,
    limits: ReadSequenceSqlLimits,
    parameters: Vec<AttributeValue>,
    ctes: Vec<String>,
    batch_ctes: Vec<String>,
}

struct ReadSequenceSqlSource {
    from: String,
    where_parts: Vec<String>,
    invocation: String,
}

impl<'a> ReadSequenceSqlEmitter<'a> {
    fn new(
        plan: &'a ReadSequencePlan,
        ir: &'a ReadSequenceSqlIr,
        dialect: ReadSequenceSqlDialect,
        limits: ReadSequenceSqlLimits,
    ) -> Self {
        Self {
            plan,
            ir,
            dialect,
            limits,
            parameters: Vec::new(),
            ctes: Vec::new(),
            batch_ctes: Vec::new(),
        }
    }

    fn emit(mut self) -> Result<ReadSequenceSqlStatement, ReadSequenceSqlCompileError> {
        if self.ir.parameter_count > self.limits.max_parameters {
            return Err(ReadSequenceSqlCompileError::ParameterLimit);
        }
        for node_id in &self.plan.graph.topological_order {
            let node = self
                .ir
                .nodes
                .iter()
                .find(|node| node.node == *node_id)
                .ok_or(ReadSequenceSqlCompileError::MissingMetadata)?;
            self.emit_node(node)?;
        }
        self.finish()
    }

    fn emit_node(
        &mut self,
        node: &crate::provider::read_sequence_sql::ReadSequenceSqlIrNode,
    ) -> Result<(), ReadSequenceSqlCompileError> {
        let metadata = &node.metadata;
        let relation = quote_identifier(metadata.relation.as_str());
        let source = self.build_source(node, relation.clone())?;
        let sql = self.render_node_sql(node, &relation, source);
        self.ctes.push(format!("n{} AS ({sql})", node.node.index()));
        Ok(())
    }

    fn build_source(
        &mut self,
        node: &crate::provider::read_sequence_sql::ReadSequenceSqlIrNode,
        relation: String,
    ) -> Result<ReadSequenceSqlSource, ReadSequenceSqlCompileError> {
        if node.metadata.mapped_source.is_some() {
            self.build_mapped_source(node, relation)
        } else if node.metadata.shape == ReadSequenceSqlShape::BatchGet {
            self.build_batch_source(node, relation)
        } else {
            self.build_predicate_source(node, relation)
        }
    }

    fn build_mapped_source(
        &mut self,
        node: &crate::provider::read_sequence_sql::ReadSequenceSqlIrNode,
        relation: String,
    ) -> Result<ReadSequenceSqlSource, ReadSequenceSqlCompileError> {
        let source = node
            .metadata
            .mapped_source
            .as_ref()
            .ok_or(ReadSequenceSqlCompileError::UnsupportedShape)?;
        if source.keys.len() != node.metadata.key_columns.len() {
            return Err(ReadSequenceSqlCompileError::InvalidKeyMetadata);
        }
        let parent = format!("p{}", node.node.index());
        let joins = node
            .metadata
            .key_columns
            .iter()
            .zip(node.metadata.key_types.iter().copied())
            .map(|(target, target_type)| {
                let binding = source
                    .keys
                    .iter()
                    .find(|binding| binding.target_attribute_name == target.as_str())
                    .ok_or(ReadSequenceSqlCompileError::MissingMetadata)?;
                let value = match &binding.source {
                    ReadSequenceSqlMappedKeySource::Indexer => {
                        if target_type != ReadSequenceSqlKeyType::String {
                            return Err(ReadSequenceSqlCompileError::InvalidKeyMetadata);
                        }
                        let ordinal = source.indexer;
                        match self.dialect {
                            ReadSequenceSqlDialect::Sqlite => {
                                format!("json_extract({parent}.indexer_json, '$[{ordinal}]')")
                            }
                            ReadSequenceSqlDialect::PostgreSql => {
                                format!("({parent}.indexer_json::json ->> {ordinal})")
                            }
                        }
                    }
                    ReadSequenceSqlMappedKeySource::Constant(value) => {
                        if !sql_key_value_matches(target_type, value) {
                            return Err(ReadSequenceSqlCompileError::InvalidKeyMetadata);
                        }
                        self.parameters.push(value.clone());
                        parameter_marker(self.dialect, self.parameters.len(), target_type)
                    }
                };
                Ok(format!(
                    "{relation}.{} = {value}",
                    quote_identifier(target.as_str())
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut joins = joins;
        if !source.iterates {
            joins.push(format!("{parent}.item_ordinal = 0"));
        }
        Ok(ReadSequenceSqlSource {
            from: format!(
                "{relation} JOIN n{} AS {parent} ON {}",
                source.parent.index(),
                joins.join(" AND "),
            ),
            where_parts: Vec::new(),
            invocation: format!("{parent}.item_ordinal"),
        })
    }

    fn build_batch_source(
        &mut self,
        node: &crate::provider::read_sequence_sql::ReadSequenceSqlIrNode,
        relation: String,
    ) -> Result<ReadSequenceSqlSource, ReadSequenceSqlCompileError> {
        let metadata = &node.metadata;
        let input_name = format!("i{}", node.node.index());
        let values = metadata
            .batch_keys
            .iter()
            .enumerate()
            .map(|(ordinal, key)| self.render_batch_values(metadata, ordinal, key))
            .collect::<Result<Vec<_>, _>>()?;
        let input_columns = std::iter::once("input_ordinal".to_string())
            .chain((0..metadata.key_columns.len()).map(|index| format!("key_{index}")))
            .collect::<Vec<_>>();
        let joins = metadata
            .key_columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                format!(
                    "{relation}.{} = {input_name}.key_{index}",
                    quote_identifier(column.as_str())
                )
            })
            .collect::<Vec<_>>();
        self.batch_ctes.push(format!(
            "{input_name} ({}) AS (VALUES {})",
            input_columns.join(", "),
            values.join(", ")
        ));
        Ok(ReadSequenceSqlSource {
            from: format!("{relation} JOIN {input_name} ON {}", joins.join(" AND ")),
            where_parts: Vec::new(),
            invocation: format!("{input_name}.input_ordinal"),
        })
    }

    fn render_batch_values(
        &mut self,
        metadata: &crate::provider::read_sequence_sql::ReadSequenceSqlNodeMetadata,
        ordinal: usize,
        key: &[AttributeValue],
    ) -> Result<String, ReadSequenceSqlCompileError> {
        let mut row = vec![ordinal.to_string()];
        for (index, value) in key.iter().enumerate() {
            let key_type = metadata
                .key_types
                .get(index)
                .copied()
                .ok_or(ReadSequenceSqlCompileError::InvalidKeyMetadata)?;
            self.parameters.push(value.clone());
            row.push(parameter_marker(
                self.dialect,
                self.parameters.len(),
                key_type,
            ));
        }
        Ok(format!("({})", row.join(", ")))
    }

    fn build_predicate_source(
        &mut self,
        node: &crate::provider::read_sequence_sql::ReadSequenceSqlIrNode,
        relation: String,
    ) -> Result<ReadSequenceSqlSource, ReadSequenceSqlCompileError> {
        let mut predicates = node
            .metadata
            .predicates
            .iter()
            .map(|predicate| self.render_predicate(&node.metadata, &relation, predicate))
            .collect::<Result<Vec<_>, _>>()?;
        if node.metadata.exclude_tombstones {
            predicates.push(format!("{relation}.\"__aux_tombstone\" = 0"));
        }
        Ok(ReadSequenceSqlSource {
            from: relation,
            where_parts: predicates,
            invocation: "0".to_string(),
        })
    }

    fn render_predicate(
        &mut self,
        metadata: &crate::provider::read_sequence_sql::ReadSequenceSqlNodeMetadata,
        relation: &str,
        predicate: &crate::provider::read_sequence_sql::ReadSequenceSqlPredicate,
    ) -> Result<String, ReadSequenceSqlCompileError> {
        let key_type = metadata
            .key_columns
            .iter()
            .position(|column| column == &predicate.column)
            .and_then(|index| metadata.key_types.get(index).copied())
            .ok_or(ReadSequenceSqlCompileError::InvalidKeyMetadata)?;
        if matches!(predicate.operator, ReadSequenceSqlOperator::Prefix)
            && matches!(key_type, ReadSequenceSqlKeyType::Number)
        {
            return Err(ReadSequenceSqlCompileError::InvalidKeyMetadata);
        }
        let value = match predicate.operator {
            ReadSequenceSqlOperator::Prefix => escape_like_prefix(&predicate.value)?,
            ReadSequenceSqlOperator::Equal | ReadSequenceSqlOperator::GreaterThan => {
                predicate.value.clone()
            }
        };
        self.parameters.push(value);
        let marker = parameter_marker(self.dialect, self.parameters.len(), key_type);
        let column = format!("{relation}.{}", quote_identifier(predicate.column.as_str()));
        Ok(match predicate.operator {
            ReadSequenceSqlOperator::Equal => format!("{column} = {marker}"),
            ReadSequenceSqlOperator::Prefix => {
                format!("{column} LIKE {marker} || '%' ESCAPE '\\'")
            }
            ReadSequenceSqlOperator::GreaterThan => format!("{column} > {marker}"),
        })
    }

    fn render_node_sql(
        &mut self,
        node: &crate::provider::read_sequence_sql::ReadSequenceSqlIrNode,
        relation: &str,
        source: ReadSequenceSqlSource,
    ) -> String {
        let metadata = &node.metadata;
        let key_projection = self.render_key_projection(metadata, relation);
        let indexer_projection = self.render_indexer_projection(metadata, relation);
        let order = order_by(relation, &metadata.order_columns, &metadata.key_columns);
        let mut sql = format!(
            "SELECT {} AS node_ordinal, {} AS invocation_ordinal, 'item' AS row_kind, \
             row_number() OVER (PARTITION BY {} ORDER BY {order}) - 1 AS item_ordinal, {}, \
             {relation}.\"attributes_blob\" AS item_json, {indexer_projection} AS indexer_json \
             FROM {}",
            node.node.index(),
            source.invocation,
            source.invocation,
            key_projection.join(", "),
            source.from,
        );
        if !source.where_parts.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&source.where_parts.join(" AND "));
        }
        self.apply_node_limit(&mut sql, metadata, &order);
        sql
    }

    fn render_indexer_projection(
        &self,
        metadata: &crate::provider::read_sequence_sql::ReadSequenceSqlNodeMetadata,
        relation: &str,
    ) -> String {
        let values = (0..metadata.max_indexers.as_usize())
            .map(|ordinal| format!("{relation}.\"__aux_indexer_{ordinal}\""))
            .collect::<Vec<_>>()
            .join(", ");
        match (self.dialect, values.is_empty()) {
            (_, true) => "'[]'".to_string(),
            (ReadSequenceSqlDialect::Sqlite, false) => format!("json_array({values})"),
            (ReadSequenceSqlDialect::PostgreSql, false) => {
                format!("json_build_array({values})::text")
            }
        }
    }

    fn render_key_projection(
        &self,
        metadata: &crate::provider::read_sequence_sql::ReadSequenceSqlNodeMetadata,
        relation: &str,
    ) -> Vec<String> {
        let mut projection = metadata
            .key_columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                format!(
                    "{} AS key_{index}",
                    project_key_column(
                        self.dialect,
                        relation,
                        column.as_str(),
                        metadata.key_types[index],
                    )
                )
            })
            .collect::<Vec<_>>();
        let envelope_width = self
            .ir
            .nodes
            .iter()
            .map(|node| node.metadata.key_columns.len())
            .max()
            .unwrap_or(0)
            .max(2);
        while projection.len() < envelope_width {
            projection.push(format!("NULL AS key_{}", projection.len()));
        }
        projection
    }

    fn apply_node_limit(
        &mut self,
        sql: &mut String,
        metadata: &crate::provider::read_sequence_sql::ReadSequenceSqlNodeMetadata,
        order: &str,
    ) {
        let Some(limit) = metadata.limit else {
            sql.push_str(" ORDER BY ");
            sql.push_str(order);
            return;
        };
        self.parameters
            .push(AttributeValue::N(limit.saturating_add(1).to_string()));
        let marker = limit_marker(self.dialect, self.parameters.len());
        if metadata.shape == ReadSequenceSqlShape::BatchGet {
            *sql = format!(
                "SELECT * FROM ({sql}) AS ranked WHERE ranked.item_ordinal < {marker} ORDER BY \
                 ranked.invocation_ordinal, ranked.item_ordinal"
            );
        } else {
            sql.push_str(" ORDER BY ");
            sql.push_str(order);
            sql.push_str(" LIMIT ");
            sql.push_str(&marker);
        }
    }

    fn finish(self) -> Result<ReadSequenceSqlStatement, ReadSequenceSqlCompileError> {
        let unions = self
            .ir
            .nodes
            .iter()
            .map(|node| format!("SELECT * FROM n{}", node.node.index()))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        let mut all_ctes = self.batch_ctes;
        all_ctes.extend(self.ctes);
        let sql = format!(
            "WITH {} SELECT * FROM ({}) AS read_sequence_rows ORDER BY node_ordinal, \
             invocation_ordinal, row_kind, item_ordinal",
            all_ctes.join(", "),
            unions,
        );
        if sql.len() > self.limits.max_sql_bytes {
            return Err(ReadSequenceSqlCompileError::StatementLimit);
        }
        Ok(ReadSequenceSqlStatement {
            sql,
            parameters: self.parameters,
            cache_key: ReadSequenceSqlCacheKey {
                structural_digest: self.plan.graph.structural_digest.clone(),
                schema_digest: self.ir.schema_digest.clone(),
                dialect: self.dialect,
                compiler_version: 1,
                max_parameters: self.limits.max_parameters,
            },
        })
    }
}

fn escape_like_prefix(
    value: &storage_types::AttributeValue,
) -> Result<storage_types::AttributeValue, ReadSequenceSqlCompileError> {
    let storage_types::AttributeValue::S(value) = value else {
        return Err(ReadSequenceSqlCompileError::InvalidKeyMetadata);
    };
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if matches!(character, '\\' | '%' | '_') {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    Ok(storage_types::AttributeValue::S(escaped))
}

fn quote_identifier(value: &str) -> String {
    format!("\"{value}\"")
}

fn parameter_marker(
    dialect: ReadSequenceSqlDialect,
    number: usize,
    key_type: ReadSequenceSqlKeyType,
) -> String {
    match (dialect, key_type) {
        (ReadSequenceSqlDialect::PostgreSql, ReadSequenceSqlKeyType::Number) => {
            format!("CAST(${number} AS TEXT)::NUMERIC")
        }
        (ReadSequenceSqlDialect::Sqlite, ReadSequenceSqlKeyType::Number) => {
            format!("CAST(?{number} AS NUMERIC)")
        }
        (ReadSequenceSqlDialect::PostgreSql, _) => format!("${number}"),
        (ReadSequenceSqlDialect::Sqlite, _) => format!("?{number}"),
    }
}

fn limit_marker(dialect: ReadSequenceSqlDialect, number: usize) -> String {
    match dialect {
        ReadSequenceSqlDialect::PostgreSql => format!("CAST(${number} AS INTEGER)"),
        ReadSequenceSqlDialect::Sqlite => format!("CAST(?{number} AS INTEGER)"),
    }
}

fn project_key_column(
    dialect: ReadSequenceSqlDialect,
    relation: &str,
    column: &str,
    key_type: ReadSequenceSqlKeyType,
) -> String {
    let qualified = format!("{relation}.{}", quote_identifier(column));
    if matches!(key_type, ReadSequenceSqlKeyType::Number) {
        match dialect {
            ReadSequenceSqlDialect::PostgreSql | ReadSequenceSqlDialect::Sqlite => {
                format!("CAST({qualified} AS TEXT)")
            }
        }
    } else {
        qualified
    }
}

fn sql_key_value_matches(expected: ReadSequenceSqlKeyType, value: &AttributeValue) -> bool {
    matches!(
        (expected, value),
        (ReadSequenceSqlKeyType::String, AttributeValue::S(_))
            | (ReadSequenceSqlKeyType::Number, AttributeValue::N(_))
            | (ReadSequenceSqlKeyType::Binary, AttributeValue::B(_))
    )
}

fn order_by(
    relation: &str,
    columns: &[ReadSequenceSqlIdentifier],
    fallback_columns: &[ReadSequenceSqlIdentifier],
) -> String {
    let columns = if columns.is_empty() {
        fallback_columns
    } else {
        columns
    };
    columns
        .iter()
        .map(|column| format!("{relation}.{} ASC", quote_identifier(column.as_str())))
        .collect::<Vec<_>>()
        .join(", ")
}
