use async_trait::async_trait;
use storage_common::TtlConfigRecord;
use storage_types::{
    AllOld, IndexerDeclaration, ItemKeyError, KeyAttributes, KeySchemaElement,
    ReturnValuesOldNewUpdated, StorageError, StorageResult, StoredTableInfo, TableKey, TableName,
    TimeToLiveStatus, TransactionKeyPreflight, validate_no_duplicate_transact_item_keys,
    validate_transact_key, validate_transact_put_item_key,
};

use super::{
    conditions::UncheckedCondition,
    models::{
        StreamWriteIntent, TableCheckInput, TableDeleteInput, TablePutInput, TableUpdateInput,
        TableWriteBatchInput, TtlWriteIntent, ValidationSnapshot, WriteReceipts,
    },
    table_finish::{
        PreparedTableBatch, PreparedTableCheck, PreparedTableDelete, PreparedTablePut,
        PreparedTableUpdate,
    },
    write_preparation::WriteObservationRequirements,
};
use crate::{
    TtlWriteContext, canonical_revision_key, key_attributes_from_item,
    update_logic::parse_update_expression,
};

/// Foreground table writes share preparation and validation. Each backend's
/// complete execution owner acquires the authoritative observations and
/// commits all effects in one native attempt.
///
/// Validation placement: wire shapes arrive validated at the request
/// boundary and are never re-checked here. This layer validates only what
/// needs table metadata (key-schema conformance, fingerprints, condition
/// and update syntax), exactly once.
#[async_trait]
pub trait TableWriteRepository {
    fn prepare_table_put(&self, input: TablePutInput) -> StorageResult<PreparedTablePut> {
        let TablePutInput {
            write,
            table,
            stream,
            ttl_config,
            item_stream_ttl_hours,
        } = input;
        let (ordinal, physical_table, request, ttl_context) = write.into_parts();
        let request = request.normalized_for_write()?;
        let storage_types::PutItemRequest {
            item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_values_on_condition_check_failure,
            ..
        } = request;
        let return_old = matches!(return_values, Some(AllOld::AllOld));
        let fields = table
            .key_schema
            .iter()
            .map(|key| key.attribute_name.as_str())
            .collect::<Vec<_>>();
        let attributes = item.project_attributes(&fields)?;
        validate_transact_put_item_key(&table, &attributes)?;
        let key_fingerprint =
            storage_types::transact_put_item_key_fingerprint(&table, &attributes)?;
        let key = key_attributes_from_item(&attributes, &table.key_schema)?;
        let key_json = canonical_revision_key(&key)?;
        let table_key = table_key_for(&physical_table, &table.key_schema, &key)?;
        let new_item = item;
        let declaration = indexers.unwrap_or_default();
        let new_indexers = IndexerDeclaration::try_new(declaration, table.max_indexers)?;
        let condition = parse_opt_condition(
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
        )?;
        let stream_context = stream;
        // Put supplies a complete indexer declaration. Only effects that
        // compare against the old image require its application bytes.
        let observation = WriteObservationRequirements::durable(
            ttl_enabled(ttl_config.as_ref())
                || gsi_write_needed(&table)
                || stream_enabled(&table)
                || condition.is_some()
                || return_old,
        );
        Ok(PreparedTablePut {
            ordinal,
            table_name: physical_table.clone(),
            key: key.clone(),
            key_json,
            table_key,
            key_fingerprint,
            new_item,
            new_indexers,
            observation,
            return_old,
            condition,
            return_old_on_condition_failure:
                storage_types::return_values_on_condition_check_failure_all_old(
                    return_values_on_condition_check_failure.as_ref(),
                ),
            stream: StreamWriteIntent {
                table_name: physical_table,
                key,
                context: stream_context,
            },
            ttl: TtlWriteIntent {
                config: ttl_config,
                context: ttl_context,
                item_stream_ttl_hours,
                table,
            },
        })
    }

    fn prepare_table_update(&self, input: TableUpdateInput) -> StorageResult<PreparedTableUpdate> {
        let request = input.write.request();
        validate_transact_key(&input.table, &request.key)?;
        let key_fingerprint = storage_types::transact_key_fingerprint(&input.table, &request.key)?;
        let key_json = canonical_revision_key(&request.key)?;
        let operations = match request.update_expression.clone() {
            Some(update_expression) => parse_update_expression(
                &update_expression,
                request.expression_attribute_names.as_ref(),
                request.expression_attribute_values.as_ref(),
            )?,
            None => Vec::new(),
        };
        let condition = parse_opt_condition(
            request.condition_expression.clone(),
            request.expression_attribute_names.clone(),
            request.expression_attribute_values.clone(),
        )?;
        let return_old = update_returns_old(request.return_values.as_ref());
        let table_key = table_key_for(
            input.write.physical_table(),
            &input.table.key_schema,
            &request.key,
        )?;
        let stream_context = input.context.stream;
        // Even an empty update preserves the current item, so every update
        // needs the old image or proof that the item is absent.
        let observation = WriteObservationRequirements::DurableOldItem;
        let new_indexers = request
            .indexers
            .clone()
            .map(|names| IndexerDeclaration::try_new(names, input.table.max_indexers))
            .transpose()?;
        Ok(PreparedTableUpdate {
            ordinal: input.write.ordinal(),
            table_name: input.write.physical_table().clone(),
            key: request.key.clone(),
            key_json,
            table_key,
            key_fingerprint,
            operations,
            observation,
            return_old,
            condition,
            return_old_on_condition_failure:
                storage_types::return_values_on_condition_check_failure_all_old(
                    request.return_values_on_condition_check_failure.as_ref(),
                ),
            return_values: request.return_values.clone(),
            new_indexers,
            stream: StreamWriteIntent {
                table_name: input.write.physical_table().clone(),
                key: request.key.clone(),
                context: stream_context,
            },
            ttl: TtlWriteIntent {
                config: input.context.ttl_config,
                context: input.context.ttl_context,
                item_stream_ttl_hours: input.context.item_stream_ttl_hours,
                table: input.table,
            },
        })
    }

    fn prepare_table_delete(&self, input: TableDeleteInput) -> StorageResult<PreparedTableDelete> {
        let request = input.write.request();
        validate_transact_key(&input.table, &request.key)?;
        let key_fingerprint = storage_types::transact_key_fingerprint(&input.table, &request.key)?;
        let key_json = canonical_revision_key(&request.key)?;
        let table_key = table_key_for(
            input.write.physical_table(),
            &input.table.key_schema,
            &request.key,
        )?;
        let condition = parse_opt_condition(
            request.condition_expression.clone(),
            request.expression_attribute_names.clone(),
            request.expression_attribute_values.clone(),
        )?;
        let return_old = matches!(request.return_values, Some(AllOld::AllOld));
        let stream_context = input.stream;
        let observation = WriteObservationRequirements::durable(
            ttl_enabled(input.ttl_config.as_ref())
                || gsi_write_needed(&input.table)
                || stream_enabled(&input.table)
                || condition.is_some()
                || return_old,
        );
        Ok(PreparedTableDelete {
            ordinal: input.write.ordinal(),
            table_name: input.write.physical_table().clone(),
            key: request.key.clone(),
            key_json,
            table_key,
            key_fingerprint,
            observation,
            return_old,
            condition,
            return_old_on_condition_failure:
                storage_types::return_values_on_condition_check_failure_all_old(
                    request.return_values_on_condition_check_failure.as_ref(),
                ),
            stream: StreamWriteIntent {
                table_name: input.write.physical_table().clone(),
                key: request.key.clone(),
                context: stream_context,
            },
            ttl: TtlWriteIntent {
                config: input.ttl_config,
                context: TtlWriteContext::clone(input.write.context()),
                item_stream_ttl_hours: input.item_stream_ttl_hours,
                table: input.table,
            },
        })
    }

    fn prepare_table_check(&self, input: TableCheckInput) -> StorageResult<PreparedTableCheck> {
        validate_transact_key(&input.table, &input.key)?;
        let key_fingerprint = storage_types::transact_key_fingerprint(&input.table, &input.key)?;
        let key_json = canonical_revision_key(&input.key)?;
        let table_key = table_key_for(&input.table_name, &input.table.key_schema, &input.key)?;
        let condition = parse_opt_condition(
            Some(input.condition_expression),
            input.expression_attribute_names,
            input.expression_attribute_values,
        )?;
        let Some(condition) = condition else {
            return Err(StorageError::internal(
                "Condition check lost its required condition expression",
            ));
        };
        Ok(PreparedTableCheck {
            ordinal: input.ordinal,
            table_name: input.table_name,
            key: input.key,
            key_json,
            table_key,
            key_fingerprint,
            condition,
            return_old_on_condition_failure:
                storage_types::return_values_on_condition_check_failure_all_old(
                    input.return_values_on_condition_check_failure.as_ref(),
                ),
        })
    }

    fn prepare_table_batch(
        &self,
        input: TableWriteBatchInput,
    ) -> StorageResult<PreparedTableBatch> {
        let mut puts = Vec::with_capacity(input.puts.len());
        for put in input.puts {
            puts.push(self.prepare_table_put(put)?);
        }
        let mut updates = Vec::with_capacity(input.updates.len());
        for update in input.updates {
            updates.push(self.prepare_table_update(update)?);
        }
        let mut deletes = Vec::with_capacity(input.deletes.len());
        for delete in input.deletes {
            deletes.push(self.prepare_table_delete(delete)?);
        }
        let mut checks = Vec::with_capacity(input.checks.len());
        for check in input.checks {
            checks.push(self.prepare_table_check(check)?);
        }
        Ok(PreparedTableBatch {
            puts,
            updates,
            deletes,
            checks,
        })
    }

    /// Duplicate detection over normalized key fingerprints, reusing the
    /// compliance-tested validator (number-equivalent keys collide). Runs
    /// once per batch; fingerprints are computed once per member in
    /// preparation, never recomputed.
    fn validate_prepared_batch(
        &self,
        batch: &PreparedTableBatch,
        _snapshot: &ValidationSnapshot,
    ) -> StorageResult<()> {
        let mut preflights = Vec::new();
        for put in &batch.puts {
            preflights.push(TransactionKeyPreflight {
                validation_reason: None,
                key_fingerprint: Some(put.key_fingerprint.clone()),
            });
        }
        for update in &batch.updates {
            preflights.push(TransactionKeyPreflight {
                validation_reason: None,
                key_fingerprint: Some(update.key_fingerprint.clone()),
            });
        }
        for delete in &batch.deletes {
            preflights.push(TransactionKeyPreflight {
                validation_reason: None,
                key_fingerprint: Some(delete.key_fingerprint.clone()),
            });
        }
        for check in &batch.checks {
            preflights.push(TransactionKeyPreflight {
                validation_reason: None,
                key_fingerprint: Some(check.key_fingerprint.clone()),
            });
        }
        validate_no_duplicate_transact_item_keys(&preflights)
    }

    async fn execute_table_batch(
        &self,
        input: TableWriteBatchInput,
    ) -> StorageResult<WriteReceipts>;
}

fn table_key_for(
    table_name: &TableName,
    key_schema: &[KeySchemaElement],
    key: &KeyAttributes,
) -> StorageResult<TableKey> {
    TableKey::from_key_schema(table_name.clone(), key_schema, key).map_err(|error: ItemKeyError| {
        let _ = error;
        StorageError::validation("Table key does not match the table key schema")
    })
}

fn stream_enabled(table: &StoredTableInfo) -> bool {
    table
        .stream_specification
        .as_ref()
        .is_some_and(|spec| spec.stream_enabled)
}

fn ttl_enabled(config: Option<&TtlConfigRecord>) -> bool {
    config.is_some_and(|config| {
        matches!(
            config.status,
            TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling
        )
    })
}

fn gsi_write_needed(table: &StoredTableInfo) -> bool {
    table
        .global_secondary_indexes
        .as_ref()
        .is_some_and(|indexes| indexes.iter().any(|index| !index.index_name.is_ttl_index()))
}

fn update_returns_old(value: Option<&ReturnValuesOldNewUpdated>) -> bool {
    matches!(
        value,
        Some(ReturnValuesOldNewUpdated::AllOld | ReturnValuesOldNewUpdated::UpdatedOld)
    )
}

fn parse_opt_condition(
    condition_expression: Option<String>,
    expression_attribute_names: Option<storage_types::VecMap<String, String>>,
    expression_attribute_values: Option<storage_types::AttributeMap>,
) -> StorageResult<Option<UncheckedCondition>> {
    UncheckedCondition::parse(
        condition_expression,
        expression_attribute_names,
        expression_attribute_values,
    )
}
