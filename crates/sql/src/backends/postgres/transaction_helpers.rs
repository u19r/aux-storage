use std::{collections::HashMap, sync::LazyLock};

use deadpool_postgres::GenericClient;
use storage_condition::{
    Condition, condition_has_repeated_root_field, parse_condition_expression,
    try_evaluate_condition_with_cached_roots, try_evaluate_condition_with_root,
};
use storage_provider::{
    apply_bound_update_operations, before_update_item, split_item_into_key_and_attributes_sync,
};
use storage_types::{
    AttributeValue, KeyAttributes, PreparedBatchOperation, StorageEnum, StorageError,
    StorageResult, StoredTableInfo, TableName, WireItem, transaction_canceled_for_item_error,
};
use tokio_postgres::types::ToSql;

use crate::{
    backends::postgres::{
        PostgresStorageProvider, sql_key_helpers::PreparedGetItemQuery, sql_statements,
        stream_helpers::PostgresWriteStreamEntriesInput,
    },
    provider_core::transaction::{
        all_old, conditional_check_failed_reason, transaction_canceled_for_reason,
        validate_transact_key, validate_transact_put_item_key,
    },
};

pub(crate) fn condition_item_ref(
    old_item: Option<&HashMap<String, AttributeValue>>,
) -> &HashMap<String, AttributeValue> {
    static EMPTY_ITEM: LazyLock<HashMap<String, AttributeValue>> = LazyLock::new(HashMap::new);
    old_item.unwrap_or(&EMPTY_ITEM)
}

pub(crate) fn evaluate_wire_condition(
    old_item: Option<&WireItem>,
    condition: &Condition,
) -> StorageResult<bool> {
    let mut root_value = |field: &str| match old_item {
        Some(item) => item.attribute_value(field),
        None => Ok(None),
    };
    if condition_has_repeated_root_field(condition) {
        return try_evaluate_condition_with_cached_roots(condition, &mut root_value);
    }
    try_evaluate_condition_with_root(condition, &mut root_value)
}

#[derive(Clone)]
pub(super) enum PreparedPostgresBatchOperation {
    Put(PreparedPostgresPut),
    Delete(PreparedPostgresDelete),
}

#[derive(Clone)]
pub(super) struct PreparedPostgresPut {
    table_name: TableName,
    table_info: StoredTableInfo,
    old_item_query: PreparedGetItemQuery,
    full_item: HashMap<String, AttributeValue>,
    combined_sql: String,
    combined_bind_values: Vec<String>,
}

#[derive(Clone)]
pub(super) struct PreparedPostgresDelete {
    table_name: TableName,
    table_info: StoredTableInfo,
    key_attributes: KeyAttributes,
    old_item_query: PreparedGetItemQuery,
    sql: String,
    bind_values: Vec<String>,
}

fn conditional_check_failed_reason_for_wire_item(
    return_all_old: bool,
    old_item: Option<&WireItem>,
) -> StorageResult<String> {
    let old_item = if return_all_old {
        old_item
            .map(WireItem::to_attribute_map)
            .transpose()?
            .map(Box::new)
    } else {
        None
    };
    conditional_check_failed_reason(old_item.as_deref())
}

impl PostgresStorageProvider {
    pub(super) fn prepare_postgres_batch_operation(
        prepared: PreparedBatchOperation,
    ) -> StorageResult<PreparedPostgresBatchOperation> {
        match prepared {
            PreparedBatchOperation::Put {
                table_name,
                table_info,
                key_attributes,
                non_key_attributes,
                full_item,
                ..
            } => {
                let attributes_blob = if non_key_attributes.is_empty() {
                    "{}".to_string()
                } else {
                    serde_json::to_string(&non_key_attributes).map_err(|err| {
                        Self::map_postgres_error("serialize non-key attributes", err)
                    })?
                };
                let key_bindings = Self::key_column_bindings_for_schema(
                    &table_info,
                    &table_info.key_schema,
                    &key_attributes,
                    None,
                )?;
                let table_name_safe = table_name.sanitized_name();
                let old_item_query =
                    Self::prepare_get_item_query(&table_name, &key_attributes, &table_info)?;
                let key_columns = key_bindings
                    .iter()
                    .map(|binding| binding.column.clone())
                    .collect::<Vec<_>>();
                let columns_sql = key_columns
                    .iter()
                    .cloned()
                    .chain(std::iter::once("attributes_blob".to_string()))
                    .collect::<Vec<_>>()
                    .join(", ");
                let mut placeholders = key_bindings
                    .iter()
                    .enumerate()
                    .map(|(idx, binding)| {
                        Self::postgres_placeholder_for_type(idx + 1, &binding.attribute_type)
                    })
                    .collect::<Vec<_>>();
                placeholders.push(format!("${}", key_bindings.len() + 1));
                let placeholders = placeholders.join(", ");
                let conflict_target = key_columns.join(", ");
                let assignments = key_columns
                    .iter()
                    .cloned()
                    .chain(std::iter::once("attributes_blob".to_string()))
                    .map(|column| format!("{column} = EXCLUDED.{column}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = sql_statements::upsert_main_row_returning(
                    &table_name_safe,
                    &columns_sql,
                    &placeholders,
                    &conflict_target,
                    &assignments,
                );
                let mut bind_values = key_bindings
                    .iter()
                    .map(|binding| binding.value.clone())
                    .collect::<Vec<_>>();
                bind_values.push(attributes_blob);
                let revision_key_json = key_attributes.canonical_dynamo_json().map_err(|err| {
                    StorageError::validation(format!(
                        "revision key must be Dynamo JSON encodable: {err}"
                    ))
                })?;
                let revision_sql = sql_statements::bump_item_revision_with_placeholders(
                    &format!("${}", bind_values.len() + 1),
                    &format!("${}", bind_values.len() + 2),
                );
                let combined_sql = sql_statements::dml_ctes_returning_last_column(
                    &[sql, revision_sql],
                    "revision",
                );
                bind_values.push(table_name.to_string());
                bind_values.push(revision_key_json);

                Ok(PreparedPostgresBatchOperation::Put(PreparedPostgresPut {
                    table_name,
                    table_info,
                    old_item_query,
                    full_item,
                    combined_sql,
                    combined_bind_values: bind_values,
                }))
            }
            PreparedBatchOperation::Delete {
                table_name,
                table_info,
                key,
                ..
            } => {
                let key_bindings = Self::key_column_bindings_for_schema(
                    &table_info,
                    &table_info.key_schema,
                    &key,
                    None,
                )?;
                let mut bind_values = Vec::with_capacity(key_bindings.len());
                let where_sql = Self::where_clause_for_bindings(&key_bindings, &mut bind_values);
                let table_name_safe = table_name.sanitized_name();
                let sql = sql_statements::delete_main_row(&table_name_safe, &where_sql);
                let old_item_query = Self::prepare_get_item_query(&table_name, &key, &table_info)?;

                Ok(PreparedPostgresBatchOperation::Delete(
                    PreparedPostgresDelete {
                        table_name,
                        table_info,
                        key_attributes: key,
                        old_item_query,
                        sql,
                        bind_values,
                    },
                ))
            }
        }
    }

    pub(super) async fn execute_prepared_batch_operation_with_client<C: GenericClient + Sync>(
        &self,
        client: &C,
        prepared: &PreparedPostgresBatchOperation,
    ) -> StorageResult<()> {
        match prepared {
            PreparedPostgresBatchOperation::Put(prepared) => {
                self.execute_prepared_batch_put_with_client(client, prepared)
                    .await
            }
            PreparedPostgresBatchOperation::Delete(prepared) => {
                self.execute_prepared_batch_delete_with_client(client, prepared)
                    .await
            }
        }
    }

    async fn execute_prepared_batch_put_with_client<C: GenericClient + Sync>(
        &self,
        client: &C,
        prepared: &PreparedPostgresPut,
    ) -> StorageResult<()> {
        let old_item_started = std::time::Instant::now();
        let old_item = self
            .execute_prepared_get_item_query(
                client,
                &prepared.old_item_query,
                "batch_write_item",
                "old_item_query",
            )
            .await?;
        self.record_transaction_phase(
            "batch_write_item",
            "old_item_read",
            old_item_started.elapsed(),
        );
        let old_item_for_condition = old_item
            .as_ref()
            .map(WireItem::to_attribute_map)
            .transpose()?;
        let params: Vec<&(dyn ToSql + Sync)> = prepared
            .combined_bind_values
            .iter()
            .map(|value| value as &(dyn ToSql + Sync))
            .collect();
        let main_write_started = std::time::Instant::now();
        let row = client
            .query_one(&prepared.combined_sql, &params)
            .await
            .map_err(|err| {
                Self::map_postgres_write_error("batch put execute and allocate revision", err)
            })?;
        self.record_transaction_phase(
            "batch_write_item",
            "main_write",
            main_write_started.elapsed(),
        );
        let revision = row
            .try_get::<_, i64>(0)
            .map_err(|err| Self::map_postgres_error("decode batch put revision", err))?;
        let item_stream_version = storage_types::ItemStreamVersion::try_from(revision)?;

        if self.immediate_gsi_consistency {
            let gsi_started = std::time::Instant::now();
            self.apply_gsi_entries_for_item_change_with_client(
                client,
                &prepared.table_name,
                &prepared.table_info,
                old_item_for_condition.as_ref(),
                Some(&prepared.full_item),
            )
            .await?;
            self.record_transaction_phase("batch_write_item", "gsi_sync", gsi_started.elapsed());
        }
        let ttl_started = std::time::Instant::now();
        self.sync_ttl_index_entries_with_client(
            client,
            &prepared.table_info,
            old_item_for_condition.as_ref(),
            Some(&prepared.full_item),
        )
        .await?;
        self.record_transaction_phase("batch_write_item", "ttl_sync", ttl_started.elapsed());
        let stream_started = std::time::Instant::now();
        self.write_stream_entries_for_item_with_client(
            client,
            &prepared.table_info,
            &prepared.full_item,
            PostgresWriteStreamEntriesInput {
                old_item: old_item_for_condition.as_ref(),
                is_deleted: false,
                item_stream_version,
                replication: None,
            },
        )
        .await?;
        self.record_transaction_phase("batch_write_item", "stream_write", stream_started.elapsed());
        Ok(())
    }

    async fn execute_prepared_batch_delete_with_client<C: GenericClient + Sync>(
        &self,
        client: &C,
        prepared: &PreparedPostgresDelete,
    ) -> StorageResult<()> {
        let old_item_started = std::time::Instant::now();
        let old_item = self
            .execute_prepared_get_item_query(
                client,
                &prepared.old_item_query,
                "batch_write_item",
                "old_item_query",
            )
            .await?;
        self.record_transaction_phase(
            "batch_write_item",
            "old_item_read",
            old_item_started.elapsed(),
        );
        let Some(old_item) = old_item else {
            return Ok(());
        };
        let params: Vec<&(dyn ToSql + Sync)> = prepared
            .bind_values
            .iter()
            .map(|value| value as &(dyn ToSql + Sync))
            .collect();
        let delete_started = std::time::Instant::now();
        client
            .execute(&prepared.sql, &params)
            .await
            .map_err(|err| Self::map_postgres_error("batch delete execute", err))?;
        self.record_transaction_phase(
            "batch_write_item",
            "delete_execute",
            delete_started.elapsed(),
        );
        let revision_started = std::time::Instant::now();
        let item_stream_version = storage_types::ItemStreamVersion::try_from(
            Self::bump_item_revision_with_client(
                client,
                &prepared.table_name,
                &prepared.key_attributes,
            )
            .await?,
        )?;
        self.record_transaction_phase(
            "batch_write_item",
            "revision_bump",
            revision_started.elapsed(),
        );

        let old_map = old_item.into_attribute_map()?;
        if self.immediate_gsi_consistency {
            let gsi_started = std::time::Instant::now();
            self.delete_gsi_entries_for_item_with_client(
                client,
                &prepared.table_name,
                &prepared.table_info,
                &old_map,
            )
            .await?;
            self.record_transaction_phase("batch_write_item", "gsi_sync", gsi_started.elapsed());
        }
        let ttl_started = std::time::Instant::now();
        self.sync_ttl_index_entries_with_client(client, &prepared.table_info, Some(&old_map), None)
            .await?;
        self.record_transaction_phase("batch_write_item", "ttl_sync", ttl_started.elapsed());
        let stream_started = std::time::Instant::now();
        self.write_stream_entries_for_item_with_client(
            client,
            &prepared.table_info,
            &old_map,
            PostgresWriteStreamEntriesInput {
                old_item: Some(&old_map),
                is_deleted: true,
                item_stream_version,
                replication: None,
            },
        )
        .await?;
        self.record_transaction_phase("batch_write_item", "stream_write", stream_started.elapsed());
        Ok(())
    }

    pub(super) async fn transact_put_with_client<C: GenericClient + Sync>(
        &self,
        client: &C,
        request: storage_types::TransactPutRequest,
        item_index: Option<usize>,
    ) -> StorageResult<()> {
        let condition = crate::storage_provider::parse_optional_condition(
            request.condition_expression.clone(),
            &request.expression_attribute_names,
            &request.expression_attribute_values,
        )?;
        let table_name = request.table_name;
        let table_info = self.get_table_info_cached_arc(&table_name).await?;
        validate_transact_put_item_key(&table_info, &request.item)?;
        let return_all_old = all_old(request.return_values_on_condition_check_failure.as_ref());
        let split_item = split_item_into_key_and_attributes_sync(request.item, &table_info)?;
        let old_item = self
            .get_item_with_client(client, &table_name, &split_item.key_attributes, &table_info)
            .await?;

        if let Some(condition) = &condition
            && !evaluate_wire_condition(old_item.as_ref(), condition)?
        {
            if let Some(item_index) = item_index {
                return Err(transaction_canceled_for_reason(
                    item_index,
                    conditional_check_failed_reason_for_wire_item(
                        return_all_old,
                        old_item.as_ref(),
                    )?,
                ));
            }
            return Err(StorageEnum::ConditionalCheckFailed.into());
        }
        let old_item_for_condition = old_item
            .as_ref()
            .map(WireItem::to_attribute_map)
            .transpose()?;
        self.upsert_transact_item_with_client(
            client,
            &table_name,
            &table_info,
            split_item.all_attributes,
            old_item_for_condition.as_ref(),
        )
        .await
    }

    pub(super) async fn upsert_transact_item_with_client<C: GenericClient + Sync>(
        &self,
        client: &C,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        item: HashMap<String, AttributeValue>,
        old_item_for_condition: Option<&HashMap<String, AttributeValue>>,
    ) -> StorageResult<()> {
        validate_transact_put_item_key(table_info, &item)?;
        let split_item = split_item_into_key_and_attributes_sync(item, table_info)?;

        let attributes_blob = if split_item.non_key_attributes.is_empty() {
            "{}".to_string()
        } else {
            serde_json::to_string(&split_item.non_key_attributes)
                .map_err(|err| Self::map_postgres_error("serialize non-key attributes", err))?
        };
        let key_bindings = Self::key_column_bindings_for_schema(
            table_info,
            &table_info.key_schema,
            &split_item.key_attributes,
            None,
        )?;
        let table_name_safe = table_name.sanitized_name();
        let key_columns = key_bindings
            .iter()
            .map(|binding| binding.column.clone())
            .collect::<Vec<_>>();
        let columns_sql = key_columns
            .iter()
            .cloned()
            .chain(std::iter::once("attributes_blob".to_string()))
            .collect::<Vec<_>>()
            .join(", ");
        let mut placeholders = key_bindings
            .iter()
            .enumerate()
            .map(|(idx, binding)| {
                Self::postgres_placeholder_for_type(idx + 1, &binding.attribute_type)
            })
            .collect::<Vec<_>>();
        placeholders.push(format!("${}", key_bindings.len() + 1));
        let placeholders = placeholders.join(", ");
        let conflict_target = key_columns.join(", ");
        let assignments = key_columns
            .iter()
            .cloned()
            .chain(std::iter::once("attributes_blob".to_string()))
            .map(|column| format!("{column} = EXCLUDED.{column}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = sql_statements::upsert_main_row_returning(
            &table_name_safe,
            &columns_sql,
            &placeholders,
            &conflict_target,
            &assignments,
        );
        let mut bind_values = key_bindings
            .iter()
            .map(|binding| binding.value.clone())
            .collect::<Vec<_>>();
        bind_values.push(attributes_blob);
        let revision_key_json =
            split_item
                .key_attributes
                .canonical_dynamo_json()
                .map_err(|err| {
                    StorageError::validation(format!(
                        "revision key must be Dynamo JSON encodable: {err}"
                    ))
                })?;
        let revision_sql = sql_statements::bump_item_revision_with_placeholders(
            &format!("${}", bind_values.len() + 1),
            &format!("${}", bind_values.len() + 2),
        );
        let combined_sql =
            sql_statements::dml_ctes_returning_last_column(&[sql, revision_sql], "revision");
        let mut combined_bind_values = bind_values;
        combined_bind_values.push(table_name.to_string());
        combined_bind_values.push(revision_key_json);
        let params: Vec<&(dyn ToSql + Sync)> = combined_bind_values
            .iter()
            .map(|value| value as &(dyn ToSql + Sync))
            .collect();
        let row = client
            .query_one(&combined_sql, &params)
            .await
            .map_err(|err| {
                Self::map_postgres_write_error("transact put execute and allocate revision", err)
            })?;
        let revision = row
            .try_get::<_, i64>(0)
            .map_err(|err| Self::map_postgres_error("decode transact put revision", err))?;
        let item_stream_version = storage_types::ItemStreamVersion::try_from(revision)?;

        if self.immediate_gsi_consistency {
            self.apply_gsi_entries_for_item_change_with_client(
                client,
                table_name,
                table_info,
                old_item_for_condition,
                Some(&split_item.all_attributes),
            )
            .await?;
        }
        self.sync_ttl_index_entries_with_client(
            client,
            table_info,
            old_item_for_condition,
            Some(&split_item.all_attributes),
        )
        .await?;
        self.write_stream_entries_for_item_with_client(
            client,
            table_info,
            &split_item.all_attributes,
            PostgresWriteStreamEntriesInput {
                old_item: old_item_for_condition,
                is_deleted: false,
                item_stream_version,
                replication: None,
            },
        )
        .await?;
        Ok(())
    }

    pub(super) async fn transact_delete_with_client<C: GenericClient + Sync>(
        &self,
        client: &C,
        request: storage_types::TransactDeleteRequest,
        item_index: Option<usize>,
    ) -> StorageResult<()> {
        if request.key.is_empty() {
            return Err(StorageError::validation(
                "Delete request must specify a key",
            ));
        }
        let condition = crate::storage_provider::parse_optional_condition(
            request.condition_expression.clone(),
            &request.expression_attribute_names,
            &request.expression_attribute_values,
        )?;
        let table_name = request.table_name;
        let table_info = self.get_table_info_cached_arc(&table_name).await?;
        let key_attributes = request.key.clone();
        validate_transact_key(&table_info, &key_attributes)?;
        let old_item = self
            .get_item_with_client(client, &table_name, &key_attributes, &table_info)
            .await?;
        if let Some(condition) = &condition
            && !evaluate_wire_condition(old_item.as_ref(), condition)?
        {
            if let Some(item_index) = item_index {
                return Err(transaction_canceled_for_reason(
                    item_index,
                    conditional_check_failed_reason_for_wire_item(
                        all_old(request.return_values_on_condition_check_failure.as_ref()),
                        old_item.as_ref(),
                    )?,
                ));
            }
            return Err(StorageEnum::ConditionalCheckFailed.into());
        }
        let Some(old_item) = old_item else {
            return Ok(());
        };

        let key_bindings = Self::key_column_bindings_for_schema(
            &table_info,
            &table_info.key_schema,
            &key_attributes,
            None,
        )?;
        let mut bind_values = Vec::with_capacity(key_bindings.len());
        let where_sql = Self::where_clause_for_bindings(&key_bindings, &mut bind_values);
        let table_name_safe = table_name.sanitized_name();
        let sql = sql_statements::delete_main_row(&table_name_safe, &where_sql);
        let params: Vec<&(dyn ToSql + Sync)> = bind_values
            .iter()
            .map(|value| value as &(dyn ToSql + Sync))
            .collect();
        client
            .execute(&sql, &params)
            .await
            .map_err(|err| Self::map_postgres_error("transact delete execute", err))?;
        let item_stream_version = storage_types::ItemStreamVersion::try_from(
            Self::bump_item_revision_with_client(client, &table_name, &key_attributes).await?,
        )?;

        let old_map = old_item.into_attribute_map()?;
        if self.immediate_gsi_consistency {
            self.delete_gsi_entries_for_item_with_client(
                client,
                &table_name,
                &table_info,
                &old_map,
            )
            .await?;
        }
        self.sync_ttl_index_entries_with_client(client, &table_info, Some(&old_map), None)
            .await?;
        self.write_stream_entries_for_item_with_client(
            client,
            &table_info,
            &old_map,
            PostgresWriteStreamEntriesInput {
                old_item: Some(&old_map),
                is_deleted: true,
                item_stream_version,
                replication: None,
            },
        )
        .await?;
        Ok(())
    }

    pub(super) async fn transact_update_with_client<C: GenericClient + Sync>(
        &self,
        client: &C,
        request: storage_types::TransactUpdateRequest,
        item_index: Option<usize>,
    ) -> StorageResult<()> {
        let (operations, condition) = before_update_item(
            request.update_expression.as_str(),
            request.condition_expression.as_deref(),
            request.expression_attribute_names.as_ref(),
            request.expression_attribute_values.as_ref(),
        )?;
        let table_name = request.table_name.clone();
        let table_info = self.get_table_info_cached_arc(&table_name).await?;
        let key_attributes = request.key.clone();
        let existing_item = self
            .get_item_with_client(client, &table_name, &key_attributes, &table_info)
            .await?;
        if let Some(condition) = &condition
            && !evaluate_wire_condition(existing_item.as_ref(), condition)?
        {
            if let Some(item_index) = item_index {
                return Err(transaction_canceled_for_reason(
                    item_index,
                    conditional_check_failed_reason_for_wire_item(
                        all_old(request.return_values_on_condition_check_failure.as_ref()),
                        existing_item.as_ref(),
                    )?,
                ));
            }
            return Err(StorageEnum::ConditionalCheckFailed.into());
        }
        let existing_item = existing_item
            .map(WireItem::into_attribute_map)
            .transpose()?;
        let item_to_update = existing_item.unwrap_or_else(|| request.key.to_attribute_map());
        let updated_item =
            apply_bound_update_operations(item_to_update, &operations).map_err(|error| {
                if let Some(index) = item_index {
                    transaction_canceled_for_item_error(index, error)
                } else {
                    error
                }
            })?;
        self.transact_put_with_client(
            client,
            storage_types::TransactPutRequest {
                table_name,
                item: updated_item,
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
            },
            None,
        )
        .await
    }

    pub(super) async fn transact_condition_check_with_client<C: GenericClient + Sync>(
        &self,
        client: &C,
        request: storage_types::TransactConditionCheckRequest,
        item_index: Option<usize>,
    ) -> StorageResult<()> {
        let table_info = self.get_table_info_cached_arc(&request.table_name).await?;
        let key_attributes = request.key.clone();
        validate_transact_key(&table_info, &key_attributes)?;
        let existing_item = self
            .get_item_with_client(client, &request.table_name, &key_attributes, &table_info)
            .await?;
        let condition = parse_condition_expression(
            &request.condition_expression,
            request.expression_attribute_names.as_ref(),
            request.expression_attribute_values.as_ref(),
        )
        .map_err(|err| StorageError::validation(format!("Invalid condition expression: {err}")))?;
        if !evaluate_wire_condition(existing_item.as_ref(), &condition)? {
            if let Some(item_index) = item_index {
                return Err(transaction_canceled_for_reason(
                    item_index,
                    conditional_check_failed_reason_for_wire_item(
                        all_old(request.return_values_on_condition_check_failure.as_ref()),
                        existing_item.as_ref(),
                    )?,
                ));
            }
            return Err(StorageEnum::ConditionalCheckFailed.into());
        }
        Ok(())
    }
}
