use deadpool_postgres::GenericClient;
use storage_condition::{evaluate_condition, parse_condition_expression};
use storage_provider::{
    apply_bound_update_operations, before_update_item, split_item_into_key_and_attributes_sync,
};
use storage_types::{
    StorageEnum, StorageError, StorageResult, WireItem, transaction_canceled_for_item_error,
};
use tokio_postgres::types::ToSql;

use crate::{
    backends::postgres::{
        PostgresStorageProvider, sql_statements, stream_helpers::PostgresWriteStreamEntriesInput,
    },
    provider_core::transaction::{
        all_old, conditional_check_failed_reason, transaction_canceled_for_reason,
        validate_transact_key, validate_transact_put_item_key,
    },
};

impl PostgresStorageProvider {
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
        let old_item_for_condition = old_item
            .as_ref()
            .map(WireItem::to_attribute_map)
            .transpose()?;

        if let Some(condition) = &condition
            && !evaluate_condition(
                &old_item_for_condition.clone().unwrap_or_default(),
                condition,
            )
        {
            if let Some(item_index) = item_index {
                return Err(transaction_canceled_for_reason(
                    item_index,
                    conditional_check_failed_reason(
                        return_all_old
                            .then_some(old_item_for_condition.as_ref())
                            .flatten(),
                    )?,
                ));
            }
            return Err(StorageEnum::ConditionalCheckFailed.into());
        }

        let attributes_blob = if split_item.non_key_attributes.is_empty() {
            "{}".to_string()
        } else {
            serde_json::to_string(&split_item.non_key_attributes)
                .map_err(|err| Self::map_postgres_error("serialize non-key attributes", err))?
        };
        let key_bindings = Self::key_column_bindings_for_schema(
            &table_info,
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
        let sql = sql_statements::upsert_main_row(
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
        let params: Vec<&(dyn ToSql + Sync)> = bind_values
            .iter()
            .map(|value| value as &(dyn ToSql + Sync))
            .collect();
        client
            .execute(&sql, &params)
            .await
            .map_err(|err| Self::map_postgres_error("transact put execute", err))?;
        let item_stream_version = storage_types::ItemStreamVersion::try_from(
            Self::bump_item_revision_with_client(client, &table_name, &split_item.key_attributes)
                .await?,
        )?;

        if self.immediate_gsi_consistency {
            self.apply_gsi_entries_for_item_change_with_client(
                client,
                &table_name,
                &table_info,
                old_item_for_condition.as_ref(),
                Some(&split_item.all_attributes),
            )
            .await?;
        }
        self.sync_ttl_index_entries_with_client(
            client,
            &table_info,
            old_item_for_condition.as_ref(),
            Some(&split_item.all_attributes),
        )
        .await?;
        self.write_stream_entries_for_item_with_client(
            client,
            &table_info,
            &split_item.all_attributes,
            PostgresWriteStreamEntriesInput {
                old_item: old_item_for_condition.as_ref(),
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
        let old_item_for_condition = old_item
            .as_ref()
            .map(WireItem::to_attribute_map)
            .transpose()?;
        if let Some(condition) = &condition
            && !evaluate_condition(
                &old_item_for_condition.clone().unwrap_or_default(),
                condition,
            )
        {
            if let Some(item_index) = item_index {
                return Err(transaction_canceled_for_reason(
                    item_index,
                    conditional_check_failed_reason(
                        all_old(request.return_values_on_condition_check_failure.as_ref())
                            .then_some(old_item_for_condition.as_ref())
                            .flatten(),
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
            .await?
            .map(WireItem::into_attribute_map)
            .transpose()?;
        let item_to_update = existing_item
            .clone()
            .unwrap_or_else(|| request.key.to_attribute_map());
        if let Some(condition) = &condition {
            let old_item_for_condition = existing_item.clone().unwrap_or_default();
            if !evaluate_condition(&old_item_for_condition, condition) {
                if let Some(item_index) = item_index {
                    return Err(transaction_canceled_for_reason(
                        item_index,
                        conditional_check_failed_reason(None)?,
                    ));
                }
                return Err(StorageEnum::ConditionalCheckFailed.into());
            }
        }
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
            .await?
            .map(WireItem::into_attribute_map)
            .transpose()?;
        let condition = parse_condition_expression(
            &request.condition_expression,
            request.expression_attribute_names.as_ref(),
            request.expression_attribute_values.as_ref(),
        )
        .map_err(|err| StorageError::validation(format!("Invalid condition expression: {err}")))?;
        if !evaluate_condition(&existing_item.clone().unwrap_or_default(), &condition) {
            if let Some(item_index) = item_index {
                return Err(transaction_canceled_for_reason(
                    item_index,
                    conditional_check_failed_reason(
                        all_old(request.return_values_on_condition_check_failure.as_ref())
                            .then_some(existing_item.as_ref())
                            .flatten(),
                    )?,
                ));
            }
            return Err(StorageEnum::ConditionalCheckFailed.into());
        }
        Ok(())
    }
}
