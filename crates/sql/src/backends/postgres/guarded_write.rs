use std::collections::HashMap;

use storage_condition::evaluate_condition;
use storage_provider::{
    apply_bound_update_operations, before_update_item_optional,
    split_item_into_key_and_attributes_sync, update_item_response, updated_attributes_for_response,
};
use storage_types::{
    AllOld, GuardedDeleteItemRequest, GuardedPutItemRequest, GuardedUpdateItemRequest,
    PutItemResponse, ReturnValuesOldNewUpdated, StorageEnum, StorageError, StorageResult,
    UpdateItemRequest, UpdateItemResponse, WireItem,
};

use crate::{
    backends::postgres::{
        PostgresStorageProvider, record_write,
        transaction_helpers::{PostgresUpsertTransactItemInput, condition_item_ref},
    },
    billing_metrics::{attr_map_payload_bytes, record_write_cost, serializable_payload_bytes},
};

impl PostgresStorageProvider {
    pub(super) async fn do_guarded_put_item(
        &self,
        request: GuardedPutItemRequest,
    ) -> StorageResult<PutItemResponse> {
        let GuardedPutItemRequest {
            table_name,
            item,
            guard,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            indexers,
        } = request;
        if item.is_empty() {
            return Err(StorageError::validation("Item is empty"));
        }
        let table_info = self.get_table_info_cached_arc(&table_name).await?;
        let split_item = split_item_into_key_and_attributes_sync(item.clone(), &table_info)?;
        let bytes_written = attr_map_payload_bytes(&item);
        let response = self
            .retry_postgres_conflicts("guarded_put_item", || {
                let table_name = table_name.clone();
                let item = item.clone();
                let guard = guard.clone();
                let key_attributes = split_item.key_attributes.clone();
                let condition_expression = condition_expression.clone();
                let expression_attribute_names = expression_attribute_names.clone();
                let expression_attribute_values = expression_attribute_values.clone();
                let return_values = return_values.clone();
                let indexers = indexers.clone();
                let table_info = table_info.clone();
                async move {
                    let mut client = self.acquire_client("guarded_put_item").await?;
                    let _connection_hold = self.connection_hold_timer("guarded_put_item");
                    let transaction = self
                        .begin_transaction(
                            &mut client,
                            "guarded_put_item",
                            "start guarded_put_item transaction",
                        )
                        .await?;
                    let _transaction_hold = self.transaction_hold_timer("guarded_put_item");
                    Self::validate_durable_guard_with_client(
                        &transaction,
                        &table_name,
                        &key_attributes,
                        &guard,
                    )
                    .await?;
                    let old_item = self
                        .get_item_with_client(
                            &transaction,
                            &table_name,
                            &key_attributes,
                            &table_info,
                        )
                        .await?;
                    self.transact_put_with_client(
                        &transaction,
                        storage_types::TransactPutRequest {
                            indexers: Some(indexers),
                            table_name: table_name.clone(),
                            item,
                            condition_expression,
                            expression_attribute_names,
                            expression_attribute_values,
                            return_values_on_condition_check_failure: None,
                            aux_item_stream_ttl_hours: None,
                        },
                        None,
                    )
                    .await?;
                    transaction.commit().await.map_err(|err| {
                        Self::map_postgres_write_error("commit guarded_put_item transaction", err)
                    })?;
                    let attributes = if matches!(return_values, Some(AllOld::AllOld)) {
                        old_item.map(WireItem::into_attribute_map).transpose()?
                    } else {
                        None
                    };
                    Ok(PutItemResponse {
                        attributes: attributes.map(Into::into),
                    })
                }
            })
            .await?;
        record_write(1, bytes_written as usize);
        record_write_cost("guarded_put_item", "put", 1, bytes_written);
        Ok(response)
    }

    pub(super) async fn do_guarded_delete_item(
        &self,
        request: GuardedDeleteItemRequest,
    ) -> StorageResult<Option<HashMap<String, storage_provider::AttributeValue>>> {
        let GuardedDeleteItemRequest {
            table_name,
            key,
            guard,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
        } = request;
        let key_bytes = attr_map_payload_bytes(&key);
        let table_info = self.get_table_info_cached_arc(&table_name).await?;
        let key_attributes = key.clone();
        let result = self
            .retry_postgres_conflicts("guarded_delete_item", || {
                let table_name = table_name.clone();
                let key = key.clone();
                let guard = guard.clone();
                let key_attributes = key_attributes.clone();
                let condition_expression = condition_expression.clone();
                let expression_attribute_names = expression_attribute_names.clone();
                let expression_attribute_values = expression_attribute_values.clone();
                let table_info = table_info.clone();
                async move {
                    let mut client = self.acquire_client("guarded_delete_item").await?;
                    let _connection_hold = self.connection_hold_timer("guarded_delete_item");
                    let transaction = self
                        .begin_transaction(
                            &mut client,
                            "guarded_delete_item",
                            "start guarded_delete_item transaction",
                        )
                        .await?;
                    let _transaction_hold = self.transaction_hold_timer("guarded_delete_item");
                    Self::validate_durable_guard_with_client(
                        &transaction,
                        &table_name,
                        &key_attributes,
                        &guard,
                    )
                    .await?;
                    let old_item = self
                        .get_item_with_client(
                            &transaction,
                            &table_name,
                            &key_attributes,
                            &table_info,
                        )
                        .await?;
                    self.transact_delete_with_client(
                        &transaction,
                        storage_types::TransactDeleteRequest {
                            table_name: table_name.clone(),
                            key,
                            condition_expression,
                            expression_attribute_names,
                            expression_attribute_values,
                            return_values_on_condition_check_failure: None,
                            aux_item_stream_ttl_hours: None,
                        },
                        None,
                    )
                    .await?;
                    transaction.commit().await.map_err(|err| {
                        Self::map_postgres_write_error(
                            "commit guarded_delete_item transaction",
                            err,
                        )
                    })?;
                    old_item.map(WireItem::into_attribute_map).transpose()
                }
            })
            .await?;
        record_write(usize::from(result.is_some()), 0);
        record_write_cost("guarded_delete_item", "delete", 1, key_bytes);
        Ok(result)
    }

    pub(super) async fn do_guarded_update_item(
        &self,
        request: GuardedUpdateItemRequest,
    ) -> StorageResult<UpdateItemResponse> {
        let GuardedUpdateItemRequest { request, guard } = request;
        let billed_bytes = serializable_payload_bytes(&request);
        let UpdateItemRequest {
            table_name,
            key,
            update_expression,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            indexers,
            ..
        } = request;

        let (operations, condition) = before_update_item_optional(
            update_expression.as_deref(),
            condition_expression.as_deref(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )?;
        let table_info = self.get_table_info_cached_arc(&table_name).await?;
        let key_attributes = key.clone();
        let response = self
            .retry_postgres_conflicts("guarded_update_item", || {
                let table_name = table_name.clone();
                let key = key.clone();
                let key_attributes = key_attributes.clone();
                let operations = operations.clone();
                let condition = condition.clone();
                let return_values = return_values.clone();
                let table_info = table_info.clone();
                let guard = guard.clone();
                let indexers = indexers.clone();
                async move {
                    let mut client = self.acquire_client("guarded_update_item").await?;
                    let _connection_hold = self.connection_hold_timer("guarded_update_item");
                    let transaction = self
                        .begin_transaction(
                            &mut client,
                            "guarded_update_item",
                            "start guarded_update_item transaction",
                        )
                        .await?;
                    let _transaction_hold = self.transaction_hold_timer("guarded_update_item");
                    Self::validate_durable_guard_with_client(
                        &transaction,
                        &table_name,
                        &key_attributes,
                        &guard,
                    )
                    .await?;

                    let prepared =
                        Self::prepare_get_item_query(&table_name, &key_attributes, &table_info)?;
                    let existing = self
                        .execute_prepared_get_item_query_with_indexers(
                            &transaction,
                            &prepared,
                            "guarded_update_item",
                            "old_item_query",
                        )
                        .await?;
                    let (existing_item, stored_indexers) = match existing {
                        Some(item) => (Some(item.item.into_attribute_map()?), item.indexers),
                        None => (None, Vec::new()),
                    };
                    let effective_indexers =
                        indexers.as_deref().unwrap_or(stored_indexers.as_slice());
                    if let Some(condition) = &condition
                        && !evaluate_condition(
                            condition_item_ref(existing_item.as_ref()),
                            condition,
                        )
                    {
                        return Err(StorageEnum::ConditionalCheckFailed.into());
                    }

                    let old_item_for_write = existing_item.clone();
                    let item_to_update = existing_item.unwrap_or_else(|| key.to_attribute_map());
                    let old_item_for_response = match return_values.as_ref() {
                        Some(ReturnValuesOldNewUpdated::AllOld) => Some(item_to_update.clone()),
                        Some(ReturnValuesOldNewUpdated::UpdatedOld) => {
                            let attributes =
                                updated_attributes_for_response(&operations, &item_to_update);
                            (!attributes.is_empty()).then_some(attributes)
                        }
                        _ => None,
                    };
                    let updated_item =
                        apply_bound_update_operations(item_to_update.clone(), &operations)?;
                    self.upsert_transact_item_with_client(
                        &transaction,
                        PostgresUpsertTransactItemInput {
                            table_name: &table_name,
                            table_info: &table_info,
                            item: updated_item.clone(),
                            indexers: effective_indexers,
                            old_item: old_item_for_write.as_ref(),
                            old_indexers: old_item_for_write
                                .as_ref()
                                .map(|_| stored_indexers.as_slice()),
                            item_stream_ttl_hours: None,
                        },
                    )
                    .await?;
                    transaction.commit().await.map_err(|err| {
                        Self::map_postgres_write_error(
                            "commit guarded_update_item transaction",
                            err,
                        )
                    })?;

                    if matches!(
                        return_values.as_ref(),
                        Some(ReturnValuesOldNewUpdated::UpdatedOld)
                    ) {
                        return Ok(UpdateItemResponse {
                            attributes: old_item_for_response.map(Into::into),
                        });
                    }

                    update_item_response(
                        &operations,
                        old_item_for_response,
                        Some(updated_item),
                        return_values.as_ref(),
                    )
                }
            })
            .await?;
        record_write(1, 0);
        record_write_cost("guarded_update_item", "update", 1, billed_bytes);
        Ok(response)
    }
}
