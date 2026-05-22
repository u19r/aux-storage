use std::collections::HashMap;

use storage_condition::evaluate_condition;
use storage_provider::{
    apply_bound_update_operations, before_update_item, return_values_need_old_item,
    split_item_into_key_and_attributes_sync, update_item_response,
};
use storage_types::{
    AllOld, GuardedDeleteItemRequest, GuardedPutItemRequest, GuardedUpdateItemRequest,
    PutItemResponse, StorageEnum, StorageError, StorageResult, UpdateItemRequest,
    UpdateItemResponse, WireItem,
};

use crate::{
    backends::postgres::{PostgresStorageProvider, record_write},
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
                let table_info = table_info.clone();
                async move {
                    let mut client = self
                        .pool
                        .get()
                        .await
                        .map_err(Self::map_postgres_client_acquire_error)?;
                    let transaction = client.transaction().await.map_err(|err| {
                        Self::map_postgres_write_error("start guarded_put_item transaction", err)
                    })?;
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
                            table_name: table_name.clone(),
                            item,
                            condition_expression,
                            expression_attribute_names,
                            expression_attribute_values,
                            return_values_on_condition_check_failure: None,
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
                    let mut client = self
                        .pool
                        .get()
                        .await
                        .map_err(Self::map_postgres_client_acquire_error)?;
                    let transaction = client.transaction().await.map_err(|err| {
                        Self::map_postgres_write_error("start guarded_delete_item transaction", err)
                    })?;
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
            ..
        } = request;

        let (operations, condition) = before_update_item(
            update_expression.as_str(),
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
                async move {
                    let mut client = self
                        .pool
                        .get()
                        .await
                        .map_err(Self::map_postgres_client_acquire_error)?;
                    let transaction = client.transaction().await.map_err(|err| {
                        Self::map_postgres_write_error("start guarded_update_item transaction", err)
                    })?;
                    Self::validate_durable_guard_with_client(
                        &transaction,
                        &table_name,
                        &key_attributes,
                        &guard,
                    )
                    .await?;

                    let existing_item = self
                        .get_item_with_client(
                            &transaction,
                            &table_name,
                            &key_attributes,
                            &table_info,
                        )
                        .await?
                        .map(WireItem::into_attribute_map)
                        .transpose()?;
                    if let Some(condition) = &condition {
                        let empty_item = HashMap::new();
                        let old_item_for_condition = existing_item.as_ref().unwrap_or(&empty_item);
                        if !evaluate_condition(old_item_for_condition, condition) {
                            return Err(StorageEnum::ConditionalCheckFailed.into());
                        }
                    }

                    let item_to_update = existing_item.unwrap_or_else(|| key.to_attribute_map());
                    let old_item_for_response = return_values_need_old_item(return_values.as_ref())
                        .then(|| item_to_update.clone());
                    let updated_item = apply_bound_update_operations(item_to_update, &operations)?;
                    self.transact_put_with_client(
                        &transaction,
                        storage_types::TransactPutRequest {
                            table_name: table_name.clone(),
                            item: updated_item.clone(),
                            condition_expression: None,
                            expression_attribute_names: None,
                            expression_attribute_values: None,
                            return_values_on_condition_check_failure: None,
                        },
                        None,
                    )
                    .await?;
                    transaction.commit().await.map_err(|err| {
                        Self::map_postgres_write_error(
                            "commit guarded_update_item transaction",
                            err,
                        )
                    })?;

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
