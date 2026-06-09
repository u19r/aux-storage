use storage_types::{StorageError, StorageResult, validate_expression_attribute_usage};

use crate::database_manager::{
    sync_condition_ops::evaluate_optional_condition, sync_resolver_ops::SyncWriteResolver,
    sync_serialization::stable_key_json,
};

impl SyncWriteResolver<'_> {
    pub(super) async fn resolve_transact_write_items(
        &mut self,
        request: storage_types::TransactWriteItemsRequest,
    ) -> StorageResult<()> {
        if request.client_request_token.is_some() {
            return Err(StorageError::unsupported(
                "TransactWriteItems client request tokens are not implemented for sync resolution \
                 yet",
            ));
        }
        for item in request.transact_items {
            match (item.put, item.update, item.delete, item.condition_check) {
                (Some(put), None, None, None) => {
                    self.resolve_put(
                        put.table_name,
                        put.item,
                        put.condition_expression,
                        put.expression_attribute_names,
                        put.expression_attribute_values,
                        None,
                    )
                    .await?;
                }
                (None, Some(update), None, None) => {
                    self.resolve_update(storage_types::UpdateItemRequest {
                        table_name: update.table_name,
                        key: update.key,
                        update_expression: Some(update.update_expression),
                        attribute_updates: None,
                        condition_expression: update.condition_expression,
                        expression_attribute_names: update.expression_attribute_names,
                        expression_attribute_values: update.expression_attribute_values,
                        expected: None,
                        conditional_operator: None,
                        return_values: None,
                        return_consumed_capacity: None,
                        return_item_collection_metrics: None,
                        return_values_on_condition_check_failure: None,
                        aux_item_stream_ttl_hours: None,
                    })
                    .await?;
                }
                (None, None, Some(delete), None) => {
                    self.resolve_delete(
                        delete.table_name,
                        delete.key,
                        delete.condition_expression,
                        delete.expression_attribute_names,
                        delete.expression_attribute_values,
                    )
                    .await?;
                }
                (None, None, None, Some(condition_check)) => {
                    self.resolve_condition_check(condition_check).await?;
                }
                _ => {
                    return Err(StorageError::validation(
                        "TransactWriteItems entries must contain exactly one operation",
                    ));
                }
            }
        }
        Ok(())
    }

    async fn resolve_condition_check(
        &mut self,
        request: storage_types::TransactConditionCheckRequest,
    ) -> StorageResult<()> {
        validate_expression_attribute_usage(
            request.expression_attribute_names.as_ref(),
            request.expression_attribute_values.as_ref(),
            Some(request.condition_expression.as_str()).into_iter(),
        )?;
        let key_json = stable_key_json(&request.key)?;
        let old_item = self
            .current_item(&request.table_name, &request.key, &key_json)
            .await?;
        evaluate_optional_condition(
            old_item.item.as_ref(),
            Some(&request.condition_expression),
            request.expression_attribute_names.as_ref(),
            request.expression_attribute_values.as_ref(),
        )?;
        self.record_read(&request.table_name, &key_json, &old_item);
        Ok(())
    }
}
