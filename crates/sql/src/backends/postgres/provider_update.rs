use storage_provider::{before_update_item, update_item_response};
use storage_types::{StorageResult, UpdateItemRequest, UpdateItemResponse, WireItem};

use crate::{
    backends::postgres::{PostgresStorageProvider, record_write},
    billing_metrics::{record_write_cost, serializable_payload_bytes},
    provider_core::write::plan_update_from_existing_item,
};

impl PostgresStorageProvider {
    pub(super) async fn do_update_item(
        &self,
        request: UpdateItemRequest,
    ) -> StorageResult<UpdateItemResponse> {
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
            .retry_postgres_conflicts("update_item", || {
                let table_name = table_name.clone();
                let key = key.clone();
                let key_attributes = key_attributes.clone();
                let operations = operations.clone();
                let condition = condition.clone();
                let return_values = return_values.clone();
                let table_info = table_info.clone();
                async move {
                    let mut client = self
                        .pool
                        .get()
                        .await
                        .map_err(Self::map_postgres_client_acquire_error)?;
                    let transaction = client.transaction().await.map_err(|err| {
                        Self::map_postgres_write_error("start update_item transaction", err)
                    })?;

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
                    let (item_to_update, updated_item) = plan_update_from_existing_item(
                        existing_item,
                        &key,
                        &operations,
                        condition.as_ref(),
                    )?;
                    self.transact_put_with_client(
                        &transaction,
                        storage_types::TransactPutRequest {
                            table_name,
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
                        Self::map_postgres_write_error("commit update_item transaction", err)
                    })?;

                    update_item_response(
                        &operations,
                        Some(item_to_update),
                        Some(updated_item),
                        return_values.as_ref(),
                    )
                }
            })
            .await?;
        record_write(1, 0);
        record_write_cost("update_item", "update", 1, billed_bytes);
        Ok(response)
    }
}
