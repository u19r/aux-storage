use http_error::HttpApiError;
use storage::PutItemInput;
use storage_sync::SyncWriteRequest;
use storage_types::{PutItemRequest, PutItemResponse};

use crate::{
    manager::{
        StorageApiManagerImpl,
        storage_manager_impl_condition_failure::{
            conditional_failure_with_old_item, key_from_item,
            should_return_old_item_on_condition_failure,
        },
        storage_manager_impl_sync_write_proposer::sync_response_at,
    },
    types::Response,
};

impl StorageApiManagerImpl {
    pub(super) async fn put_item_internal(
        &self,
        request: PutItemRequest,
    ) -> Result<Response, HttpApiError> {
        let table_info = self.db().get_table_info(&request.table_name).await?;

        // Validate that all required key attributes are provided
        for key_element in &table_info.key_schema {
            if !request.item.contains_key(&key_element.attribute_name) {
                return Err(HttpApiError::validation_error(
                    "One or more parameter values were invalid".to_string(),
                ));
            }
        }
        let old_item_on_condition_failure = if should_return_old_item_on_condition_failure(
            request.condition_expression.as_deref(),
            request.return_values_on_condition_check_failure.as_ref(),
        ) {
            let key = key_from_item(&table_info.key_schema, &request.item);
            if let Some(key) = key {
                self.db()
                    .get_item_map_with_consistent_read(request.table_name.clone(), key, true)
                    .await?
                    .map(Into::into)
            } else {
                None
            }
        } else {
            None
        };

        if let Some(response) = self
            .propose_sync_write_if_configured(SyncWriteRequest::PutItem(request.clone()))
            .await?
        {
            return Ok(Response::PutItem(sync_response_at(
                &response,
                0,
                PutItemResponse { attributes: None },
            )?));
        }

        let response = self
            .db()
            .put_item(PutItemInput {
                table_name: request.table_name,
                item: request.item.into(),
                condition_expression: request.condition_expression,
                expression_attribute_names: request.expression_attribute_names,
                expression_attribute_values: request.expression_attribute_values,
                return_values: request.return_values,
                aux_item_stream_ttl_hours: request.aux_item_stream_ttl_hours,
            })
            .await
            .map_err(|error| {
                conditional_failure_with_old_item(error, old_item_on_condition_failure)
            })?;

        Ok(Response::PutItem(response))
    }
}
