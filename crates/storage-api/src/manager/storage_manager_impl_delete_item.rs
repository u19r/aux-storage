use http_error::HttpApiError;
use storage::DeleteItemInput;
use storage_sync::SyncWriteRequest;
use storage_types::{AllOld, DeleteItemRequest, DeleteItemResponse};

use crate::{
    manager::{
        StorageApiManagerImpl,
        storage_manager_impl_condition_failure::{
            conditional_failure_with_old_item, should_return_old_item_on_condition_failure,
        },
        storage_manager_impl_sync_write_proposer::sync_response_at,
    },
    types::Response,
};

impl StorageApiManagerImpl {
    pub(super) async fn delete_item_internal(
        &self,
        request: DeleteItemRequest,
    ) -> Result<Response, HttpApiError> {
        let key_schema = self.db().get_table_key_schema(&request.table_name).await?;

        // Validate that all required key attributes are provided
        for key_element in &key_schema {
            if !request.key.contains_key(&key_element.attribute_name) {
                return Err(HttpApiError::validation_error(
                    "One or more parameter values were invalid".to_string(),
                ));
            }
        }
        let old_item_on_condition_failure = if should_return_old_item_on_condition_failure(
            request.condition_expression.as_deref(),
            request.return_values_on_condition_check_failure.as_ref(),
        ) {
            self.db()
                .get_item_map_with_consistent_read(
                    request.table_name.clone(),
                    request.key.clone(),
                    true,
                )
                .await?
                .map(Into::into)
        } else {
            None
        };

        if let Some(response) = self
            .propose_sync_write_if_configured(SyncWriteRequest::DeleteItem(request.clone()))
            .await?
        {
            return Ok(Response::DeleteItem(sync_response_at(
                &response,
                0,
                DeleteItemResponse { attributes: None },
            )?));
        }

        let return_deleted_item = matches!(request.return_values, Some(AllOld::AllOld));
        let deleted_item = self
            .db()
            .delete_item(DeleteItemInput {
                table_name: request.table_name,
                key: request.key,
                condition_expression: request.condition_expression,
                expression_attribute_names: request.expression_attribute_names,
                expression_attribute_values: request.expression_attribute_values,
                aux_item_stream_ttl_hours: request.aux_item_stream_ttl_hours,
            })
            .await
            .map_err(|error| {
                conditional_failure_with_old_item(error, old_item_on_condition_failure)
            })?;

        let response = DeleteItemResponse {
            attributes: if return_deleted_item
                && deleted_item.as_ref().is_some_and(|di| !di.is_empty())
            {
                deleted_item.map(Into::into)
            } else {
                None
            },
        };
        Ok(Response::DeleteItem(response))
    }
}
