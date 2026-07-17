use http_error::HttpApiError;
use storage::DeleteItemInput;
use storage_sync::SyncWriteRequest;
use storage_types::{AllOld, DeleteItemRequest, DeleteItemResponse};

use crate::{
    manager::{
        StorageApiManagerImpl,
        storage_manager_impl_condition_failure::should_return_old_item_on_condition_failure,
        storage_manager_impl_sync_write_proposer::sync_response_at,
    },
    types::Response,
};

impl StorageApiManagerImpl {
    pub(super) async fn delete_item_internal(
        &self,
        request: DeleteItemRequest,
    ) -> Result<Response, HttpApiError> {
        let operation = self
            .db()
            .resolve_storage_operation(request.table_name.clone())
            .await?;

        // Validate that all required key attributes are provided
        for key_element in &operation.table_info().key_schema {
            if !request.key.contains_key(&key_element.attribute_name) {
                return Err(HttpApiError::validation_error(
                    "One or more parameter values were invalid".to_string(),
                ));
            }
        }
        let return_old_on_condition_failure = should_return_old_item_on_condition_failure(
            request.condition_expression.as_deref(),
            request.return_values_on_condition_check_failure.as_ref(),
        );

        if let Some(response) = self
            .propose_sync_write_if_configured(|| SyncWriteRequest::DeleteItem(request.clone()))
            .await?
        {
            return Ok(Response::DeleteItem(sync_response_at(
                &response,
                0,
                DeleteItemResponse { attributes: None },
            )?));
        }

        let return_deleted_item = matches!(request.return_values, Some(AllOld::AllOld));
        let input = DeleteItemInput {
            table_name: request.table_name,
            key: request.key,
            condition_expression: request.condition_expression,
            expression_attribute_names: request.expression_attribute_names,
            expression_attribute_values: request.expression_attribute_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours: request.aux_item_stream_ttl_hours,
        };
        let deleted_item = self
            .db()
            .delete_item_with_resolved_operation(operation, input)
            .await?;

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
