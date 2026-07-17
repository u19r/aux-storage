use http_error::HttpApiError;
use storage::PutItemInput;
use storage_sync::SyncWriteRequest;
use storage_types::{PutItemRequest, PutItemResponse};

use crate::{
    manager::{
        StorageApiManagerImpl,
        storage_manager_impl_condition_failure::should_return_old_item_on_condition_failure,
        storage_manager_impl_sync_write_proposer::sync_response_at,
    },
    types::Response,
};

impl StorageApiManagerImpl {
    pub(super) async fn put_item_internal(
        &self,
        request: PutItemRequest,
    ) -> Result<Response, HttpApiError> {
        let operation = self
            .db()
            .resolve_storage_operation(request.table_name.clone())
            .await?;

        // Validate that all required key attributes are provided
        for key_element in &operation.table_info().key_schema {
            if !request.item.contains_key(&key_element.attribute_name) {
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
            .propose_sync_write_if_configured(|| SyncWriteRequest::PutItem(request.clone()))
            .await?
        {
            return Ok(Response::PutItem(sync_response_at(
                &response,
                0,
                PutItemResponse { attributes: None },
            )?));
        }

        let input = PutItemInput {
            table_name: request.table_name,
            item: request.item.into(),
            condition_expression: request.condition_expression,
            expression_attribute_names: request.expression_attribute_names,
            expression_attribute_values: request.expression_attribute_values,
            return_values: request.return_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours: request.aux_item_stream_ttl_hours,
        };
        let response = self
            .db()
            .put_item_with_resolved_operation(operation, input)
            .await?;

        Ok(Response::PutItem(response))
    }
}
