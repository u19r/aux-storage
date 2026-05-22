use http_error::HttpApiError;
use storage::UpdateItemInput;
use storage_sync::SyncWriteRequest;
use storage_types::{StorageError, UpdateItemRequest, UpdateItemResponse, validate_transact_key};

use crate::{
    manager::{StorageApiManagerImpl, storage_manager_impl_sync_write_proposer::sync_response_at},
    types::Response,
};

impl StorageApiManagerImpl {
    pub(super) async fn update_item_internal(
        &self,
        request: UpdateItemRequest,
    ) -> Result<Response, HttpApiError> {
        let table_info = self.db().get_table_info(&request.table_name).await?;
        validate_transact_key(&table_info, &request.key)
            .map_err(|_| key_schema_validation_error())?;

        if let Some(response) = self
            .propose_sync_write_if_configured(SyncWriteRequest::UpdateItem(request.clone()))
            .await?
        {
            return Ok(Response::UpdateItem(sync_response_at(
                &response,
                0,
                UpdateItemResponse { attributes: None },
            )?));
        }

        let result = self
            .db()
            .update_item(UpdateItemInput {
                table_name: request.table_name,
                key: request.key,
                update_expression: request.update_expression,
                condition_expression: request.condition_expression,
                expression_attribute_names: request.expression_attribute_names,
                expression_attribute_values: request.expression_attribute_values,
                return_values: request.return_values,
            })
            .await?;

        Ok(Response::UpdateItem(result))
    }
}

fn key_schema_validation_error() -> HttpApiError {
    HttpApiError::from(StorageError::validation(
        "The provided key element does not match the schema",
    ))
}
