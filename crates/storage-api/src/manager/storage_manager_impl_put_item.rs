use http_error::HttpApiError;
use storage::PutItemInput;
use storage_sync::SyncWriteRequest;
use storage_types::{PutItemRequest, PutItemResponse};

use crate::{
    manager::{StorageApiManagerImpl, storage_manager_impl_sync_write_proposer::sync_response_at},
    types::Response,
};

impl StorageApiManagerImpl {
    pub(super) async fn put_item_internal(
        &self,
        request: PutItemRequest,
    ) -> Result<Response, HttpApiError> {
        let key_schema = self.db().get_table_key_schema(&request.table_name).await?;

        // Validate that all required key attributes are provided
        for key_element in &key_schema {
            if !request.item.contains_key(&key_element.attribute_name) {
                return Err(HttpApiError::validation_error(
                    "One or more parameter values were invalid".to_string(),
                ));
            }
        }

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
            })
            .await?;

        Ok(Response::PutItem(response))
    }
}
