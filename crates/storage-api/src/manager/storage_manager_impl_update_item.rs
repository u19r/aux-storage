use http_error::HttpApiError;
use storage::UpdateItemInput;
use storage_sync::SyncWriteRequest;
use storage_types::{
    StorageEnum, StorageError, UpdateItemRequest, UpdateItemResponse, context::WrappedError as _,
    validate_transact_key,
};

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
    pub(super) async fn update_item_internal(
        &self,
        request: UpdateItemRequest,
    ) -> Result<Response, HttpApiError> {
        let table_info = self.db().get_table_info(&request.table_name).await?;
        validate_transact_key(&table_info, &request.key).map_err(key_validation_error)?;
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
                update_expression: request.update_expression.unwrap_or_default(),
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

        Ok(Response::UpdateItem(result))
    }
}

fn key_schema_validation_error() -> HttpApiError {
    HttpApiError::from(StorageError::validation(
        "The provided key element does not match the schema",
    ))
}

fn key_validation_error(error: StorageError) -> HttpApiError {
    let StorageEnum::Validation { message } = error.to_enum() else {
        return HttpApiError::from(error);
    };
    if message == "The provided key element does not match the schema" {
        return key_schema_validation_error();
    }
    HttpApiError::from(StorageError::validation(message.clone()))
}
