use http_error::HttpApiError;
use storage_types::{
    GetItemRequest, StorageEnum, StorageError, context::WrappedError,
    validate_transact_key,
};

use crate::{
    get_wire_response::GetWireResponse,
    manager::{StorageApiManagerImpl, storage_manager_impl_expression::project_attribute_map},
    types::Response,
};

impl StorageApiManagerImpl {
    pub(super) async fn get_item_internal(
        &self,
        request: GetItemRequest,
    ) -> Result<Response, HttpApiError> {
        let operation = self
            .db()
            .resolve_storage_operation(request.table_name)
            .await?;
        validate_transact_key(operation.table_info(), &request.key)
            .map_err(key_validation_error)?;
        let operation = operation
            .validate_key(request.key)
            .map_err(get_item_key_validation_error)?;

        let consistent_read = request.consistent_read.unwrap_or(false);
        self.ensure_sync_read_barrier(consistent_read).await?;
        let item = self
            .db()
            .get_item_with_resolved_operation(operation, consistent_read)
            .await?;

        if request.projection_expression.is_some() || request.attributes_to_get.is_some() {
            let item = item
                .map(storage_types::WireItem::into_attribute_map)
                .transpose()?
                .map(|item| {
                    project_attribute_map(
                        item.into(),
                        request.projection_expression.as_deref(),
                        request.attributes_to_get.as_deref(),
                        request.expression_attribute_names.as_ref(),
                    )
                });
            return Ok(Response::GetItem(storage_types::GetItemResponse { item }));
        }

        Ok(Response::GetWire(GetWireResponse::from(item)))
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
    if message == "The parameter cannot be converted to a numeric value" {
        return HttpApiError::from(StorageError::raw_validation(
            "The parameter cannot be converted to a numeric value: ",
        ));
    }
    if message == "Attempting to store more than 38 significant digits in a Number"
        || message
            == "Number underflow. Attempting to store a number with magnitude smaller than \
                supported range"
    {
        return HttpApiError::from(StorageError::raw_validation(message.clone()));
    }
    HttpApiError::from(StorageError::validation(message.clone()))
}

fn get_item_key_validation_error(error: StorageError) -> HttpApiError {
    let StorageEnum::Validation { message } = error.to_enum() else {
        return HttpApiError::from(error);
    };
    if message == "The parameter cannot be converted to a numeric value" {
        return HttpApiError::from(StorageError::validation(
            "The parameter cannot be converted to a numeric value: ",
        ));
    }
    HttpApiError::from(StorageError::validation(message.clone()))
}
