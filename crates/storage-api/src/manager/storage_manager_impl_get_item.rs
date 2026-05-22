use http_error::HttpApiError;
use storage_types::{GetItemRequest, StorageError, validate_transact_key};

use crate::{get_wire_response::GetWireResponse, manager::StorageApiManagerImpl, types::Response};

impl StorageApiManagerImpl {
    pub(super) async fn get_item_internal(
        &self,
        request: GetItemRequest,
    ) -> Result<Response, HttpApiError> {
        let table_info = self.db().get_table_info(&request.table_name).await?;
        validate_transact_key(&table_info, &request.key)
            .map_err(|_| key_schema_validation_error())?;

        let consistent_read = request.consistent_read.unwrap_or(false);
        self.ensure_sync_read_barrier(consistent_read).await?;
        let item = self
            .db()
            .get_item_with_consistent_read(request.table_name, request.key, consistent_read)
            .await?;

        Ok(Response::GetWire(GetWireResponse::from(item)))
    }
}

fn key_schema_validation_error() -> HttpApiError {
    HttpApiError::from(StorageError::validation(
        "The provided key element does not match the schema",
    ))
}
