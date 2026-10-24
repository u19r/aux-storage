use storage_types::{StorageResult, TransactGetItemsRequest, TransactGetItemsResponse};

use crate::{
    admission::AdmissionClass,
    database_manager::{DatabaseManager, ROUTED_DEFAULT_CONNECTION_ID, record_storage_operation},
};

impl DatabaseManager {
    pub async fn transact_get_items(
        &self,
        request: TransactGetItemsRequest,
    ) -> StorageResult<TransactGetItemsResponse> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::RangeRead,
            |dynamo| async move {
                record_storage_operation("transact_get_items", dynamo.transact_get_items(request))
                    .await
            },
        )
        .await
    }
}
