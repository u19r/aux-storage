use storage_types::{
    BatchWriteItemEncodeRequest, BatchWriteItemRequest, BatchWriteItemResponse, StorageResult,
    TransactWriteItemsEncodeRequest, TransactWriteItemsResponse,
};

use crate::{
    admission::AdmissionClass,
    database_manager::{DatabaseManager, record_storage_operation},
    updated_at_apply::stamp_batch_write_request,
};

impl DatabaseManager {
    pub async fn batch_write_item(
        &self,
        mut request: BatchWriteItemRequest,
    ) -> StorageResult<BatchWriteItemResponse> {
        if self.single_table_mode_enabled() {
            stamp_batch_write_request(&mut request)?;
        }
        self.run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
            record_storage_operation("batch_write_item", dynamo.batch_write_item(request)).await
        })
        .await
    }

    pub async fn batch_write_item_encode(
        &self,
        request: BatchWriteItemEncodeRequest,
    ) -> StorageResult<BatchWriteItemResponse> {
        self.batch_write_item(request.try_into()?).await
    }

    pub async fn transact_write_items_encode(
        &self,
        request: TransactWriteItemsEncodeRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        self.transact_write_items(request.try_into()?).await
    }
}
