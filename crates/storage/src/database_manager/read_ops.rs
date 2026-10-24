use storage_types::{
    BatchGetItemRequest, BatchGetItemResponse, GetItemRequest, GetStreamRecordsRequest,
    GetStreamRecordsResponse, KeyAttributes, StorageError, StorageResult, TableName,
    TryFromWireItem, WireItem,
};

use crate::database_manager::{DatabaseManager, ResolvedGetItem, record_storage_operation};

impl DatabaseManager {
    pub async fn get_item<K>(
        &self,
        table_name: TableName,
        key: K,
    ) -> StorageResult<Option<WireItem>>
    where
        K: Into<KeyAttributes>,
    {
        self.get_item_with_consistent_read(table_name, key, true)
            .await
    }

    pub async fn get_item_with_consistent_read<K>(
        &self,
        table_name: TableName,
        key: K,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>>
    where
        K: Into<KeyAttributes>,
    {
        self.get_item_from_table_name(table_name, key.into(), consistent_read)
            .await
    }

    pub async fn get_item_with_resolved_operation(
        &self,
        resolved: ResolvedGetItem,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        let ResolvedGetItem { operation, key } = resolved;
        self.get_item_from_table_name(operation.logical_table_name, key, consistent_read)
            .await
    }

    async fn get_item_from_table_name(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        if key.is_empty() {
            return Err(StorageError::invalid_or_missing_key());
        }
        let mut request = GetItemRequest::new(table_name, key);
        request.consistent_read = Some(consistent_read);
        self.run_dynamo_admitted(
            "default",
            crate::admission::AdmissionClass::PointRead,
            |dynamo| async move {
                let response =
                    record_storage_operation("get_item", dynamo.get_item(request)).await?;
                Ok(response.item)
            },
        )
        .await
    }

    pub async fn get_item_decode<K, T>(
        &self,
        table_name: TableName,
        key: K,
    ) -> StorageResult<Option<T>>
    where
        K: Into<KeyAttributes>,
        T: TryFromWireItem,
    {
        self.get_item_decode_with_consistent_read(table_name, key, true)
            .await
    }

    pub async fn get_item_decode_with_consistent_read<K, T>(
        &self,
        table_name: TableName,
        key: K,
        consistent_read: bool,
    ) -> StorageResult<Option<T>>
    where
        K: Into<KeyAttributes>,
        T: TryFromWireItem,
    {
        let item = self
            .get_item_with_consistent_read(table_name, key, consistent_read)
            .await?;
        item.as_ref().map(T::try_from_wire_item).transpose()
    }

    pub async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetItemResponse> {
        self.batch_get_item_retaining(request, ())
            .await
            .map(|(response, ())| response)
    }

    pub(crate) async fn batch_get_item_retaining<R: Send + 'static>(
        &self,
        request: BatchGetItemRequest,
        reservation: R,
    ) -> StorageResult<(BatchGetItemResponse, R)> {
        self.run_dynamo_admitted(
            "default",
            crate::admission::AdmissionClass::RangeRead,
            |dynamo| async move {
                let response =
                    record_storage_operation("batch_get_item", dynamo.batch_get_item(request))
                        .await?;
                // Detached task output destroys the payload before its
                // reservation.
                Ok((response, reservation))
            },
        )
        .await
    }

    pub async fn get_stream_records_for_table_name(
        &self,
        table_name: &TableName,
        page_token: Option<&str>,
        limit: Option<u32>,
    ) -> StorageResult<GetStreamRecordsResponse> {
        let request = GetStreamRecordsRequest::table(
            table_name.clone(),
            page_token.map(str::to_owned),
            limit,
        );
        self.run_dynamo_admitted(
            "default",
            crate::admission::AdmissionClass::RangeRead,
            |dynamo| async move { dynamo.get_stream_records(request).await },
        )
        .await
    }
}
