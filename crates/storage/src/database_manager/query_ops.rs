use storage_provider::{RangeReadItem, RangeReadPage};
use storage_types::{
    QueryTableRequest, StorageResult, StoredEntity, TryFromWireItem, WireItem,
    single_table_entity::SingleTableEntity,
};

use crate::{
    QueryIndexInput, QueryTableInput,
    database_manager::{DatabaseManager, record_storage_operation},
};

impl DatabaseManager {
    pub async fn query_table_map(
        &self,
        input: QueryTableInput,
    ) -> StorageResult<RangeReadPage<storage_types::AttributeMap>> {
        self.query_table_request(input.into())
            .await?
            .try_map(|item| item.to_attribute_map())
    }

    pub async fn query_table(
        &self,
        input: QueryTableInput,
    ) -> StorageResult<RangeReadPage<WireItem>> {
        self.query_table_request(input.into()).await
    }

    pub async fn query_table_decode<T>(
        &self,
        input: QueryTableInput,
    ) -> StorageResult<RangeReadPage<T>>
    where
        T: TryFromWireItem,
    {
        self.query_table(input)
            .await?
            .try_map(T::try_from_wire_item)
    }

    /// Admit complete entities only when their persisted identity agrees with
    /// the independently observed physical table/key.
    pub async fn query_table_stored_entity<T>(
        &self,
        input: QueryTableInput,
    ) -> StorageResult<RangeReadPage<T>>
    where
        T: StoredEntity + SingleTableEntity,
    {
        let request: QueryTableRequest = input.into();
        let table = request.table_name.clone();
        self.query_source_page(request).await?.try_map(|source| {
            let entity = T::try_from_stored_item(&source.item)?;
            entity.validate_stored_identity(&source.item)?;
            entity.validate_addressed_identity(&table, &source.key)?;
            Ok(entity)
        })
    }

    pub async fn query_index_map(
        &self,
        input: QueryIndexInput,
    ) -> StorageResult<RangeReadPage<storage_types::AttributeMap>> {
        self.query_table_request(input.into())
            .await?
            .try_map(|item| item.to_attribute_map())
    }

    pub async fn query_index(
        &self,
        input: QueryIndexInput,
    ) -> StorageResult<RangeReadPage<WireItem>> {
        self.query_table_request(input.into()).await
    }

    pub async fn query_index_decode<T>(
        &self,
        input: QueryIndexInput,
    ) -> StorageResult<RangeReadPage<T>>
    where
        T: TryFromWireItem,
    {
        self.query_index(input)
            .await?
            .try_map(T::try_from_wire_item)
    }

    /// Returns physical addresses independently of the persisted images.
    pub async fn query_source_page(
        &self,
        request: QueryTableRequest,
    ) -> StorageResult<RangeReadPage<RangeReadItem>> {
        self.run_dynamo_admitted(
            "default",
            crate::admission::AdmissionClass::RangeRead,
            |dynamo| async move {
                record_storage_operation("query_source_page", dynamo.query_source_page(request))
                    .await
            },
        )
        .await
    }

    async fn query_table_request(
        &self,
        request: QueryTableRequest,
    ) -> StorageResult<RangeReadPage<WireItem>> {
        self.run_dynamo_admitted(
            "default",
            crate::admission::AdmissionClass::RangeRead,
            |dynamo| async move {
                record_storage_operation("query_table", dynamo.query_page(request)).await
            },
        )
        .await
    }
}
