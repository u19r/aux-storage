use std::{borrow::Cow, sync::Arc};

use storage_provider::{
    BatchGetPlan, DynamoAttempt, TransactGetPlan, batch_get_item_from_tables, get_item_from_table,
    transact_get_items_from_tables,
};
use storage_types::{
    BatchGetItemResponse, BoundTableInfo, GetItemRequest, GetItemResponse, StorageResult,
    TableName, TransactGetItemsResponse,
};

use super::reads::RegionalPointRead;

impl RegionalPointRead for GetItemRequest {
    type Response = GetItemResponse;
    fn table_names(&self) -> Cow<'_, [TableName]> {
        std::slice::from_ref(&self.table_name).into()
    }
    async fn execute<A: DynamoAttempt>(
        self,
        attempt: &mut A,
        tables: Vec<StorageResult<Arc<BoundTableInfo>>>,
    ) -> StorageResult<Self::Response> {
        let table = single_table(tables)?;
        get_item_from_table(attempt, &table, &self).await
    }
}

impl RegionalPointRead for BatchGetPlan {
    type Response = BatchGetItemResponse;
    fn metadata_failure(
        self,
        _error: storage_types::StorageError,
    ) -> StorageResult<Self::Response> {
        let results = (0..self.tables.len())
            .map(|_| storage_provider::BatchGetTableResult::Unprocessed)
            .collect();
        self.finish_api_response(results)
    }
    fn table_names(&self) -> Cow<'_, [TableName]> {
        self.tables
            .iter()
            .map(|table| table.table_name.clone())
            .collect::<Vec<_>>()
            .into()
    }
    async fn execute<A: DynamoAttempt>(
        self,
        attempt: &mut A,
        tables: Vec<StorageResult<Arc<BoundTableInfo>>>,
    ) -> StorageResult<Self::Response> {
        batch_get_item_from_tables(attempt, self, tables.into_iter()).await
    }
}

impl RegionalPointRead for TransactGetPlan {
    type Response = TransactGetItemsResponse;
    fn table_names(&self) -> Cow<'_, [TableName]> {
        self.table_names().into()
    }
    async fn execute<A: DynamoAttempt>(
        self,
        attempt: &mut A,
        tables: Vec<StorageResult<Arc<BoundTableInfo>>>,
    ) -> StorageResult<Self::Response> {
        transact_get_items_from_tables(attempt, self, tables.into_iter()).await
    }
}

pub(super) fn single_table(
    mut tables: Vec<StorageResult<Arc<BoundTableInfo>>>,
) -> StorageResult<Arc<BoundTableInfo>> {
    if tables.len() != 1 {
        return Err(crate::native_record::corrupt_record());
    }
    tables
        .pop()
        .ok_or_else(crate::native_record::corrupt_record)?
}
