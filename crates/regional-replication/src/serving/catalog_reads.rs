use std::{borrow::Cow, sync::Arc};

use storage_provider::{
    DynamoAttempt, describe_time_to_live_from_table, get_table_stream_records_from_table,
    latest_table_stream_pointer_from_table,
};
use storage_types::{
    BoundTableInfo, DescribeTimeToLiveRequest, DescribeTimeToLiveResponse, GetStreamRecordsRequest,
    GetStreamRecordsResponse, StorageResult, StoredTableInfo, StreamItemId, TableName,
};

use super::{read_operations::single_table, reads::RegionalPointRead};

pub(super) struct TableInfoRead(pub(super) TableName);
pub(super) struct StreamPointerRead(pub(super) TableName);

impl RegionalPointRead for TableInfoRead {
    type Response = Arc<StoredTableInfo>;
    fn table_names(&self) -> Cow<'_, [TableName]> {
        std::slice::from_ref(&self.0).into()
    }
    async fn execute<A: DynamoAttempt>(
        self,
        _attempt: &mut A,
        tables: Vec<StorageResult<Arc<BoundTableInfo>>>,
    ) -> StorageResult<Self::Response> {
        Ok(Arc::clone(&single_table(tables)?.info))
    }
}

impl RegionalPointRead for DescribeTimeToLiveRequest {
    type Response = DescribeTimeToLiveResponse;
    fn table_names(&self) -> Cow<'_, [TableName]> {
        std::slice::from_ref(&self.table_name).into()
    }
    async fn execute<A: DynamoAttempt>(
        self,
        attempt: &mut A,
        tables: Vec<StorageResult<Arc<BoundTableInfo>>>,
    ) -> StorageResult<Self::Response> {
        describe_time_to_live_from_table(attempt, single_table(tables)?.as_ref()).await
    }
}

impl RegionalPointRead for GetStreamRecordsRequest {
    type Response = GetStreamRecordsResponse;
    fn table_names(&self) -> Cow<'_, [TableName]> {
        self.table_name()
            .map_or(&[][..], std::slice::from_ref)
            .into()
    }
    async fn execute<A: DynamoAttempt>(
        self,
        attempt: &mut A,
        tables: Vec<StorageResult<Arc<BoundTableInfo>>>,
    ) -> StorageResult<Self::Response> {
        get_table_stream_records_from_table(attempt, single_table(tables)?.as_ref(), &self).await
    }
}

impl RegionalPointRead for StreamPointerRead {
    type Response = Option<StreamItemId>;
    fn table_names(&self) -> Cow<'_, [TableName]> {
        std::slice::from_ref(&self.0).into()
    }
    async fn execute<A: DynamoAttempt>(
        self,
        attempt: &mut A,
        tables: Vec<StorageResult<Arc<BoundTableInfo>>>,
    ) -> StorageResult<Self::Response> {
        latest_table_stream_pointer_from_table(attempt, single_table(tables)?.as_ref()).await
    }
}
