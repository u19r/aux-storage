use std::{borrow::Cow, sync::Arc};

use storage_provider::{
    DynamoAttempt, describe_time_to_live_from_table, get_table_stream_records_from_table,
    table_stream_checkpoint_from_table,
};
use storage_types::{
    BoundTableInfo, DescribeTimeToLiveRequest, DescribeTimeToLiveResponse, GetStreamRecordsRequest,
    GetStreamRecordsResponse, StorageResult, StoredTableInfo, TableName,
};

use super::{read_operations::single_table, reads::RegionalPointRead};

pub(super) struct TableInfoRead(pub(super) TableName);
pub(super) struct StreamPointerRead(pub(super) storage_types::TableHistoryTarget);

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

pub(super) struct TableStreamRead(
    pub(super) GetStreamRecordsRequest,
    pub(super) storage_types::TimestampMillis,
);

impl RegionalPointRead for TableStreamRead {
    type Response = GetStreamRecordsResponse;
    fn table_names(&self) -> Cow<'_, [TableName]> {
        self.0
            .table_name()
            .map_or(&[][..], std::slice::from_ref)
            .into()
    }
    async fn execute<A: DynamoAttempt>(
        self,
        attempt: &mut A,
        tables: Vec<StorageResult<Arc<BoundTableInfo>>>,
    ) -> StorageResult<Self::Response> {
        get_table_stream_records_from_table(
            attempt,
            single_table(tables)?.as_ref(),
            &self.0,
            self.1,
        )
        .await
    }
}

impl RegionalPointRead for StreamPointerRead {
    type Response = storage_types::TableStreamCheckpoint;
    fn table_names(&self) -> Cow<'_, [TableName]> {
        std::slice::from_ref(&self.0.table_name).into()
    }
    async fn execute<A: DynamoAttempt>(
        self,
        attempt: &mut A,
        tables: Vec<StorageResult<Arc<BoundTableInfo>>>,
    ) -> StorageResult<Self::Response> {
        let table = single_table(tables)?;
        if table.history_id != self.0.history_id {
            return Err(storage_types::StorageError::table_not_found(
                &self.0.table_name,
            ));
        }
        table_stream_checkpoint_from_table(attempt, &table).await
    }
}

pub(super) struct DynamoStreamRead<S>(pub(super) S, pub(super) storage_types::TimestampMillis);

impl<S: storage_provider::DynamoStreamRead> RegionalPointRead for DynamoStreamRead<S> {
    type Response = S::Response;
    fn metadata_failure(self, error: storage_types::StorageError) -> StorageResult<Self::Response> {
        self.0.metadata_failure(error)
    }
    fn table_names(&self) -> Cow<'_, [TableName]> {
        std::slice::from_ref(&self.0.target().table_name).into()
    }
    async fn execute<A: DynamoAttempt>(
        self,
        attempt: &mut A,
        tables: Vec<StorageResult<Arc<BoundTableInfo>>>,
    ) -> StorageResult<Self::Response> {
        match single_table(tables) {
            Ok(table) => self.0.execute(attempt, table.as_ref(), self.1).await,
            Err(error) => self.0.metadata_failure(error),
        }
    }
}
