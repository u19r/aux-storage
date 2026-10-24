//! A graph reads only catalog observations qualified by its caller.

use std::sync::Arc;

use async_trait::async_trait;
use storage_types::{
    BatchGetItemRequest, BatchGetItemResponse, BoundTableInfo, GetItemRequest, GetItemResponse,
    KeyAttributes, QueryRequest, QueryResponse, StorageError, StorageResult, TableName,
};

use crate::{
    BatchGetPlan, CachePublication, DynamoRangeReadAttempt, DynamoRangeReadShape, GetItemAttempt,
    ReadSequenceStepReader, batch_get_item_from_tables,
    dynamo_range_read::{ValidatedDynamoRangeRequest, range_read_output_on_table},
    get_item_from_table,
};

/// Every graph invocation uses the same catalog snapshot. The graph cannot
/// discover another table after its caller has qualified this closed set.
pub struct BoundReadSequenceReader<'a, A> {
    attempt: &'a mut A,
    tables: Vec<Arc<BoundTableInfo>>,
}

impl<'a, A> BoundReadSequenceReader<'a, A> {
    pub fn new(attempt: &'a mut A, tables: Vec<Arc<BoundTableInfo>>) -> Self {
        Self { attempt, tables }
    }

    fn find_table(&self, name: &TableName) -> StorageResult<Arc<BoundTableInfo>> {
        self.tables
            .iter()
            .find(|table| {
                table.info.table_name.dynamodb_resource_name() == name.dynamodb_resource_name()
            })
            .cloned()
            .ok_or_else(|| StorageError::internal("ReadSequence table was not qualified"))
    }
}

#[async_trait]
impl<A: GetItemAttempt + DynamoRangeReadAttempt + Send> ReadSequenceStepReader
    for BoundReadSequenceReader<'_, A>
{
    async fn get(
        &mut self,
        request: GetItemRequest,
    ) -> StorageResult<(GetItemResponse, Option<CachePublication>)> {
        let table = self.find_table(&request.table_name)?;
        Ok((
            get_item_from_table(self.attempt, &table, &request).await?,
            None,
        ))
    }

    async fn batch_get_once(
        &mut self,
        request: BatchGetItemRequest,
    ) -> StorageResult<(BatchGetItemResponse, Vec<CachePublication>)> {
        let plan = BatchGetPlan::from(request);
        let tables = plan
            .tables
            .iter()
            .map(|table| self.find_table(&table.table_name))
            .collect::<StorageResult<Vec<_>>>()?;
        let response =
            batch_get_item_from_tables(self.attempt, plan, tables.into_iter().map(Ok)).await?;
        Ok((response, Vec::new()))
    }

    async fn query(
        &mut self,
        request: QueryRequest,
    ) -> StorageResult<(
        QueryResponse,
        Option<Vec<KeyAttributes>>,
        Option<CachePublication>,
    )> {
        let table = self.find_table(&request.table_name)?;
        let request = ValidatedDynamoRangeRequest::try_from(request.into_request())?;
        let mut output = range_read_output_on_table(self.attempt, table, request, true).await?;
        let cursors = output.item_cursors.take();
        Ok((QueryRequest::finish(output), cursors, None))
    }
}
