use std::{borrow::Cow, future::Future, sync::Arc};

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::{
    CachePublication, DynamoAttempt, DynamoPort, DynamoRangeReadShape, ValidatedDynamoRangeRequest,
    range_read_output_on_table, resolve_table_in_attempt, resolve_tables_batch_in_attempt,
};
use storage_types::{AttemptMode, BoundTableInfo, StorageResult, TableName};

use super::runtime::RegionalDynamo;

pub(super) trait RegionalPointRead: Send + 'static {
    type Response: Send + 'static;
    fn metadata_failure(self, error: storage_types::StorageError) -> StorageResult<Self::Response>
    where Self: Sized {
        Err(error)
    }
    fn table_names(&self) -> Cow<'_, [TableName]>;
    fn execute<A: DynamoAttempt>(
        self,
        attempt: &mut A,
        tables: Vec<StorageResult<Arc<BoundTableInfo>>>,
    ) -> impl Future<Output = StorageResult<Self::Response>> + Send;
}

impl<P: DynamoPort> RegionalDynamo<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(super) async fn read_point<R: RegionalPointRead>(
        &self,
        request: R,
    ) -> StorageResult<R::Response> {
        let cache = self.port.metadata_cache();
        let publication_cache = Arc::clone(&cache);
        let journal = Arc::clone(&self.journal);
        let (response, publications) = self
            .port
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(
                    async move { read_point_in_attempt(&journal, attempt, &cache, request).await },
                )
            })
            .await?;
        publication_cache.publish(publications);
        Ok(response)
    }

    pub(super) async fn read_range<R: DynamoRangeReadShape>(
        &self,
        request: R,
    ) -> StorageResult<R::Response> {
        let request = ValidatedDynamoRangeRequest::try_from(request.into_request())?;
        let cache = self.port.metadata_cache();
        let publication_cache = Arc::clone(&cache);
        let journal = Arc::clone(&self.journal);
        let (response, publication) = self
            .port
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    let (table, publication) =
                        resolve_table_in_attempt(attempt, &cache, request.as_ref()).await?;
                    journal
                        .require_range_readable(attempt, &table, request.selected_index())
                        .await?;
                    Ok((
                        range_read_output_on_table(attempt, table, request, false).await?,
                        publication,
                    ))
                })
            })
            .await?;
        publication_cache.publish(publication);
        Ok(R::finish(response))
    }
}

pub(super) async fn read_point_in_attempt<
    A: DynamoAttempt + PhysicalRecordAttempt,
    R: RegionalPointRead,
>(
    journal: &crate::journal::JournalStore,
    attempt: &mut A,
    cache: &storage_provider::TableMetadataCache,
    request: R,
) -> StorageResult<(R::Response, Vec<CachePublication>)> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let observed =
            resolve_tables_batch_in_attempt(attempt, cache, request.table_names().as_ref()).await;
        let observed = match observed {
            Ok(observed) => observed,
            Err(error) => return Ok((request.metadata_failure(error)?, Vec::new())),
        };
        let mut publications: Vec<CachePublication> = Vec::new();
        let mut tables = Vec::with_capacity(observed.len());
        for result in observed {
            tables.push(match result {
                Ok((table, publication)) => {
                    let qualified = journal
                        .require_readable(attempt, &table)
                        .await
                        .map(|()| Arc::clone(&table));
                    publications.extend(publication);
                    qualified
                }
                Err(error) => Err(error),
            });
        }
        Ok((request.execute(attempt, tables).await?, publications))
    }
    .await;
    settlement.complete(result)
}
