use std::sync::Arc;

use async_trait::async_trait;
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::{
    CachePublication, DynamoAttempt, DynamoPort, DynamoRangeReadShape, ReadSequenceStepReader,
    TableMetadataCache, ValidatedDynamoRangeRequest, range_read_output_on_table,
    read_sequence_with_reader, resolve_table_in_attempt,
};
use storage_types::{
    AttemptMode, BatchGetItemRequest, BatchGetItemResponse, GetItemRequest, GetItemResponse,
    KeyAttributes, QueryRequest, QueryResponse, ReadSequenceRequest, ReadSequenceResponse,
    ReadSequenceValidationCapabilities, StorageResult,
};

use super::{reads::read_point_in_attempt, runtime::RegionalDynamo};
use crate::journal::JournalStore;

struct RegionalSequenceReader<'a, A> {
    attempt: &'a mut A,
    cache: &'a TableMetadataCache,
    journal: &'a JournalStore,
}

#[async_trait]
impl<A: DynamoAttempt + PhysicalRecordAttempt> ReadSequenceStepReader
    for RegionalSequenceReader<'_, A>
{
    async fn get(
        &mut self,
        request: GetItemRequest,
    ) -> StorageResult<(GetItemResponse, Option<CachePublication>)> {
        let (response, mut publications) =
            read_point_in_attempt(self.journal, self.attempt, self.cache, request).await?;
        Ok((response, publications.pop()))
    }

    async fn batch_get_once(
        &mut self,
        request: BatchGetItemRequest,
    ) -> StorageResult<(BatchGetItemResponse, Vec<CachePublication>)> {
        read_point_in_attempt(
            self.journal,
            self.attempt,
            self.cache,
            storage_provider::BatchGetPlan::from(request),
        )
        .await
    }

    async fn query(
        &mut self,
        request: QueryRequest,
    ) -> StorageResult<(
        QueryResponse,
        Option<Vec<KeyAttributes>>,
        Option<CachePublication>,
    )> {
        let request = ValidatedDynamoRangeRequest::try_from(request.into_request())?;
        let (table, publication) =
            resolve_table_in_attempt(self.attempt, self.cache, request.as_ref()).await?;
        self.journal
            .require_range_readable(self.attempt, &table, request.selected_index())
            .await?;
        let mut output = range_read_output_on_table(self.attempt, table, request, true).await?;
        let cursors = output.item_cursors.take();
        Ok((QueryRequest::finish(output), cursors, publication))
    }
}

impl<P: DynamoPort> RegionalDynamo<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(super) async fn read_sequence(
        &self,
        request: ReadSequenceRequest,
    ) -> StorageResult<ReadSequenceResponse> {
        let cache = self.port.metadata_cache();
        let publication_cache = Arc::clone(&cache);
        let journal = Arc::clone(&self.journal);
        let (response, publications) = self
            .port
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    let mut reader = RegionalSequenceReader {
                        attempt,
                        cache: &cache,
                        journal: &journal,
                    };
                    read_sequence_with_reader(
                        &mut reader,
                        request,
                        ReadSequenceValidationCapabilities {
                            immediate_gsi_consistency: true,
                            ..Default::default()
                        },
                    )
                    .await
                })
            })
            .await?;
        publication_cache.publish(publications);
        Ok(response)
    }
}
