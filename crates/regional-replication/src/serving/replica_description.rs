use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::{DynamoPort, describe_table_from_table, resolve_table_in_attempt};
use storage_types::{AttemptMode, DescribeTableRequest, DescribeTableResponse, StorageResult};

use super::runtime::RegionalDynamo;

impl<P: DynamoPort> RegionalDynamo<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(super) async fn describe_regional_table(
        &self,
        request: DescribeTableRequest,
    ) -> StorageResult<DescribeTableResponse> {
        let cache = self.port.metadata_cache();
        let journal = Arc::clone(&self.journal);
        let (response, publication) = self
            .port
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    let (table, publication) =
                        resolve_table_in_attempt(attempt, &cache, &request.table_name).await?;
                    journal.require_readable(attempt, &table).await?;
                    let mut response = describe_table_from_table(attempt, &table).await?;
                    journal
                        .project_regional_description(attempt, &table, &mut response.table)
                        .await?;
                    Ok((response, publication))
                })
            })
            .await?;
        self.port.metadata_cache().publish(publication);
        Ok(response)
    }
}
