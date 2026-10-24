use std::sync::Arc;

use async_trait::async_trait;
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{
    DYNAMO_WRITE_RETRY, DynamoPort, TableReclaimAttempt, TableReclaimRepository, TableReclaimStep,
    native_admission::NativeWork, reclaim_table_page,
};
use storage_types::{StorageResult, TimestampMillis};

use super::runtime::RegionalDynamo;
use crate::journal::NativeAttempt;

#[async_trait]
impl<P: DynamoPort> TableReclaimRepository for RegionalDynamo<P>
where for<'native> P::Attempt<'native>: TableReclaimAttempt + PhysicalRecordAttempt
{
    async fn reclaim_table_page(&self) -> StorageResult<TableReclaimStep> {
        self.reclaim_page(None).await
    }

    async fn reclaim_table_page_at(
        &self,
        observed_at: TimestampMillis,
    ) -> StorageResult<TableReclaimStep> {
        self.reclaim_page(Some(observed_at)).await
    }
}

impl<P: DynamoPort> RegionalDynamo<P>
where for<'native> P::Attempt<'native>: TableReclaimAttempt + PhysicalRecordAttempt
{
    async fn reclaim_page(
        &self,
        observed_at: Option<TimestampMillis>,
    ) -> StorageResult<TableReclaimStep> {
        retry_native_conflicts(self.port.as_ref(), DYNAMO_WRITE_RETRY, || {
            Arc::clone(&self.journal).with_admitted_attempt(
                (self.port.as_ref(), NativeWork::Background),
                NativeAttempt::Schema,
                move |attempt, now| {
                    Box::pin(reclaim_table_page(attempt, observed_at.unwrap_or(now)))
                },
            )
        })
        .await
    }
}
