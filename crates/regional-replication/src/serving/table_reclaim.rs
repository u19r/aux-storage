use std::sync::Arc;

use async_trait::async_trait;
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{
    DYNAMO_WRITE_RETRY, DynamoPort, TableReclaimAttempt, TableReclaimPhase, TableReclaimRepository,
    TableReclaimStep, native_admission::NativeWork, reclaim_table_page,
};
use storage_types::{StorageResult, TableHistoryId};

use super::runtime::RegionalDynamo;
use crate::journal::{NativeAttempt, TableReclaimWork};

#[async_trait]
impl<P: DynamoPort> TableReclaimRepository for RegionalDynamo<P>
where for<'native> P::Attempt<'native>: TableReclaimAttempt + PhysicalRecordAttempt
{
    async fn reclaim_table_page(&self) -> StorageResult<TableReclaimStep> {
        let work = retry_native_conflicts(self.port.as_ref(), DYNAMO_WRITE_RETRY, || {
            let journal = Arc::clone(&self.journal);
            Arc::clone(&journal).with_admitted_attempt(
                (self.port.as_ref(), NativeWork::Background),
                NativeAttempt::Schema,
                move |attempt, _| {
                    Box::pin(async move {
                        let target = attempt.load_reclaim_target().await?;
                        if let Some(target) = target
                            && target.phase == TableReclaimPhase::ExternalData
                        {
                            journal.reclaim_external_table_page(attempt, &target).await
                        } else {
                            reclaim_table_page(attempt)
                                .await
                                .map(TableReclaimWork::Settled)
                        }
                    })
                },
            )
        })
        .await?;
        match work {
            TableReclaimWork::Settled(step) => Ok(step),
            TableReclaimWork::Archive(history) => self.reclaim_table_archive(history).await,
        }
    }
}

impl<P: DynamoPort> RegionalDynamo<P>
where for<'native> P::Attempt<'native>: TableReclaimAttempt + PhysicalRecordAttempt
{
    async fn reclaim_table_archive(
        &self,
        history: TableHistoryId,
    ) -> StorageResult<TableReclaimStep> {
        let complete = match self.journal.archive.get() {
            Some(archive) => {
                archive
                    .reclaim_table_objects(history, self.journal.physical_limits().max_records())
                    .await?
            }
            None => true,
        };
        if complete {
            retry_native_conflicts(self.port.as_ref(), DYNAMO_WRITE_RETRY, || {
                let journal = Arc::clone(&self.journal);
                Arc::clone(&journal).with_admitted_attempt(
                    (self.port.as_ref(), NativeWork::Background),
                    NativeAttempt::Schema,
                    move |attempt, _| {
                        Box::pin(async move {
                            journal
                                .acknowledge_table_archive_erasure(attempt, history)
                                .await
                        })
                    },
                )
            })
            .await?;
        }
        Ok(TableReclaimStep::Advanced)
    }
}
