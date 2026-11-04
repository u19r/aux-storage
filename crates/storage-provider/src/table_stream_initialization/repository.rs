//! Initialization owns a separate native attempt within each index maintenance
//! turn.

use async_trait::async_trait;
use storage_common::retry::retry_native_conflicts;
use storage_types::{AttemptMode, StorageResult};

use crate::{DYNAMO_WRITE_RETRY, DynamoPort, PrimaryStateScanAttempt};

#[async_trait]
pub trait TableStreamInitializationRepository: Send + Sync {
    async fn advance_table_stream_initialization(&self) -> StorageResult<bool>;
}

#[async_trait]
impl<P: DynamoPort> TableStreamInitializationRepository for P
where for<'native> P::Attempt<'native>: PrimaryStateScanAttempt
{
    async fn advance_table_stream_initialization(&self) -> StorageResult<bool> {
        retry_native_conflicts(self, DYNAMO_WRITE_RETRY, || {
            self.with_dynamo_attempt(AttemptMode::ReadWrite, |attempt| {
                Box::pin(super::capture::advance_table_stream_initialization(
                    attempt, 32,
                ))
            })
        })
        .await
    }
}
