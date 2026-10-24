use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use serde::de::DeserializeOwned;
use storage_provider::DynamoPort;
use storage_types::StorageResult;

use super::runtime::RegionalDynamo;
use crate::origin::{OriginFailure, OriginReceiptObservation, reconcile_item_write};

impl<P: DynamoPort> RegionalDynamo<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    /// Reconcile a lost native response without re-executing the request. An
    /// absent receipt or failed observation preserves the original uncertainty.
    pub(super) async fn settle_response<R: DeserializeOwned>(
        &self,
        result: Result<R, OriginFailure>,
    ) -> StorageResult<R> {
        let failure = match result {
            Ok(response) => return Ok(response),
            Err(failure) => failure,
        };
        if failure.error.is_commit_unknown()
            && let Ok(OriginReceiptObservation::Durable(receipt)) = reconcile_item_write(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                failure.recovery,
            )
            .await
            && let Ok(response) = serde_json::from_str(receipt.response.get())
        {
            return Ok(response);
        }
        Err(failure.error)
    }
}
