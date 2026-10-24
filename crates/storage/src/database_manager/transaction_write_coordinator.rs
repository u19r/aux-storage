use storage_types::{StorageResult, TransactWriteItemsRequest, TransactWriteItemsResponse};

use crate::{
    admission::AdmissionClass,
    database_manager::{DatabaseManager, record_storage_operation},
    updated_at_apply::stamp_transact_write_item,
};

impl DatabaseManager {
    /// Performs one atomic destination write under a common runtime claim.
    /// Entity owners supply their metadata; legacy raw-write timestamp stamping
    /// does not alter this replayable intent.
    pub async fn transact_write_job_effect(
        &self,
        request: TransactWriteItemsRequest,
        authority: scheduled_bg_jobs::JobEffectAuthority,
    ) -> StorageResult<TransactWriteItemsResponse> {
        self.run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
            record_storage_operation(
                "transact_write_job_effect",
                dynamo.transact_write_job_effect(request, authority),
            )
            .await
        })
        .await
    }

    /// Reconciles a logical destination effect from its durable native receipt.
    /// Include every immutable business identity in the caller token. A missing
    /// receipt does not establish rollback or grant current execution
    /// authority.
    pub async fn find_job_effect_receipt(
        &self,
        token: &str,
        authority: scheduled_bg_jobs::JobEffectAuthority,
    ) -> StorageResult<Option<TransactWriteItemsResponse>> {
        let token = token.to_owned();
        self.run_dynamo_admitted("default", AdmissionClass::PointRead, |dynamo| async move {
            record_storage_operation(
                "find_job_effect_receipt",
                dynamo.find_job_effect_receipt(&token, authority),
            )
            .await
        })
        .await
    }

    /// Publishes application terminal status atomically with common lifecycle
    /// authority and its canonical replay receipt.
    pub async fn transact_write_job_projection(
        &self,
        request: storage_provider::JobTerminalProjection,
        authority: scheduled_bg_jobs::JobTerminalAuthority,
    ) -> StorageResult<TransactWriteItemsResponse> {
        self.run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
            record_storage_operation(
                "transact_write_job_projection",
                dynamo.transact_write_job_projection(request, authority),
            )
            .await
        })
        .await
    }

    pub async fn transact_write_items(
        &self,
        mut request: TransactWriteItemsRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        self.stamp_transaction(&mut request)?;
        self.run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
            record_storage_operation("transact_write_items", dynamo.transact_write_items(request))
                .await
        })
        .await
    }
    fn stamp_transaction(&self, request: &mut TransactWriteItemsRequest) -> StorageResult<()> {
        if self.single_table_mode_enabled() {
            for item in &mut request.transact_items {
                stamp_transact_write_item(item)?;
            }
        }
        Ok(())
    }
}
