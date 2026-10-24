use storage_common::ttl::{TtlSweepPageCommand, TtlSweepPageResult, TtlSweepRequest};
use storage_types::{StorageError, StorageResult, TimestampMillis};

use super::{DatabaseManager, ROUTED_DEFAULT_CONNECTION_ID};

impl DatabaseManager {
    pub async fn prepare_ttl_sweep_command(
        &self,
        request: TtlSweepRequest,
    ) -> StorageResult<TtlSweepPageCommand> {
        let repository = self.ttl_sweep_repository.clone().ok_or_else(|| {
            StorageError::unsupported(
                "configured backend does not support deterministic TTL sweep commands",
            )
        })?;
        self.run_scoped_control(ROUTED_DEFAULT_CONNECTION_ID, || async move {
            let policy = repository
                .read_ttl_sweep_policy(&request.table_name)
                .await?
                .ok_or_else(|| {
                    StorageError::validation("ttl sweep requires an enabled ttl policy")
                })?;
            TtlSweepPageCommand::try_new(
                request.table_name,
                request.input,
                policy.policy_generation,
                TimestampMillis::now(),
            )
        })
        .await
    }

    pub async fn run_ttl_sweep_command(
        &self,
        command: TtlSweepPageCommand,
    ) -> StorageResult<TtlSweepPageResult> {
        let repository = self.ttl_sweep_repository.clone().ok_or_else(|| {
            StorageError::unsupported(
                "configured backend does not support deterministic TTL sweep commands",
            )
        })?;
        self.run_scoped_control(ROUTED_DEFAULT_CONNECTION_ID, || async move {
            repository.execute_ttl_sweep_page(command).await
        })
        .await
    }

    pub async fn run_ttl_sweep(
        &self,
        request: TtlSweepRequest,
    ) -> StorageResult<TtlSweepPageResult> {
        let repository = self.ttl_sweep_repository.clone().ok_or_else(|| {
            StorageError::unsupported(
                "configured backend does not support deterministic TTL sweep commands",
            )
        })?;
        self.run_scoped_control(ROUTED_DEFAULT_CONNECTION_ID, || async move {
            repository.execute_ttl_sweep_request(request).await
        })
        .await
    }
}
