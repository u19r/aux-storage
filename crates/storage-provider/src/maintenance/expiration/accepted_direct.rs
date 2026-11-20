//! Accepted per-table TTL facts retain the attempt that observed them.

use std::sync::Arc;

use storage_common::{TtlSweepPageCommand, TtlSweepPageResult};
use storage_types::{StorageEnum, StorageError, StorageResult};

use super::{
    DirectExpirationPageWrite, DirectExpirationScanRead, DirectExpirationTransaction,
    ExpirationAttempt, ExpirationPort, accept_expiration_item_deletes, direct::prepare_direct_scan,
    expiration_index_deletes,
};
use crate::{AcceptedDeleteItem, prepare_accepted_delete};

/// The upper owner supplies authenticated table/job scope before preparation.
/// This value exposes facts, not transferable authority or another transaction.
///
/// ```no_run
/// use storage_common::TtlSweepPageCommand;
/// use storage_provider::maintenance::{ExpirationAttempt, ExpirationPort};
/// use storage_types::StorageResult;
/// async fn apply<P: ExpirationPort>(
///     attempt: &mut P,
///     command: &TtlSweepPageCommand,
/// ) -> StorageResult<()> {
///     let page = ExpirationAttempt::new(attempt, Default::default())
///         .prepare_direct_expiration(command)
///         .await?;
///     page.apply().await?;
///     drop(ExpirationAttempt::new(attempt, Default::default()));
///     Ok(())
/// }
/// ```
///
/// The original attempt cannot be borrowed again while accepted effects are
/// pending.
///
/// ```compile_fail
/// use storage_common::TtlSweepPageCommand;
/// use storage_provider::maintenance::{ExpirationAttempt, ExpirationPort};
/// use storage_types::StorageResult;
/// async fn reborrow<P: ExpirationPort>(
///     attempt: &mut P, command: &TtlSweepPageCommand,
/// ) -> StorageResult<()> {
///     let page = ExpirationAttempt::new(attempt, Default::default()).prepare_direct_expiration(command).await?;
///     let other = ExpirationAttempt::new(attempt, Default::default());
///     page.apply().await?;
///     drop(other);
///     Ok(())
/// }
/// ```
#[must_use = "accepted TTL effects must be applied in their original native attempt"]
pub struct AcceptedDirectExpirationPage<'native, P: ExpirationPort> {
    pub(super) attempt: &'native mut P,
    pub(super) write: DirectExpirationPageWrite,
    pub(super) items: Vec<AcceptedDeleteItem>,
}

impl<'native, P: ExpirationPort> ExpirationAttempt<'native, P> {
    pub async fn prepare_direct_expiration(
        mut self,
        command: &TtlSweepPageCommand,
    ) -> StorageResult<AcceptedDirectExpirationPage<'native, P>> {
        let (config, table) = self
            .read_direct_policy(&command.target.table_name)
            .await?
            .ok_or_else(|| StorageError::from(StorageEnum::ConditionalCheckFailed))?;
        let scan = DirectExpirationScanRead::try_new(command, config, (*table).clone())?;
        let mut write = prepare_direct_scan(&mut self, scan).await?;
        let items = accept_expiration_item_deletes(
            self.port,
            table,
            std::mem::take(&mut write.deletions.items),
        )
        .await?
        .collect::<StorageResult<Vec<_>>>()?;
        Ok(AcceptedDirectExpirationPage {
            attempt: self.port,
            write,
            items,
        })
    }
}

impl<P: ExpirationPort> AcceptedDirectExpirationPage<'_, P> {
    pub fn items(&self) -> &[AcceptedDeleteItem] {
        &self.items
    }

    pub const fn outcome(&self) -> &TtlSweepPageResult {
        &self.write.result
    }

    /// All effects, TTL index cleanup and
    /// continuation remain in the original borrowed native attempt.
    pub async fn apply(self) -> StorageResult<TtlSweepPageResult> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self.attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if self
                .items
                .iter()
                .any(|item| item.old_item.is_some() && item.history_route.is_some())
            {
                crate::prepare_writer_routes_in_attempt(attempt, None).await?;
            }
            for mut item in self.items {
                item.user_identity = Some(storage_types::StreamUserIdentity::service_ttl());
                prepare_accepted_delete(attempt, &mut item, self.write.captured_at).await?;
            }
            attempt.flush_dynamo_write().await?;
            let table = Arc::new(self.write.table_info);
            attempt
                .delete_expiration_index(
                    &table,
                    expiration_index_deletes(self.write.deletions.index_entries),
                )
                .await?;
            attempt
                .write_ttl_checkpoint(&table, self.write.config)
                .await?;
            Ok(self.write.result)
        }
        .await;
        settlement.complete(result)
    }
}
