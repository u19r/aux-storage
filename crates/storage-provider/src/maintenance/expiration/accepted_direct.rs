//! Accepted per-table TTL facts retain the attempt that observed them.

use std::sync::Arc;

use storage_common::{TtlSweepPageCommand, TtlSweepPageResult};
use storage_types::{ItemProvenance, StorageEnum, StorageError, StorageResult};

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
/// use storage_types::{ItemProvenance, StorageResult};
/// async fn apply<P: ExpirationPort>(
///     attempt: &mut P,
///     command: &TtlSweepPageCommand,
/// ) -> StorageResult<()> {
///     let page = ExpirationAttempt::new(attempt)
///         .prepare_direct_expiration(command)
///         .await?;
///     let provenance = vec![ItemProvenance::default(); page.items().len()];
///     page.apply(&provenance).await?;
///     drop(ExpirationAttempt::new(attempt));
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
///     let page = ExpirationAttempt::new(attempt).prepare_direct_expiration(command).await?;
///     let other = ExpirationAttempt::new(attempt);
///     page.apply(&[]).await?;
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
            .read_direct_policy(&command.table_name)
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

    /// Count validation precedes effects. All effects, TTL index cleanup and
    /// continuation remain in the original borrowed native attempt.
    pub async fn apply(self, provenance: &[ItemProvenance]) -> StorageResult<TtlSweepPageResult> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self.attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if provenance.len() != self.items.len() {
                return Err(StorageError::validation(
                    "expiration provenance count differs from accepted deletions",
                ));
            }
            for (item, winner) in self.items.into_iter().zip(provenance) {
                prepare_accepted_delete(attempt, &item, self.write.captured_at, winner.clone())
                    .await?;
            }
            attempt.flush_dynamo_write().await?;
            let table = Arc::new(self.write.table_info);
            attempt
                .delete_expiration_index(
                    &table,
                    expiration_index_deletes(self.write.deletions.index_entries),
                )
                .await?;
            attempt.write_ttl_config(&table, self.write.config).await?;
            Ok(self.write.result)
        }
        .await;
        settlement.complete(result)
    }
}
