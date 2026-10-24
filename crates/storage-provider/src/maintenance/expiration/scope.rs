use std::{future::Future, sync::Arc};

use storage_types::{BoundTableInfo, StorageResult, TimestampMillis};

use super::ExpirationDelete;

/// The upper domain selects eligible tables inside the native attempt that
/// observes the TTL policy and applies effects. Physical providers own no
/// regional admission decisions.
pub trait ExpirationTargetAdmission<A>: Send + Sync {
    fn admits_target(
        &self,
        attempt: &mut A,
        table: &BoundTableInfo,
    ) -> impl Future<Output = StorageResult<bool>> + Send;
}

pub trait ExpirationEffectPolicy<A>: ExpirationTargetAdmission<A> {
    fn delete_items(
        &self,
        attempt: &mut A,
        table: Arc<BoundTableInfo>,
        items: Vec<ExpirationDelete>,
        captured_at: TimestampMillis,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}

pub struct OrdinaryExpiration;

impl<A: Send> ExpirationTargetAdmission<A> for OrdinaryExpiration {
    async fn admits_target(
        &self,
        _attempt: &mut A,
        _table: &BoundTableInfo,
    ) -> StorageResult<bool> {
        Ok(true)
    }
}

impl<A: crate::DeleteItemAttempt> ExpirationEffectPolicy<A> for OrdinaryExpiration {
    async fn delete_items(
        &self,
        attempt: &mut A,
        table: Arc<BoundTableInfo>,
        items: Vec<ExpirationDelete>,
        captured_at: TimestampMillis,
    ) -> StorageResult<()> {
        super::prepare_expiration_item_deletes(attempt, table, items, captured_at).await
    }
}
