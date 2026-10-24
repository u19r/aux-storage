use async_trait::async_trait;
use storage_types::StorageResult;

use super::{
    StoreRepository, StoreTransaction,
    models::{TtlReceipts, TtlWriteInput},
};

/// Foreground TTL index writes execute through the transaction owner.
/// Table-driven TTL diffs ride along table writes via `finish_with_old`;
/// this entry covers direct index maintenance with no pre-transaction
/// derivation, so no separate staging representation is needed.
#[async_trait]
pub trait TtlRepository: StoreRepository {
    async fn execute_ttl_writes(&self, input: TtlWriteInput) -> StorageResult<TtlReceipts> {
        let mut txn = self.open_transaction().await?;
        txn.apply_ttl_writes(input).await
    }
}

impl<T: StoreRepository> TtlRepository for T {}
