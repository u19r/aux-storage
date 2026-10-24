use async_trait::async_trait;
use storage_types::{StorageResult, maintenance::TargetIncarnation};

use super::{
    QueueNativePage, QueueNativeRead, QueueReceipts, QueueWriteInput, StreamNativeRead,
    StreamNativeState, StreamReceipts, StreamWriteInput, TtlReceipts, TtlWriteInput,
};

/// Harmonised repository state shared by backend providers.
///
/// This maintenance handle does not prove a native transaction lifetime.
/// A backend routes a complete customer table attempt through its native
/// owner; queue, stream, and TTL methods here contain no table request policy.
pub trait StoreTransaction: Send {
    type Cursor: PartialEq + Send + Sync;

    fn read_maintenance_identity(
        &mut self,
    ) -> impl Future<Output = StorageResult<TargetIncarnation>>
    where Self: Sized;

    fn fetch_queue_pages_for_write(
        &mut self,
        read: &QueueNativeRead,
    ) -> impl Future<Output = StorageResult<QueueNativePage>>
    where
        Self: Sized;

    fn apply_queue_writes(
        &mut self,
        writes: QueueWriteInput,
    ) -> impl Future<Output = StorageResult<QueueReceipts>>
    where
        Self: Sized;

    fn fetch_stream_state_for_write(
        &mut self,
        read: &StreamNativeRead,
    ) -> impl Future<Output = StorageResult<StreamNativeState>>
    where
        Self: Sized;

    fn apply_stream_writes(
        &mut self,
        writes: StreamWriteInput,
    ) -> impl Future<Output = StorageResult<StreamReceipts>> + Send
    where
        Self: Sized;

    fn apply_ttl_writes(
        &mut self,
        writes: TtlWriteInput,
    ) -> impl Future<Output = StorageResult<TtlReceipts>> + Send
    where
        Self: Sized;
}

/// Entry-point supertrait for every harmonised repository.
///
/// It exists so `execute_*` and `advance_*` can use one transaction owner.
/// Backends implement `open_transaction` and the typed reads/writes;
/// narrower transaction bounds go on `where`
/// `Self::Transaction: …` clauses, never by redeclaring the type.
#[async_trait]
pub trait StoreRepository: Send + Sync {
    type Transaction: StoreTransaction;

    async fn open_transaction(&self) -> StorageResult<Self::Transaction>;
}
