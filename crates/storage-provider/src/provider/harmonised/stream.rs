use async_trait::async_trait;
use storage_types::StorageResult;
pub use stream_provider::StreamPage;
use stream_provider::StreamProvider;

use super::{
    StoreRepository, StoreTransaction,
    models::{StreamReadInput, StreamReceipts, StreamWriteInput, StreamWriteReceipt},
};

/// Foreground stream appends execute through the transaction owner. Appends
/// need no pre-transaction derivation: batch and item bounds are proven by
/// `StreamWriteInput` construction, and ids plus timestamps are
/// backend-allocated inside the transaction, so no separate staging
/// representation is needed.
#[async_trait]
pub trait StreamRepository: StoreRepository {
    async fn execute_stream_appends(
        &self,
        input: StreamWriteInput,
    ) -> StorageResult<StreamReceipts> {
        let mut txn = self.open_transaction().await?;
        txn.apply_stream_writes(input).await
    }

    async fn fetch_stream_page(&self, query: StreamReadInput) -> StorageResult<StreamPage>;
}

/// Append one harmonised batch through any `StreamProvider`: ids and
/// timestamps are backend-allocated per append, so there is one native
/// commit per append — exactly like the legacy path, which exposes no batch
/// append entry. Shared by every backend's `apply_stream_writes`; the serial
/// loop is intentional (appends are independent single commits, not one
/// atomic batch).
pub async fn append_stream_inputs(
    provider: &impl StreamProvider,
    writes: StreamWriteInput,
) -> StorageResult<StreamReceipts> {
    let stream = writes.stream;
    let mut receipts = Vec::new();
    for append in writes.appends.into_inner() {
        let item_id = provider
            .append_item(
                stream.clone(),
                append.item_data.as_slice(),
                append.partition_key.as_ref().map(|key| key.as_str()),
            )
            .await
            .map_err(stream_provider::StreamError::into_storage_error)?;
        receipts.push(StreamWriteReceipt {
            ordinal: append.ordinal,
            item_id,
        });
    }
    Ok(StreamReceipts { receipts })
}

/// Read one stream page through any `StreamProvider`, resolving direction
/// from the query. Shared by every backend's `fetch_stream_page`.
pub async fn read_stream_page(
    provider: &impl StreamProvider,
    query: StreamReadInput,
) -> StorageResult<StreamPage> {
    let page = if query.scan_forward {
        provider
            .read_forward(query.stream, query.exclusive_start_key, query.limit.get())
            .await
    } else {
        provider
            .read_backward(query.stream, query.exclusive_start_key, query.limit.get())
            .await
    }
    .map_err(stream_provider::StreamError::into_storage_error)?;
    Ok(page)
}
