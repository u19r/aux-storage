//! Public stream operations share catalog and page decisions across backends.

use async_trait::async_trait;
use storage_types::{
    AttemptMode, DurationSeconds, StreamItemId, StreamName, SystemStreamScope, UserStreamName,
};

use crate::{
    CursorName, CursorPage, CursorPosition, ReadDirection, Stream, StreamAttempt, StreamCursor,
    StreamError, StreamPage, StreamPartitioningMode, StreamPort, StreamResult, StreamRowKind,
    SystemStreamHistory, SystemStreamLineage, SystemStreamPage, SystemStreamPageRequest,
    SystemStreamSplitRequest, SystemStreamSplitResult,
};

#[async_trait]
pub trait StreamOperations: Send + Sync {
    async fn create_stream(
        &self,
        name: UserStreamName,
        ttl_seconds: Option<DurationSeconds>,
        partitioning_mode: StreamPartitioningMode,
    ) -> StreamResult<StreamName>;

    async fn get_stream(&self, name: UserStreamName) -> StreamResult<Option<Stream>>;

    async fn delete_stream(&self, name: UserStreamName) -> StreamResult<()>;

    async fn append_item(
        &self,
        name: UserStreamName,
        item_data: &[u8],
        partition_key: Option<&str>,
    ) -> StreamResult<StreamItemId>;

    async fn create_cursor(
        &self,
        stream: StreamRowKind,
        cursor_name: CursorName,
        position: CursorPosition,
    ) -> StreamResult<()>;

    async fn get_cursor(
        &self,
        stream: StreamRowKind,
        cursor_name: CursorName,
    ) -> StreamResult<Option<StreamCursor>>;

    async fn delete_cursor(
        &self,
        stream: StreamRowKind,
        cursor_name: CursorName,
    ) -> StreamResult<()>;

    async fn advance_cursor(
        &self,
        stream: StreamRowKind,
        cursor_name: CursorName,
        to_item_id: StreamItemId,
    ) -> StreamResult<()>;

    async fn read_from_cursor(
        &self,
        stream: StreamRowKind,
        cursor_name: CursorName,
        limit: u32,
    ) -> StreamResult<CursorPage>;

    async fn read_forward(
        &self,
        stream: StreamRowKind,
        exclusive_start: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage>;

    async fn read_backward(
        &self,
        stream: StreamRowKind,
        exclusive_start: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage>;

    /// Logical history has lineage and checkpoint state that ordinary stream
    /// pages do not carry. Physical reads still use `StreamReadKind::System`.
    async fn system_stream_scope(&self) -> StreamResult<SystemStreamScope>;

    async fn system_stream_lineage_catalog(
        &self,
        source_scope: SystemStreamScope,
    ) -> StreamResult<Vec<SystemStreamLineage>>;

    async fn get_system_stream_page(
        &self,
        request: SystemStreamPageRequest,
    ) -> StreamResult<SystemStreamPage>;

    async fn split_system_stream(
        &self,
        request: SystemStreamSplitRequest,
    ) -> StreamResult<SystemStreamSplitResult>;
}

/// A backend read context provides one coherent logical authority snapshot.
async fn load_system_stream_history<P: StreamPort>(port: &P) -> StreamResult<SystemStreamHistory> {
    port.with_stream_attempt(AttemptMode::ReadOnly, move |attempt| {
        Box::pin(async move {
            attempt
                .read_stream(crate::StreamReadKind::System(
                    crate::SystemStreamReadKind::Catalog,
                ))
                .await?
                .into_system_catalog()
        })
    })
    .await
    .map_err(StreamError::from_storage_error)
}

#[async_trait]
impl<P: StreamPort> StreamOperations for P {
    async fn create_stream(
        &self,
        name: UserStreamName,
        ttl_seconds: Option<DurationSeconds>,
        partitioning_mode: StreamPartitioningMode,
    ) -> StreamResult<StreamName> {
        crate::stream_catalog::create_stream(self, name, ttl_seconds, partitioning_mode).await
    }

    async fn get_stream(&self, name: UserStreamName) -> StreamResult<Option<Stream>> {
        crate::stream_catalog::get_stream(self, name).await
    }

    async fn delete_stream(&self, name: UserStreamName) -> StreamResult<()> {
        crate::stream_catalog::delete_stream(self, name).await
    }

    async fn append_item(
        &self,
        name: UserStreamName,
        item_data: &[u8],
        partition_key: Option<&str>,
    ) -> StreamResult<StreamItemId> {
        crate::stream_append::append_item(self, name, item_data, partition_key).await
    }

    async fn create_cursor(
        &self,
        stream: StreamRowKind,
        cursor_name: CursorName,
        position: CursorPosition,
    ) -> StreamResult<()> {
        crate::stream_cursor::create_cursor(self, stream, cursor_name, position).await
    }

    async fn get_cursor(
        &self,
        stream: StreamRowKind,
        cursor_name: CursorName,
    ) -> StreamResult<Option<StreamCursor>> {
        crate::stream_cursor::get_cursor(self, stream, cursor_name).await
    }

    async fn delete_cursor(
        &self,
        stream: StreamRowKind,
        cursor_name: CursorName,
    ) -> StreamResult<()> {
        crate::stream_cursor::delete_cursor(self, stream, cursor_name).await
    }

    async fn advance_cursor(
        &self,
        stream: StreamRowKind,
        cursor_name: CursorName,
        to_item_id: StreamItemId,
    ) -> StreamResult<()> {
        crate::stream_cursor::advance_cursor(self, stream, cursor_name, to_item_id).await
    }

    async fn read_from_cursor(
        &self,
        stream: StreamRowKind,
        cursor_name: CursorName,
        limit: u32,
    ) -> StreamResult<CursorPage> {
        crate::stream_cursor::read_from_cursor(self, stream, cursor_name, limit).await
    }

    async fn read_forward(
        &self,
        stream: StreamRowKind,
        exclusive_start: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        crate::stream_read::read(self, stream, exclusive_start, ReadDirection::Forward, limit).await
    }

    async fn read_backward(
        &self,
        stream: StreamRowKind,
        exclusive_start: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        crate::stream_read::read(
            self,
            stream,
            exclusive_start,
            ReadDirection::Backward,
            limit,
        )
        .await
    }

    async fn system_stream_scope(&self) -> StreamResult<SystemStreamScope> {
        let history = load_system_stream_history(self).await?;
        Ok(history.as_ref().source_scope().clone())
    }

    async fn system_stream_lineage_catalog(
        &self,
        source_scope: SystemStreamScope,
    ) -> StreamResult<Vec<SystemStreamLineage>> {
        let history = load_system_stream_history(self).await?;
        history
            .as_ref()
            .validate_scope(&source_scope)
            .map_err(|error| crate::StreamError::validation(error.to_string()))?;
        Ok(history.as_ref().entries().to_vec())
    }

    async fn get_system_stream_page(
        &self,
        request: SystemStreamPageRequest,
    ) -> StreamResult<SystemStreamPage> {
        crate::system_stream::read::get_system_stream_page(self, request).await
    }

    async fn split_system_stream(
        &self,
        request: SystemStreamSplitRequest,
    ) -> StreamResult<SystemStreamSplitResult> {
        storage_common::retry::retry_native_conflicts(
            self,
            crate::constants::STREAM_WRITE_RETRY,
            || {
                let request = request.clone();
                self.with_topology_attempt(move |attempt| {
                    Box::pin(async move {
                        let (history, prior) = attempt
                            .read_system_split_state(request.operation_id())
                            .await?;
                        if let Some(prior) = prior {
                            return history.replay_split(&request, &prior).map_err(|error| {
                                storage_types::StorageError::validation(error.to_string())
                            });
                        }
                        let receipt = history.plan_split(&request).map_err(|error| {
                            storage_types::StorageError::validation(error.to_string())
                        })?;
                        attempt.write_system_split(&receipt).await?;
                        Ok(receipt.split)
                    })
                })
            },
        )
        .await
        .map_err(StreamError::from_storage_error)
    }
}
