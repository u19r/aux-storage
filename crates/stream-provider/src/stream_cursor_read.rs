//! Read policies keep native frontiers out of caller-bounded cursor operations.

use std::future::Future;

use storage_types::{StorageError, StorageResult, StreamItemId};

use crate::{StreamAttempt, StreamReadResult, StreamRowKind, StreamRowReadKind};

/// Ordinary cursors use the provider's existing native frontier.
pub struct NativeStreamRead;

/// Application-owned user cursors read the complete committed snapshot;
/// their logical IDs do not belong to the provider's native commit frontier.
pub struct CommittedUserStreamRead;

/// The cursor flow is shared; the selected read policy is monomorphized.
pub trait StreamCursorRead: Send + Sync {
    fn read<A: StreamAttempt>(
        &self,
        attempt: &mut A,
        stream: &StreamRowKind,
        request: StreamRowReadKind<'_>,
    ) -> impl Future<Output = StorageResult<StreamReadResult>> + Send;
}

impl StreamCursorRead for NativeStreamRead {
    async fn read<A: StreamAttempt>(
        &self,
        attempt: &mut A,
        stream: &StreamRowKind,
        request: StreamRowReadKind<'_>,
    ) -> StorageResult<StreamReadResult> {
        attempt.read_stream(stream.row_read(request)).await
    }
}

impl StreamCursorRead for CommittedUserStreamRead {
    async fn read<A: StreamAttempt>(
        &self,
        attempt: &mut A,
        stream: &StreamRowKind,
        request: StreamRowReadKind<'_>,
    ) -> StorageResult<StreamReadResult> {
        let StreamRowKind::User(name) = stream else {
            return Err(StorageError::internal(
                "bounded user cursor has another stream kind",
            ));
        };
        match request {
            StreamRowReadKind::Page(request) => attempt
                .read_user_page_through(name, request, StreamItemId::MAX)
                .await
                .map(StreamReadResult::Items),
            // Addressed point reads already bypass native commit frontiers.
            point => attempt.read_stream(stream.row_read(point)).await,
        }
    }
}
