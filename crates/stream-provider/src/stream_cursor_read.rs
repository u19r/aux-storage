//! Read policies keep native frontiers out of caller-bounded cursor operations.

use storage_types::{StorageResult, StreamItemId};

use crate::{StreamAttempt, StreamReadResult, StreamRowReadKind};

/// The cursor flow is shared; the read policy selects the native frontier.
#[derive(Clone, Copy)]
pub enum StreamCursorRead {
    /// Ordinary cursors use the provider's existing native frontier.
    Native,
    /// Application-owned user cursors read the complete committed snapshot;
    /// their logical IDs do not belong to the provider's native commit
    /// frontier.
    CommittedUser,
}

impl StreamCursorRead {
    pub async fn read<A: StreamAttempt>(
        self,
        attempt: &mut A,
        stream: &storage_types::StreamName,
        request: StreamRowReadKind<'_>,
    ) -> StorageResult<StreamReadResult> {
        match (self, request) {
            (Self::CommittedUser, StreamRowReadKind::Page(request)) => attempt
                .read_user_page_through(stream, request, StreamItemId::MAX)
                .await
                .map(StreamReadResult::Items),
            // Addressed point reads already bypass native commit frontiers.
            (_, read) => {
                attempt
                    .read_stream(crate::StreamReadKind { stream, read })
                    .await
            }
        }
    }
}
