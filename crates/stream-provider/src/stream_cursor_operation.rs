//! Cursor writes consume one protected stream observation on one native
//! attempt.

use std::num::NonZeroU32;

use infrastructure_provider::physical_attempt::PhysicalRecordOperation;
use storage_types::{SemanticDecision, StorageResult, StreamItemId, TimestampMillis};

use crate::{
    CommittedUserStreamRead, CursorName, CursorPage, CursorPosition, NativeStreamRead,
    ReadDirection, Stream, StreamAttempt, StreamCursor, StreamCursorRead, StreamError,
    StreamPartitioningMode, StreamReadRequest, StreamRowKind, StreamRowReadKind,
    accept_cursor_creation, accept_cursor_page, prepare_cursor_advance,
};

/// This is a short-lived borrow, not a transaction opener or durable owner.
/// Consuming methods keep the selected stream, layout and native attempt
/// together. A cursor operation cannot accidentally read another stream.
pub struct StreamCursorOperation<'native, A, R = NativeStreamRead> {
    attempt: &'native mut A,
    stream: StreamRowKind,
    partitioning_mode: StreamPartitioningMode,
    read: R,
}

impl<'native, A: StreamAttempt> StreamCursorOperation<'native, A> {
    /// Ordinary callers resolve the catalog once on this native attempt.
    pub async fn load(attempt: &'native mut A, stream: StreamRowKind) -> StorageResult<Self> {
        let partitioning_mode = {
            let mut settlement = PhysicalRecordOperation::new(attempt);
            let result =
                crate::stream_kind::read_partitioning_mode(settlement.as_mut(), &stream).await;
            settlement.complete(result)?
        };
        Ok(Self {
            attempt,
            stream,
            partitioning_mode,
            read: NativeStreamRead,
        })
    }

    /// The caller must bind the original user catalog and its complete
    /// committed application snapshot on this same native attempt.
    pub fn from_committed_stream(
        attempt: &'native mut A,
        stream: Stream,
    ) -> StreamCursorOperation<'native, A, CommittedUserStreamRead> {
        StreamCursorOperation {
            attempt,
            stream: StreamRowKind::User(stream.internal_id),
            partitioning_mode: stream.partitioning_mode,
            read: CommittedUserStreamRead,
        }
    }
}

impl<A: StreamAttempt, R: StreamCursorRead> StreamCursorOperation<'_, A, R> {
    pub async fn create(
        self,
        name: CursorName,
        position: CursorPosition,
        captured_at: TimestampMillis,
    ) -> StorageResult<SemanticDecision<()>> {
        let mut settlement = PhysicalRecordOperation::new(self.attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if let SemanticDecision::Rejected(error) =
                accept_cursor_creation(&name, attempt.read_cursor(&self.stream, &name).await?)
            {
                return Ok(SemanticDecision::Rejected(error));
            }
            let position = match position {
                CursorPosition::Head => StreamItemId::default(),
                CursorPosition::Tail => {
                    read_tail(attempt, &self.stream, self.partitioning_mode, &self.read).await?
                }
            };
            attempt
                .write_cursor(
                    StreamCursor {
                        name,
                        stream_name: self.stream.into_value(),
                        position,
                        created_at: captured_at,
                    },
                    None,
                )
                .await?;
            Ok(SemanticDecision::Accepted(()))
        }
        .await;
        settlement.complete(result)
    }

    pub async fn delete(self, name: &CursorName) -> StorageResult<SemanticDecision<()>> {
        let mut settlement = PhysicalRecordOperation::new(self.attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if attempt.read_cursor(&self.stream, name).await?.is_none() {
                return Ok(missing_cursor(name));
            }
            attempt.delete_cursor(&self.stream, name).await?;
            Ok(SemanticDecision::Accepted(()))
        }
        .await;
        settlement.complete(result)
    }

    pub async fn advance(
        self,
        name: &CursorName,
        to_item_id: StreamItemId,
    ) -> StorageResult<SemanticDecision<()>> {
        let mut settlement = PhysicalRecordOperation::new(self.attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(cursor) = attempt.read_cursor(&self.stream, name).await? else {
                return Ok(missing_cursor(name));
            };
            let pending = match prepare_cursor_advance(cursor, to_item_id) {
                SemanticDecision::Accepted(Some(pending)) => pending,
                SemanticDecision::Accepted(None) => return Ok(SemanticDecision::Accepted(())),
                SemanticDecision::Rejected(error) => return Ok(SemanticDecision::Rejected(error)),
            };
            let request = StreamRowReadKind::Point {
                partitioning_mode: self.partitioning_mode,
                item_id: to_item_id,
            };
            let target = self
                .read
                .read(attempt, &self.stream, request)
                .await?
                .into_item()?;
            let (cursor, previous) = match pending.accept(target) {
                SemanticDecision::Accepted(write) => write,
                SemanticDecision::Rejected(error) => return Ok(SemanticDecision::Rejected(error)),
            };
            attempt.write_cursor(cursor, Some(previous)).await?;
            Ok(SemanticDecision::Accepted(()))
        }
        .await;
        settlement.complete(result)
    }

    pub async fn read(
        self,
        name: &CursorName,
        limit: NonZeroU32,
    ) -> StorageResult<SemanticDecision<CursorPage>> {
        let mut settlement = PhysicalRecordOperation::new(self.attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(cursor) = attempt.read_cursor(&self.stream, name).await? else {
                return Ok(missing_cursor(name));
            };
            let request = StreamReadRequest {
                partitioning_mode: self.partitioning_mode,
                exclusive_start: Some(cursor.position),
                direction: ReadDirection::Forward,
                limit,
            };
            let page = self
                .read
                .read(attempt, &self.stream, StreamRowReadKind::Page(&request))
                .await?
                .into_items()?
                .finish(&request, &self.stream)?;
            let (page, write) = accept_cursor_page(cursor, page);
            if let Some((cursor, previous)) = write {
                attempt.write_cursor(cursor, Some(previous)).await?;
            }
            Ok(SemanticDecision::Accepted(page))
        }
        .await;
        settlement.complete(result)
    }
}

fn missing_cursor<T>(name: &CursorName) -> SemanticDecision<T> {
    SemanticDecision::Rejected(StreamError::cursor_not_found(name.as_str()).into_storage_error())
}

async fn read_tail<A: StreamAttempt, R: StreamCursorRead>(
    attempt: &mut A,
    stream: &StreamRowKind,
    partitioning_mode: StreamPartitioningMode,
    read: &R,
) -> StorageResult<StreamItemId> {
    let request = StreamReadRequest {
        partitioning_mode,
        exclusive_start: None,
        direction: ReadDirection::Backward,
        limit: NonZeroU32::MIN,
    };
    Ok(read
        .read(attempt, stream, StreamRowReadKind::Page(&request))
        .await?
        .into_items()?
        .finish(&request, stream)?
        .items
        .first()
        .map_or(StreamItemId::default(), |item| item.id))
}
