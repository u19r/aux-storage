//! Protected history cursors use the ordinary cursor rows and native attempt.

use std::num::NonZeroU32;

use storage_common::retry::retry_native_conflicts;
use storage_types::{AttemptMode, StorageError, StorageResult, StreamItemId, StreamName};

use crate::{
    BootstrapCursorName, CursorPosition, ProtectedCursorPage, ProtectedCursorPageEnd,
    ProtectedCursorReadRequest, ReadDirection, StreamAttempt, StreamContinuityKind, StreamCursor,
    StreamError, StreamPartitioningMode, StreamPort, StreamReadRequest, StreamResult,
    StreamRowKind, StreamRowReadKind, constants::STREAM_WRITE_RETRY,
};

pub(crate) fn system_stream(stream_name: StreamName) -> StreamResult<StreamRowKind> {
    if stream_name != StreamName::system_table_stream() {
        return Err(StreamError::validation(
            "protected history requires the System stream",
        ));
    }
    Ok(StreamRowKind::System(stream_name))
}

pub(crate) async fn create_hold<P: StreamPort>(
    port: &P,
    stream_name: StreamName,
    cursor_name: BootstrapCursorName,
    position: CursorPosition,
) -> StreamResult<StreamCursor> {
    let stream = system_stream(stream_name)?;
    let captured_at = port.now();
    retry_native_conflicts(port, STREAM_WRITE_RETRY, || {
        let stream = stream.clone();
        let name = cursor_name.cursor_name().clone();
        port.with_stream_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                if let Some(cursor) = attempt.read_cursor(&stream, &name).await? {
                    require_retained_position(attempt, &stream, cursor.position).await?;
                    return Ok(cursor);
                }
                let position = match position {
                    CursorPosition::Head => StreamItemId::default(),
                    CursorPosition::Tail => read_tail(attempt, &stream).await?,
                };
                let cursor = StreamCursor {
                    name,
                    stream_name: stream.value().clone(),
                    position,
                    created_at: captured_at,
                };
                attempt.write_cursor(cursor.clone(), None).await?;
                Ok(cursor)
            })
        })
    })
    .await
    .map_err(StreamError::from_storage_error)
}

pub(crate) async fn read_hold<P: StreamPort>(
    port: &P,
    request: ProtectedCursorReadRequest,
) -> StreamResult<ProtectedCursorPage> {
    let stream = system_stream(request.stream_name().clone())?;
    port.with_stream_attempt(AttemptMode::ReadOnly, move |attempt| {
        Box::pin(async move {
            let cursor = attempt
                .read_cursor(&stream, request.cursor_name().cursor_name())
                .await?
                .ok_or_else(|| continuity(StreamContinuityKind::CursorMissing))?;
            if cursor.position != request.expected_position() {
                return Err(continuity(StreamContinuityKind::BoundaryMismatch));
            }
            if cursor.position < request.protected_boundary() {
                return Err(continuity(StreamContinuityKind::CursorPositionRegressed));
            }
            require_retained_position(attempt, &stream, cursor.position).await?;
            let page_request = page_request(
                Some(cursor.position),
                ReadDirection::Forward,
                request.limit(),
            )?;
            let page = attempt
                .read_stream(stream.row_read(StreamRowReadKind::Page(&page_request)))
                .await?
                .into_items()?
                .finish(&page_request, &stream)?;
            let position = page.items.last().map_or(cursor.position, |item| item.id);
            let end = if page.has_more {
                ProtectedCursorPageEnd::More
            } else {
                ProtectedCursorPageEnd::Terminal
            };
            Ok(ProtectedCursorPage::from_provider(
                page.items, position, end,
            ))
        })
    })
    .await
    .map_err(StreamError::from_storage_error)
}

async fn require_retained_position<A: StreamAttempt>(
    attempt: &mut A,
    stream: &StreamRowKind,
    position: StreamItemId,
) -> StorageResult<()> {
    if position == StreamItemId::default() {
        return Ok(());
    }
    if attempt
        .read_stream(stream.row_read(StreamRowReadKind::Point {
            partitioning_mode: StreamPartitioningMode::Single,
            item_id: position,
        }))
        .await?
        .into_item()?
        .is_none()
    {
        return Err(continuity(StreamContinuityKind::ProtectedRetentionLost));
    }
    Ok(())
}

async fn read_tail<A: StreamAttempt>(
    attempt: &mut A,
    stream: &StreamRowKind,
) -> StorageResult<StreamItemId> {
    let request = page_request(None, ReadDirection::Backward, 1)?;
    Ok(attempt
        .read_stream(stream.row_read(StreamRowReadKind::Page(&request)))
        .await?
        .into_items()?
        .finish(&request, stream)?
        .items
        .first()
        .map_or(StreamItemId::default(), |item| item.id))
}

fn page_request(
    exclusive_start: Option<StreamItemId>,
    direction: ReadDirection,
    limit: u32,
) -> StorageResult<StreamReadRequest> {
    Ok(StreamReadRequest {
        partitioning_mode: StreamPartitioningMode::Single,
        exclusive_start,
        direction,
        limit: NonZeroU32::new(limit)
            .ok_or_else(|| StorageError::internal("validated protected page limit is zero"))?,
    })
}

fn continuity(kind: StreamContinuityKind) -> StorageError {
    StreamError::continuity(kind, kind.message()).into_storage_error()
}
