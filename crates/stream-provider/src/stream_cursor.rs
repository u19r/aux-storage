//! Cursor decisions share one native attempt with catalog and stream rows.

use std::num::NonZeroU32;

use storage_common::retry::retry_native_conflicts;
use storage_types::{AttemptMode, SemanticDecision, StorageError, StreamItemId};

use crate::{
    CursorName, CursorPage, CursorPosition, StreamAttempt, StreamCursor, StreamError, StreamItem,
    StreamPage, StreamPort, StreamResult, StreamRowKind, StreamValidationKind,
    constants::STREAM_WRITE_RETRY, validate_limit,
};

pub(crate) async fn create_cursor<P: StreamPort>(
    port: &P,
    stream: StreamRowKind,
    cursor_name: CursorName,
    position: CursorPosition,
) -> StreamResult<()> {
    let captured_at = port.now();
    retry_native_conflicts(port, STREAM_WRITE_RETRY, || {
        let stream = stream.clone();
        let cursor_name = cursor_name.clone();
        port.with_stream_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                crate::StreamCursorOperation::load(attempt, stream)
                    .await?
                    .create(cursor_name, position, captured_at)
                    .await?
                    .into_result()
            })
        })
    })
    .await
    .map_err(StreamError::from_storage_error)
}

pub(crate) async fn get_cursor<P: StreamPort>(
    port: &P,
    stream: StreamRowKind,
    cursor_name: CursorName,
) -> StreamResult<Option<StreamCursor>> {
    load_cursor(port, stream, cursor_name).await
}

pub(crate) async fn load_cursor<P: StreamPort>(
    port: &P,
    stream: StreamRowKind,
    cursor_name: CursorName,
) -> StreamResult<Option<StreamCursor>> {
    port.with_stream_attempt(AttemptMode::ReadOnly, move |attempt| {
        Box::pin(async move {
            // Catalog validation and cursor read share one native attempt.
            crate::stream_kind::resolve_stream_layout(attempt, &stream).await?;
            attempt.read_cursor(&stream, &cursor_name).await
        })
    })
    .await
    .map_err(StreamError::from_storage_error)
}

pub(crate) async fn delete_cursor<P: StreamPort>(
    port: &P,
    stream: StreamRowKind,
    cursor_name: CursorName,
) -> StreamResult<()> {
    remove_cursor(port, stream, cursor_name).await
}

pub(crate) async fn remove_cursor<P: StreamPort>(
    port: &P,
    stream: StreamRowKind,
    cursor_name: CursorName,
) -> StreamResult<()> {
    retry_native_conflicts(port, STREAM_WRITE_RETRY, || {
        let stream = stream.clone();
        let cursor_name = cursor_name.clone();
        port.with_stream_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                crate::StreamCursorOperation::load(attempt, stream)
                    .await?
                    .delete(&cursor_name)
                    .await?
                    .into_result()
            })
        })
    })
    .await
    .map_err(StreamError::from_storage_error)
}

pub(crate) async fn advance_cursor<P: StreamPort>(
    port: &P,
    stream: StreamRowKind,
    cursor_name: CursorName,
    to_item_id: StreamItemId,
) -> StreamResult<()> {
    retry_native_conflicts(port, STREAM_WRITE_RETRY, || {
        let stream = stream.clone();
        let cursor_name = cursor_name.clone();
        port.with_stream_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                crate::StreamCursorOperation::load(attempt, stream)
                    .await?
                    .advance(&cursor_name, to_item_id)
                    .await?
                    .into_result()
            })
        })
    })
    .await
    .map_err(StreamError::from_storage_error)
}

/// Reading advances the cursor atomically, so two consumers cannot both
/// commit the same page from one cursor position.
pub(crate) async fn read_from_cursor<P: StreamPort>(
    port: &P,
    stream: StreamRowKind,
    cursor_name: CursorName,
    limit: u32,
) -> StreamResult<CursorPage> {
    validate_limit(limit)?;
    retry_native_conflicts(port, STREAM_WRITE_RETRY, || {
        let stream = stream.clone();
        let cursor_name = cursor_name.clone();
        port.with_stream_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                crate::StreamCursorOperation::load(attempt, stream)
                    .await?
                    .read(
                        &cursor_name,
                        NonZeroU32::new(limit).ok_or_else(|| {
                            StorageError::internal("validated stream cursor limit is zero")
                        })?,
                    )
                    .await?
                    .into_result()
            })
        })
    })
    .await
    .map_err(StreamError::from_storage_error)
}

pub fn accept_cursor_creation(
    name: &CursorName,
    existing: Option<StreamCursor>,
) -> SemanticDecision<()> {
    if existing.is_some() {
        return SemanticDecision::Rejected(
            StreamError::cursor_already_exists(name.as_str()).into_storage_error(),
        );
    }
    SemanticDecision::Accepted(())
}

/// A forward transition that still requires its actual target observation.
pub struct PendingCursorAdvance {
    cursor: StreamCursor,
    target: StreamItemId,
}

pub fn prepare_cursor_advance(
    cursor: StreamCursor,
    target: StreamItemId,
) -> SemanticDecision<Option<PendingCursorAdvance>> {
    if target < cursor.position {
        return SemanticDecision::Rejected(StorageError::validation(
            "cursor position cannot move backward",
        ));
    }
    if target == cursor.position {
        return SemanticDecision::Accepted(None);
    }
    SemanticDecision::Accepted(Some(PendingCursorAdvance { cursor, target }))
}

impl PendingCursorAdvance {
    pub fn accept(
        mut self,
        target: Option<StreamItem>,
    ) -> SemanticDecision<(StreamCursor, StreamItemId)> {
        if target.is_none() {
            return SemanticDecision::Rejected(
                StreamError::validation(StreamValidationKind::TargetItemNotFound)
                    .into_storage_error(),
            );
        }
        let previous = self.cursor.position;
        self.cursor.position = self.target;
        SemanticDecision::Accepted((self.cursor, previous))
    }
}

/// Consume the page verified against this cursor's physical address and start.
pub fn accept_cursor_page(
    mut cursor: StreamCursor,
    page: StreamPage,
) -> (CursorPage, Option<(StreamCursor, StreamItemId)>) {
    let previous = cursor.position;
    let position = page.items.last().map_or(previous, |item| item.id);
    let write = if page.items.is_empty() {
        None
    } else {
        cursor.position = position;
        Some((cursor, previous))
    };
    (
        CursorPage {
            items: page.items,
            cursor_position: position,
            has_more: page.has_more,
        },
        write,
    )
}
