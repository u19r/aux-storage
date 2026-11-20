//! Cursor decisions share one native attempt with catalog and stream rows.

use std::{num::NonZeroU32, sync::Arc};

use storage_types::{SemanticDecision, StorageError, StorageResult, StreamItemId};

use crate::{
    CursorName, CursorPage, CursorPosition, StreamAddress, StreamAttempt, StreamCursor,
    StreamError, StreamItem, StreamPage, StreamPort, StreamResult, StreamValidationKind,
    constants::{CURSOR_PAGE_READ, STREAM_POINT_READ, STREAM_WRITE},
    validate_limit,
};

struct CreateCursorInput {
    stream: StreamAddress,
    cursor_name: CursorName,
    position: CursorPosition,
    captured_at: storage_types::TimestampMillis,
}

pub(crate) async fn create_cursor<P: StreamPort>(
    port: &P,
    stream: StreamAddress,
    cursor_name: CursorName,
    position: CursorPosition,
) -> StreamResult<()> {
    crate::validation::validate_cursor_name(&cursor_name)?;
    let captured_at = port.now();
    let input = Arc::new(CreateCursorInput {
        stream,
        cursor_name,
        position,
        captured_at,
    });
    port.run(STREAM_WRITE, input, |attempt, input, _| {
        Box::pin(create_cursor_in_attempt(attempt, input))
    })
    .await
    .map_err(StreamError::from_storage_error)
}

/// One try: the stream's cursor family under its catalog row, then the new
/// cursor.
async fn create_cursor_in_attempt<A: StreamAttempt>(
    attempt: &mut A,
    input: &CreateCursorInput,
) -> StorageResult<()> {
    crate::StreamCursorOperation::load(attempt, input.stream.clone())
        .await?
        .create(input.cursor_name.clone(), input.position, input.captured_at)
        .await?
        .into_result()
}

pub(crate) async fn get_cursor<P: StreamPort>(
    port: &P,
    stream: StreamAddress,
    cursor_name: CursorName,
) -> StreamResult<Option<StreamCursor>> {
    load_cursor(port, stream, cursor_name).await
}

pub(crate) async fn load_cursor<P: StreamPort>(
    port: &P,
    stream: StreamAddress,
    cursor_name: CursorName,
) -> StreamResult<Option<StreamCursor>> {
    port.with_attempt(
        STREAM_POINT_READ.start(port.attempt_deadlines()),
        move |attempt| {
            Box::pin(async move {
                // Catalog resolution and cursor read share one native attempt.
                let (stream, _) = crate::stream_kind::resolve_stream(attempt, &stream).await?;
                attempt.read_cursor(&stream, &cursor_name).await
            })
        },
    )
    .await
    .map_err(StreamError::from_storage_error)
}

pub(crate) async fn delete_cursor<P: StreamPort>(
    port: &P,
    stream: StreamAddress,
    cursor_name: CursorName,
) -> StreamResult<()> {
    remove_cursor(port, stream, cursor_name).await
}

struct CursorInput {
    stream: StreamAddress,
    cursor_name: CursorName,
}

pub(crate) async fn remove_cursor<P: StreamPort>(
    port: &P,
    stream: StreamAddress,
    cursor_name: CursorName,
) -> StreamResult<()> {
    let input = Arc::new(CursorInput {
        stream,
        cursor_name,
    });
    port.run(STREAM_WRITE, input, |attempt, input, _| {
        Box::pin(remove_cursor_in_attempt(attempt, input))
    })
    .await
    .map_err(StreamError::from_storage_error)
}

/// One try: the stream's cursor family, then the cursor delete.
async fn remove_cursor_in_attempt<A: StreamAttempt>(
    attempt: &mut A,
    input: &CursorInput,
) -> StorageResult<()> {
    crate::StreamCursorOperation::load(attempt, input.stream.clone())
        .await?
        .delete(&input.cursor_name)
        .await?
        .into_result()
}

struct AdvanceCursorInput {
    stream: StreamAddress,
    cursor_name: CursorName,
    to_item_id: StreamItemId,
}

pub(crate) async fn advance_cursor<P: StreamPort>(
    port: &P,
    stream: StreamAddress,
    cursor_name: CursorName,
    to_item_id: StreamItemId,
) -> StreamResult<()> {
    let input = Arc::new(AdvanceCursorInput {
        stream,
        cursor_name,
        to_item_id,
    });
    port.run(STREAM_WRITE, input, |attempt, input, _| {
        Box::pin(advance_cursor_in_attempt(attempt, input))
    })
    .await
    .map_err(StreamError::from_storage_error)
}

/// One try: the stream's cursor family, then the cursor advance.
async fn advance_cursor_in_attempt<A: StreamAttempt>(
    attempt: &mut A,
    input: &AdvanceCursorInput,
) -> StorageResult<()> {
    crate::StreamCursorOperation::load(attempt, input.stream.clone())
        .await?
        .advance(&input.cursor_name, input.to_item_id)
        .await?
        .into_result()
}

struct ReadCursorInput {
    stream: StreamAddress,
    cursor_name: CursorName,
    limit: u32,
}

/// Reading advances the cursor atomically, so two consumers cannot both
/// commit the same page from one cursor position.
pub(crate) async fn read_from_cursor<P: StreamPort>(
    port: &P,
    stream: StreamAddress,
    cursor_name: CursorName,
    limit: u32,
) -> StreamResult<CursorPage> {
    validate_limit(limit)?;
    let input = Arc::new(ReadCursorInput {
        stream,
        cursor_name,
        limit,
    });
    port.run(CURSOR_PAGE_READ, input, |attempt, input, _| {
        Box::pin(read_cursor_in_attempt(attempt, input))
    })
    .await
    .map_err(StreamError::from_storage_error)
}

/// One try: the stream's cursor family, then the page read and cursor advance.
async fn read_cursor_in_attempt<A: StreamAttempt>(
    attempt: &mut A,
    input: &ReadCursorInput,
) -> StorageResult<CursorPage> {
    crate::StreamCursorOperation::load(attempt, input.stream.clone())
        .await?
        .read(
            &input.cursor_name,
            NonZeroU32::new(input.limit)
                .ok_or_else(|| StorageError::internal("validated stream cursor limit is zero"))?,
        )
        .await?
        .into_result()
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
