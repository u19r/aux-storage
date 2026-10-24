//! Cursor decisions share one native attempt with catalog and stream rows.

use std::num::NonZeroU32;

use infrastructure_provider::physical_attempt::PhysicalRecordOperation;
use storage_common::retry::retry_native_conflicts;
use storage_types::{AttemptMode, SemanticDecision, StorageError, StorageResult, StreamItemId};

use crate::{
    BootstrapCursorName, CursorName, CursorPage, CursorPosition, ReadDirection, StreamAttempt,
    StreamCursor, StreamError, StreamItem, StreamPage, StreamPartitioningMode, StreamPort,
    StreamReadKind, StreamReadRequest, StreamResult, StreamRowKind, StreamRowReadKind,
    StreamValidationKind, SystemStreamReadKind, constants::STREAM_WRITE_RETRY, validate_limit,
};

pub(crate) async fn create_cursor<P: StreamPort>(
    port: &P,
    stream: StreamRowKind,
    cursor_name: CursorName,
    position: CursorPosition,
) -> StreamResult<()> {
    validate_user_cursor_name(&cursor_name)?;
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
    validate_user_cursor_name(&cursor_name)?;
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
            crate::stream_kind::read_partitioning_mode(attempt, &stream).await?;
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
    validate_user_cursor_name(&cursor_name)?;
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
    validate_user_cursor_name(&cursor_name)?;
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
    validate_user_cursor_name(&cursor_name)?;
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

/// Observe protected System cursors and their retained rows in the caller's
/// native attempt. A missing position fails closed before any trim is staged.
pub async fn protected_cursor_floor_in_attempt<A: StreamAttempt>(
    attempt: &mut A,
) -> StorageResult<Option<StreamItemId>> {
    let mut settlement = PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let stream = StreamRowKind::System(storage_types::StreamName::system_table_stream());
        let cursors = attempt
            .read_stream(StreamReadKind::System(SystemStreamReadKind::Cursors))
            .await?
            .into_system_cursors()?;
        let (floor, has_head) = select_protected_cursor_floor(cursors, &stream)?;
        require_protected_cursor_position(attempt, &stream, floor).await?;
        if has_head {
            read_protected_cursor_head(attempt, &stream, floor).await
        } else {
            Ok(floor)
        }
    }
    .await;
    settlement.complete(result)
}

fn select_protected_cursor_floor(
    cursors: Vec<StreamCursor>,
    stream: &StreamRowKind,
) -> StorageResult<(Option<StreamItemId>, bool)> {
    let mut floor: Option<StreamItemId> = None;
    let mut has_head = false;
    for cursor in cursors {
        if !cursor.name.is_reserved_bootstrap() {
            continue;
        }
        if cursor.stream_name != *stream.value()
            || BootstrapCursorName::parse(&cursor.name).is_err()
        {
            return Err(StorageError::internal(
                "protected System cursor identity is invalid",
            ));
        }
        if cursor.position == StreamItemId::default() {
            has_head = true;
        } else {
            floor = Some(floor.map_or(cursor.position, |current| current.min(cursor.position)));
        }
    }
    Ok((floor, has_head))
}

async fn require_protected_cursor_position<A: StreamAttempt>(
    attempt: &mut A,
    stream: &StreamRowKind,
    floor: Option<StreamItemId>,
) -> StorageResult<()> {
    // Only the earliest cursor can affect this trim. Its successor is validated
    // on the next attempt after the current earliest position is released.
    let Some(position) = floor else {
        return Ok(());
    };
    let item = attempt
        .read_stream(stream.row_read(StreamRowReadKind::Point {
            partitioning_mode: StreamPartitioningMode::Single,
            item_id: position,
        }))
        .await?
        .into_item()?;
    if item.is_none() {
        return Err(StorageError::internal(
            "protected System cursor position is missing",
        ));
    }
    Ok(())
}

async fn read_protected_cursor_head<A: StreamAttempt>(
    attempt: &mut A,
    stream: &StreamRowKind,
    floor: Option<StreamItemId>,
) -> StorageResult<Option<StreamItemId>> {
    let request = page_request(
        StreamPartitioningMode::Single,
        None,
        ReadDirection::Forward,
        1,
    )?;
    let head = attempt
        .read_stream(stream.row_read(StreamRowReadKind::Page(&request)))
        .await?
        .into_items()?
        .finish(&request, stream)?
        .items
        .first()
        .map(|item| item.id);
    match head {
        Some(position) => Ok(Some(
            floor.map_or(position, |current| current.min(position)),
        )),
        None if floor.is_some() => Err(StorageError::internal(
            "protected System cursor has no retained rows",
        )),
        None => Ok(None),
    }
}

pub fn validate_user_cursor_name(cursor_name: &CursorName) -> StreamResult<()> {
    if cursor_name.is_reserved_bootstrap() {
        return Err(StreamError::validation(
            "protected cursor names are reserved",
        ));
    }
    Ok(())
}

fn page_request(
    partitioning_mode: StreamPartitioningMode,
    exclusive_start: Option<StreamItemId>,
    direction: ReadDirection,
    limit: u32,
) -> StorageResult<StreamReadRequest> {
    Ok(StreamReadRequest {
        partitioning_mode,
        exclusive_start,
        direction,
        limit: NonZeroU32::new(limit)
            .ok_or_else(|| StorageError::internal("validated stream cursor limit is zero"))?,
    })
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
