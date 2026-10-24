//! Shared ordered stream paging over family-owned physical row reads.

use std::num::NonZeroU32;

use storage_types::{AttemptMode, StorageError, StorageResult, StreamItemId, StreamName};

use crate::{
    ReadDirection, StreamAttempt, StreamCursor, StreamError, StreamItem, StreamKind, StreamPage,
    StreamPartitioningMode, StreamPort, StreamResult, StreamRowKind,
    system_stream::{
        SystemStreamCheckpointLookup, SystemStreamCheckpointReceipt, SystemStreamHistory,
        SystemStreamHistoryRecord, SystemStreamStoredRecord,
    },
    validate_limit,
};

/// One bounded physical read. The family lowerer fetches one extra row so
/// the shared result can distinguish an exhausted page from a continuation.
#[derive(Clone)]
pub struct StreamReadRequest {
    pub partitioning_mode: StreamPartitioningMode,
    pub exclusive_start: Option<StreamItemId>,
    pub direction: ReadDirection,
    pub limit: NonZeroU32,
}

pub struct StreamRowPage {
    pub rows: Vec<StreamItem>,
    pub more: bool,
}

pub type StreamReadKind<'a> = StreamKind<AddressedStreamRead<'a>, SystemStreamReadKind<'a>>;

/// The stream kind supplies the physical address for either row operation.
/// Page and point payloads therefore cannot name a different stream.
pub struct AddressedStreamRead<'a> {
    stream: &'a StreamRowKind,
    read: StreamRowReadKind<'a>,
}

impl<'a> AddressedStreamRead<'a> {
    #[must_use]
    pub fn into_parts(self) -> (&'a StreamRowKind, StreamRowReadKind<'a>) {
        (self.stream, self.read)
    }
}

pub enum StreamRowReadKind<'a> {
    Page(&'a StreamReadRequest),
    Point {
        partitioning_mode: StreamPartitioningMode,
        item_id: StreamItemId,
    },
}

pub enum SystemStreamReadKind<'a> {
    Rows(AddressedStreamRead<'a>),
    Catalog,
    Checkpoint(SystemStreamCheckpointLookup<'a>),
    Cursors,
}

impl StreamRowKind {
    pub fn row_read<'a>(&'a self, request: StreamRowReadKind<'a>) -> StreamReadKind<'a> {
        let addressed = AddressedStreamRead {
            stream: self,
            read: request,
        };
        match self {
            Self::System(_) => StreamReadKind::System(SystemStreamReadKind::Rows(addressed)),
            Self::Table(_) => StreamReadKind::Table(addressed),
            Self::Item(_) => StreamReadKind::Item(addressed),
            Self::User(_) => StreamReadKind::User(addressed),
        }
    }
}

pub enum StreamReadResult {
    Items(StreamRowPage),
    Item(Option<StreamItem>),
    SystemCatalog(SystemStreamHistory),
    SystemCheckpoint(Option<SystemStreamCheckpointReceipt>),
    SystemCursors(Vec<StreamCursor>),
}

/// One physical logical-history read, with its pointer rows in the same order.
pub struct SystemStreamHistoryReadPage {
    pub retired_through: Option<storage_types::SystemStreamPosition>,
    pub records: Vec<SystemStreamStoredRecord>,
    pub observations: Vec<SystemStreamHistoryRecord>,
    pub pointers: Vec<Option<StreamItem>>,
    pub more: bool,
}

impl StreamReadResult {
    pub fn into_items(self) -> StorageResult<StreamRowPage> {
        match self {
            Self::Items(page) => Ok(page),
            _ => Err(StorageError::internal(
                "stream backend returned another read kind",
            )),
        }
    }

    pub(crate) fn into_item(self) -> StorageResult<Option<StreamItem>> {
        match self {
            Self::Item(item) => Ok(item),
            _ => Err(StorageError::internal(
                "stream backend returned another read kind",
            )),
        }
    }

    pub fn into_system_catalog(self) -> StorageResult<SystemStreamHistory> {
        match self {
            Self::SystemCatalog(history) => Ok(history),
            _ => Err(StorageError::internal(
                "stream backend returned another read kind",
            )),
        }
    }

    pub(crate) fn into_system_checkpoint(
        self,
    ) -> StorageResult<Option<SystemStreamCheckpointReceipt>> {
        match self {
            Self::SystemCheckpoint(receipt) => Ok(receipt),
            _ => Err(StorageError::internal(
                "stream backend returned another read kind",
            )),
        }
    }

    pub(crate) fn into_system_cursors(self) -> StorageResult<Vec<StreamCursor>> {
        match self {
            Self::SystemCursors(cursors) => Ok(cursors),
            _ => Err(StorageError::internal(
                "stream backend returned another read kind",
            )),
        }
    }
}

/// A physical point read must agree with both components of its address.
pub fn verify_stream_item_identity(
    item: StreamItem,
    stream_name: &StreamName,
    item_id: StreamItemId,
) -> StorageResult<StreamItem> {
    if item.stream_name.as_ref() != Some(stream_name) || item.id != item_id {
        return Err(StorageError::internal(
            "stream item identity differs from its address",
        ));
    }
    Ok(item)
}

pub(crate) async fn read<P: StreamPort>(
    port: &P,
    kind: StreamRowKind,
    exclusive_start: Option<StreamItemId>,
    direction: ReadDirection,
    limit: u32,
) -> StreamResult<StreamPage> {
    validate_limit(limit)?;
    let request = StreamReadRequest {
        partitioning_mode: StreamPartitioningMode::Single,
        exclusive_start,
        direction,
        limit: NonZeroU32::new(limit)
            .ok_or_else(|| StreamError::validation(crate::StreamValidationKind::InvalidLimit))?,
    };
    let physical = port
        .with_stream_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let mut request = request;
                request.partitioning_mode =
                    crate::stream_kind::read_partitioning_mode(attempt, &kind).await?;
                let rows = attempt
                    .read_stream(kind.row_read(StreamRowReadKind::Page(&request)))
                    .await?
                    .into_items()?;
                Ok((rows, request, kind))
            })
        })
        .await
        .map_err(StreamError::from_storage_error)?;
    let (physical, request, kind) = physical;
    physical
        .finish(&request, &kind)
        .map_err(StreamError::from_storage_error)
}

impl StreamRowPage {
    pub fn finish(
        mut self,
        request: &StreamReadRequest,
        stream: &StreamRowKind,
    ) -> StorageResult<StreamPage> {
        let max_rows = request.limit.get() as usize + 1;
        if self.rows.len() > max_rows || self.more && self.rows.is_empty() {
            return Err(StorageError::internal(
                "stream physical page has invalid bounds",
            ));
        }
        let mut previous = request.exclusive_start;
        for row in &self.rows {
            if row.stream_name.as_ref() != Some(stream.value())
                || !ordered_after(previous, row.id, request.direction)
            {
                return Err(StorageError::internal(
                    "stream physical page has invalid order",
                ));
            }
            previous = Some(row.id);
        }
        let overflow = self.rows.len() > request.limit.get() as usize;
        self.rows.truncate(request.limit.get() as usize);
        let last_evaluated_key = self.rows.last().map(|row| row.id);
        Ok(StreamPage {
            items: self.rows,
            last_evaluated_key,
            has_more: overflow || self.more,
        })
    }
}

fn ordered_after(
    previous: Option<StreamItemId>,
    current: StreamItemId,
    direction: ReadDirection,
) -> bool {
    previous.is_none_or(|previous| match direction {
        ReadDirection::Forward => current > previous,
        ReadDirection::Backward => current < previous,
    })
}
