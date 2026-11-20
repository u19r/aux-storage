//! Shared ordered stream paging over family-owned physical row reads.

use std::num::NonZeroU32;

use storage_types::{StorageError, StorageResult, StreamItemId, StreamName};

use crate::{
    ReadDirection, StreamAddress, StreamAttempt, StreamError, StreamItem, StreamPage,
    StreamPartitioningMode, StreamPort, StreamResult, validate_limit,
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

/// One ordinary row read carries its physical address directly.
pub struct StreamReadKind<'a> {
    pub stream: &'a StreamName,
    pub read: StreamRowReadKind<'a>,
}

pub enum StreamRowReadKind<'a> {
    Page(&'a StreamReadRequest),
    Point {
        partitioning_mode: StreamPartitioningMode,
        item_id: StreamItemId,
    },
}

pub enum StreamReadResult {
    Items(StreamRowPage),
    Item(Option<StreamItem>),
}

/// One physical logical-history page, with canonical entries in source order.
pub struct SystemStreamHistoryReadPage {
    pub retired_through: Option<storage_types::SystemStreamPosition>,
    pub records: Vec<(
        crate::PositionedStreamEntry,
        Option<storage_types::TableHistoryState>,
    )>,
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
    address: StreamAddress,
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
    let (physical, request, stream) = port
        .with_attempt(
            crate::constants::STREAM_RANGE_READ.start(port.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    let mut request = request;
                    let (stream, partitioning_mode) =
                        crate::stream_kind::resolve_stream(attempt, &address).await?;
                    request.partitioning_mode = partitioning_mode;
                    let rows = attempt
                        .read_stream(StreamReadKind {
                            stream: &stream,
                            read: StreamRowReadKind::Page(&request),
                        })
                        .await?
                        .into_items()?;
                    Ok((rows, request, stream))
                })
            },
        )
        .await
        .map_err(StreamError::from_storage_error)?;
    physical
        .finish(&request, &stream)
        .map_err(StreamError::from_storage_error)
}

impl StreamRowPage {
    pub fn finish(
        mut self,
        request: &StreamReadRequest,
        stream: &storage_types::StreamName,
    ) -> StorageResult<StreamPage> {
        let max_rows = request.limit.get() as usize + 1;
        if self.rows.len() > max_rows || self.more && self.rows.is_empty() {
            return Err(StorageError::internal(
                "stream physical page has invalid bounds",
            ));
        }
        let mut previous = request.exclusive_start;
        for row in &self.rows {
            if row.stream_name.as_ref() != Some(stream)
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
