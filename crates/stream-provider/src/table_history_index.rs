//! Table indexes contain references ordered within one qualified lineage.

use std::{future::Future, num::NonZeroU32};

use storage_types::{
    ItemHistoryPointer, ItemHistoryPublication, StorageError, StorageResult, TableHistoryLane,
    TableStreamSequence, storage_serde,
};

use crate::{
    ReadDirection, StreamDataType, StreamItem, StreamPartitioningMode, StreamReadRequest,
    StreamRowKind, StreamRowPage,
};

#[derive(Clone)]
pub struct TableHistoryIndexRead {
    pub lane: TableHistoryLane,
    pub exclusive_start: Option<TableStreamSequence>,
    pub through: TableStreamSequence,
    pub direction: ReadDirection,
    pub limit: NonZeroU32,
}

impl TableHistoryIndexRead {
    pub fn row_read(&self) -> StreamReadRequest {
        StreamReadRequest {
            partitioning_mode: StreamPartitioningMode::Single,
            exclusive_start: self.exclusive_start.map(TableStreamSequence::native_key),
            direction: self.direction,
            limit: self.limit,
        }
    }

    fn finish(&self, rows: StreamRowPage) -> StorageResult<TableHistoryIndexPage> {
        let kind = StreamRowKind::Table(self.lane.physical_stream_name()?);
        let page = rows.finish(&self.row_read(), &kind)?;
        let pointers = page
            .items
            .into_iter()
            .map(|row| self.decode_pointer(row))
            .collect::<StorageResult<Vec<_>>>()?;
        let last_position = pointers
            .last()
            .map(|pointer| pointer_event(pointer).map(|(_, position, _)| position.clone()))
            .transpose()?;
        Ok(TableHistoryIndexPage {
            pointers,
            more: page.has_more,
            last_position,
        })
    }

    fn decode_pointer(&self, row: StreamItem) -> StorageResult<ItemHistoryPointer> {
        if row.data_type != StreamDataType::StreamPointer || row.id > self.through.native_key() {
            return Err(invalid_index());
        }
        let pointer = decode_table_history_pointer(&row)?;
        let (scope, position, _) = pointer_event(&pointer)?;
        let lane =
            TableHistoryLane::try_from((pointer.metadata().address().history_id, scope, position))?;
        if lane != self.lane {
            return Err(invalid_index());
        }
        Ok(pointer)
    }
}

pub struct TableHistoryIndexPage {
    pub pointers: Vec<ItemHistoryPointer>,
    pub last_position: Option<storage_types::SystemStreamPosition>,
    pub more: bool,
}

/// Physical row reads join the caller's catalog/source-authority snapshot.
/// They neither resolve payloads nor open another transaction.
pub trait TableHistoryIndexAttempt:
    infrastructure_provider::physical_attempt::PhysicalRecordAttempt
{
    fn load_table_history_rows(
        &mut self,
        reads: &[TableHistoryIndexRead],
    ) -> impl Future<Output = StorageResult<Vec<StreamRowPage>>> + Send;
}

pub async fn read_table_history_indexes(
    attempt: &mut impl TableHistoryIndexAttempt,
    reads: &[TableHistoryIndexRead],
) -> StorageResult<Vec<TableHistoryIndexPage>> {
    let mut operation =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let result = async {
        let pages = operation.as_mut().load_table_history_rows(reads).await?;
        if pages.len() != reads.len() {
            return Err(invalid_index());
        }
        reads
            .iter()
            .zip(pages)
            .map(|(read, rows)| read.finish(rows))
            .collect()
    }
    .await;
    operation.complete(result)
}

/// Canonical index values use the ordinary opaque envelope; they own no
/// retained-image manifest. Native FDB keys bind a pending pointer at decode.
impl TryFrom<&ItemHistoryPointer> for StreamItem {
    type Error = StorageError;

    fn try_from(pointer: &ItemHistoryPointer) -> StorageResult<Self> {
        let (scope, position, created_at) = pointer_event(pointer)?;
        let lane =
            TableHistoryLane::try_from((pointer.metadata().address().history_id, scope, position))?;
        Ok(Self {
            id: TableStreamSequence::try_from(position.offset())?.native_key(),
            stream_name: Some(lane.physical_stream_name()?),
            data: storage_serde::to_bytes(pointer)?,
            data_type: StreamDataType::StreamPointer,
            created_at,
        })
    }
}

pub(crate) fn decode_table_history_pointer(row: &StreamItem) -> StorageResult<ItemHistoryPointer> {
    if row.data_type != StreamDataType::StreamPointer {
        return Err(invalid_index());
    }
    let mut pointer: ItemHistoryPointer = storage_serde::from_bytes(&row.data)?;
    if pointer_event(&pointer)?.1.offset().versionstamp().is_some() {
        pointer = pointer.with_versionstamp(*row.id.as_bytes())?;
    }
    let (scope, position, created_at) = pointer_event(&pointer)?;
    let lane =
        TableHistoryLane::try_from((pointer.metadata().address().history_id, scope, position))?;
    if row.stream_name.as_ref() != Some(&lane.physical_stream_name()?)
        || created_at != row.created_at
        || TableStreamSequence::try_from(position.offset())?.native_key() != row.id
    {
        return Err(invalid_index());
    }
    Ok(pointer)
}

fn pointer_event(
    pointer: &ItemHistoryPointer,
) -> StorageResult<(
    &storage_types::SystemStreamScope,
    &storage_types::SystemStreamPosition,
    storage_types::TimestampMillis,
)> {
    match pointer.metadata().publication() {
        ItemHistoryPublication::Event {
            source_scope,
            table_position,
            created_at,
            ..
        } => Ok((source_scope, table_position, *created_at)),
        ItemHistoryPublication::PredecessorAnchor => Err(invalid_index()),
    }
}

fn invalid_index() -> StorageError {
    StorageError::internal("Table history index differs from its qualified source")
}
