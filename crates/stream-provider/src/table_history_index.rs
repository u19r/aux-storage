//! Canonical entries ordered within one qualified table incarnation and
//! lineage.

use std::{future::Future, num::NonZeroU32};

use storage_types::{StorageError, StorageResult, TableHistoryLane, TableStreamSequence};

use crate::{PositionedStreamEntry, ReadDirection};

#[derive(Clone)]
pub struct TableHistoryIndexRead {
    pub lane: TableHistoryLane,
    pub exclusive_start: Option<TableStreamSequence>,
    pub through: TableStreamSequence,
    pub direction: ReadDirection,
    pub limit: NonZeroU32,
}

/// One oldest or newest address within the captured publication bound.
pub struct TableHistoryBoundaryRead {
    pub lane: TableHistoryLane,
    pub through: TableStreamSequence,
    pub direction: ReadDirection,
}

pub struct TableHistoryIndexPage {
    pub entries: Vec<PositionedStreamEntry>,
    pub last_position: Option<storage_types::SystemStreamPosition>,
    pub more: bool,
}

pub struct TableHistoryObservation {
    pub history_id: storage_types::TableHistoryId,
    pub position: storage_types::SystemStreamPosition,
    pub created_at: storage_types::TimestampMillis,
}

pub struct TableHistoryObservationPage {
    pub entries: Vec<TableHistoryObservation>,
    pub last_position: Option<storage_types::SystemStreamPosition>,
    pub more: bool,
}

/// Physical rows and boundary addresses join the caller's source-authority
/// snapshot. Neither opens another transaction.
pub trait TableHistoryIndexAttempt:
    infrastructure_provider::physical_attempt::PhysicalRecordAttempt
{
    fn load_table_history_boundaries(
        &mut self,
        reads: &[TableHistoryBoundaryRead],
    ) -> impl Future<
        Output = StorageResult<storage_types::Exact<Option<storage_types::SystemStreamPosition>>>,
    > + Send;

    fn load_table_history_observations(
        &mut self,
        reads: &[TableHistoryIndexRead],
    ) -> impl Future<Output = StorageResult<storage_types::Exact<TableHistoryObservationPage>>> + Send;

    fn load_table_history_rows(
        &mut self,
        reads: &[TableHistoryIndexRead],
    ) -> impl Future<Output = StorageResult<storage_types::Exact<TableHistoryIndexPage>>> + Send;
}

pub async fn read_table_history_indexes(
    attempt: &mut impl TableHistoryIndexAttempt,
    reads: &[TableHistoryIndexRead],
) -> StorageResult<Vec<TableHistoryIndexPage>> {
    let mut operation =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let result = async {
        let pages = operation.as_mut().load_table_history_rows(reads).await?;
        pages
            .try_zip_map(reads, |read, page| {
                for entry in &page.entries {
                    if &entry.source_scope != read.lane.source_scope() {
                        return Err(StorageError::internal(
                            "Table entry differs from qualified source",
                        ));
                    }
                    require_qualified_position(read, entry.history_id, &entry.position)?;
                }
                Ok(page)
            })
            .map(storage_types::Exact::into_vec)
    }
    .await;
    operation.complete(result)
}

pub async fn read_table_history_boundaries(
    attempt: &mut impl TableHistoryIndexAttempt,
    reads: &[TableHistoryBoundaryRead],
) -> StorageResult<Vec<Option<storage_types::SystemStreamPosition>>> {
    let mut operation =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let result = async {
        let positions = operation
            .as_mut()
            .load_table_history_boundaries(reads)
            .await?;
        for (read, position) in reads.iter().zip(&positions) {
            if let Some(position) = position {
                let sequence = TableStreamSequence::try_from(position.offset())?;
                if position.shard_id() != read.lane.shard_id()
                    || position.lineage_id() != read.lane.lineage_id()
                    || sequence > read.through
                {
                    return Err(StorageError::internal(
                        "Table boundary differs from qualified source",
                    ));
                }
            }
        }
        Ok(positions.into_vec())
    }
    .await;
    operation.complete(result)
}

pub async fn read_table_history_observations(
    attempt: &mut impl TableHistoryIndexAttempt,
    reads: &[TableHistoryIndexRead],
) -> StorageResult<storage_types::Exact<TableHistoryObservationPage>> {
    let mut operation =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let result = async {
        let pages = operation
            .as_mut()
            .load_table_history_observations(reads)
            .await?;
        for (read, page) in reads.iter().zip(&pages) {
            if page.entries.len() > read.limit.get() as usize
                || page.more && page.entries.is_empty()
                || page.last_position.as_ref() != page.entries.last().map(|entry| &entry.position)
            {
                return Err(StorageError::internal("Invalid table observation page"));
            }
            for entry in &page.entries {
                require_qualified_position(read, entry.history_id, &entry.position)?;
            }
        }
        Ok(pages)
    }
    .await;
    operation.complete(result)
}

fn require_qualified_position(
    read: &TableHistoryIndexRead,
    history_id: storage_types::TableHistoryId,
    position: &storage_types::SystemStreamPosition,
) -> StorageResult<()> {
    let sequence = TableStreamSequence::try_from(position.offset())?;
    let outside_start = read
        .exclusive_start
        .is_some_and(|start| match read.direction {
            ReadDirection::Forward => sequence <= start,
            ReadDirection::Backward => sequence >= start,
        });
    if history_id != read.lane.history_id()
        || position.shard_id() != read.lane.shard_id()
        || position.lineage_id() != read.lane.lineage_id()
        || sequence > read.through
        || outside_start
    {
        return Err(StorageError::internal(
            "Table entry differs from qualified source",
        ));
    }
    Ok(())
}
