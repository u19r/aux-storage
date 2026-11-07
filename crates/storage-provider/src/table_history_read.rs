//! A table read captures source publication bounds and retention authority
//! together.

use storage_types::{
    BoundTableInfo, StorageError, StorageResult, SystemStreamPosition, TableHistoryLane,
    TableStreamCheckpoint, TableStreamFrontier, TableStreamLaneFrontier, TableStreamSequence,
};
use stream_provider::{
    ReadDirection, SystemStreamHistory, SystemStreamLineage, TableHistoryIndexRead,
};

use crate::{DynamoTableStreamAttempt, StreamTrimState};

pub(crate) struct TableHistoryReadLane {
    pub lane: TableHistoryLane,
    pub lineage: SystemStreamLineage,
    pub origin: SystemStreamPosition,
    pub through: SystemStreamPosition,
}

pub(crate) struct TableHistoryReadState {
    pub source: SystemStreamHistory,
    pub retention: StreamTrimState,
    pub lanes: Vec<TableHistoryReadLane>,
}

impl TableHistoryReadState {
    pub(crate) async fn load(
        attempt: &mut impl DynamoTableStreamAttempt,
        table: &BoundTableInfo,
    ) -> StorageResult<Self> {
        let source = attempt.read_system_page_start().await?;
        let retention = attempt
            .load_table_history_retention(table, storage_types::ReadProtection::Snapshot)
            .await?
            .ok_or_else(|| StorageError::internal("Table history has no retention authority"))?;
        retention.require_valid_frontier()?;
        if retention.frontier.table_history_id != table.history_id
            || retention.frontier.source_scope != *source.as_ref().source_scope()
        {
            return Err(StorageError::internal(
                "Table history source authority differs",
            ));
        }
        let origins = source
            .retirement_origins()
            .map_err(|_| StorageError::internal("Invalid source origins"))?;
        let boundaries = source
            .retirement_boundaries()
            .map_err(|_| StorageError::internal("Invalid source publication bounds"))?;
        let lanes = origins
            .into_iter()
            .zip(boundaries)
            .map(|bounds| TableHistoryReadLane::bind_source(table, &source, bounds))
            .collect::<StorageResult<Vec<_>>>()?;
        Ok(Self {
            source,
            retention,
            lanes,
        })
    }

    pub(crate) fn select_lanes(
        &self,
        shard: Option<&str>,
    ) -> StorageResult<Vec<&TableHistoryReadLane>> {
        let lanes = self
            .lanes
            .iter()
            .filter(|lane| {
                shard.is_none_or(|shard| lane.lane.public_shard_id().is_ok_and(|id| id == shard))
            })
            .collect::<Vec<_>>();
        if lanes.is_empty() {
            return Err(crate::dynamodb_streams::read::stream_not_found());
        }
        Ok(lanes)
    }

    pub(crate) fn checkpoint(
        &self,
        table: &BoundTableInfo,
    ) -> StorageResult<TableStreamCheckpoint> {
        TableStreamCheckpoint::try_new(
            table.history_id,
            self.source.as_ref().source_scope().clone(),
            self.lanes.iter().map(|lane| lane.through.clone()).collect(),
            storage_types::TableStreamReadProgress {
                next_lane: 0,
                initial_visibility_cutoff: None,
            },
        )
    }

    pub(crate) async fn frontier(
        &self,
        attempt: &mut impl DynamoTableStreamAttempt,
        table: &BoundTableInfo,
    ) -> StorageResult<TableStreamFrontier> {
        let reads = self
            .lanes
            .iter()
            .map(TableHistoryReadLane::boundary_reads)
            .collect::<StorageResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let pages = stream_provider::read_table_history_indexes(attempt, &reads).await?;
        let lanes = self
            .lanes
            .iter()
            .zip(pages.as_chunks::<2>().0)
            .map(|(lane, pages)| lane.decode_frontier(pages))
            .collect::<StorageResult<Vec<_>>>()?;
        Ok(TableStreamFrontier {
            table_history_id: table.history_id,
            source_scope: self.source.as_ref().source_scope().clone(),
            lanes,
        })
    }
}

impl TableHistoryReadLane {
    fn boundary_reads(&self) -> StorageResult<[TableHistoryIndexRead; 2]> {
        let through = TableStreamSequence::try_from(self.through.offset())?;
        Ok(
            [ReadDirection::Forward, ReadDirection::Backward].map(|direction| {
                TableHistoryIndexRead {
                    lane: self.lane.clone(),
                    exclusive_start: None,
                    through,
                    direction,
                    limit: std::num::NonZeroU32::MIN,
                }
            }),
        )
    }

    fn decode_frontier(
        &self,
        pages: &[stream_provider::TableHistoryIndexPage],
    ) -> StorageResult<TableStreamLaneFrontier> {
        let [oldest, head] = pages else {
            return Err(StorageError::internal("Table frontier omitted a boundary"));
        };
        Ok(TableStreamLaneFrontier {
            shard_id: self.lane.shard_id().clone(),
            lineage_id: self.lane.lineage_id().clone(),
            oldest: oldest
                .last_position
                .as_ref()
                .map(|position| TableStreamSequence::try_from(position.offset()))
                .transpose()?,
            head: head
                .last_position
                .as_ref()
                .map(|position| TableStreamSequence::try_from(position.offset()))
                .transpose()?,
        })
    }

    fn bind_source(
        table: &BoundTableInfo,
        source: &SystemStreamHistory,
        (origin, through): (SystemStreamPosition, SystemStreamPosition),
    ) -> StorageResult<Self> {
        let lineage = source
            .as_ref()
            .lineage(origin.lineage_id())
            .ok_or_else(|| StorageError::internal("Table history lineage is missing"))?
            .clone();
        Ok(Self {
            lane: TableHistoryLane::try_from((
                table.history_id,
                source.as_ref().source_scope(),
                &origin,
            ))?,
            lineage,
            origin,
            through,
        })
    }
}
