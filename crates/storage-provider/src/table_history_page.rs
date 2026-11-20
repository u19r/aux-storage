//! Admission, visibility and continuation advance one qualified source lane at
//! a time.

use std::num::NonZeroU32;

use storage_types::{
    BoundTableInfo, StorageEnum, StorageError, StorageResult, SystemStreamPosition,
    TableStreamCheckpoint, TableStreamFrontier, TableStreamSequence,
};
use stream_provider::{PositionedStreamEntry, ReadDirection, TableHistoryIndexRead};

use crate::{
    StreamRetentionPort,
    dynamo_table_stream::{
        DynamoTableStreamPage, TableStreamImageSelection, TableStreamReadOptions,
    },
    dynamodb_streams::record::DynamoStreamRecord,
    table_history_read::{TableHistoryReadLane, TableHistoryReadState},
};

pub(crate) struct TableHistoryPage<'a> {
    table: &'a BoundTableInfo,
    state: &'a TableHistoryReadState,
    lanes: Vec<&'a TableHistoryReadLane>,
    positions: Vec<SystemStreamPosition>,
    next_lane: usize,
    initial_visibility_cutoff: Option<storage_types::TimestampMillis>,
    options: TableStreamReadOptions<'a>,
    records: Vec<DynamoStreamRecord>,
    response_bytes: usize,
}

impl<'a> TableHistoryPage<'a> {
    pub(crate) fn prepare(
        table: &'a BoundTableInfo,
        state: &'a TableHistoryReadState,
        token: Option<&str>,
        options: TableStreamReadOptions<'a>,
    ) -> StorageResult<Self> {
        let lanes = state.select_lanes(options.shard_id)?;
        let checkpoint = Self::parse_checkpoint(table, state, token, &lanes)?;
        let positions = lanes
            .iter()
            .map(|lane| Self::start_position(state, lane, checkpoint.as_ref()))
            .collect::<StorageResult<Vec<_>>>()?;
        let next_lane = checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.next_lane() % lanes.len());
        Ok(Self {
            table,
            state,
            lanes,
            positions,
            next_lane,
            initial_visibility_cutoff: checkpoint.as_ref().map_or_else(
                || state.retention.retention.cutoff_at(options.now),
                TableStreamCheckpoint::initial_visibility_cutoff,
            ),
            options,
            records: Vec::new(),
            response_bytes: 0,
        })
    }

    fn parse_checkpoint(
        table: &BoundTableInfo,
        state: &TableHistoryReadState,
        token: Option<&str>,
        lanes: &[&TableHistoryReadLane],
    ) -> StorageResult<Option<TableStreamCheckpoint>> {
        let checkpoint = token.map(str::parse::<TableStreamCheckpoint>).transpose()?;
        if let Some(checkpoint) = &checkpoint {
            if checkpoint.history_id() != table.history_id {
                return Err(StorageError::table_not_found(&table.info.table_name));
            }
            if checkpoint.source_scope() != state.source.as_ref().source_scope()
                || checkpoint.positions().iter().any(|position| {
                    !lanes.iter().any(|lane| {
                        lane.origin.shard_id() == position.shard_id()
                            && lane.origin.lineage_id() == position.lineage_id()
                    })
                })
            {
                return Err(history_unavailable());
            }
        }
        Ok(checkpoint)
    }

    fn start_position(
        state: &TableHistoryReadState,
        lane: &TableHistoryReadLane,
        checkpoint: Option<&TableStreamCheckpoint>,
    ) -> StorageResult<SystemStreamPosition> {
        let retired = state
            .retention
            .frontier
            .lineages
            .iter()
            .find(|entry| {
                entry.shard_id == *lane.lane.shard_id()
                    && entry.lineage_id == *lane.lane.lineage_id()
            })
            .and_then(|entry| entry.retired_through.as_ref());
        let saved = checkpoint.and_then(|checkpoint| {
            checkpoint.positions().iter().find(|position| {
                position.shard_id() == lane.lane.shard_id()
                    && position.lineage_id() == lane.lane.lineage_id()
            })
        });
        let position = saved.cloned().unwrap_or_else(|| lane.origin.clone());
        if position
            .offset()
            .compare_order(lane.through.offset())
            .map_err(|_| history_unavailable())?
            .is_gt()
        {
            return Err(history_unavailable());
        }
        let Some(retired) = retired else {
            return Ok(position);
        };
        if !position
            .offset()
            .compare_order(retired)
            .map_err(|_| history_unavailable())?
            .is_lt()
        {
            return Ok(position);
        }
        if saved.is_some() {
            return Err(history_unavailable());
        }
        SystemStreamPosition::try_new(
            lane.lane.shard_id().as_str(),
            lane.lane.lineage_id().as_str(),
            retired.as_str(),
        )
        .map_err(|_| history_unavailable())
    }

    fn checkpoint(&self, next_lane: usize) -> StorageResult<String> {
        Ok(TableStreamCheckpoint::try_new(
            self.table.history_id,
            self.state.source.as_ref().source_scope().clone(),
            self.positions.clone(),
            storage_types::TableStreamReadProgress {
                next_lane,
                initial_visibility_cutoff: self.initial_visibility_cutoff,
            },
        )?
        .to_string())
    }

    pub(crate) async fn read(
        mut self,
        attempt: &mut impl StreamRetentionPort,
        limit: NonZeroU32,
        frontier: TableStreamFrontier,
    ) -> StorageResult<DynamoTableStreamPage> {
        if matches!(self.options.selection, TableStreamImageSelection::Snapshot) {
            self.response_bytes = serde_json::to_vec(&frontier)?.len();
        }
        let reads = (0..self.lanes.len())
            .map(|step| self.lane_read((self.next_lane + step) % self.lanes.len(), limit))
            .collect::<StorageResult<Vec<_>>>()?;
        let pages = stream_provider::read_table_history_indexes(attempt, &reads).await?;
        let first_lane = self.next_lane;
        for (step, page) in pages.into_iter().enumerate() {
            let index = (first_lane + step) % self.lanes.len();
            if self.include_page(index, page, limit)? {
                break;
            }
        }
        let sealed_end = self.lanes.len() == 1
            && self.lanes[0].lineage.sealed()
            && self.positions[0] == self.lanes[0].through;
        let checkpoint = Some(self.checkpoint(self.next_lane)?);
        Ok(DynamoTableStreamPage {
            records: self.records,
            checkpoint,
            frontier,
            sealed_end,
        })
    }

    fn lane_read(&self, index: usize, limit: NonZeroU32) -> StorageResult<TableHistoryIndexRead> {
        Ok(TableHistoryIndexRead {
            lane: self.lanes[index].lane.clone(),
            exclusive_start: Some(TableStreamSequence::try_from(
                self.positions[index].offset(),
            )?),
            through: TableStreamSequence::try_from(self.lanes[index].through.offset())?,
            direction: ReadDirection::Forward,
            limit: NonZeroU32::new(limit.get().div_ceil(self.lanes.len() as u32))
                .ok_or_else(|| StorageError::internal("table history lane quota is zero"))?,
        })
    }

    fn include_page(
        &mut self,
        index: usize,
        page: stream_provider::TableHistoryIndexPage,
        limit: NonZeroU32,
    ) -> StorageResult<bool> {
        for entry in page.entries {
            if entry.history_id != self.table.history_id
                || entry.source_table_name != self.table.table_name
                || &entry.source_scope != self.state.source.as_ref().source_scope()
            {
                return Err(history_unavailable());
            }
            if !self.is_visible(&entry)? {
                self.positions[index] = entry.position;
                continue;
            }
            if !entry.entry.is_public() {
                self.positions[index] = entry.position;
                continue;
            }
            if self.records.len() == limit.get() as usize {
                self.next_lane = index;
                return Ok(true);
            }
            let previous = self.positions[index].clone();
            self.positions[index] = entry.position.clone();
            let cursor = self.checkpoint((index + 1) % self.lanes.len())?;
            if !self.include_record(decode_record(entry, cursor)?)? {
                self.positions[index] = previous;
                self.next_lane = index;
                return Ok(true);
            }
        }
        if !page.more {
            self.positions[index] = self.lanes[index].through.clone();
        }
        self.next_lane = (index + 1) % self.lanes.len();
        Ok(self.records.len() == limit.get() as usize)
    }

    fn is_visible(&self, entry: &PositionedStreamEntry) -> StorageResult<bool> {
        let retired = self
            .state
            .retention
            .frontier
            .lineages
            .iter()
            .find(|lane| {
                &lane.shard_id == entry.position.shard_id()
                    && &lane.lineage_id == entry.position.lineage_id()
            })
            .and_then(|lane| lane.retired_through.as_ref())
            .map(|through| {
                entry
                    .position
                    .offset()
                    .compare_order(through)
                    .map(|order| !order.is_gt())
                    .map_err(|_| history_unavailable())
            })
            .transpose()?
            .unwrap_or(false);
        let created_at = entry.entry.created_at();
        let visible = !retired
            && self
                .state
                .retention
                .retention
                .cutoff_at(self.options.now)
                .is_none_or(|cutoff| created_at > cutoff);
        if !visible
            && !self
                .initial_visibility_cutoff
                .is_some_and(|cutoff| created_at <= cutoff)
        {
            return Err(history_unavailable());
        }
        Ok(visible)
    }

    fn measure_snapshot_record_bytes(record: &DynamoStreamRecord) -> StorageResult<usize> {
        Ok(serde_json::to_vec(&(
            &record.keys,
            None::<&storage_types::AttributeMap>,
            record.new_image.as_ref(),
            &record.source_table_name,
            &record.sequence_number,
        ))?
        .len()
            + 1024)
    }

    fn include_record(&mut self, mut record: DynamoStreamRecord) -> StorageResult<bool> {
        let (bytes, maximum_bytes, overhead) = match self.options.selection {
            TableStreamImageSelection::Snapshot => (
                Self::measure_snapshot_record_bytes(&record)?,
                storage_types::STREAM_RECORDS_MAX_ENCODED_BYTES as usize,
                self.checkpoint(self.next_lane)?.len()
                    + crate::constants::TABLE_STREAM_RESPONSE_RESERVE_BYTES,
            ),
            TableStreamImageSelection::DynamoDbImages => {
                // Standard Streams limits logical data, including raw binary
                // bytes. Snapshot framing and checkpoint reserves belong only
                // to Aux TABLE responses.
                match self
                    .table
                    .info
                    .stream_specification
                    .as_ref()
                    .and_then(|spec| spec.stream_view_type.as_ref())
                {
                    Some(storage_types::StreamViewType::KeysOnly) => {
                        record.old_image = None;
                        record.new_image = None;
                    }
                    Some(storage_types::StreamViewType::OldImage) => record.new_image = None,
                    Some(storage_types::StreamViewType::NewImage) => record.old_image = None,
                    _ => {}
                }
                record.size_bytes = storage_types::LogicalItemSize::sum_attribute_bytes(
                    record
                        .keys
                        .iter()
                        .chain(record.old_image.iter().flat_map(|image| image.iter()))
                        .chain(record.new_image.iter().flat_map(|image| image.iter())),
                )?;
                (
                    record.size_bytes as usize,
                    crate::constants::DYNAMODB_STREAM_RESPONSE_BYTES,
                    0,
                )
            }
        };
        if self.response_bytes + bytes + overhead > maximum_bytes {
            if self.records.is_empty() {
                return Err(StorageError::raw_validation(
                    "The first table stream record exceeds the response byte limit",
                ));
            }
            return Ok(false);
        }
        self.response_bytes += bytes
            + usize::from(matches!(
                self.options.selection,
                TableStreamImageSelection::Snapshot
            ));
        self.records.push(record);
        Ok(true)
    }
}

fn decode_record(
    record: PositionedStreamEntry,
    cursor: String,
) -> StorageResult<DynamoStreamRecord> {
    Ok(DynamoStreamRecord {
        size_bytes: 0,
        cursor: Some(cursor),
        source_table_name: Some(record.source_table_name),
        keys: record.entry.keys().clone(),
        sequence_number: TableStreamSequence::try_from(record.position.offset())?.to_string(),
        change: record.entry.change(),
        created_at: record.entry.created_at(),
        user_identity: record.entry.user_identity().cloned(),
        old_image: record
            .entry
            .old_image()
            .map(|image| image.item.to_attribute_map())
            .transpose()?,
        new_image: record
            .entry
            .new_image()
            .map(|image| image.item.to_attribute_map())
            .transpose()?,
    })
}

fn history_unavailable() -> StorageError {
    StorageEnum::HistoryUnavailable.into()
}
