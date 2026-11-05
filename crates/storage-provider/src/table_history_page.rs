//! Admission, visibility and continuation advance one qualified source lane at
//! a time.

use std::num::NonZeroU32;

use storage_types::{
    BoundTableInfo, ItemHistoryChange, ItemHistoryPointer, ItemHistoryPublication, StorageEnum,
    StorageError, StorageResult, SystemStreamPosition, TableStreamCheckpoint, TableStreamFrontier,
    TableStreamSequence,
};
use stream_provider::{
    ItemHistoryImages, ReadDirection, ResolvedItemHistory, TableHistoryIndexRead,
};

use crate::{
    DynamoTableStreamAttempt,
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
    encoded_bytes: usize,
}

struct PointerAdmission {
    pointers: Vec<ItemHistoryPointer>,
    observations: Vec<(SystemStreamPosition, bool)>,
    is_truncated: bool,
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
            encoded_bytes: 0,
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
        attempt: &mut impl DynamoTableStreamAttempt,
        limit: NonZeroU32,
        frontier: TableStreamFrontier,
    ) -> StorageResult<DynamoTableStreamPage> {
        self.encoded_bytes = serde_json::to_vec(&frontier)?.len();
        let reads = (0..self.lanes.len())
            .map(|step| self.lane_read((self.next_lane + step) % self.lanes.len(), limit))
            .collect::<StorageResult<Vec<_>>>()?;
        let pages = stream_provider::read_table_history_indexes(attempt, &reads).await?;
        let first_lane = self.next_lane;
        for (step, page) in pages.into_iter().enumerate() {
            let index = (first_lane + step) % self.lanes.len();
            if self.include_page(attempt, index, page, limit).await? {
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

    async fn include_page(
        &mut self,
        attempt: &mut impl DynamoTableStreamAttempt,
        index: usize,
        page: stream_provider::TableHistoryIndexPage,
        limit: NonZeroU32,
    ) -> StorageResult<bool> {
        let admission = self.admit_pointers(page.pointers)?;
        let images = match self.options.selection {
            TableStreamImageSelection::Snapshot => ItemHistoryImages::Snapshot,
            TableStreamImageSelection::DynamoDbImages => ItemHistoryImages::WithPredecessor,
        };
        let resolved = stream_provider::resolve_item_history(
            attempt,
            &admission.pointers,
            images,
            crate::constants::TABLE_HISTORY_READ_BYTES,
        )
        .await?;
        if self.include_observations(index, admission.observations, resolved, limit)? {
            return Ok(true);
        }
        if !page.more && !admission.is_truncated {
            self.positions[index] = self.lanes[index].through.clone();
        }
        self.next_lane = (index + 1) % self.lanes.len();
        Ok(self.records.len() == limit.get() as usize || admission.is_truncated)
    }

    fn include_observations(
        &mut self,
        index: usize,
        observations: Vec<(SystemStreamPosition, bool)>,
        resolved: Vec<Option<ResolvedItemHistory>>,
        limit: NonZeroU32,
    ) -> StorageResult<bool> {
        let mut resolved = resolved.into_iter();
        for (position, visible) in observations {
            if !visible {
                self.positions[index] = position;
                continue;
            }
            let resolved = resolved.next().flatten().ok_or_else(history_unavailable)?;
            if self.records.len() == limit.get() as usize {
                self.next_lane = index;
                return Ok(true);
            }
            let previous = self.positions[index].clone();
            self.positions[index] = position;
            let cursor = self.checkpoint((index + 1) % self.lanes.len())?;
            let record = decode_record(resolved, cursor)?;
            if !self.include_record(record)? {
                self.positions[index] = previous;
                self.next_lane = index;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn admit_pointers(&self, pointers: Vec<ItemHistoryPointer>) -> StorageResult<PointerAdmission> {
        let mut admission = PointerAdmission {
            pointers: Vec::new(),
            observations: Vec::new(),
            is_truncated: false,
        };
        let mut bytes = 0usize;
        for pointer in pointers {
            if pointer.metadata().address().history_id != self.table.history_id {
                return Err(history_unavailable());
            }
            let position = event_position(&pointer)?.clone();
            if !self.is_visible(&pointer)? {
                admission.observations.push((position, false));
                continue;
            }
            let bound = self.pointer_read_bytes(&pointer)?;
            if bytes.saturating_add(bound) > crate::constants::TABLE_HISTORY_READ_BYTES.get() {
                admission.is_truncated = true;
                break;
            }
            bytes += bound;
            admission.observations.push((position, true));
            admission.pointers.push(pointer);
        }
        Ok(admission)
    }

    fn pointer_read_bytes(&self, pointer: &ItemHistoryPointer) -> StorageResult<usize> {
        let mut bound = pointer.record_bytes()
            + stream_provider::StreamRowValue::history_envelope_bytes(
                pointer.metadata().address(),
            )?;
        if matches!(
            self.options.selection,
            TableStreamImageSelection::DynamoDbImages
        ) {
            bound += pointer.predecessor_record_bytes_bound().unwrap_or(0)
                + stream_provider::StreamRowValue::history_envelope_bytes(
                    pointer.metadata().address(),
                )?;
        }
        Ok(bound)
    }

    fn is_visible(&self, pointer: &ItemHistoryPointer) -> StorageResult<bool> {
        let ItemHistoryPublication::Event { created_at, .. } = pointer.metadata().publication()
        else {
            return Err(history_unavailable());
        };
        let visible = !self
            .state
            .retention
            .frontier
            .is_retired(pointer.metadata())?
            && self
                .state
                .retention
                .retention
                .cutoff_at(self.options.now)
                .is_none_or(|cutoff| *created_at > cutoff);
        if !visible
            && !self
                .initial_visibility_cutoff
                .is_some_and(|cutoff| *created_at <= cutoff)
        {
            return Err(history_unavailable());
        }
        Ok(visible)
    }

    fn record_bytes(&self, record: &DynamoStreamRecord) -> StorageResult<usize> {
        let (old, new) = match self.options.selection {
            TableStreamImageSelection::Snapshot => (None, record.new_image.as_ref()),
            TableStreamImageSelection::DynamoDbImages => match self
                .table
                .info
                .stream_specification
                .as_ref()
                .and_then(|spec| spec.stream_view_type.as_ref())
            {
                Some(storage_types::StreamViewType::KeysOnly) => (None, None),
                Some(storage_types::StreamViewType::OldImage) => (record.old_image.as_ref(), None),
                Some(storage_types::StreamViewType::NewImage) => (None, record.new_image.as_ref()),
                _ => (record.old_image.as_ref(), record.new_image.as_ref()),
            },
        };
        let bytes = serde_json::to_vec(&(
            &record.keys,
            old,
            new,
            &record.source_table_name,
            &record.sequence_number,
        ))?
        .len()
            + 1024;
        Ok(bytes)
    }

    fn include_record(&mut self, record: DynamoStreamRecord) -> StorageResult<bool> {
        let bytes = self.record_bytes(&record)?;
        let maximum_bytes = match self.options.selection {
            TableStreamImageSelection::Snapshot => {
                storage_types::STREAM_RECORDS_MAX_ENCODED_BYTES as usize
            }
            TableStreamImageSelection::DynamoDbImages => {
                crate::constants::DYNAMODB_STREAM_RESPONSE_BYTES
            }
        };
        if self.encoded_bytes
            + bytes
            + self.checkpoint(self.next_lane)?.len()
            + crate::constants::TABLE_STREAM_RESPONSE_RESERVE_BYTES
            > maximum_bytes
        {
            if self.records.is_empty() {
                return Err(StorageError::raw_validation(
                    "The first table stream record exceeds the response byte limit",
                ));
            }
            return Ok(false);
        }
        self.encoded_bytes += bytes + 1;
        self.records.push(record);
        Ok(true)
    }
}

fn event_position(pointer: &ItemHistoryPointer) -> StorageResult<&SystemStreamPosition> {
    match pointer.metadata().publication() {
        ItemHistoryPublication::Event { table_position, .. } => Ok(table_position),
        ItemHistoryPublication::PredecessorAnchor => Err(history_unavailable()),
    }
}

fn decode_record(
    resolved: ResolvedItemHistory,
    cursor: String,
) -> StorageResult<DynamoStreamRecord> {
    let snapshot = storage_types::StreamRecord::try_from(&resolved)?;
    let sequence_number = TableStreamSequence::try_from(
        snapshot
            .system_position
            .as_ref()
            .ok_or_else(history_unavailable)?
            .offset(),
    )?
    .to_string();
    let old_image = resolved
        .predecessor
        .as_ref()
        .map(|entry| match entry.change() {
            ItemHistoryChange::Image { item, .. } => item.to_attribute_map().map(Some),
            ItemHistoryChange::Deleted { .. } => Ok(None),
        })
        .transpose()?
        .flatten();
    let (created_at, user_identity) = match resolved.current.metadata().publication() {
        ItemHistoryPublication::Event {
            created_at,
            user_identity,
            ..
        } => (*created_at, user_identity.clone()),
        ItemHistoryPublication::PredecessorAnchor => return Err(history_unavailable()),
    };
    Ok(DynamoStreamRecord {
        cursor: Some(cursor),
        source_table_name: snapshot.source_table_name,
        keys: snapshot.keys,
        sequence_number,
        created_at,
        user_identity,
        old_image,
        new_image: snapshot.item,
    })
}

fn history_unavailable() -> StorageError {
    StorageEnum::HistoryUnavailable.into()
}
