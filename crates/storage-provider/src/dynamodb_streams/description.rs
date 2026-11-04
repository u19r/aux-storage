use storage_types::{
    BoundTableInfo, DescribeStreamRequest, DescribeStreamResponse, SequenceNumberRange, Shard,
    StorageError, StorageResult, StreamDescription, TableHistoryTarget, stream_identity,
};

use super::{DynamoStreamRead, iterator::validate_streams_page_limit, read::stream_not_found};
use crate::{
    dynamo_table_stream::DynamoTableStreamAttempt, table_history_read::TableHistoryReadState,
};

pub struct DescribeDynamoStream {
    request: DescribeStreamRequest,
    target: TableHistoryTarget,
}

impl TryFrom<DescribeStreamRequest> for DescribeDynamoStream {
    type Error = StorageError;
    fn try_from(request: DescribeStreamRequest) -> StorageResult<Self> {
        validate_streams_page_limit(request.limit, 100)?;
        if request
            .shard_filter
            .as_ref()
            .is_some_and(|filter| filter.filter_type != "CHILD_SHARDS")
        {
            return Err(StorageError::raw_validation(
                "ShardFilter Type must be CHILD_SHARDS",
            ));
        }
        let target = TableHistoryTarget::try_from(request.stream_arn.as_str())?;
        Ok(Self { request, target })
    }
}

impl DynamoStreamRead for DescribeDynamoStream {
    type Response = DescribeStreamResponse;
    fn target(&self) -> &TableHistoryTarget {
        &self.target
    }
    fn stream_arn(&self) -> &str {
        &self.request.stream_arn
    }
    async fn read<A: DynamoTableStreamAttempt>(
        self,
        attempt: &mut A,
        table: &BoundTableInfo,
        _now: storage_types::TimestampMillis,
    ) -> StorageResult<Self::Response> {
        let view = table
            .info
            .stream_specification
            .as_ref()
            .and_then(|spec| spec.stream_view_type.clone())
            .ok_or_else(stream_not_found)?;
        let state = TableHistoryReadState::load(attempt, table).await?;
        let (shards, last_evaluated_shard_id) = describe_shards(&self.request, &state)?;
        let stream_label = stream_identity(&table.info.table_name, table.stream_identity()).1;
        Ok(DescribeStreamResponse {
            stream_description: StreamDescription {
                stream_arn: self.request.stream_arn,
                stream_label,
                stream_status: "ENABLED".into(),
                stream_view_type: view,
                creation_request_date_time: table.info.created_at.into(),
                table_name: table.info.table_name.clone(),
                key_schema: table.info.key_schema.clone(),
                shards,
                last_evaluated_shard_id,
            },
        })
    }
}

fn describe_shards(
    request: &DescribeStreamRequest,
    state: &TableHistoryReadState,
) -> StorageResult<(Vec<Shard>, Option<String>)> {
    let mut shards = state
        .lanes
        .iter()
        .map(|lane| describe_lane(lane, state))
        .collect::<StorageResult<Vec<_>>>()?;
    shards.sort_unstable_by(|left, right| left.shard_id.cmp(&right.shard_id));
    if let Some(filter) = &request.shard_filter {
        let parent = filter
            .shard_id
            .as_ref()
            .ok_or_else(|| StorageError::raw_validation("ShardId is required for CHILD_SHARDS"))?;
        shards.retain(|shard| shard.parent_shard_id.as_ref() == Some(parent));
    }
    if let Some(start) = &request.exclusive_start_shard_id {
        shards.retain(|shard| &shard.shard_id > start);
    }
    let limit = request.limit.unwrap_or(100) as usize;
    let more = shards.len() > limit;
    shards.truncate(limit);
    let next = more
        .then(|| shards.last().map(|shard| shard.shard_id.clone()))
        .flatten();
    Ok((shards, next))
}

fn describe_lane(
    lane: &crate::table_history_read::TableHistoryReadLane,
    state: &TableHistoryReadState,
) -> StorageResult<Shard> {
    let parent = lane
        .lineage
        .parent_lineage_id()
        .map(|id| {
            state
                .lanes
                .iter()
                .find(|parent| parent.lane.lineage_id() == id)
                .ok_or_else(stream_not_found)?
                .lane
                .public_shard_id()
        })
        .transpose()?;
    Ok(Shard {
        shard_id: lane.lane.public_shard_id()?,
        parent_shard_id: parent,
        sequence_number_range: SequenceNumberRange {
            starting_sequence_number: storage_types::TableStreamSequence::try_from(
                lane.origin.offset(),
            )?
            .to_string(),
            ending_sequence_number: lane
                .lineage
                .sealed()
                .then(|| {
                    storage_types::TableStreamSequence::try_from(lane.through.offset())
                        .map(|sequence| sequence.to_string())
                })
                .transpose()?,
        },
    })
}
