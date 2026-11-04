use storage_types::{
    DYNAMODB_STREAM_RECORDS_LIMIT_MAX, DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE,
    DYNAMODB_STREAM_RECORDS_LIMIT_MIN, StorageError, StorageResult, TableStreamSequence,
};

use crate::constants::DYNAMODB_STREAM_ITERATOR_VERSION;

pub(super) struct StreamIterator {
    pub stream_arn: String,
    pub shard_id: String,
    pub(super) page_token: Option<String>,
}

impl StreamIterator {
    pub(super) fn new(stream_arn: String, shard_id: String) -> Self {
        Self {
            stream_arn,
            shard_id,
            page_token: None,
        }
    }

    pub(super) fn with_page_token(mut self, page_token: Option<String>) -> Self {
        self.page_token = page_token;
        self
    }

    pub(super) fn encode(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            DYNAMODB_STREAM_ITERATOR_VERSION,
            self.stream_arn,
            self.shard_id,
            self.page_token.as_deref().unwrap_or("")
        )
    }

    pub(super) fn decode(value: &str) -> StorageResult<Self> {
        let parts = value.split('|').collect::<Vec<_>>();
        if parts.len() != 4 || parts[0] != DYNAMODB_STREAM_ITERATOR_VERSION {
            return Err(storage_types::StorageEnum::TrimmedDataAccess.into());
        }
        Ok(Self {
            stream_arn: parts[1].to_string(),
            shard_id: parts[2].to_string(),
            page_token: (!parts[3].is_empty()).then(|| parts[3].to_string()),
        })
    }
}

pub(super) fn validate_streams_page_limit(limit: Option<u32>, max: u32) -> StorageResult<()> {
    if let Some(limit) = limit
        && !(DYNAMODB_STREAM_RECORDS_LIMIT_MIN..=max).contains(&limit)
    {
        let message = if max == DYNAMODB_STREAM_RECORDS_LIMIT_MAX {
            DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE.to_string()
        } else {
            format!("Limit must be between 1 and {max}")
        };
        return Err(StorageError::raw_validation(message));
    }
    Ok(())
}

pub struct GetDynamoShardIterator {
    request: storage_types::GetShardIteratorRequest,
    target: storage_types::TableHistoryTarget,
}

impl TryFrom<storage_types::GetShardIteratorRequest> for GetDynamoShardIterator {
    type Error = StorageError;
    fn try_from(request: storage_types::GetShardIteratorRequest) -> StorageResult<Self> {
        let target = storage_types::TableHistoryTarget::try_from(request.stream_arn.as_str())?;
        match request.shard_iterator_type.as_str() {
            "TRIM_HORIZON" | "LATEST" => {}
            "AT_SEQUENCE_NUMBER" | "AFTER_SEQUENCE_NUMBER" => {
                request
                    .sequence_number
                    .as_deref()
                    .ok_or_else(|| StorageError::raw_validation("SequenceNumber is required"))?
                    .parse::<TableStreamSequence>()
                    .map_err(|_| StorageError::raw_validation("Invalid sequence number"))?;
            }
            _ => {
                return Err(StorageError::raw_validation(
                    "ShardIteratorType must be TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER or \
                     AFTER_SEQUENCE_NUMBER",
                ));
            }
        }
        Ok(Self { request, target })
    }
}

impl super::DynamoStreamRead for GetDynamoShardIterator {
    type Response = storage_types::GetShardIteratorResponse;
    fn target(&self) -> &storage_types::TableHistoryTarget {
        &self.target
    }
    fn stream_arn(&self) -> &str {
        &self.request.stream_arn
    }
    async fn read<A: crate::dynamo_table_stream::DynamoTableStreamAttempt>(
        self,
        attempt: &mut A,
        table: &storage_types::BoundTableInfo,
        _now: storage_types::TimestampMillis,
    ) -> StorageResult<Self::Response> {
        let state = crate::table_history_read::TableHistoryReadState::load(attempt, table).await?;
        state.select_lanes(Some(&self.request.shard_id))?;
        let request = self.request;
        let iterator = StreamIterator::new(request.stream_arn, request.shard_id);
        let iterator = match request.shard_iterator_type.as_str() {
            "LATEST" => {
                let lanes = state.select_lanes(Some(&iterator.shard_id))?;
                let checkpoint = storage_types::TableStreamCheckpoint::try_new(
                    table.history_id,
                    state.source.as_ref().source_scope().clone(),
                    vec![lanes[0].through.clone()],
                    storage_types::TableStreamReadProgress {
                        next_lane: 0,
                        initial_visibility_cutoff: None,
                    },
                )?;
                iterator.with_page_token(Some(checkpoint.to_string()))
            }
            "AT_SEQUENCE_NUMBER" | "AFTER_SEQUENCE_NUMBER" => {
                let lane = state.select_lanes(Some(&iterator.shard_id))?[0];
                let position = sequence_position(
                    lane,
                    request.sequence_number.as_deref(),
                    request.shard_iterator_type == "AFTER_SEQUENCE_NUMBER",
                )?;
                let checkpoint = storage_types::TableStreamCheckpoint::try_new(
                    table.history_id,
                    state.source.as_ref().source_scope().clone(),
                    vec![position],
                    storage_types::TableStreamReadProgress {
                        next_lane: 0,
                        initial_visibility_cutoff: None,
                    },
                )?;
                iterator.with_page_token(Some(checkpoint.to_string()))
            }
            _ => iterator,
        };
        Ok(storage_types::GetShardIteratorResponse {
            shard_iterator: iterator.encode(),
        })
    }
}

fn sequence_position(
    lane: &crate::table_history_read::TableHistoryReadLane,
    sequence: Option<&str>,
    after: bool,
) -> StorageResult<storage_types::SystemStreamPosition> {
    let sequence = sequence
        .ok_or_else(|| StorageError::raw_validation("SequenceNumber is required"))?
        .parse::<TableStreamSequence>()?;
    let key = if after {
        Some(sequence.native_key())
    } else {
        sequence.native_key().predecessor()
    };
    let offset = match key {
        None => lane.origin.offset().clone(),
        Some(key) if lane.through.offset().versionstamp().is_some() => lane
            .origin
            .with_versionstamp(*key.as_bytes())
            .map_err(|_| StorageError::raw_validation("Invalid sequence number"))?
            .offset()
            .clone(),
        Some(key) => {
            if key.as_bytes()[..4] != [0; 4] {
                return Err(StorageError::raw_validation("Invalid sequence number"));
            }
            let mut bytes = [0; 8];
            bytes.copy_from_slice(&key.as_bytes()[4..]);
            storage_types::SystemStreamOffset::try_new(u64::from_be_bytes(bytes).to_string())
                .map_err(|_| StorageError::raw_validation("Invalid sequence number"))?
        }
    };
    let position = storage_types::SystemStreamPosition::try_new(
        lane.lane.shard_id().as_str(),
        lane.lane.lineage_id().as_str(),
        offset.as_str(),
    )
    .map_err(|_| StorageError::raw_validation("Invalid sequence number"))?;
    Ok(position)
}
