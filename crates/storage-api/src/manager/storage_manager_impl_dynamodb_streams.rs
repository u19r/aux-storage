use http_error::HttpApiError;
use storage::{AdmissionClass, Tables};
use storage_types::{
    DYNAMODB_RESOURCE_NOT_FOUND_MESSAGE, DYNAMODB_STREAM_RECORDS_LIMIT_MAX,
    DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE, DYNAMODB_STREAM_RECORDS_LIMIT_MIN,
    DescribeStreamRequest, DescribeStreamResponse, DynamoDbStreamsRecord, GetRecordsRequest,
    GetRecordsResponse, GetShardIteratorRequest, GetShardIteratorResponse, ListStreamsRequest,
    ListStreamsResponse, SequenceNumberRange, Shard, StreamDescription, StreamDescriptor,
    StreamRecord, StreamRecordDetails, StreamSpecification, StreamViewType, TableName,
    TimestampSecondsFractional,
};

use crate::{manager::StorageApiManagerImpl, types::Response};

const MERGED_SHARD_ID: &str = "shardId-00000000000000000000-auxfn1";
const STREAM_ITERATOR_VERSION: &str = "aux-stream-v1";
const DEFAULT_STREAMS_LIMIT: u32 = 100;

#[derive(Debug, Clone)]
struct ParsedStreamArn {
    table_name: TableName,
    stream_label: String,
}

#[derive(Debug, Clone)]
struct StreamIterator {
    stream_arn: String,
    shard_id: String,
    page_token: Option<String>,
    sequence_number: Option<String>,
    after_sequence_number: bool,
}

impl StorageApiManagerImpl {
    pub(super) async fn list_streams_internal(
        &self,
        request: ListStreamsRequest,
    ) -> Result<Response, HttpApiError> {
        validate_streams_page_limit(request.limit, 100)?;

        let limit = request.limit.unwrap_or(DEFAULT_STREAMS_LIMIT) as usize;
        let mut descriptors = Vec::new();
        let mut start_seen = request.exclusive_start_stream_arn.is_none();
        let (tables, _) = self.db().list_tables(None, None).await?;

        for table in tables {
            if Tables::should_hide_from_list_tables(&table.table_name) {
                continue;
            }
            if request
                .table_name
                .as_ref()
                .is_some_and(|requested| requested != &table.table_name)
            {
                continue;
            }
            let Some(descriptor) = stream_descriptor_for_table(
                &table.table_name,
                table.created_at,
                table.stream_specification.as_ref(),
            ) else {
                continue;
            };
            if !start_seen {
                start_seen = request
                    .exclusive_start_stream_arn
                    .as_ref()
                    .is_some_and(|start| start == &descriptor.stream_arn);
                continue;
            }
            if descriptors.len() >= limit {
                break;
            }
            descriptors.push(descriptor);
        }

        let last_evaluated_stream_arn = if descriptors.len() >= limit {
            descriptors.last().map(|stream| stream.stream_arn.clone())
        } else {
            None
        };

        Ok(Response::ListStreams(ListStreamsResponse {
            streams: descriptors,
            last_evaluated_stream_arn,
        }))
    }

    pub(super) async fn describe_stream_internal(
        &self,
        request: DescribeStreamRequest,
    ) -> Result<Response, HttpApiError> {
        validate_streams_page_limit(request.limit, 100)?;
        let stream = self.stream_table_info(&request.stream_arn).await?;
        let Some(stream_specification) = stream.stream_specification else {
            return Err(stream_not_found());
        };
        let Some(stream_view_type) = stream_specification.stream_view_type else {
            return Err(stream_not_found());
        };

        let suppress_merged_shard = request
            .shard_filter
            .as_ref()
            .is_some_and(|filter| filter.filter_type == "CHILD_SHARDS")
            || request
                .exclusive_start_shard_id
                .as_deref()
                .is_some_and(|shard_id| shard_id == MERGED_SHARD_ID);
        let shards = if suppress_merged_shard {
            Vec::new()
        } else {
            vec![Shard {
                shard_id: MERGED_SHARD_ID.to_string(),
                sequence_number_range: SequenceNumberRange {
                    starting_sequence_number: "0".to_string(),
                    ending_sequence_number: None,
                },
                parent_shard_id: None,
            }]
        };

        let parsed = parse_stream_arn(&request.stream_arn)?;
        Ok(Response::DescribeStream(DescribeStreamResponse {
            stream_description: StreamDescription {
                stream_arn: request.stream_arn,
                stream_label: parsed.stream_label,
                stream_status: "ENABLED".to_string(),
                stream_view_type,
                creation_request_date_time: TimestampSecondsFractional::from(stream.created_at),
                table_name: stream.table_name,
                key_schema: stream.key_schema,
                shards,
                last_evaluated_shard_id: None,
            },
        }))
    }

    pub(super) async fn get_shard_iterator_internal(
        &self,
        request: GetShardIteratorRequest,
    ) -> Result<Response, HttpApiError> {
        let stream = self.stream_table_info(&request.stream_arn).await?;
        if stream.stream_specification.is_none() || request.shard_id != MERGED_SHARD_ID {
            return Err(stream_not_found());
        }

        let iterator = match request.shard_iterator_type.as_str() {
            "TRIM_HORIZON" => StreamIterator::new(request.stream_arn, request.shard_id),
            "LATEST" => {
                let latest = self.latest_table_stream_pointer(&stream.table_name).await?;
                StreamIterator::new(request.stream_arn, request.shard_id).with_page_token(latest)
            }
            "AT_SEQUENCE_NUMBER" => StreamIterator::new(request.stream_arn, request.shard_id)
                .with_sequence_number(request.sequence_number, false),
            "AFTER_SEQUENCE_NUMBER" => StreamIterator::new(request.stream_arn, request.shard_id)
                .with_sequence_number(request.sequence_number, true),
            _ => {
                return Err(HttpApiError::validation_error(format!(
                    "Value '{}' at 'shardIteratorType' failed to satisfy constraint: Member must \
                     satisfy enum value set: [TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, \
                     AFTER_SEQUENCE_NUMBER]",
                    request.shard_iterator_type
                )));
            }
        };

        Ok(Response::GetShardIterator(GetShardIteratorResponse {
            shard_iterator: iterator.encode(),
        }))
    }

    pub(super) async fn get_records_internal(
        &self,
        request: GetRecordsRequest,
    ) -> Result<Response, HttpApiError> {
        validate_streams_page_limit(request.limit, DYNAMODB_STREAM_RECORDS_LIMIT_MAX)?;
        let iterator = StreamIterator::decode(&request.shard_iterator)?;
        if iterator.shard_id != MERGED_SHARD_ID {
            return Err(stream_not_found());
        }
        let parsed = parse_stream_arn(&iterator.stream_arn)?;
        let table_info = self.stream_table_info(&iterator.stream_arn).await?;
        let Some(stream_specification) = table_info.stream_specification.as_ref() else {
            return Err(stream_not_found());
        };
        let limit = request.limit.unwrap_or(DYNAMODB_STREAM_RECORDS_LIMIT_MAX);

        let (records, last_evaluated_key) = if iterator.sequence_number.is_some() {
            self.get_records_from_sequence(
                &parsed.table_name,
                stream_specification,
                &iterator,
                limit,
            )
            .await?
        } else {
            self.get_stream_records_page(
                &parsed.table_name,
                stream_specification,
                iterator.page_token.as_deref(),
                Some(limit),
            )
            .await?
        };

        let next_page_token = last_evaluated_key.or_else(|| {
            records
                .last()
                .and_then(|record| record.cursor.as_ref())
                .cloned()
        });
        let next_shard_iterator = next_page_token.map(|page_token| {
            StreamIterator::new(iterator.stream_arn, iterator.shard_id)
                .with_page_token(Some(page_token))
                .encode()
        });

        Ok(Response::GetRecords(GetRecordsResponse {
            next_shard_iterator,
            records: records
                .into_iter()
                .map(|record| dynamodb_stream_record(record, stream_specification))
                .collect(),
        }))
    }

    async fn stream_table_info(
        &self,
        stream_arn: &str,
    ) -> Result<storage_types::StoredTableInfo, HttpApiError> {
        let parsed = parse_stream_arn(stream_arn)?;
        let table_info = self
            .db()
            .get_table_info(&parsed.table_name)
            .await
            .map_err(|_| stream_not_found())?;
        let (latest_stream_arn, _) = Self::latest_stream_metadata(
            &table_info.table_name,
            table_info.created_at,
            table_info.stream_specification.as_ref(),
        );
        if latest_stream_arn.as_deref() != Some(stream_arn) {
            return Err(stream_not_found());
        }
        Ok(table_info)
    }

    async fn latest_table_stream_pointer(
        &self,
        table_name: &TableName,
    ) -> Result<Option<String>, HttpApiError> {
        let admitted = self
            .db()
            .admit_default_provider(AdmissionClass::RangeRead)
            .await
            .map_err(HttpApiError::from)?;
        let page = admitted
            .run_stream(|provider| async move {
                provider
                    .read_backward(storage_types::StreamName::table_stream(table_name), None, 1)
                    .await
            })
            .await
            .map_err(stream_error)?;
        Ok(page.items.first().map(|item| item.id.to_string()))
    }

    async fn get_stream_records_page(
        &self,
        table_name: &TableName,
        stream_specification: &StreamSpecification,
        page_token: Option<&str>,
        limit: Option<u32>,
    ) -> Result<(Vec<StreamRecord>, Option<String>), HttpApiError> {
        let table_info = self.db().get_table_info(table_name).await?;
        self.db()
            .get_stream_records(
                table_name,
                table_info.key_schema.as_slice(),
                stream_specification,
                page_token,
                limit,
            )
            .await
            .map(|response| (response.records, response.last_evaluated_key))
            .map_err(Into::into)
    }

    async fn get_records_from_sequence(
        &self,
        table_name: &TableName,
        stream_specification: &StreamSpecification,
        iterator: &StreamIterator,
        limit: u32,
    ) -> Result<(Vec<StreamRecord>, Option<String>), HttpApiError> {
        let Some(sequence_number) = iterator.sequence_number.as_deref() else {
            return self
                .get_stream_records_page(
                    table_name,
                    stream_specification,
                    iterator.page_token.as_deref(),
                    Some(limit),
                )
                .await;
        };
        let (records, _) = self
            .get_stream_records_page(table_name, stream_specification, None, Some(1000))
            .await?;
        let start = records
            .iter()
            .position(|record| record.sequence_number == sequence_number)
            .map(|index| index + usize::from(iterator.after_sequence_number))
            .unwrap_or(records.len());
        let records = records
            .into_iter()
            .skip(start)
            .take(limit as usize)
            .collect::<Vec<_>>();
        Ok((records, None))
    }
}

impl StreamIterator {
    fn new(stream_arn: String, shard_id: String) -> Self {
        Self {
            stream_arn,
            shard_id,
            page_token: None,
            sequence_number: None,
            after_sequence_number: false,
        }
    }

    fn with_page_token(mut self, page_token: Option<String>) -> Self {
        self.page_token = page_token;
        self
    }

    fn with_sequence_number(
        mut self,
        sequence_number: Option<String>,
        after_sequence_number: bool,
    ) -> Self {
        self.sequence_number = sequence_number;
        self.after_sequence_number = after_sequence_number;
        self
    }

    fn encode(&self) -> String {
        format!(
            "{}|{}|{}|{}|{}|{}",
            STREAM_ITERATOR_VERSION,
            self.stream_arn,
            self.shard_id,
            self.page_token.as_deref().unwrap_or(""),
            self.sequence_number.as_deref().unwrap_or(""),
            u8::from(self.after_sequence_number)
        )
    }

    fn decode(value: &str) -> Result<Self, HttpApiError> {
        let parts = value.split('|').collect::<Vec<_>>();
        if parts.len() != 6 || parts[0] != STREAM_ITERATOR_VERSION {
            return Err(HttpApiError::dynamodb_protocol_error(
                "TrimmedDataAccessException",
                "The data you are trying to access has been trimmed.",
                400,
            ));
        }
        Ok(Self {
            stream_arn: parts[1].to_string(),
            shard_id: parts[2].to_string(),
            page_token: (!parts[3].is_empty()).then(|| parts[3].to_string()),
            sequence_number: (!parts[4].is_empty()).then(|| parts[4].to_string()),
            after_sequence_number: parts[5] == "1",
        })
    }
}

fn stream_descriptor_for_table(
    table_name: &TableName,
    created_at: storage_types::TimestampMillis,
    stream_specification: Option<&StreamSpecification>,
) -> Option<StreamDescriptor> {
    let (stream_arn, stream_label) =
        StorageApiManagerImpl::latest_stream_metadata(table_name, created_at, stream_specification);
    Some(StreamDescriptor {
        stream_arn: stream_arn?,
        table_name: table_name.clone(),
        stream_label: stream_label?,
    })
}

fn parse_stream_arn(stream_arn: &str) -> Result<ParsedStreamArn, HttpApiError> {
    let Some((_, table_and_stream)) = stream_arn.split_once(":table/") else {
        return Err(stream_not_found());
    };
    let Some((table_name, stream_label)) = table_and_stream.split_once("/stream/") else {
        return Err(stream_not_found());
    };
    Ok(ParsedStreamArn {
        table_name: TableName::new(table_name),
        stream_label: stream_label.to_string(),
    })
}

fn validate_streams_page_limit(limit: Option<u32>, max: u32) -> Result<(), HttpApiError> {
    if let Some(limit) = limit
        && !(DYNAMODB_STREAM_RECORDS_LIMIT_MIN..=max).contains(&limit)
    {
        let message = if max == DYNAMODB_STREAM_RECORDS_LIMIT_MAX {
            DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE.to_string()
        } else {
            format!("Limit must be between 1 and {max}")
        };
        return Err(HttpApiError::validation_error(message));
    }
    Ok(())
}

fn dynamodb_stream_record(
    record: StreamRecord,
    stream_specification: &StreamSpecification,
) -> DynamoDbStreamsRecord {
    let event_name = match (&record.old_image, &record.new_image) {
        (None, Some(_)) => "INSERT",
        (Some(_), None) => "REMOVE",
        _ => "MODIFY",
    }
    .to_string();
    let size_bytes = stream_record_size_bytes(&record);
    let sequence_number = record.sequence_number;
    DynamoDbStreamsRecord {
        event_id: sequence_number.clone(),
        event_name,
        event_version: "1.0".to_string(),
        event_source: "aws:dynamodb".to_string(),
        aws_region: "us-east-1".to_string(),
        dynamodb: StreamRecordDetails {
            keys: record.keys,
            sequence_number,
            stream_view_type: stream_specification
                .stream_view_type
                .clone()
                .unwrap_or(StreamViewType::NewAndOldImages),
            size_bytes,
            old_image: record.old_image,
            new_image: record.new_image,
        },
    }
}

fn stream_record_size_bytes(record: &StreamRecord) -> u64 {
    serde_json::to_vec(&(&record.keys, &record.old_image, &record.new_image))
        .map(|bytes| bytes.len() as u64)
        .unwrap_or(0)
}

fn stream_error(error: impl std::fmt::Display) -> HttpApiError {
    HttpApiError::internal_server_error(format!("Stream error: {error}"))
}

fn stream_not_found() -> HttpApiError {
    HttpApiError::dynamodb_protocol_error(
        "ResourceNotFoundException",
        DYNAMODB_RESOURCE_NOT_FOUND_MESSAGE,
        400,
    )
}
