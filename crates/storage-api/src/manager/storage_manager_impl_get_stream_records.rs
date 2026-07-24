use std::collections::HashMap;

use http_error::HttpApiError;
use storage_types::{
    AttributeValue, DYNAMODB_STREAM_RECORDS_LIMIT_MAX, DYNAMODB_STREAM_RECORDS_LIMIT_MIN,
    GetStreamRecordsRequest, GetStreamRecordsResponse, KeySchemaElement,
    STREAM_RECORDS_MAX_ENCODED_BYTES, SYSTEM_STREAM_RECORDS_LIMIT_MAX, StreamItemId, StreamName,
    StreamRecord, TableName, dynamodb_table_not_found_message,
};
use stream_provider::{StreamDataType, StreamError, StreamItem, StreamPointer, StreamProvider};

use crate::{manager::StorageApiManagerImpl, types::Response};

const PROVIDER_PAGE_MAX_RECORDS: u32 = DYNAMODB_STREAM_RECORDS_LIMIT_MAX;
const RESPONSE_ENVELOPE_RESERVE_BYTES: usize = 256;

struct SystemResponseBuilder {
    records: Vec<StreamRecord>,
    schemas: HashMap<TableName, Vec<KeySchemaElement>>,
    encoded_bytes: usize,
    max_bytes: u32,
}

impl StorageApiManagerImpl {
    pub(super) async fn get_stream_records_internal(
        &self,
        request: GetStreamRecordsRequest,
    ) -> Result<Response, HttpApiError> {
        validate_request(&request)?;
        if request.system_stream {
            return self
                .get_system_stream_records(&request)
                .await
                .map(Response::GetStreamRecords);
        }
        self.get_table_stream_records(&request)
            .await
            .map(Response::GetStreamRecords)
    }

    async fn get_table_stream_records(
        &self,
        request: &GetStreamRecordsRequest,
    ) -> Result<GetStreamRecordsResponse, HttpApiError> {
        let table_name = request
            .table_name
            .as_ref()
            .ok_or_else(|| HttpApiError::validation_error("TableName is required".to_string()))?;
        let table_info = self
            .db()
            .get_table_info(table_name)
            .await
            .map_err(|_error| {
                HttpApiError::resource_not_found_error(dynamodb_table_not_found_message(
                    table_name.as_ref(),
                ))
            })?;
        let stream_spec = table_info.stream_specification.as_ref().ok_or_else(|| {
            HttpApiError::validation_error(format!(
                "Table '{table_name}' does not have streams enabled"
            ))
        })?;

        self.db()
            .get_stream_records(
                table_name,
                table_info.key_schema.as_slice(),
                stream_spec,
                request.last_evaluated_key.as_deref(),
                request.limit,
            )
            .await
            .map_err(Into::into)
    }

    async fn get_system_stream_records(
        &self,
        request: &GetStreamRecordsRequest,
    ) -> Result<GetStreamRecordsResponse, HttpApiError> {
        let mut cursor = parse_cursor(request.last_evaluated_key.as_deref())?;
        let provider = self.db().stream_provider();
        let max_bytes = STREAM_RECORDS_MAX_ENCODED_BYTES;
        let mut remaining = request.limit.unwrap_or(100);
        let mut builder = SystemResponseBuilder::new(max_bytes, remaining as usize);
        let has_more = loop {
            let page_limit = remaining.min(PROVIDER_PAGE_MAX_RECORDS);
            let page = provider
                .get_items_from_pointer_stream(
                    StreamName::system_table_stream(),
                    cursor,
                    Some(page_limit),
                )
                .await
                .map_err(stream_http_error)?;
            let previous_count = builder.records.len();
            let byte_limited = builder
                .append(self, provider.as_ref(), page.records)
                .await?;
            let accepted =
                u32::try_from(builder.records.len() - previous_count).map_err(|_error| {
                    HttpApiError::internal_server_error("stream page is too large")
                })?;
            remaining = remaining.saturating_sub(accepted);
            cursor = builder.last_cursor();
            if byte_limited || !page.has_more || remaining == 0 {
                break byte_limited || page.has_more;
            }
        };
        let last_evaluated_key = has_more
            .then(|| {
                builder
                    .records
                    .last()
                    .map(|record| record.sequence_number.clone())
            })
            .flatten();
        let response = GetStreamRecordsResponse {
            table_name: None,
            records: builder.records,
            last_evaluated_key,
        };
        ensure_response_size(&response, max_bytes)?;
        Ok(response)
    }
}

impl SystemResponseBuilder {
    fn new(max_bytes: u32, capacity: usize) -> Self {
        Self {
            records: Vec::with_capacity(capacity),
            schemas: HashMap::new(),
            encoded_bytes: 0,
            max_bytes,
        }
    }

    fn last_cursor(&self) -> Option<StreamItemId> {
        self.records
            .last()
            .and_then(|record| record.sequence_number.parse().ok())
    }

    async fn append(
        &mut self,
        manager: &StorageApiManagerImpl,
        provider: &dyn StreamProvider,
        pointer_records: Vec<(StreamPointer, Vec<StreamItem>)>,
    ) -> Result<bool, HttpApiError> {
        for (pointer, images) in pointer_records {
            let schema = load_key_schema(manager, &mut self.schemas, &pointer.table_name).await?;
            let record = system_stream_record(provider, pointer, &images, schema)?;
            let record_bytes = serde_json::to_vec(&record)
                .map_err(serialization_http_error)?
                .len();
            let delimiter_bytes = usize::from(!self.records.is_empty());
            if self
                .encoded_bytes
                .saturating_add(record_bytes)
                .saturating_add(delimiter_bytes)
                .saturating_add(RESPONSE_ENVELOPE_RESERVE_BYTES)
                > self.max_bytes as usize
            {
                if self.records.is_empty() {
                    return Err(HttpApiError::validation_error(
                        "The first stream record exceeds the response byte limit".to_string(),
                    ));
                }
                return Ok(true);
            }
            self.encoded_bytes += record_bytes + delimiter_bytes;
            self.records.push(record);
        }
        Ok(false)
    }
}

fn validate_request(request: &GetStreamRecordsRequest) -> Result<(), HttpApiError> {
    match (&request.table_name, request.system_stream) {
        (Some(_), false) | (None, true) => {}
        (Some(_), true) => {
            return Err(HttpApiError::validation_error(
                "TableName and SystemStream cannot be used together".to_string(),
            ));
        }
        (None, false) => {
            return Err(HttpApiError::validation_error(
                "TableName or SystemStream is required".to_string(),
            ));
        }
    }
    let maximum = if request.system_stream {
        SYSTEM_STREAM_RECORDS_LIMIT_MAX
    } else {
        DYNAMODB_STREAM_RECORDS_LIMIT_MAX
    };
    if let Some(limit) = request.limit
        && !(DYNAMODB_STREAM_RECORDS_LIMIT_MIN..=maximum).contains(&limit)
    {
        return Err(HttpApiError::validation_error(format!(
            "Limit must be between 1 and {maximum}"
        )));
    }
    Ok(())
}

fn parse_cursor(cursor: Option<&str>) -> Result<Option<StreamItemId>, HttpApiError> {
    cursor
        .map(|value| {
            value.parse().map_err(|_error| {
                HttpApiError::validation_error("Invalid LastEvaluatedKey".to_string())
            })
        })
        .transpose()
}

async fn load_key_schema<'a>(
    manager: &StorageApiManagerImpl,
    schemas: &'a mut HashMap<TableName, Vec<KeySchemaElement>>,
    table_name: &TableName,
) -> Result<&'a [KeySchemaElement], HttpApiError> {
    if !schemas.contains_key(table_name) {
        let table_info = manager
            .db()
            .storage_provider()
            .get_table_info(table_name)
            .await?;
        schemas.insert(table_name.clone(), table_info.key_schema);
    }
    schemas
        .get(table_name)
        .map(Vec::as_slice)
        .ok_or_else(|| HttpApiError::internal_server_error("stream table schema was not cached"))
}

fn system_stream_record(
    provider: &dyn StreamProvider,
    pointer: StreamPointer,
    images: &[StreamItem],
    key_schema: &[KeySchemaElement],
) -> Result<StreamRecord, HttpApiError> {
    let (old_image, new_image) = decode_images(images)?;
    let item_for_key = new_image
        .as_ref()
        .or(old_image.as_ref())
        .ok_or_else(|| HttpApiError::internal_server_error("stream pointer has no item image"))?;
    let keys = provider
        .get_key_attributes(item_for_key, key_schema)
        .map_err(stream_http_error)?;
    Ok(StreamRecord {
        cursor: Some(pointer.stream_item_id.to_string()),
        source_table_name: Some(pointer.table_name),
        keys,
        sequence_number: pointer.stream_item_id.to_string(),
        old_image,
        new_image,
    })
}

type ItemImage = HashMap<String, AttributeValue>;

fn decode_images(
    images: &[StreamItem],
) -> Result<(Option<ItemImage>, Option<ItemImage>), HttpApiError> {
    let newest = images
        .first()
        .ok_or_else(|| HttpApiError::internal_server_error("stream pointer target is missing"))?;
    let new_image = decode_image(newest)?;
    let old_image = images.get(1).map(decode_image).transpose()?.flatten();
    Ok((old_image, new_image))
}

fn decode_image(image: &StreamItem) -> Result<Option<ItemImage>, HttpApiError> {
    if matches!(image.data_type, StreamDataType::DeleteMarker) {
        return Ok(None);
    }
    storage_types::storage_serde::from_bytes(&image.data)
        .map(Some)
        .map_err(Into::into)
}

fn ensure_response_size(
    response: &GetStreamRecordsResponse,
    maximum: u32,
) -> Result<(), HttpApiError> {
    if serde_json::to_vec(response)
        .map_err(serialization_http_error)?
        .len()
        <= maximum as usize
    {
        return Ok(());
    }
    Err(HttpApiError::internal_server_error(
        "stream response exceeded the byte limit",
    ))
}

fn stream_http_error(error: StreamError) -> HttpApiError {
    storage_types::StorageError::from(error.into_storage_enum()).into()
}

fn serialization_http_error(error: serde_json::Error) -> HttpApiError {
    storage_types::StorageError::from(error).into()
}
