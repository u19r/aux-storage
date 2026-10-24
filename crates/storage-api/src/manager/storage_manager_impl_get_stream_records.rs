use http_error::HttpApiError;
use storage::AdmissionClass;
use storage_types::{
    DYNAMODB_STREAM_RECORDS_LIMIT_MAX, DYNAMODB_STREAM_RECORDS_LIMIT_MIN, GetStreamRecordsRequest,
    GetStreamRecordsResponse, SYSTEM_STREAM_RECORDS_LIMIT_MAX,
};
use stream_provider::{StreamError, SystemStreamPageRequest};

use crate::{manager::StorageApiManagerImpl, types::Response};

impl StorageApiManagerImpl {
    pub(super) async fn get_stream_records_internal(
        &self,
        request: GetStreamRecordsRequest,
    ) -> Result<Response, HttpApiError> {
        validate_request(&request)?;
        if request.is_system_stream() {
            return self
                .get_system_stream_records(&request)
                .await
                .map(Response::GetStreamRecords);
        }
        self.db()
            .run_dynamo_admitted("default", AdmissionClass::RangeRead, |dynamo| async move {
                dynamo.get_stream_records(request).await
            })
            .await
            .map(Response::GetStreamRecords)
            .map_err(HttpApiError::from)
    }

    async fn get_system_stream_records(
        &self,
        request: &GetStreamRecordsRequest,
    ) -> Result<GetStreamRecordsResponse, HttpApiError> {
        let source_scope = request.source_scope().cloned().ok_or_else(|| {
            HttpApiError::validation_error("SystemStream SourceScope is required")
        })?;
        let page_request = SystemStreamPageRequest::try_new(
            source_scope,
            request.checkpoint().cloned(),
            request.limit().unwrap_or(100),
        )
        .map_err(system_stream_request_error)?;
        let page = self
            .db()
            .run_stream_admitted("default", AdmissionClass::RangeRead, |stream| async move {
                stream
                    .get_system_stream_page(page_request)
                    .await
                    .map_err(|error| {
                        storage_types::StorageError::from(StreamError::into_storage_enum(error))
                    })
            })
            .await
            .map_err(HttpApiError::from)?;
        page.into_response()
            .map_err(|error| HttpApiError::validation_error(error.to_string()))
    }
}

fn validate_request(request: &GetStreamRecordsRequest) -> Result<(), HttpApiError> {
    match (request.table_name(), request.is_system_stream()) {
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
    let maximum = if request.is_system_stream() {
        SYSTEM_STREAM_RECORDS_LIMIT_MAX
    } else {
        DYNAMODB_STREAM_RECORDS_LIMIT_MAX
    };
    if let Some(limit) = request.limit()
        && !(DYNAMODB_STREAM_RECORDS_LIMIT_MIN..=maximum).contains(&limit)
    {
        return Err(HttpApiError::validation_error(format!(
            "Limit must be between 1 and {maximum}"
        )));
    }
    request.validate().map_err(system_stream_request_error)?;
    Ok(())
}

fn system_stream_request_error(error: storage_types::SystemStreamContractError) -> HttpApiError {
    match error {
        storage_types::SystemStreamContractError::BackendContract => {
            storage_types::StorageError::unsupported(
                "SystemStream logical lineage is unavailable from this backend",
            )
            .into()
        }
        error => HttpApiError::validation_error(error.to_string()),
    }
}
