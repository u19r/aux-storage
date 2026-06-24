use http_error::HttpApiError;
use storage_types::{
    DYNAMODB_STREAM_RECORDS_LIMIT_MAX, DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE,
    DYNAMODB_STREAM_RECORDS_LIMIT_MIN, GetStreamRecordsRequest, dynamodb_table_not_found_message,
};

use crate::{manager::StorageApiManagerImpl, types::Response};

impl StorageApiManagerImpl {
    pub(super) async fn get_stream_records_internal(
        &self,
        request: GetStreamRecordsRequest,
    ) -> Result<Response, HttpApiError> {
        if let Some(limit) = request.limit
            && !(DYNAMODB_STREAM_RECORDS_LIMIT_MIN..=DYNAMODB_STREAM_RECORDS_LIMIT_MAX)
                .contains(&limit)
        {
            return Err(HttpApiError::validation_error(
                DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE.to_string(),
            ));
        }

        let table_info = self
            .db()
            .get_table_info(&request.table_name)
            .await
            .map_err(|_e| {
                HttpApiError::resource_not_found_error(dynamodb_table_not_found_message(
                    request.table_name.as_ref(),
                ))
            })?;

        let Some(stream_spec) = &table_info.stream_specification else {
            return Err(HttpApiError::validation_error(format!(
                "Table '{}' does not have streams enabled",
                request.table_name
            )));
        };

        self.db()
            .get_stream_records(
                &request.table_name,
                table_info.key_schema.as_slice(),
                stream_spec,
                request.last_evaluated_key.as_deref(),
                request.limit,
            )
            .await
            .map(Response::GetStreamRecords)
            .map_err(Into::into)
    }
}
