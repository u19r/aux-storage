use axum::{
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::Json,
};
use http_error::{ErrorResponse, HttpApiError};

pub type StorageApiError = (StatusCode, Json<ErrorResponse>);

pub(crate) fn http_api_error_headers(error: &HttpApiError) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in &error.response_headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            headers.insert(name, value);
        }
    }
    headers
}

pub fn validation_error(message: impl Into<String>) -> StorageApiError {
    HttpApiError::dynamodb_protocol_error("ValidationException", message, 400).into()
}
