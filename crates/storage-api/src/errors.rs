use axum::{http::StatusCode, response::Json};
use http_error::ErrorResponse;

pub type StorageApiError = (StatusCode, Json<ErrorResponse>);

pub fn validation_error(message: impl Into<String>) -> StorageApiError {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error_type: "com.amazon.coral.validate#ValidationException".to_owned(),
            message: message.into(),
            ..Default::default()
        }),
    )
}
