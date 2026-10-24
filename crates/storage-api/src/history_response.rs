//! Bytes ownership keeps the native source reservation through HTTP body
//! consumption.
use std::sync::Arc;

use axum::{
    body::{Body, Bytes},
    http::header,
    response::Response,
};
use storage::CanonicalHistoryResponse;
struct HistoryBody(Arc<CanonicalHistoryResponse>);
impl AsRef<[u8]> for HistoryBody {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref().as_ref()
    }
}
pub(crate) fn into_http_response(reply: Arc<CanonicalHistoryResponse>) -> Response {
    let body = Body::from(Bytes::from_owner(HistoryBody(reply)));
    let mut response = Response::new(body);
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/x-amz-json-1.0"),
    );
    response
}
