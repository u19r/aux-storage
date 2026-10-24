//! Encoded history retains its native reservation until the consumer releases
//! the body.
pub struct CanonicalHistoryResponse {
    body: Vec<u8>,
    #[cfg(feature = "regional")]
    _source: Option<regional_replication::transport::SourceReservation>,
}
impl AsRef<[u8]> for CanonicalHistoryResponse {
    fn as_ref(&self) -> &[u8] {
        &self.body
    }
}
#[cfg(feature = "regional")]
impl From<regional_replication::history::encoded_response::EncodedHistoryResponse>
    for CanonicalHistoryResponse
{
    fn from(
        value: regional_replication::history::encoded_response::EncodedHistoryResponse,
    ) -> Self {
        let (body, source) = value.into_parts();
        Self {
            body,
            _source: source,
        }
    }
}

impl std::fmt::Debug for CanonicalHistoryResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CanonicalHistoryResponse")
            .field("bytes", &self.body.len())
            .finish_non_exhaustive()
    }
}
