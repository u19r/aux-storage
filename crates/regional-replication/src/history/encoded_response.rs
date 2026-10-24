//! Serialization retains native source ownership and transfers it with encoded
//! bytes.
use serde::Serialize;
use storage_types::{StorageError, StorageResult};

use crate::transport::SourceReservation;

pub struct EncodedHistoryResponse {
    body: Vec<u8>,
    source: Option<SourceReservation>,
}
impl EncodedHistoryResponse {
    pub fn into_parts(self) -> (Vec<u8>, Option<SourceReservation>) {
        (self.body, self.source)
    }
}
pub trait HistoryResponsePayload: Serialize + Sized {
    fn into_source(self) -> Option<SourceReservation>;
    fn into_encoded(self) -> StorageResult<EncodedHistoryResponse> {
        let body = serde_json::to_vec(&self)
            .map_err(|_| StorageError::internal("Canonical history response encoding failed"))?;
        Ok(EncodedHistoryResponse {
            body,
            source: self.into_source(),
        })
    }
}
impl<T: HistoryResponsePayload> HistoryResponsePayload for Option<T> {
    fn into_source(self) -> Option<SourceReservation> {
        self.and_then(HistoryResponsePayload::into_source)
    }
}
impl HistoryResponsePayload for super::read::CanonicalHistoryPage {
    fn into_source(self) -> Option<SourceReservation> {
        Some(self._source)
    }
}
impl HistoryResponsePayload for super::discovery::CanonicalHistorySources {
    fn into_source(self) -> Option<SourceReservation> {
        Some(self._source)
    }
}
impl HistoryResponsePayload for super::view::CanonicalViewPage {
    fn into_source(self) -> Option<SourceReservation> {
        Some(self._source)
    }
}
impl HistoryResponsePayload for super::checkpoint::CanonicalCheckpointStatus {
    fn into_source(self) -> Option<SourceReservation> {
        Some(self._source)
    }
}
impl HistoryResponsePayload for super::source_cut::CanonicalSourceCutObservation {
    fn into_source(self) -> Option<SourceReservation> {
        Some(self._source)
    }
}
impl HistoryResponsePayload for super::table_cut::CanonicalTableCutObservation {
    fn into_source(self) -> Option<SourceReservation> {
        Some(self._source)
    }
}
impl HistoryResponsePayload for super::portable_checkpoint::PortableCheckpointStatus {
    fn into_source(self) -> Option<SourceReservation> {
        Some(self._source)
    }
}
impl HistoryResponsePayload for super::hold_discovery::ReaderHoldPage {
    fn into_source(self) -> Option<SourceReservation> {
        Some(self._source)
    }
}
impl HistoryResponsePayload for super::causal_read::CanonicalCausalReadPage {
    fn into_source(self) -> Option<SourceReservation> {
        Some(self._source)
    }
}
impl HistoryResponsePayload for super::causal_stream::CanonicalCausalStreamPage {
    fn into_source(self) -> Option<SourceReservation> {
        self._source
    }
}
