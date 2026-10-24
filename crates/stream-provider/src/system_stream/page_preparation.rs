//! Request-time checkpoint identity remains stable across native retries.

use std::fmt::Write as _;

use storage_types::{StorageError, StorageResult, SystemStreamCheckpointToken, TimestampMillis};
use uuid::Uuid;

use super::SystemStreamPageRequest;

#[derive(Clone)]
pub(super) struct PreparedSystemStreamPage {
    pub(super) request: SystemStreamPageRequest,
    pub(super) issued_token: SystemStreamCheckpointToken,
    pub(super) issued_at: TimestampMillis,
}

impl PreparedSystemStreamPage {
    pub(super) fn prepare(request: SystemStreamPageRequest) -> StorageResult<Self> {
        let mut encoded = String::with_capacity(35);
        encoded.push_str("v1.");
        for byte in Uuid::new_v4().into_bytes() {
            write!(&mut encoded, "{byte:02x}")
                .map_err(|_| StorageError::internal("SystemStream token encoding failed"))?;
        }
        let issued_token = SystemStreamCheckpointToken::try_from(encoded)
            .map_err(|error| StorageError::validation(error.to_string()))?;
        Ok(Self {
            request,
            issued_token,
            issued_at: TimestampMillis::now(),
        })
    }
}
