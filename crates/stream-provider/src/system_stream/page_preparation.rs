//! Request-time page clock remains stable across native retries.

use storage_types::TimestampMillis;

use super::SystemStreamPageRequest;

#[derive(Clone)]
pub(super) struct PreparedSystemStreamPage {
    pub(super) request: SystemStreamPageRequest,
    pub(super) issued_at: TimestampMillis,
}

impl PreparedSystemStreamPage {
    pub(super) const fn new(request: SystemStreamPageRequest, issued_at: TimestampMillis) -> Self {
        Self { request, issued_at }
    }
}
