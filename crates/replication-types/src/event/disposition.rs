use serde::{Deserialize, Serialize};

use super::identity::CanonicalEventId;
use crate::protocol::CanonicalDigest;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventDisposition {
    WinnerApplied,
    HistoryAccepted,
    DuplicateAccepted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalAcceptanceReceipt {
    pub event: CanonicalEventId,
    pub digest: CanonicalDigest,
    pub disposition: EventDisposition,
}
