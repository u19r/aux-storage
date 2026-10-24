use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HistoryCaptureStatus {
    Holding,
    CapturingHigh,
    Ready,
    Releasing,
    Released,
    Cancelling,
    Cancelled,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryCaptureVectorPage {
    pub content: CanonicalDigest,
    pub schema: CanonicalDigest,
    pub vector: CanonicalDigest,
    pub cuts: Vec<crate::bootstrap::SourceCaptureCut>,
    pub next_partition: Option<u32>,
}
