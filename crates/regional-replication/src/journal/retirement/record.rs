use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};

use crate::lifecycle::{SealReceptionProgress, SealRequest, SealedOriginCut};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReceivedSeal {
    pub request: SealRequest,
    pub vector: CanonicalDigest,
    pub folded: CanonicalDigest,
    pub next: u32,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetiredOrigin {
    pub operation: replication_types::identity::resource::ControlOperationId,
    pub region: replication_types::identity::replica::RegionId,
    pub content: CanonicalDigest,
    pub cut: SealedOriginCut,
}

impl ReceivedSeal {
    pub fn progress(&self, count: u32) -> SealReceptionProgress {
        SealReceptionProgress {
            next_partition: self.next,
            complete: self.next == count && self.folded == self.vector,
        }
    }
}

impl RetiredOrigin {
    pub(super) fn is_seal_cut(
        &self,
        request: &SealRequest,
        cut: &SealedOriginCut,
        content: CanonicalDigest,
    ) -> bool {
        self.operation == request.proposal.operation().id
            && self.region == request.region
            && self.content == content
            && self.cut == *cut
    }
}
