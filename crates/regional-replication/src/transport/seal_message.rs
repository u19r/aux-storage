use std::sync::Arc;

use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};

use super::{error::ImportNack, message::WireIdentity};
use crate::lifecycle::{SealReceptionProgress, SealRequest, SealVectorPage};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SealMessage {
    pub from: WireIdentity,
    pub to: WireIdentity,
    pub request: Arc<SealRequest>,
    pub action: SealAction,
}

#[derive(Serialize, Deserialize)]
pub(super) enum SealAction {
    Fetch(u32),
    Observe,
    Receive(SealVectorPage),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SealReply {
    pub from: WireIdentity,
    pub to: WireIdentity,
    pub content: CanonicalDigest,
    pub result: Result<SealOutcome, ImportNack>,
}

#[derive(Serialize, Deserialize)]
pub(super) enum SealOutcome {
    Page(SealVectorPage),
    Progress(SealReceptionProgress),
}
