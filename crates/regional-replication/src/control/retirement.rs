use replication_types::{identity::replica::RegionId, protocol::CanonicalDigest};
use serde::{Deserialize, Serialize};

/// Ordered source seal-vector bindings carried by the committed decision and
/// every installation receipt. A vector's native cuts live at each survivor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetiredSealEvidence {
    pub region: RegionId,
    pub vector: CanonicalDigest,
}
