use replication_types::{
    error::ReplicationProtocolError,
    event::{identity::CanonicalEventId, order::WinnerProvenance},
    protocol::CanonicalDigest,
};

use crate::error::ReplicationResult;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WinnerDecision {
    Replace,
    RetainHistoryOnly,
    Duplicate,
}
#[derive(Clone, Debug)]
pub struct WinnerEffectPlan<'a> {
    pub incoming: &'a WinnerProvenance,
    pub observed: Option<&'a WinnerProvenance>,
    pub known_identity: Option<(CanonicalEventId, CanonicalDigest)>,
}
impl WinnerEffectPlan<'_> {
    pub fn decide_winner(&self) -> ReplicationResult<WinnerDecision> {
        self.incoming.validate()?;
        if let Some(current) = self.observed {
            current.validate()?;
        }
        if let Some((id, digest)) = &self.known_identity {
            if self.incoming.event.as_ref() != Some(id) || *digest != self.incoming.digest {
                return Err(ReplicationProtocolError::IdentityContentMismatch {
                    context: "winner.decide_winner",
                }
                .into());
            }
            return Ok(WinnerDecision::Duplicate);
        }
        let Some(current) = self.observed else {
            return Ok(WinnerDecision::Replace);
        };
        if self.incoming.lineage != current.lineage || self.incoming.key != current.key {
            return Err(ReplicationProtocolError::LineageMismatch {
                context: "winner.decide_winner",
            }
            .into());
        }
        match self.incoming.order.cmp(&current.order) {
            std::cmp::Ordering::Greater => Ok(WinnerDecision::Replace),
            std::cmp::Ordering::Less => Ok(WinnerDecision::RetainHistoryOnly),
            std::cmp::Ordering::Equal if self.incoming == current => {
                Ok(WinnerDecision::RetainHistoryOnly)
            }
            std::cmp::Ordering::Equal => Err(ReplicationProtocolError::IdentityContentMismatch {
                context: "winner.decide_winner",
            }
            .into()),
        }
    }
}
