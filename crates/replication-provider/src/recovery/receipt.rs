use replication_types::{identity::resource::OperationIdentity, protocol::CanonicalDigest};

use crate::error::{ReplicationDecisionError, ReplicationResult};
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptBinding {
    pub operation: OperationIdentity,
    pub content: CanonicalDigest,
}
/// Receipt inspection may start before the submitting peer delivered its
/// issued content digest. An operation-only lookup returns original evidence;
/// it never grants retry or publication permission.
pub enum ReceiptLookup {
    Bound(AttemptBinding),
    Operation(OperationIdentity),
}

impl From<AttemptBinding> for ReceiptLookup {
    fn from(binding: AttemptBinding) -> Self {
        Self::Bound(binding)
    }
}

impl From<OperationIdentity> for ReceiptLookup {
    fn from(operation: OperationIdentity) -> Self {
        Self::Operation(operation)
    }
}

pub enum ReceiptObservation<R> {
    Durable { binding: AttemptBinding, receipt: R },
    ProvenAborted { binding: AttemptBinding },
    Unresolved,
}
pub enum RecoveryDecision<R> {
    Completed(R),
    RetryFresh,
    Wait,
}
impl AttemptBinding {
    pub fn reconcile<R>(
        self,
        observation: ReceiptObservation<R>,
    ) -> ReplicationResult<RecoveryDecision<R>> {
        match observation {
            ReceiptObservation::Durable { binding, receipt } if binding == self => {
                Ok(RecoveryDecision::Completed(receipt))
            }
            ReceiptObservation::ProvenAborted { binding } if binding == self => {
                Ok(RecoveryDecision::RetryFresh)
            }
            ReceiptObservation::Unresolved => Ok(RecoveryDecision::Wait),
            _ => Err(ReplicationDecisionError::MissingEvidence {
                context: "receipt evidence belongs to another attempt",
            }),
        }
    }
}
