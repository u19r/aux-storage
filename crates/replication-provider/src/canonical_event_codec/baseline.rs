use replication_types::{
    error::ReplicationProtocolError,
    event::{
        baseline::BaselineItem,
        content::{CanonicalEventCodec, SourceImagesRef},
    },
    protocol::{CanonicalBytes, SemanticVersion},
};

use super::CanonicalJsonCodec;

impl CanonicalJsonCodec {
    pub fn encode_baseline(
        &self,
        baseline: &BaselineItem<SourceImagesRef<'_>>,
    ) -> Result<CanonicalBytes, ReplicationProtocolError> {
        baseline.validate()?;
        if self.encode_key(&baseline.key)? != baseline.canonical_key {
            return Err(invalid_baseline());
        }
        self.write_event(&BaselineEnvelope {
            semantics: crate::CANONICAL_EVENT_SEMANTIC_VERSION.into(),
            baseline,
        })
    }

    pub fn decode_baseline(&self, bytes: &[u8]) -> Result<BaselineItem, ReplicationProtocolError> {
        self.validate_size(bytes.len(), self.bounds.event_bytes)?;
        let envelope: BaselineEnvelope<BaselineItem> =
            serde_json::from_slice(bytes).map_err(|_| invalid_baseline())?;
        self.validate_semantics(envelope.semantics)?;
        let baseline = envelope.baseline;
        baseline.validate()?;
        if self.encode_key(&baseline.key)? != baseline.canonical_key {
            return Err(invalid_baseline());
        }
        Ok(baseline)
    }
}

fn invalid_baseline() -> ReplicationProtocolError {
    ReplicationProtocolError::IdentityContentMismatch {
        context: "canonical baseline key",
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BaselineEnvelope<B> {
    semantics: SemanticVersion,
    baseline: B,
}
