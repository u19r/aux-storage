use serde::{Deserialize, Serialize, Serializer};
use storage_types::{KeyAttributes, WireItem};

use super::{
    identity::{CanonicalEventId, CanonicalItemKey, VersionIdentity},
    order::{WinnerOrder, WinnerProvenance},
};
use crate::{
    configuration::table::SchemaRevision,
    error::ReplicationProtocolError,
    identity::resource::{OperationIdentity, TableIdentity, TableLineage},
    protocol::{CanonicalBytes, CanonicalDigest, PayloadBounds, SemanticVersion},
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceImages {
    #[serde(with = "storage_types::dynamo_json::optional")]
    pub old: Option<WireItem>,
    #[serde(with = "storage_types::dynamo_json::optional")]
    pub new: Option<WireItem>,
    pub old_indexers: Option<Vec<String>>,
    pub new_indexers: Option<Vec<String>>,
}

/// Encode the accepted images before native application consumes them.
#[derive(Clone, Copy, Serialize)]
pub struct SourceImagesRef<'a> {
    #[serde(serialize_with = "serialize_optional_image")]
    pub old: Option<&'a WireItem>,
    #[serde(serialize_with = "serialize_optional_image")]
    pub new: Option<&'a WireItem>,
    pub old_indexers: Option<&'a [String]>,
    pub new_indexers: Option<&'a [String]>,
}

impl<'a> From<&'a SourceImages> for SourceImagesRef<'a> {
    fn from(images: &'a SourceImages) -> Self {
        Self {
            old: images.old.as_ref(),
            new: images.new.as_ref(),
            old_indexers: images.old_indexers.as_deref(),
            new_indexers: images.new_indexers.as_deref(),
        }
    }
}

fn serialize_optional_image<S>(
    image: &Option<&WireItem>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    image
        .map(storage_types::dynamo_json::ItemRef)
        .serialize(serializer)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationCause {
    Put,
    Update,
    Delete,
    Expiration,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedItemEvent<I = SourceImages> {
    pub source_table: TableIdentity,
    pub lineage: TableLineage,
    pub schema: SchemaRevision,
    pub key: KeyAttributes,
    pub canonical_key: CanonicalItemKey,
    pub images: I,
    pub order: WinnerOrder,
    pub version: VersionIdentity,
    pub cause: MutationCause,
    pub victim_order: Option<WinnerOrder>,
    pub parents: Vec<CanonicalEventId>,
    pub predecessor: Option<VersionIdentity>,
    pub transaction: Option<OperationIdentity>,
    pub observations: Vec<WinnerProvenance>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedControlEvent {
    pub proposal: crate::configuration::proposal::ControlProposal,
    pub publication: super::identity::OriginEventId,
    #[serde(deserialize_with = "parse_control_predecessor")]
    pub predecessor: Option<crate::identity::resource::ControlOperationId>,
}

// Option normally accepts an absent field; this maintained format requires
// an explicit null for a root, rather than a missing-field legacy fallback.
fn parse_control_predecessor<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<crate::identity::resource::ControlOperationId>, D::Error> {
    Option::deserialize(deserializer)
}

impl AcceptedControlEvent {
    fn validate(&self) -> Result<(), ReplicationProtocolError> {
        self.proposal.validate()?;
        let operation = self.proposal.operation();
        let requires_predecessor = operation.before.as_ref().is_some_and(|before| {
            !matches!(
                operation.change,
                crate::configuration::control::ConfigurationChange::EnableGlobal { .. }
            ) || u64::from(before.config) != 0
        });
        if self.predecessor.is_some() != requires_predecessor
            || self.predecessor == Some(operation.id)
        {
            return Err(ReplicationProtocolError::IdentityContentMismatch {
                context: "control history predecessor",
            });
        }
        let primary = self.proposal.participant(&operation.home_region)?;
        let source = primary
            .proposed
            .sources
            .iter()
            .find(|source| source.region == operation.home_region)
            .ok_or(ReplicationProtocolError::InvalidOrigin {
                context: "control history primary",
            })?;
        let publication = &self.publication;
        publication.validate()?;
        if publication.lineage != operation.lineage
            || publication.origin.region != source.region
            || publication.origin.database_group != source.database_group
            || publication.origin.journal_generation != source.generation
            || u16::from(publication.origin.journal_partition) != 0
            || u32::from(publication.ordinal) != 0
        {
            return Err(ReplicationProtocolError::InvalidOrigin {
                context: "control history publication",
            });
        }
        Ok(())
    }

    /// The immutable before configuration owns the predecessor's lineage,
    /// including the disable decision crossed by a fresh-lineage re-enable.
    pub fn predecessor_identity(&self) -> Option<CanonicalEventId> {
        let operation = self.predecessor?;
        let before = self.proposal.operation().before.as_ref()?;
        Some(CanonicalEventId::Control {
            lineage: before.lineage,
            operation,
        })
    }
}

#[derive(Serialize, Deserialize)]
pub enum CanonicalPayload {
    Item(Box<AcceptedItemEvent>),
    /// Typed table-scoped System history. Retained decision content never
    /// grants metadata installation authority to its receiver.
    System(Box<AcceptedControlEvent>),
    TableStream {
        table: TableIdentity,
        lineage: TableLineage,
        bytes: CanonicalBytes,
    },
}

/// Wire/durable candidate. Deserialization does not create an accepted event.
#[derive(Serialize, Deserialize)]
pub struct CanonicalEventCandidate {
    pub semantics: SemanticVersion,
    pub id: CanonicalEventId,
    pub payload: CanonicalPayload,
    pub bytes: CanonicalBytes,
    pub digest: CanonicalDigest,
}

/// Pure codec belongs to the wire boundary; no storage calls or domain
/// evaluation.
pub trait CanonicalEventCodec {
    fn encode_event(
        &self,
        semantics: SemanticVersion,
        identity: &CanonicalEventId,
        payload: &CanonicalPayload,
    ) -> Result<CanonicalBytes, ReplicationProtocolError>;
    fn decode_event_content(
        &self,
        bytes: &[u8],
    ) -> Result<(SemanticVersion, CanonicalEventId, CanonicalPayload), ReplicationProtocolError>;
    fn encode_key(&self, key: &KeyAttributes)
    -> Result<CanonicalItemKey, ReplicationProtocolError>;
}

#[derive(Clone)]
pub struct CanonicalEvent {
    candidate: std::sync::Arc<CanonicalEventCandidate>,
}

impl CanonicalEventCandidate {
    pub fn try_decode(
        bytes: CanonicalBytes,
        digest: CanonicalDigest,
        codec: &impl CanonicalEventCodec,
        bounds: PayloadBounds,
    ) -> Result<CanonicalEvent, ReplicationProtocolError> {
        if bytes.as_ref().len() > bounds.event_bytes.get() {
            return Err(ReplicationProtocolError::PayloadTooLarge {
                context: "event decode",
                actual: bytes.as_ref().len(),
                limit: bounds.event_bytes.get(),
            });
        }
        digest.verify(bytes.as_ref())?;
        let (semantics, id, payload) = codec.decode_event_content(bytes.as_ref())?;
        let decoded = Self {
            semantics,
            id,
            payload,
            bytes,
            digest,
        };
        decoded.validate_content(codec)
    }

    fn validate_content(
        self,
        codec: &impl CanonicalEventCodec,
    ) -> Result<CanonicalEvent, ReplicationProtocolError> {
        let lineage = match &self.payload {
            CanonicalPayload::Item(item) => item.lineage,
            CanonicalPayload::System(control) => control.proposal.operation().lineage,
            CanonicalPayload::TableStream { lineage, .. } => *lineage,
        };
        if !self.id.is_bound_to(lineage) {
            return Err(ReplicationProtocolError::LineageMismatch {
                context: "content.try_validate",
            });
        }

        if let CanonicalPayload::Item(item) = &self.payload {
            item.require_canonical_key(codec)?;
            self.validate_item(item)?;
        } else {
            self.validate_history_identity()?;
        }

        Ok(CanonicalEvent {
            candidate: std::sync::Arc::new(self),
        })
    }

    fn validate_history_identity(&self) -> Result<(), ReplicationProtocolError> {
        self.id.validate()?;
        if let CanonicalPayload::System(control) = &self.payload {
            control.validate()?;
            let proposal = &control.proposal;
            if self.id
                == (CanonicalEventId::Control {
                    lineage: proposal.operation().lineage,
                    operation: proposal.operation().id,
                })
            {
                return Ok(());
            }
        } else if matches!(&self.id, CanonicalEventId::Origin(_))
            || matches!(&self.id, CanonicalEventId::Adopted { .. })
        {
            return Ok(());
        }
        Err(ReplicationProtocolError::IdentityContentMismatch {
            context: "content.history identity",
        })
    }

    fn validate_item(&self, item: &AcceptedItemEvent) -> Result<(), ReplicationProtocolError> {
        item.require_causal_shape(&self.id)?;
        SourceImagesRef::from(&item.images).require_item_shape(&item.key, item.cause)
    }
}

impl CanonicalEvent {
    /// Derive expiration content from the original accepted mutation, never a
    /// receiver's current table, schema or images. Eligibility, conservative
    /// time and scheduler authority are separate upstream requirements.
    /// The selected winner must match every canonical content reference; a
    /// valid event from another item or version cannot become this victim.
    /// Encode the returned payload under this victim's semantic version.
    ///
    /// A deserialized candidate has no child construction operation.
    ///
    /// ```compile_fail
    /// use replication_types::event::content::CanonicalEventCandidate;
    /// fn expire_unvalidated(candidate: &CanonicalEventCandidate,
    ///     winner: &replication_types::event::order::WinnerProvenance) {
    ///     let _ = candidate.try_expiration_child(winner);
    /// }
    /// ```
    pub fn try_expiration_child(
        &self,
        winner: &WinnerProvenance,
    ) -> Result<(CanonicalEventId, AcceptedItemEvent<SourceImagesRef<'_>>), ReplicationProtocolError>
    {
        let victim = self.bind_expiration_victim(winner)?;
        let old = victim
            .images
            .new
            .as_ref()
            .ok_or(ReplicationProtocolError::InvalidScope {
                context: "content.try_expiration_child",
            })?;
        let identity = victim.version.clone().expiration_id();
        Ok((
            identity,
            AcceptedItemEvent {
                source_table: victim.source_table,
                lineage: victim.lineage,
                schema: victim.schema,
                key: victim.key.clone(),
                canonical_key: victim.canonical_key.clone(),
                images: SourceImagesRef {
                    old: Some(old),
                    new: None,
                    old_indexers: victim.images.new_indexers.as_deref(),
                    new_indexers: None,
                },
                order: victim.order.expiration_child(),
                version: victim.version.clone(),
                cause: MutationCause::Expiration,
                victim_order: Some(victim.order.clone()),
                parents: vec![self.candidate.id.clone()],
                predecessor: Some(victim.version.clone()),
                transaction: None,
                observations: Vec::new(),
            },
        ))
    }

    fn bind_expiration_victim(
        &self,
        winner: &WinnerProvenance,
    ) -> Result<&AcceptedItemEvent, ReplicationProtocolError> {
        let CanonicalPayload::Item(victim) = &self.candidate.payload else {
            return Err(ReplicationProtocolError::InvalidScope {
                context: "content.try_expiration_child",
            });
        };
        if winner.lineage != victim.lineage
            || winner.key != victim.canonical_key
            || winner.version != victim.version
            || winner.order != victim.order
            || winner.event.as_ref() != Some(&self.candidate.id)
            || winner.digest != self.candidate.digest
        {
            return Err(ReplicationProtocolError::IdentityContentMismatch {
                context: "content.try_expiration_child",
            });
        }
        Ok(victim)
    }

    pub fn source_table(&self) -> TableIdentity {
        match &self.candidate.payload {
            CanonicalPayload::Item(item) => item.source_table,
            CanonicalPayload::System(control) => control.proposal.operation().table,
            CanonicalPayload::TableStream { table, .. } => *table,
        }
    }
    pub fn lineage(&self) -> TableLineage {
        match &self.candidate.payload {
            CanonicalPayload::Item(item) => item.lineage,
            CanonicalPayload::System(control) => control.proposal.operation().lineage,
            CanonicalPayload::TableStream { lineage, .. } => *lineage,
        }
    }
}

impl std::fmt::Debug for CanonicalEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CanonicalEvent")
            .field("id", &self.candidate.id)
            .field("digest", &self.candidate.digest)
            .finish_non_exhaustive()
    }
}

impl AsRef<CanonicalEventCandidate> for CanonicalEvent {
    fn as_ref(&self) -> &CanonicalEventCandidate {
        &self.candidate
    }
}
