mod baseline;
mod winner;

use std::{io, num::NonZeroUsize, sync::Arc};

use replication_types::{
    error::ReplicationProtocolError,
    event::{
        content::{
            AcceptedItemEvent, CanonicalEvent, CanonicalEventCandidate, CanonicalEventCodec,
            CanonicalPayload, SourceImagesRef,
        },
        identity::{CanonicalEventId, CanonicalItemKey},
    },
    protocol::{CanonicalBytes, CanonicalDigest, PayloadBounds, SemanticVersion},
};
use serde::{Deserialize, Serialize};
use storage_types::KeyAttributes;

use crate::{bounded_json::BoundedJsonWriter, constants::CANONICAL_EVENT_SEMANTIC_VERSION};

/// Bounded event-only JSON. Accepted bytes are immutable, including image
/// spelling.
pub struct CanonicalJsonCodec {
    bounds: PayloadBounds,
    winner_bytes: std::sync::OnceLock<Result<NonZeroUsize, ReplicationProtocolError>>,
}

impl CanonicalJsonCodec {
    pub fn require_event_length(&self, length: usize) -> Result<(), ReplicationProtocolError> {
        self.validate_size(length, self.bounds.event_bytes)
    }

    pub fn new(bounds: PayloadBounds) -> Self {
        Self {
            bounds,
            winner_bytes: std::sync::OnceLock::new(),
        }
    }

    pub fn encode_item_event(
        &self,
        semantics: SemanticVersion,
        identity: &CanonicalEventId,
        item: &AcceptedItemEvent<SourceImagesRef<'_>>,
    ) -> Result<CanonicalBytes, ReplicationProtocolError> {
        self.author_item_event(semantics, identity, item)
            .map(crate::canonical_item_original::AuthoredItemOriginal::into_bytes)
    }

    pub fn author_item_event(
        &self,
        semantics: SemanticVersion,
        identity: &CanonicalEventId,
        item: &AcceptedItemEvent<SourceImagesRef<'_>>,
    ) -> Result<crate::canonical_item_original::AuthoredItemOriginal, ReplicationProtocolError>
    {
        self.validate_semantics(semantics)?;
        let content = replication_types::event::publication::ValidatedItemEvent::try_new(
            identity, item, self,
        )?;
        let bytes = self.write_event(&EventEnvelope {
            semantics,
            id: content.identity(),
            payload: ItemPayloadRef::Item(content.item()),
        })?;
        Ok(crate::canonical_item_original::AuthoredItemOriginal::from_encoding(bytes, &content))
    }

    pub fn decode_event(&self, bytes: &[u8]) -> Result<CanonicalEvent, ReplicationProtocolError> {
        self.validate_size(bytes.len(), self.bounds.event_bytes)?;
        CanonicalEventCandidate::try_decode(
            CanonicalBytes::try_new(Arc::from(bytes), self.bounds.event_bytes)?,
            CanonicalDigest::of(bytes),
            self,
            self.bounds,
        )
    }

    fn write_event<T: Serialize>(
        &self,
        value: &T,
    ) -> Result<CanonicalBytes, ReplicationProtocolError> {
        CanonicalBytes::try_new(
            Arc::from(self.write_json(value, self.bounds.event_bytes)?),
            self.bounds.event_bytes,
        )
    }

    fn write_json<T: Serialize>(
        &self,
        value: &T,
        limit: NonZeroUsize,
    ) -> Result<Vec<u8>, ReplicationProtocolError> {
        let mut writer = BoundedJsonWriter::new(limit.get());
        let encoded = serde_json::to_writer(&mut writer, value);
        writer
            .require_capacity()
            .map_err(|error| self.map_output_error(error.kind(), limit))?;
        encoded.map_err(|error| self.map_json_error(error, limit))?;
        Ok(Vec::from(writer))
    }

    fn map_json_error(
        &self,
        error: serde_json::Error,
        limit: NonZeroUsize,
    ) -> ReplicationProtocolError {
        error
            .io_error_kind()
            .map_or_else(invalid_encoding, |kind| self.map_output_error(kind, limit))
    }

    fn map_output_error(
        &self,
        kind: io::ErrorKind,
        limit: NonZeroUsize,
    ) -> ReplicationProtocolError {
        match kind {
            io::ErrorKind::FileTooLarge => self.over_bound(limit),
            io::ErrorKind::OutOfMemory => ReplicationProtocolError::CapacityRejected,
            _ => invalid_encoding(),
        }
    }

    fn over_bound(&self, limit: NonZeroUsize) -> ReplicationProtocolError {
        ReplicationProtocolError::PayloadTooLarge {
            context: "regional event bytes",
            actual: limit.get().saturating_add(1),
            limit: limit.get(),
        }
    }

    fn validate_semantics(
        &self,
        semantics: SemanticVersion,
    ) -> Result<(), ReplicationProtocolError> {
        if u32::from(semantics) != CANONICAL_EVENT_SEMANTIC_VERSION {
            return Err(ReplicationProtocolError::ProtocolIncompatible {
                context: "regional event semantic version",
            });
        }
        Ok(())
    }

    fn validate_size(
        &self,
        actual: usize,
        limit: NonZeroUsize,
    ) -> Result<(), ReplicationProtocolError> {
        if actual > limit.get() {
            return Err(ReplicationProtocolError::PayloadTooLarge {
                context: "regional event bytes",
                actual,
                limit: limit.get(),
            });
        }
        Ok(())
    }
}

impl CanonicalEventCodec for CanonicalJsonCodec {
    fn encode_event(
        &self,
        semantics: SemanticVersion,
        identity: &CanonicalEventId,
        payload: &CanonicalPayload,
    ) -> Result<CanonicalBytes, ReplicationProtocolError> {
        self.validate_semantics(semantics)?;
        if let CanonicalPayload::Item(item) = payload {
            item.require_causal_shape(identity)?;
            SourceImagesRef::from(&item.images).require_item_shape(&item.key, item.cause)?;
        }
        validate_extension_payload(payload)?;
        self.write_event(&EventEnvelope {
            semantics,
            id: identity,
            payload,
        })
    }

    fn decode_event_content(
        &self,
        bytes: &[u8],
    ) -> Result<(SemanticVersion, CanonicalEventId, CanonicalPayload), ReplicationProtocolError>
    {
        self.validate_size(bytes.len(), self.bounds.event_bytes)?;
        let envelope: EventEnvelope<CanonicalEventId, CanonicalPayload> =
            serde_json::from_slice(bytes).map_err(|_| invalid_encoding())?;
        self.validate_semantics(envelope.semantics)?;
        validate_extension_payload(&envelope.payload)?;
        Ok((envelope.semantics, envelope.id, envelope.payload))
    }

    fn encode_key(
        &self,
        key: &KeyAttributes,
    ) -> Result<CanonicalItemKey, ReplicationProtocolError> {
        let canonical =
            storage_types::CanonicalKeyAttributes::try_from(key).map_err(|_| invalid_encoding())?;
        self.write_event(&canonical)?.try_into()
    }
}

fn validate_extension_payload(payload: &CanonicalPayload) -> Result<(), ReplicationProtocolError> {
    if let CanonicalPayload::TableStream { bytes, .. } = payload {
        stream_provider::UserStreamAppend::validate_data(bytes.as_ref())
            .map_err(|_| invalid_encoding())?;
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventEnvelope<I, P> {
    semantics: SemanticVersion,
    id: I,
    payload: P,
}

#[derive(Serialize)]
enum ItemPayloadRef<'a> {
    Item(&'a AcceptedItemEvent<SourceImagesRef<'a>>),
}

fn invalid_encoding() -> ReplicationProtocolError {
    ReplicationProtocolError::InvalidEncoding {
        context: "regional event JSON",
    }
}

impl replication_types::event::interval::IntervalCodec for CanonicalJsonCodec {
    fn encode_interval(
        &self,
        manifest: &replication_types::event::interval::SourceIntervalManifest,
    ) -> Result<CanonicalBytes, ReplicationProtocolError> {
        manifest.validate_shape()?;
        let mut writer = BoundedJsonWriter::new(self.bounds.manifest_bytes.get());
        let content = IntervalContent::from(manifest);
        let encoded = serde_json::to_writer(&mut writer, &content);
        writer
            .require_capacity()
            .map_err(|error| self.map_manifest_output_error(error.kind()))?;
        encoded.map_err(|error| {
            error.io_error_kind().map_or_else(invalid_encoding, |kind| {
                self.map_manifest_output_error(kind)
            })
        })?;
        CanonicalBytes::try_new(Arc::from(Vec::from(writer)), self.bounds.manifest_bytes)
    }
}

impl CanonicalJsonCodec {
    fn map_manifest_output_error(&self, kind: io::ErrorKind) -> ReplicationProtocolError {
        match kind {
            io::ErrorKind::FileTooLarge => ReplicationProtocolError::PayloadTooLarge {
                context: "regional manifest bytes",
                actual: self.bounds.manifest_bytes.get().saturating_add(1),
                limit: self.bounds.manifest_bytes.get(),
            },
            io::ErrorKind::OutOfMemory => ReplicationProtocolError::CapacityRejected,
            _ => invalid_encoding(),
        }
    }

    /// Partial selection can be measured before its complete coverage is
    /// sealed.
    pub fn interval_content_length(
        &self,
        manifest: &replication_types::event::interval::SourceIntervalManifest,
        maximum: usize,
    ) -> Result<usize, ReplicationProtocolError> {
        let maximum = maximum.min(self.bounds.manifest_bytes.get());
        let mut counter = crate::bounded_json::BoundedJsonCounter::new(maximum);
        let encoded = serde_json::to_writer(&mut counter, &IntervalContent::from(manifest));
        counter
            .require_capacity()
            .map_err(|_| ReplicationProtocolError::PayloadTooLarge {
                context: "regional manifest bytes",
                actual: maximum.saturating_add(1),
                limit: maximum,
            })?;
        encoded.map_err(|error| {
            if error.io_error_kind() == Some(io::ErrorKind::FileTooLarge) {
                ReplicationProtocolError::PayloadTooLarge {
                    context: "regional manifest bytes",
                    actual: maximum.saturating_add(1),
                    limit: maximum,
                }
            } else {
                invalid_encoding()
            }
        })?;
        Ok(counter.length())
    }
}

#[derive(Serialize)]
struct IntervalContent<'a> {
    origin: &'a replication_types::identity::origin::OriginId,
    membership: replication_types::configuration::membership::MembershipRevision,
    range: replication_types::event::interval::ClosedSourceInterval,
    links: &'a [replication_types::event::interval::CanonicalEventLink],
    coverage: &'a [replication_types::event::interval::PositionCoverage],
}

impl<'a> From<&'a replication_types::event::interval::SourceIntervalManifest>
    for IntervalContent<'a>
{
    fn from(manifest: &'a replication_types::event::interval::SourceIntervalManifest) -> Self {
        Self {
            origin: &manifest.origin,
            membership: manifest.membership,
            range: manifest.range,
            links: &manifest.links,
            coverage: &manifest.coverage,
        }
    }
}
