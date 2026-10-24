use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec,
    regional_application::{import_admission::BoundedImport, observation::TablePageAcceptance},
    runtime_configuration::limits::WorkLimits,
};
use replication_types::{
    configuration::membership::RegionalTableBinding,
    event::interval::SourceIntervalManifest,
    identity::{
        origin::JournalGeneration,
        replica::{DatabaseGroupId, RegionId, ReplicaId},
        resource::DeploymentId,
    },
    regional_import::RegionalImport,
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::error::{ImportNack, TransportError, TransportResult};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireIdentity {
    pub deployment: DeploymentId,
    pub replica: ReplicaId,
    pub region: RegionId,
    pub group: DatabaseGroupId,
    pub generation: JournalGeneration,
}

#[derive(Serialize)]
pub(super) struct OutgoingPage<'a, E> {
    pub from: WireIdentity,
    pub to: WireIdentity,
    pub manifest: &'a SourceIntervalManifest,
    pub bindings: &'a [RegionalTableBinding],
    pub fragment: Option<replication_types::event::interval::ImportFragment>,
    pub events: E,
}

pub(super) struct OriginalEvents<'a>(pub &'a [replication_types::event::content::CanonicalEvent]);

impl Serialize for OriginalEvents<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{Error, SerializeSeq};
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for event in self.0 {
            let original =
                std::str::from_utf8(event.as_ref().bytes.as_ref()).map_err(S::Error::custom)?;
            sequence.serialize_element(original)?;
        }
        sequence.end()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IncomingPage {
    pub from: WireIdentity,
    pub to: WireIdentity,
    pub manifest: SourceIntervalManifest,
    pub bindings: Vec<RegionalTableBinding>,
    pub fragment: Option<replication_types::event::interval::ImportFragment>,
    pub events: Box<RawValue>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ImportReply {
    pub from: WireIdentity,
    pub to: WireIdentity,
    pub result: Result<ImportAcknowledgement, ImportNack>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ImportAcknowledgement {
    pub pages: Vec<TablePageAcceptance>,
    pub completed: Option<Vec<TablePageAcceptance>>,
}

impl IncomingPage {
    pub(super) fn into_import(
        self,
        codec: &CanonicalJsonCodec,
        limits: &WorkLimits,
    ) -> TransportResult<RegionalImport> {
        BoundedImport::require_declaration(&self.manifest, &self.bindings, limits)?;
        let events = decode_events(self.events.get(), codec, limits)?;
        let input = (self.bindings, self.manifest, events);
        Ok(match self.fragment {
            Some(fragment) => RegionalImport::try_fragment(input, fragment, codec)?,
            None => RegionalImport::try_new(input, codec)?,
        })
    }
}

fn decode_events(
    raw: &str,
    codec: &CanonicalJsonCodec,
    limits: &WorkLimits,
) -> TransportResult<Vec<replication_types::event::content::CanonicalEvent>> {
    use serde::de::DeserializeSeed;
    let mut deserializer = serde_json::Deserializer::from_str(raw);
    let events = EventSeed { codec, limits }
        .deserialize(&mut deserializer)
        .map_err(|_| TransportError::Frame)?;
    deserializer.end().map_err(|_| TransportError::Frame)?;
    Ok(events)
}

struct EventSeed<'a> {
    codec: &'a CanonicalJsonCodec,
    limits: &'a WorkLimits,
}

impl<'de> serde::de::DeserializeSeed<'de> for EventSeed<'_> {
    type Value = Vec<replication_types::event::content::CanonicalEvent>;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de> serde::de::Visitor<'de> for EventSeed<'_> {
    type Value = Vec<replication_types::event::content::CanonicalEvent>;
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("bounded original regional events")
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(
        self,
        mut sequence: A,
    ) -> Result<Self::Value, A::Error> {
        use serde::de::Error;
        let mut events = Vec::new();
        let mut remaining = self.limits.import_bytes.get();
        while let Some(event) = sequence.next_element::<std::borrow::Cow<'de, str>>()? {
            if events.len() == self.limits.import_events.get() {
                return Err(A::Error::custom("regional event count bound"));
            }
            remaining = remaining
                .checked_sub(event.len())
                .ok_or_else(|| A::Error::custom("regional original byte bound"))?;
            events.push(
                self.codec
                    .decode_event(event.as_bytes())
                    .map_err(|_| A::Error::custom("invalid original regional event"))?,
            );
        }
        Ok(events)
    }
}
