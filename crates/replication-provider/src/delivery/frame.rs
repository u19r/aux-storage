use replication_types::{
    error::ReplicationProtocolError,
    event::{
        content::CanonicalEvent,
        interval::{IntervalCodec, SourceIntervalManifest},
    },
};

use crate::runtime_configuration::limits::WorkLimits;
pub struct SourceFrame {
    manifest: SourceIntervalManifest,
    events: Vec<CanonicalEvent>,
}
impl SourceFrame {
    pub fn try_new(
        input: (SourceIntervalManifest, Vec<CanonicalEvent>),
        codec: &impl IntervalCodec,
        limits: &WorkLimits,
    ) -> Result<Self, ReplicationProtocolError> {
        let (manifest, events) = input;
        validate_frame(&manifest, &events, codec, limits)?;
        Ok(Self { manifest, events })
    }
    pub fn into_parts(self) -> (SourceIntervalManifest, Vec<CanonicalEvent>) {
        (self.manifest, self.events)
    }
    pub fn manifest(&self) -> &SourceIntervalManifest {
        &self.manifest
    }
    pub fn events(&self) -> &[CanonicalEvent] {
        &self.events
    }
}
fn validate_frame(
    manifest: &SourceIntervalManifest,
    events: &[CanonicalEvent],
    codec: &impl IntervalCodec,
    limits: &WorkLimits,
) -> Result<(), ReplicationProtocolError> {
    manifest.validate_shape()?;
    let bytes = codec.encode_interval(manifest)?;
    manifest.digest.verify(bytes.as_ref())?;
    if manifest.links.len() != events.len()
        || events.len() > limits.frame_events.get()
        || manifest.coverage.len() > limits.frame_events.get()
    {
        return Err(ReplicationProtocolError::InvalidRange {
            context: "frame event/coverage bound",
        });
    }
    let mut size = bytes.as_ref().len();
    for (link, event) in manifest.links.iter().zip(events) {
        let candidate = event.as_ref();
        link.require_event(event, &manifest.origin)?;
        size = size.checked_add(candidate.bytes.as_ref().len()).ok_or(
            ReplicationProtocolError::CounterOverflow {
                context: "frame byte count",
            },
        )?;
    }
    if size > limits.frame_bytes.get() {
        return Err(ReplicationProtocolError::PayloadTooLarge {
            context: "frame bytes",
            actual: size,
            limit: limits.frame_bytes.get(),
        });
    }
    Ok(())
}
