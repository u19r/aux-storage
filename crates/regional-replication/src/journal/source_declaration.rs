//! An accepted position must fit a complete selected or excluded wire
//! declaration.

use std::num::NonZeroUsize;

use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::NativeBudgets,
};
use replication_types::{
    error::ReplicationProtocolError,
    event::interval::{
        ClosedSourceInterval, ExclusionReason, PositionCoverage, SourceIntervalManifest,
    },
    protocol::PayloadBounds,
};
use storage_types::{InfrastructureResultKind, StorageResult};

use super::{JournalStore, SourcePosition};
use crate::native_record::corrupt_record;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct SourcePublicationLimits {
    pub(super) original_bytes: NonZeroUsize,
    pub(super) declaration_bytes: NonZeroUsize,
    pub(super) maximum_events: NonZeroUsize,
}

impl SourcePublicationLimits {
    pub(super) fn new(native: NativeBudgets, maximum_events: NonZeroUsize) -> Self {
        Self {
            original_bytes: native.maximum_original_position_bytes,
            declaration_bytes: native.maximum_history_declaration_bytes,
            maximum_events,
        }
    }

    fn require_declaration(&self, source: &mut SourcePosition) -> StorageResult<()> {
        if source.events.len() > self.maximum_events.get() {
            return Err(InfrastructureResultKind::NativeCapacityRejected.into());
        }
        let mut ordinals = Vec::new();
        ordinals
            .try_reserve_exact(source.events.len())
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        ordinals.extend(source.events.iter().map(|link| link.ordinal));
        let range = ClosedSourceInterval::try_from((
            (u64::from(source.position) - 1).into(),
            source.position,
        ))
        .map_err(|_| corrupt_record())?;
        let codec = CanonicalJsonCodec::new(PayloadBounds {
            event_bytes: self.original_bytes,
            manifest_bytes: self.declaration_bytes,
        });
        // Membership may advance before delivery. Its maximum encoded width is
        // finite.
        let mut manifest = SourceIntervalManifest {
            origin: source.origin.clone(),
            membership: u64::MAX.into(),
            range,
            links: std::mem::take(&mut source.events),
            coverage: vec![PositionCoverage::Selected {
                position: source.position,
                ordinals,
            }],
            digest: [0; 32].into(),
        };
        let selected = codec.interval_content_length(&manifest, self.declaration_bytes.get());
        source.events = std::mem::take(&mut manifest.links);
        // Every digest byte can occupy three decimal digits in the maintained
        // JSON grammar.
        manifest.coverage[0] = PositionCoverage::Excluded {
            position: source.position,
            sealed_receipt: [u8::MAX; 32].into(),
            reason: if source.events.is_empty() {
                ExclusionReason::NoEvent
            } else {
                ExclusionReason::UnselectedTables
            },
        };
        let excluded = codec.interval_content_length(&manifest, self.declaration_bytes.get());
        selected
            .and(excluded)
            .map(|_| ())
            .map_err(|error| match error {
                ReplicationProtocolError::PayloadTooLarge { .. }
                | ReplicationProtocolError::CapacityRejected => {
                    InfrastructureResultKind::NativeCapacityRejected.into()
                }
                _ => corrupt_record(),
            })
    }
}

impl JournalStore {
    pub(super) fn require_source_declaration(
        &self,
        source: &mut SourcePosition,
    ) -> StorageResult<()> {
        self.source_byte_allowances
            .get()
            .ok_or_else(|| {
                storage_types::StorageError::internal("source publication allowance is not bound")
            })?
            .require_declaration(source)
    }
}
