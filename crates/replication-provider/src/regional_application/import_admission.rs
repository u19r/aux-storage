use std::sync::Arc;

use replication_types::{
    configuration::membership::RegionalTableBinding, error::ReplicationProtocolError,
    event::interval::SourceIntervalManifest, regional_import::RegionalImport,
};

use super::error::RegionalApplicationResult;
use crate::runtime_configuration::limits::WorkLimits;

/// Immutable originals qualified before recovery admission or native
/// observations.
#[derive(Clone)]
pub struct BoundedImport {
    import: Arc<RegionalImport>,
}

impl BoundedImport {
    pub fn try_new(
        import: Arc<RegionalImport>,
        limits: &WorkLimits,
    ) -> RegionalApplicationResult<Self> {
        Self::require_declaration(import.manifest(), import.bindings(), limits)?;
        if import.events().len() > limits.import_events.get() {
            return Err(ReplicationProtocolError::InvalidRange {
                context: "import event bound",
            }
            .into());
        }
        let size = import
            .events()
            .iter()
            .try_fold(0usize, |size, event| {
                size.checked_add(event.as_ref().bytes.as_ref().len())
            })
            .ok_or(ReplicationProtocolError::CounterOverflow {
                context: "import byte count",
            })?;
        if size > limits.import_bytes.get() {
            return Err(ReplicationProtocolError::PayloadTooLarge {
                context: "import bytes",
                actual: size,
                limit: limits.import_bytes.get(),
            }
            .into());
        }
        Ok(Self { import })
    }

    /// Full source authority remains intact when selecting a smaller native
    /// fragment.
    pub fn require_declaration(
        manifest: &SourceIntervalManifest,
        bindings: &[RegionalTableBinding],
        limits: &WorkLimits,
    ) -> Result<(), ReplicationProtocolError> {
        if manifest.links.len() > limits.frame_events.get()
            || manifest.coverage.len() > limits.frame_events.get()
            || bindings.len() > limits.import_events.get()
        {
            return Err(ReplicationProtocolError::InvalidRange {
                context: "import declaration bound",
            });
        }
        Ok(())
    }
}

impl AsRef<RegionalImport> for BoundedImport {
    fn as_ref(&self) -> &RegionalImport {
        &self.import
    }
}
