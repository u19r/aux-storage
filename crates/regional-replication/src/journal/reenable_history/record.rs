//! Native source inventory; these records do not authorize a target activation.
use infrastructure_provider::record::DurableRecordKey;
use replication_types::{
    configuration::{
        installed::{InstalledTableState, RegionalSourceBinding},
        proposal::ControlProposal,
    },
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetainedLineage {
    pub disable: ControlProposal,
    pub survivor: InstalledTableState,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum InventoryPhase {
    Lineages {
        after: Option<DurableRecordKey>,
    },
    Baselines {
        lineage: RetainedLineage,
        initial: Box<crate::bootstrap::SourceCaptureDefinition>,
        after: Option<DurableRecordKey>,
        vector: CanonicalDigest,
        count: u64,
        next: DurableRecordKey,
    },
    Survivor {
        lineage: RetainedLineage,
        partition: u32,
        next: DurableRecordKey,
    },
    Catalog {
        lineage: RetainedLineage,
        after: Option<RegionalSourceBinding>,
        next: DurableRecordKey,
    },
    Retired {
        lineage: RetainedLineage,
        source: RegionalSourceBinding,
        partition: u32,
        next: DurableRecordKey,
    },
    Inherited {
        custody: crate::bootstrap::ReenableHistoryCustody,
        vector: CanonicalDigest,
    },
    Ready,
    Released,
    Cancelled,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InventoryRecord {
    pub proposal: ControlProposal,
    pub phase: InventoryPhase,
    pub vector: CanonicalDigest,
    pub cuts: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetainedCut {
    pub previous: CanonicalDigest,
    pub operation: replication_types::identity::resource::ControlOperationId,
    pub history: crate::bootstrap::ReenableHistoryCut,
}

#[derive(Clone, Copy)]
pub(in crate::journal) enum InventorySettlement {
    Cancel,
    Release,
}
