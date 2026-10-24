use infrastructure_provider::record::DurableRecordKey;
use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_types::KeyAttributes;

/// Public progress is descriptive. No variant grants catalog activation.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HiddenVerificationStatus {
    CheckingCuts,
    Reconstructing,
    VerifyingPrimary,
    VerifyingProjections,
    Verified,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerificationProgress {
    pub vector: CanonicalDigest,
    pub history: CanonicalDigest,
    pub winners: CanonicalDigest,
    pub stage: VerificationStage,
    pub projection_counts: Vec<u64>,
    pub ttl_count: u64,
    pub cut_count: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum VerificationStage {
    Cuts(u32),
    RetainedCuts(u64),
    RetainedBaselines {
        ordinal: u64,
        after: Option<DurableRecordKey>,
        count: u64,
        vector: CanonicalDigest,
    },
    AdoptedCuts(Option<DurableRecordKey>),
    Baselines(Option<DurableRecordKey>),
    Events(Option<DurableRecordKey>),
    Expected(Option<DurableRecordKey>),
    Live(Option<String>),
    Deleted(Option<KeyAttributes>),
    ProjectionExpected(Option<DurableRecordKey>),
    ProjectionRows {
        index: u32,
        after: Option<String>,
        seen: u64,
    },
    TtlRows {
        after: Option<storage_common::TtlMaintenanceCursor>,
        seen: u64,
    },
    ProjectionMetadataBaselines(Option<DurableRecordKey>),
    ProjectionMetadataEvents(Option<DurableRecordKey>),
    ProjectionTombstones {
        index: u32,
        after: Option<storage_provider::ProjectionMetadataCursor>,
    },
    MaintenanceAdmission,
    Verified(super::evidence::BootstrapVerification),
}

impl VerificationProgress {
    pub(crate) fn history_frozen(&self) -> bool {
        !matches!(
            self.stage,
            VerificationStage::Cuts(_)
                | VerificationStage::AdoptedCuts(_)
                | VerificationStage::RetainedCuts(_)
                | VerificationStage::RetainedBaselines { .. }
        )
    }
}

impl VerificationStage {
    pub(crate) fn status(&self) -> HiddenVerificationStatus {
        match self {
            Self::Cuts(_)
            | Self::AdoptedCuts(_)
            | Self::RetainedCuts(_)
            | Self::RetainedBaselines { .. } => HiddenVerificationStatus::CheckingCuts,
            Self::Baselines(_) | Self::Events(_) => HiddenVerificationStatus::Reconstructing,
            Self::Expected(_) | Self::Live(_) | Self::Deleted(_) => {
                HiddenVerificationStatus::VerifyingPrimary
            }
            Self::ProjectionExpected(_) | Self::ProjectionRows { .. } | Self::TtlRows { .. } => {
                HiddenVerificationStatus::VerifyingProjections
            }
            Self::ProjectionMetadataBaselines(_)
            | Self::ProjectionMetadataEvents(_)
            | Self::ProjectionTombstones { .. }
            | Self::MaintenanceAdmission => HiddenVerificationStatus::VerifyingProjections,
            Self::Verified(_) => HiddenVerificationStatus::Verified,
        }
    }
}
