use std::sync::Arc;

use replication_types::{
    configuration::installed::InstalledTableState, identity::resource::ControlOperationId,
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};

use super::{error::ImportNack, message::WireIdentity};
use crate::bootstrap::{
    BaselineCopyPage, CommittedBaselinePage, HiddenTargetRequest, SourceCaptureDefinition,
    TargetBootstrapDefinition,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BootstrapMessage {
    pub from: WireIdentity,
    pub to: WireIdentity,
    pub action: BootstrapAction,
}

#[derive(Serialize, Deserialize)]
pub(super) enum BootstrapAction {
    RetainedBaselines {
        target: Arc<HiddenTargetRequest>,
        ordinal: u64,
        page: Arc<crate::bootstrap::ReenableBaselinePage>,
    },
    BeginRetained {
        target: Arc<HiddenTargetRequest>,
        definition: Arc<crate::bootstrap::ReenableHistoryDefinition>,
    },
    ObserveRetained {
        target: Arc<HiddenTargetRequest>,
        definition: Arc<crate::bootstrap::ReenableHistoryDefinition>,
    },
    AcceptRetained {
        target: Arc<HiddenTargetRequest>,
        definition: Arc<crate::bootstrap::ReenableHistoryDefinition>,
        page: Arc<crate::bootstrap::ReenableHistoryPage>,
    },
    BeginAdoption {
        target: Arc<HiddenTargetRequest>,
        definition: Arc<crate::history_collection::HistoryAdoptionDefinition>,
    },
    ObserveAdoption {
        target: Arc<HiddenTargetRequest>,
        definition: Arc<crate::history_collection::HistoryAdoptionDefinition>,
    },
    AcceptAdoption {
        target: Arc<HiddenTargetRequest>,
        definition: Arc<crate::history_collection::HistoryAdoptionDefinition>,
        revision: storage_types::Revision,
        change: Box<crate::history_collection::HistoryCollectionChange>,
    },
    AdvanceVerification {
        target: Arc<HiddenTargetRequest>,
        definition: Arc<TargetBootstrapDefinition>,
    },
    DeclareCut {
        target: Arc<HiddenTargetRequest>,
        definition: Arc<SourceCaptureDefinition>,
        cut: Arc<crate::bootstrap::SourceCaptureCut>,
    },
    Reserve(Arc<HiddenTargetRequest>),
    Observe(Arc<HiddenTargetRequest>),
    Cancel(Arc<HiddenTargetRequest>),
    ObserveCancellation(Arc<HiddenTargetRequest>),
    Copy {
        target: Arc<HiddenTargetRequest>,
        definition: Arc<TargetBootstrapDefinition>,
        page: Arc<BaselineCopyPage>,
    },
    ObserveCopy {
        target: Arc<HiddenTargetRequest>,
        definition: Arc<TargetBootstrapDefinition>,
        page: Arc<BaselineCopyPage>,
    },
}

impl BootstrapAction {
    pub fn request(&self) -> &HiddenTargetRequest {
        match self {
            Self::Reserve(request)
            | Self::Observe(request)
            | Self::Cancel(request)
            | Self::ObserveCancellation(request) => request,
            Self::RetainedBaselines { target, .. }
            | Self::BeginRetained { target, .. }
            | Self::ObserveRetained { target, .. }
            | Self::AcceptRetained { target, .. }
            | Self::BeginAdoption { target, .. }
            | Self::ObserveAdoption { target, .. }
            | Self::AcceptAdoption { target, .. }
            | Self::Copy { target, .. }
            | Self::ObserveCopy { target, .. }
            | Self::DeclareCut { target, .. }
            | Self::AdvanceVerification { target, .. } => target,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BootstrapReply {
    pub from: WireIdentity,
    pub to: WireIdentity,
    pub operation: ControlOperationId,
    pub content: CanonicalDigest,
    pub result: Result<BootstrapOutcome, ImportNack>,
}

#[derive(Serialize, Deserialize)]
pub(super) enum BootstrapOutcome {
    RetainedBaselines(crate::bootstrap::ReenableBaselineReceipt),
    Retained(Option<crate::bootstrap::TargetRetainedHistoryObservation>),
    AdoptionStored,
    Adoption(Option<Box<crate::history_collection::HistoryCollectionObservation>>),
    Verification(crate::bootstrap::HiddenVerificationStatus),
    Cut(crate::bootstrap::SourceCaptureCut),
    Reservation(InstalledTableState),
    Missing,
    Cancellation(bool),
    Baseline(CommittedBaselinePage),
}
