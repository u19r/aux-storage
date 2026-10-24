pub(crate) mod definition;
mod executor;
mod job;
mod target;
mod target_executor;

pub(crate) use definition::CaptureSchema;
pub use definition::{CaptureTtlPolicy, SourceCaptureDefinition, SourceSchemaObservation};
pub(crate) use executor::release_source_capture;
pub use executor::{
    advance_source_capture, begin_source_capture, cancel_source_capture, observe_source_schema,
    read_reenable_history_definition, read_reenable_history_page, read_reenable_original_frame,
    read_source_capture_baselines, read_source_capture_cut, read_source_capture_definition,
    reconcile_source_capture,
};
pub use job::SourceCaptureJob;
pub(crate) use target::baseline_page_content;
pub use target::{
    BaselineCopyPage, CommittedBaselinePage, HiddenTargetRequest, HiddenTargetReservation,
};
pub use target_executor::{
    BaselineCopyExecution, cancel_hidden_target, copy_baseline_page, reconcile_baseline_page,
    reconcile_hidden_cancellation, reconcile_hidden_target, reserve_hidden_target,
};

/// Source preparation progress only. BaselineReady is not target verification,
/// a participant grant, or authority to publish global membership.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceCaptureStatus {
    Holding,
    Scanning,
    CapturingHigh,
    RetainingHistory,
    BaselineReady,
    Releasing,
    Released,
    Cancelling,
    Cancelled,
}

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCaptureCut {
    pub origin: replication_types::identity::origin::OriginId,
    pub through: replication_types::identity::origin::OriginPosition,
}

impl SourceCaptureCut {
    pub(crate) fn fold_vector(
        &self,
        previous: replication_types::protocol::CanonicalDigest,
    ) -> storage_types::StorageResult<replication_types::protocol::CanonicalDigest> {
        let serialized_digest =
            replication_types::protocol::CanonicalDigest::of_json(&(previous, self))
                .map_err(|_| crate::native_record::corrupt_record())?;
        Ok(serialized_digest)
    }
}

mod copy_executor;
pub(crate) mod copy_scope;
mod copy_transfer;
pub use copy_scope::BaselineCopyScope;

mod tail_executor;
mod tail_scope;
mod tail_transfer;
pub use tail_scope::BootstrapTailScope;

mod cut_executor;
pub(crate) use cut_executor::declare_bootstrap_cut;

mod transfer_executor;
mod transfer_job;
pub use transfer_job::BootstrapTransferJob;

mod verification;
mod verification_executor;
pub use verification::HiddenVerificationStatus;
pub(crate) use verification::{VerificationProgress, VerificationStage};
pub(crate) use verification_executor::advance_hidden_verification;

mod evidence;
pub(crate) use evidence::BootstrapVerification;

mod target_definition;
pub use target_definition::TargetBootstrapDefinition;

mod original_executor;
mod original_scope;
mod original_transfer;
pub use original_scope::{AdoptedOriginalPage, AdoptedOriginalScope};

mod reenable_definition;
pub use reenable_definition::{
    ReenableHistoryCut, ReenableHistoryDefinition, ReenableHistoryPage, ReenableOriginalRequest,
    TargetRetainedHistoryObservation,
};

pub(crate) mod reenable_executor;

mod reenable_original_executor;
pub(crate) mod retained_transfer;

mod reenable_baseline;
pub(crate) use reenable_baseline::BaselineDependencyProgress;
pub use reenable_baseline::{
    ReenableBaselineDefinition, ReenableBaselinePage, ReenableBaselineReceipt,
};
pub(crate) mod reenable_baseline_executor;

mod reenable_custody;
pub use reenable_custody::ReenableHistoryCustody;

mod retained_inventory;
pub use retained_inventory::RetainedHistoryInventory;

pub(crate) mod retained_transfer_executor;

mod transfer;
pub(crate) use transfer::BootstrapTransfer;
