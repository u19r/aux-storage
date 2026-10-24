use std::sync::Arc;

use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};

use super::{error::ImportNack, message::WireIdentity};
use crate::history_capture::{
    HistoryCaptureRequest, HistoryCaptureStatus, HistoryCaptureVectorPage,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HistoryCaptureMessage {
    pub from: WireIdentity,
    pub to: WireIdentity,
    pub request: Arc<HistoryCaptureRequest>,
    pub action: HistoryCaptureAction,
}

#[derive(Serialize, Deserialize)]
pub(super) enum HistoryCaptureAction {
    Advance,
    Observe,
    Fetch(u32),
    Definition,
    BaselineDefinition,
    Baselines(crate::history_capture::RetainedBaselineScope),
    Catalog(crate::history_capture::HistoryCatalogScope),
    Retired {
        source: replication_types::configuration::installed::RegionalSourceBinding,
        after: u32,
    },
    Cancel,
    Release,
    DeclareCollection(Box<crate::history_collection::HistoryAdoptionDefinition>),
    ObserveCollection,
    AcceptCollection {
        revision: storage_types::Revision,
        change: Box<crate::history_collection::HistoryCollectionChange>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HistoryCaptureReply {
    pub from: WireIdentity,
    pub to: WireIdentity,
    pub content: CanonicalDigest,
    pub result: Result<HistoryCaptureOutcome, ImportNack>,
}

#[derive(Serialize, Deserialize)]
pub(super) enum HistoryCaptureOutcome {
    Status(Option<HistoryCaptureStatus>),
    Vector(HistoryCaptureVectorPage),
    BaselineDefinition(crate::history_capture::RetainedBaselineDefinition),
    Definition(crate::history_capture::HistoryCaptureDefinition),
    Retired(crate::history_capture::RetiredHistoryPage),
    Catalog(crate::history_capture::RetiredHistoryCatalogPage),
    Baselines(Box<crate::history_capture::RetainedBaselinePage>),
    Collection(Option<Box<crate::history_collection::HistoryCollectionObservation>>),
    CollectionAccepted,
}
