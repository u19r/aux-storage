use serde::{Deserialize, Serialize};

use crate::constants::ARTIFACT_SCHEMA_VERSION;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnomalyKind {
    AuditMissing,
    AuditUnexpected,
    AuditValueMismatch,
    BackgroundLeaseViolation,
    OperationFailed,
    SharedFinalStateUnexplained,
    SharedHistoryNotSerializable,
    UnknownCommit,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Anomaly {
    pub kind: AnomalyKind,
    pub client_id: i32,
    pub key: String,
    pub expected: Option<String>,
    pub actual: Option<String>,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AnomalyReport {
    pub schema_version: u32,
    pub workload: String,
    pub client_id: i32,
    pub anomaly_count: usize,
    pub anomalies: Vec<Anomaly>,
}

impl AnomalyReport {
    #[must_use]
    pub fn new(workload: String, client_id: i32, anomalies: Vec<Anomaly>) -> Self {
        Self {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            workload,
            client_id,
            anomaly_count: anomalies.len(),
            anomalies,
        }
    }
}
