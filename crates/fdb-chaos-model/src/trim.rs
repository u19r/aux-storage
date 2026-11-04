use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    anomaly::{Anomaly, AnomalyKind},
    constants::ARTIFACT_SCHEMA_VERSION,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TrimScopeReport {
    pub schema_version: u32,
    pub client_id: i32,
    pub table_scopes: Vec<String>,
}

impl TrimScopeReport {
    pub fn new(client_id: i32, table_scopes: Vec<String>) -> Self {
        Self {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            client_id,
            table_scopes,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TrimProviderSnapshot {
    pub schema_version: u32,
    pub client_id: i32,
    pub table_scopes: Vec<String>,
}

impl TrimProviderSnapshot {
    pub fn new(client_id: i32, table_scopes: Vec<String>) -> Self {
        Self {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            client_id,
            table_scopes,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AggregateTrimCheckReport {
    pub schema_version: u32,
    pub checked_client_count: usize,
    pub expected_table_scope_count: usize,
    pub actual_table_scope_count: usize,
    pub anomaly_count: usize,
    pub anomalies: Vec<Anomaly>,
}

pub fn check_aggregate_trim_scopes(
    reports: &[TrimScopeReport],
    snapshot: &TrimProviderSnapshot,
) -> AggregateTrimCheckReport {
    let expected = reports
        .iter()
        .flat_map(|report| report.table_scopes.iter())
        .collect::<BTreeSet<_>>();
    let actual = snapshot.table_scopes.iter().collect::<BTreeSet<_>>();
    let anomalies = if expected == actual {
        Vec::new()
    } else {
        vec![Anomaly {
            kind: AnomalyKind::AuditValueMismatch,
            client_id: snapshot.client_id,
            key: "stream-trim/table-scopes".to_string(),
            expected: Some(format!("{expected:?}")),
            actual: Some(format!("{actual:?}")),
            detail: "aggregate table retention scopes differ from the captured immutable table IDs"
                .to_string(),
        }]
    };
    AggregateTrimCheckReport {
        schema_version: ARTIFACT_SCHEMA_VERSION,
        checked_client_count: reports.len(),
        expected_table_scope_count: expected.len(),
        actual_table_scope_count: actual.len(),
        anomaly_count: anomalies.len(),
        anomalies,
    }
}
