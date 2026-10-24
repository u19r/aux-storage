use serde::Serialize;
use serde_json::Value;

use crate::{
    canonical_json,
    constants::{AUDIT_SCHEMA_VERSION, POSTGRES_PROFILE_ENVIRONMENT},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExpectedObserved<T> {
    pub expected: T,
    pub observed: T,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditRow {
    pub key: Value,
    pub value: Value,
    pub revision: Value,
    pub result: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DigestEvidence {
    pub count: usize,
    pub key_digest: String,
    pub value_digest: String,
    pub revision_digest: String,
    pub result_digest: String,
    pub row_digest: String,
}

pub fn expected_observed<T>(expected: T, observed: T) -> ExpectedObserved<T> {
    ExpectedObserved { expected, observed }
}

pub fn canonical_value<T: Serialize + ?Sized>(value: &T) -> Value {
    serde_json::to_value(value).expect("storage profile audit value must serialize")
}

pub fn canonical_digest<T: Serialize + ?Sized>(value: &T) -> String {
    canonical_json::digest_value(&canonical_value(value))
}

pub fn emit_audit_record(event: &str, payload: Value) {
    let Value::Object(mut fields) = payload else {
        panic!("storage profile audit record payload must be a JSON object");
    };
    fields.insert("event".to_string(), Value::String(event.to_string()));
    fields.insert(
        "schema_version".to_string(),
        Value::from(AUDIT_SCHEMA_VERSION),
    );
    println!("{}", canonical_json::serialize(&Value::Object(fields)));
}

/// Build a canonical non-acceptance record for a profile blocked by missing
/// external test environment. The digest covers the emitted event and schema
/// framing as well as the blocked-boundary fields, while intentionally omitting
/// timing and coverage claims.
pub fn blocked_environment_payload(event: &str, expected: Value, observed: Value) -> Value {
    let mut payload = serde_json::json!({
        "provider": "postgres",
        "status": "BLOCKED_ENVIRONMENT",
        "acceptance_evidence": false,
        "required_environment": POSTGRES_PROFILE_ENVIRONMENT,
        "reason": "TEST_POSTGRES_DSN or CUCUMBER_POSTGRES_DSN is not set",
        "expected": expected,
        "observed": observed,
    });
    let Value::Object(fields) = &mut payload else {
        unreachable!("blocked profile payload is an object");
    };
    let mut digest_input = fields.clone();
    digest_input.insert("event".to_string(), Value::String(event.to_string()));
    digest_input.insert(
        "schema_version".to_string(),
        Value::from(AUDIT_SCHEMA_VERSION),
    );
    fields.insert(
        "digest".to_string(),
        Value::String(canonical_digest(&Value::Object(digest_input))),
    );
    payload
}

pub fn digest_rows(rows: &[AuditRow]) -> DigestEvidence {
    let mut rows = rows.to_vec();
    rows.sort_by_key(canonical_row_json);
    let keys: Vec<&Value> = rows.iter().map(|row| &row.key).collect();
    let values: Vec<&Value> = rows.iter().map(|row| &row.value).collect();
    let revisions: Vec<&Value> = rows.iter().map(|row| &row.revision).collect();
    let results: Vec<&Value> = rows.iter().map(|row| &row.result).collect();
    DigestEvidence {
        count: rows.len(),
        key_digest: canonical_digest(&keys),
        value_digest: canonical_digest(&values),
        revision_digest: canonical_digest(&revisions),
        result_digest: canonical_digest(&results),
        row_digest: canonical_digest(&rows),
    }
}

fn canonical_row_json(row: &AuditRow) -> String {
    canonical_json::serialize(&canonical_value(row))
}
