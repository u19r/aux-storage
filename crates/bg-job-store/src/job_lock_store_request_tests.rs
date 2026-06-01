use std::collections::HashMap;

use bg_jobs::{BackgroundJobName, DatabaseJobKind, ImmediateJobKind};
use storage_types::{AttributeValue, WireItem};

use crate::{
    SysJobLockStore,
    constants::{
        JOB_LOCK_ATTR_JOB_ID, JOB_LOCK_ATTR_LEASE_UNTIL_MS, JOB_LOCK_ATTR_LEASED_BY,
        JOB_LOCK_KEY_PK, JOB_LOCK_KEY_SK, JOB_LOCK_PK_PREFIX, JOB_LOCK_SK, SYS_JOBS_TABLE,
    },
    job_lock_store::decode_lease_until_ms,
};

#[test]
fn given_job_id_when_building_lock_key_then_uses_system_job_partition_and_lock_sort_key() {
    let key = SysJobLockStore::key_map(BackgroundJobName::Immediate {
        kind: ImmediateJobKind::Task,
    });

    assert_eq!(
        key.get(JOB_LOCK_KEY_PK),
        Some(&AttributeValue::S(format!("{JOB_LOCK_PK_PREFIX}task")))
    );
    assert_eq!(
        key.get(JOB_LOCK_KEY_SK),
        Some(&AttributeValue::S(JOB_LOCK_SK.to_string()))
    );
}

#[test]
fn given_expired_or_missing_lock_when_building_acquire_then_writes_worker_lease_and_job_id() {
    let request = SysJobLockStore::acquire_update_request(
        BackgroundJobName::Database {
            kind: DatabaseJobKind::TtlSweep,
        },
        "worker-1",
        2000,
        1000,
    );

    assert_eq!(request.table_name.as_ref(), SYS_JOBS_TABLE);
    assert_eq!(
        request.condition_expression.as_deref(),
        Some("(attribute_not_exists(lease_until_ms) OR lease_until_ms < :now)")
    );
    let update_expression = request
        .update_expression
        .as_deref()
        .expect("update expression");
    assert!(update_expression.contains(JOB_LOCK_ATTR_LEASED_BY));
    assert!(update_expression.contains(JOB_LOCK_ATTR_LEASE_UNTIL_MS));
    assert!(update_expression.contains(JOB_LOCK_ATTR_JOB_ID));

    let values = request
        .expression_attribute_values
        .as_ref()
        .expect("expression values");
    assert_eq!(
        values.get(":worker"),
        Some(&AttributeValue::S("worker-1".to_string()))
    );
    assert_eq!(
        values.get(":lease"),
        Some(&AttributeValue::N("2000".to_string()))
    );
    assert_eq!(
        values.get(":now"),
        Some(&AttributeValue::N("1000".to_string()))
    );
    assert_eq!(
        values.get(":job_id"),
        Some(&AttributeValue::S("ttl-sweep".to_string()))
    );
}

#[test]
fn given_current_owner_when_building_renew_then_requires_same_worker_and_live_lease() {
    let request = SysJobLockStore::renew_update_request(
        BackgroundJobName::Database {
            kind: DatabaseJobKind::StreamTrim,
        },
        "worker-2",
        3000,
        2500,
    );

    assert_eq!(
        request.condition_expression.as_deref(),
        Some("leased_by = :worker AND lease_until_ms >= :now")
    );

    let values = request
        .expression_attribute_values
        .as_ref()
        .expect("expression values");
    assert_eq!(
        values.get(":worker"),
        Some(&AttributeValue::S("worker-2".to_string()))
    );
    assert_eq!(
        values.get(":lease"),
        Some(&AttributeValue::N("3000".to_string()))
    );
    assert_eq!(
        values.get(":now"),
        Some(&AttributeValue::N("2500".to_string()))
    );
    assert!(!values.contains_key(":job_id"));
}

#[test]
fn given_lock_item_when_decoding_lease_then_returns_optional_expiry() {
    let item = WireItem::from_attribute_map(&HashMap::from([(
        JOB_LOCK_ATTR_LEASE_UNTIL_MS.to_string(),
        AttributeValue::N("12345".to_string()),
    )]))
    .expect("wire item");
    let missing = WireItem::from_attribute_map(&HashMap::new()).expect("wire item");

    assert_eq!(decode_lease_until_ms(&item).expect("lease"), Some(12345));
    assert_eq!(decode_lease_until_ms(&missing).expect("missing"), None);
}

#[test]
fn given_invalid_lock_lease_when_decoding_then_returns_storage_error() {
    let item = WireItem::from_attribute_map(&HashMap::from([(
        JOB_LOCK_ATTR_LEASE_UNTIL_MS.to_string(),
        AttributeValue::S("not-a-number".to_string()),
    )]))
    .expect("wire item");

    assert!(decode_lease_until_ms(&item).is_err());
}
