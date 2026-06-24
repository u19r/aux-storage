use std::collections::BTreeMap;

use crate::profile_integer_overrides;

#[test]
fn local_soak_profiles_materialize_larger_queue_work() {
    let overrides =
        BTreeMap::from_iter(profile_integer_overrides("local-soak", "queue_visibility"));

    assert_eq!(overrides.get("operationCount"), Some(&3));
}

#[test]
fn nightly_soak_profiles_materialize_table_concurrency_shape() {
    let overrides =
        BTreeMap::from_iter(profile_integer_overrides("nightly-soak", "table_atomicity"));

    assert_eq!(overrides.get("operationCount"), Some(&256));
    assert_eq!(overrides.get("activeClientCount"), Some(&3));
    assert_eq!(overrides.get("sharedKeyCount"), Some(&12));
    assert_eq!(overrides.get("historySampleLimit"), Some(&512));
}

#[test]
fn smoke_profiles_keep_descriptor_shape() {
    assert!(profile_integer_overrides("smoke", "table_atomicity").is_empty());
    assert!(profile_integer_overrides("smoke", "queue_visibility").is_empty());
}
