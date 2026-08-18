use crate::{MAX_INDEXERS_CAPACITY, MaxIndexers};

#[test]
fn given_capacity_at_maximum_when_constructed_then_value_is_preserved() {
    let capacity = MaxIndexers::try_new(MAX_INDEXERS_CAPACITY).expect("valid capacity");

    assert_eq!(capacity.get(), MAX_INDEXERS_CAPACITY);
}

#[test]
fn given_capacity_above_maximum_when_constructed_then_validation_fails() {
    let error = MaxIndexers::try_new(MAX_INDEXERS_CAPACITY + 1).expect_err("invalid capacity");

    assert!(error.to_string().contains("MaxIndexers:too_many"));
}

#[test]
fn given_json_above_maximum_when_deserialized_then_validation_fails() {
    let error = serde_json::from_str::<MaxIndexers>("33").expect_err("invalid capacity");

    assert!(error.to_string().contains("MaxIndexers:too_many"));
}
