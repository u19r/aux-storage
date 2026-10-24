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

#[test]
fn given_capacity_when_replacement_requested_then_only_growth_is_permitted() {
    let current = MaxIndexers::try_new(2).unwrap();
    assert_eq!(current.requested_increase(None).unwrap(), None);
    assert_eq!(current.requested_increase(Some(current)).unwrap(), None);
    let higher = MaxIndexers::try_new(3).unwrap();
    assert_eq!(
        current.requested_increase(Some(higher)).unwrap(),
        Some(higher)
    );
    assert!(
        current
            .requested_increase(Some(MaxIndexers::ZERO))
            .unwrap_err()
            .to_string()
            .contains("cannot_decrease")
    );
}
