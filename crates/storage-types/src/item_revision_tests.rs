use super::ItemRevision;

#[test]
fn given_new_item_when_physical_floor_is_allocated_then_revision_uses_floor() {
    let floor = ItemRevision::try_from(100_u64).expect("physical position");
    assert_eq!(
        ItemRevision::next_at_least(None, floor.get()).expect("initial revision"),
        floor
    );
}

#[test]
fn given_existing_revision_when_physical_floor_is_lower_then_revision_advances() {
    let current = ItemRevision::try_from(101_u64).expect("durable revision");
    let floor = ItemRevision::try_from(100_u64).expect("physical position");
    assert_eq!(
        ItemRevision::next_at_least(Some(current), floor.get())
            .expect("monotonic revision")
            .get(),
        102
    );
}

#[test]
fn given_existing_revision_when_physical_floor_is_ahead_then_revision_uses_floor() {
    let current = ItemRevision::try_from(10_u64).expect("durable revision");
    let floor = ItemRevision::try_from(100_u64).expect("physical position");
    assert_eq!(
        ItemRevision::next_at_least(Some(current), floor.get()).expect("monotonic revision"),
        floor
    );
}

#[test]
fn given_exhausted_revision_when_floor_is_lower_then_write_fails() {
    let current = ItemRevision::try_from(u64::MAX).expect("durable revision");
    let floor = ItemRevision::initial();
    ItemRevision::next_at_least(Some(current), floor.get())
        .expect_err("counter overflow must fail");
}

#[test]
fn given_existing_revision_when_physical_floor_is_zero_then_revision_advances() {
    let current = ItemRevision::try_from(10_u64).expect("durable revision");
    assert_eq!(
        ItemRevision::next_at_least(Some(current), 0)
            .expect("physical position may be zero")
            .get(),
        11
    );
}
