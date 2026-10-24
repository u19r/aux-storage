use super::non_zero_vec::NonZeroVec;

#[test]
fn given_items_when_try_new_then_ok() {
    let batch = NonZeroVec::try_new(vec![1, 2, 3]).expect("within bounds");
    assert_eq!(batch.len(), 3);
    assert_eq!(batch.as_slice(), &[1, 2, 3]);
}

#[test]
fn given_empty_when_try_new_then_error() {
    NonZeroVec::<u8>::try_new(Vec::new()).expect_err("empty rejects");
}

#[test]
fn given_single_item_when_try_new_then_min_edge_ok() {
    let batch = NonZeroVec::try_new(vec![1]).expect("min edge");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.into_inner(), vec![1]);
}
