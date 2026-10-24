use super::bounded_vec::BoundedVec;

#[test]
fn given_count_within_bounds_when_construct_then_ok() {
    let batch = BoundedVec::<u8, 1, 10>::try_new(vec![1, 2, 3]).expect("within bounds");
    assert_eq!(batch.len(), 3);
    assert_eq!(batch.as_slice(), &[1, 2, 3]);
}

#[test]
fn given_empty_vec_when_min_is_one_then_error() {
    BoundedVec::<u8, 1, 10>::try_new(Vec::new()).expect_err("empty rejects");
}

#[test]
fn given_over_max_when_construct_then_error() {
    BoundedVec::<u8, 1, 2>::try_new(vec![1, 2, 3]).expect_err("over max rejects");
}

#[test]
fn given_exact_bounds_when_construct_then_ok() {
    BoundedVec::<u8, 1, 2>::try_new(vec![1]).expect("min edge");
    BoundedVec::<u8, 1, 2>::try_new(vec![1, 2]).expect("max edge");
}
