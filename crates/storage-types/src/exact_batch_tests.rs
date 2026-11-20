use super::exact_batch::{Aligned, Exact};

#[test]
fn given_matching_count_when_try_new_then_zip_pairs_in_order() {
    let exact = Exact::try_new(2, vec!['a', 'b']).expect("count matches");
    let pairs: Vec<_> = exact.zip([1, 2]).collect();
    assert_eq!(pairs, vec![(1, 'a'), (2, 'b')]);
}

#[test]
fn given_wrong_count_when_try_new_then_error() {
    assert!(Exact::try_new(3, vec![1, 2]).is_err());
}

#[test]
fn given_one_value_when_into_single_then_some() {
    assert_eq!(Exact::try_new(1, vec![7]).unwrap().into_single(), Some(7));
    assert_eq!(Exact::try_new(2, vec![7, 8]).unwrap().into_single(), None);
}

#[test]
fn given_array_width_when_into_array_then_checked() {
    assert_eq!(
        Exact::try_new(2, vec![1, 2])
            .unwrap()
            .into_array::<2>()
            .unwrap(),
        [1, 2]
    );
    assert!(
        Exact::try_new(2, vec![1, 2])
            .unwrap()
            .into_array::<3>()
            .is_err()
    );
}

#[test]
fn given_aligned_when_get_then_finds_by_key() {
    let aligned = Aligned::try_new(vec!["x", "y"], vec![1, 2]).expect("aligned");
    assert_eq!(aligned.get(&"y"), Some(&2));
    assert_eq!(aligned.get(&"z"), None);
    assert!(Aligned::try_new(vec!["x"], vec![1, 2]).is_err());
}
