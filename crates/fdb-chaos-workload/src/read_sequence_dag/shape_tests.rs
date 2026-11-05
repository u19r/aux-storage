#[test]
fn given_parent_pages_when_child_ordinals_repeat_then_child_invocations_remain_separate() {
    let mut results = Default::default();
    for _ in 0..2 {
        super::shape::merge_fixture_invocation(&mut results, "child", 0, vec![Default::default()]);
        super::shape::merge_fixture_invocation(&mut results, "query", 0, vec![Default::default()]);
    }
    assert_eq!(results["child"].len(), 2);
    assert!(results["child"].values().all(|items| items.len() == 1));
    assert_eq!(results["query"].len(), 1);
    assert_eq!(results["query"][&0].len(), 2);
}
