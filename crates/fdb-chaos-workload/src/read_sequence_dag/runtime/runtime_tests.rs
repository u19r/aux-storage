use super::{NormalizedResult, validate_mapped_result};

#[test]
fn deliberate_mapped_result_mutation_is_rejected() {
    let expected = NormalizedResult::new();
    let mut mutated = expected.clone();
    mutated.insert("child".to_string(), vec![vec![Default::default()]]);

    assert!(validate_mapped_result("mapped", &expected, &mutated).is_err());
}
