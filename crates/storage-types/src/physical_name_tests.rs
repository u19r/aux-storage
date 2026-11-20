use crate::AttributeName;

#[test]
fn given_raw_attribute_when_wrapped_then_dashes_and_spaces_become_underscores() {
    let name = AttributeName::new("order-id line");

    assert_eq!(name.sanitized(), "order_id_line");
}
