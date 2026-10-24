use crate::{AttributeName, GsiPhysicalName};

#[test]
fn given_parts_when_gsi_name_is_composed_then_format_matches_backends() {
    let name = GsiPhysicalName::compose("orders", "by-status");

    assert_eq!(name.to_string(), "gsi_orders_by-status");
}

#[test]
fn given_raw_attribute_when_wrapped_then_dashes_and_spaces_become_underscores() {
    let name = AttributeName::new("order-id line");

    assert_eq!(name.sanitized(), "order_id_line");
}
