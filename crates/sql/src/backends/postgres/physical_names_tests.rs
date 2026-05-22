use storage_types::{IndexName, TableName};

use super::physical_names::{physical_gsi_table_name, physical_table_name};

#[test]
fn short_names_remain_readable() {
    assert_eq!(
        physical_gsi_table_name(&TableName::new("orders"), &IndexName::new("by_status")),
        "gsi_orders_by_status"
    );
}

#[test]
fn long_gsi_names_do_not_collide_after_postgres_truncation() {
    let table = TableName::new(
        "very_long_customer_facing_table_name_that_would_otherwise_force_identifier_truncation",
    );
    let first = physical_gsi_table_name(&table, &IndexName::new("index_suffix_a"));
    let second = physical_gsi_table_name(&table, &IndexName::new("index_suffix_b"));

    assert_ne!(first, second);
    assert!(first.len() <= 63);
    assert!(second.len() <= 63);
}

#[test]
fn long_table_names_fit_postgres_identifier_limit() {
    let table = TableName::new(
        "very_long_customer_facing_table_name_that_would_otherwise_force_identifier_truncation",
    );
    assert!(physical_table_name(&table).len() <= 63);
}
