use storage_types::StorageError;

use crate::{SqlStatement, SqlValue, catalog::SqlDynamoCount, statement::counted_write};

fn failure() -> StorageError {
    StorageError::internal("count assertion")
}

/// Regional pair-end deletes are unbounded.
#[test]
fn given_unbounded_delete_count_when_counted_write_rendered_then_accepted_without_capacity_error() {
    let delete = SqlStatement::new(
        "DELETE FROM regional_markers WHERE table_id = $1",
        vec![SqlValue::Blob(vec![1])],
    );
    let counted = counted_write(delete, SqlDynamoCount::Unbounded, failure);
    assert!(
        counted.is_ok(),
        "an unbounded count must render, not fail with a capacity error"
    );
}

#[test]
fn given_bounded_delete_counts_when_counted_write_rendered_then_accepted() {
    for count in [
        SqlDynamoCount::ExactlyOne,
        SqlDynamoCount::ExactlyRows(3),
        SqlDynamoCount::AtMostRows(0),
        SqlDynamoCount::AtMostRows(i64::MAX as usize),
    ] {
        let delete = SqlStatement::new("DELETE FROM t WHERE k = $1", vec![SqlValue::Integer(1)]);
        assert!(counted_write(delete, count, failure).is_ok());
    }
}
