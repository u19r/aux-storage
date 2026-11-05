use super::decode::match_observation_rows;
use crate::{SqlRow, SqlValue};

fn row(ordinal: SqlValue) -> SqlRow {
    SqlRow::new(vec![ordinal, SqlValue::Integer(7)])
}

#[test]
fn given_reordered_observations_when_matched_then_missing_slots_and_request_order_are_preserved() {
    let rows = match_observation_rows(
        3,
        vec![row(SqlValue::Integer(2)), row(SqlValue::Integer(0))],
    )
    .unwrap();
    assert!(rows[0].is_some());
    assert!(rows[1].is_none());
    assert!(rows[2].is_some());
}

#[test]
fn given_invalid_observation_ordinals_when_matched_then_results_are_rejected() {
    for rows in [
        vec![SqlRow::new(vec![])],
        vec![row(SqlValue::Null)],
        vec![row(SqlValue::Integer(-1))],
        vec![row(SqlValue::Integer(1))],
        vec![row(SqlValue::Integer(0)), row(SqlValue::Integer(0))],
    ] {
        assert!(match_observation_rows(1, rows).is_err());
    }
}
