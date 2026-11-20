use std::num::NonZeroU16;

use regional_replication::message::LineageRef;
use storage_provider::{LineageCursor, ProgressDirection};
use storage_types::{
    BoundTableInfo, CreateTableRequest, PairId, RegionId, RegionalMarker, RegionalOrder,
    SystemStreamOffset, SystemStreamPosition, TimestampMillis,
};

use crate::{
    SqlEngine, SqlRow, SqlValue,
    regional::{
        decode_marker_row, decode_progress_values, progress_lock_identity, progress_read_statement,
    },
};

fn row(values: [i64; 4]) -> SqlRow {
    SqlRow::new(values.into_iter().map(SqlValue::Integer).collect())
}

#[test]
fn given_marker_row_when_decoded_then_order_and_flag() {
    let marker = decode_marker_row(row([1_024_000, 2, 0, 0])).unwrap();
    assert_eq!(
        marker,
        RegionalMarker {
            order: RegionalOrder::from_parts(
                1_024_000,
                RegionId(NonZeroU16::new(2).unwrap()),
                false
            ),
            received: false,
        }
    );
    let marker = decode_marker_row(row([7, 1, 1, 1])).unwrap();
    assert_eq!(
        marker,
        RegionalMarker {
            order: RegionalOrder::from_parts(7, RegionId(NonZeroU16::new(1).unwrap()), true),
            received: true,
        }
    );
}

#[test]
fn given_invalid_marker_rows_when_decoded_then_error() {
    // Region 0, expiry 2, and three columns.
    assert!(decode_marker_row(row([5, 0, 0, 0])).is_err());
    assert!(decode_marker_row(row([5, 1, 2, 0])).is_err());
    assert!(decode_marker_row(row([5, 1, 0, 2])).is_err());
    assert!(decode_marker_row(row([-1, 1, 0, 0])).is_err());
    let short = SqlRow::new(vec![
        SqlValue::Integer(5),
        SqlValue::Integer(1),
        SqlValue::Integer(0),
    ]);
    assert!(decode_marker_row(short).is_err());
}

fn table() -> BoundTableInfo {
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": "orders",
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]
    }))
    .unwrap();
    let info = request
        .stored_table_info_at(TimestampMillis::from_timestamp(0))
        .unwrap();
    BoundTableInfo {
        history_id: 7_u32.into(),
        info: std::sync::Arc::new(info),
        binding: storage_types::TableBinding::default(),
    }
}

fn lineage(shard: &str, lineage: &str) -> LineageRef {
    let position = SystemStreamPosition::try_new(shard, lineage, "0").unwrap();
    LineageRef {
        shard: position.shard_id().clone(),
        lineage: position.lineage_id().clone(),
    }
}

#[test]
fn given_progress_values_when_decoded_then_cursor() {
    let offset = "00000000000000000007-vs00000001";
    assert_eq!(
        decode_progress_values(SqlValue::Null, SqlValue::Integer(0)).unwrap(),
        LineageCursor {
            through: None,
            sealed: false
        }
    );
    assert_eq!(
        decode_progress_values(SqlValue::Text(offset.to_owned()), SqlValue::Integer(1)).unwrap(),
        LineageCursor {
            through: Some(SystemStreamOffset::try_new(offset).unwrap()),
            sealed: true
        }
    );
    assert!(decode_progress_values(SqlValue::Integer(3), SqlValue::Integer(0)).is_err());
    assert!(decode_progress_values(SqlValue::Null, SqlValue::Integer(2)).is_err());
}

#[test]
fn given_lineages_when_read_statement_built_then_one_values_row_each() {
    let lineages = [
        lineage("s1", "l1"),
        lineage("s1", "l2"),
        lineage("s2", "l3"),
    ];
    let pair = PairId::from_bytes([3; 16]);
    let (sql, binds) = progress_read_statement(
        SqlEngine::Sqlite,
        &table(),
        pair,
        ProgressDirection::Receive,
        &lineages,
    )
    .unwrap()
    .into_parts();
    assert_eq!(binds.len(), 3 + 6);
    assert!(sql.contains("VALUES (0, "), "{sql}");
    assert!(sql.contains("(2, "), "{sql}");
    assert!(!sql.contains("(3, "), "{sql}");
    let (postgres, _) = progress_read_statement(
        SqlEngine::Postgres,
        &table(),
        pair,
        ProgressDirection::Receive,
        &lineages,
    )
    .unwrap()
    .into_parts();
    assert!(postgres.contains("CAST($4 AS TEXT)"), "{postgres}");
}

#[test]
fn given_records_when_lock_identities_then_distinct_per_direction_and_lineage() {
    let table = table();
    let pair = PairId::from_bytes([3; 16]);
    let identities = [
        progress_lock_identity(
            &table,
            pair,
            ProgressDirection::Receive,
            &lineage("s1", "l1"),
        ),
        progress_lock_identity(&table, pair, ProgressDirection::Send, &lineage("s1", "l1")),
        progress_lock_identity(
            &table,
            pair,
            ProgressDirection::Receive,
            &lineage("s1", "l2"),
        ),
        progress_lock_identity(
            &table,
            pair,
            ProgressDirection::Receive,
            &lineage("s1:l", "1"),
        ),
    ];
    let distinct = identities.iter().collect::<std::collections::BTreeSet<_>>();
    assert_eq!(distinct.len(), 4, "{identities:?}");
}
