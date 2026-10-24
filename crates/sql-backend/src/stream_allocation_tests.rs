use super::stream_allocation::{NativeStreamAllocation, mapped_offset};
use crate::{SqlQueryPage, SqlRow, SqlValue};

#[test]
fn given_native_transaction_when_reserving_records_then_ordinals_are_unique_and_bounded() {
    let mut columns = (0..12).map(|_| SqlValue::Null).collect::<Vec<_>>();
    columns.push(SqlValue::Integer(42));
    let mut page = SqlQueryPage::new();
    page.rows.push(SqlRow::new(columns));
    let mut allocation = NativeStreamAllocation::from_history(&page).unwrap();
    for ordinal in 0..65_536 {
        assert_eq!(allocation.reserve().unwrap(), 42 * 65_536 + ordinal);
    }
    assert!(allocation.reserve().is_err());
}

#[test]
fn given_invalid_native_transaction_when_mapping_offset_then_fail_without_wrapping() {
    assert!(mapped_offset(0).is_err());
    assert!(mapped_offset(u64::MAX).is_err());
    assert!(mapped_offset(i64::MAX as u64 / 65_536 + 1).is_err());
}

#[test]
fn given_postgres_writer_when_loading_topology_then_native_id_is_allocated_without_shared_tail_read()
 {
    use super::stream_history::{HistoryRead, history_statement};
    let (writer, _) =
        history_statement(crate::SqlEngine::Postgres, HistoryRead::Writer).into_parts();
    let (reader, _) =
        history_statement(crate::SqlEngine::Postgres, HistoryRead::Reader).into_parts();
    assert!(!writer.contains("sys_dynamo_stream_records"));
    assert!(reader.contains("sys_dynamo_stream_records"));
    assert!(writer.contains("pg_current_xact_id()"));
    assert!(!writer.contains("pg_current_xact_id_if_assigned"));
    assert!(reader.contains("pg_current_xact_id_if_assigned"));
    assert!(!reader.contains("pg_current_xact_id()"));
}
