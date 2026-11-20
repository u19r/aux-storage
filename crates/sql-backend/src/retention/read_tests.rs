use super::read::decode_oldest_assigned_transaction_age;
use crate::{SqlQueryPage, SqlRow, SqlValue};

#[test]
fn given_native_age_rows_when_decoding_then_preserve_age_and_reject_incomplete_results() {
    let page = |values, more| {
        let mut page = SqlQueryPage::new();
        page.rows.push(SqlRow::new(values));
        page.more = more;
        page
    };
    assert_eq!(
        decode_oldest_assigned_transaction_age(page(vec![SqlValue::Integer(30001)], false))
            .unwrap(),
        30001
    );
    assert_eq!(
        decode_oldest_assigned_transaction_age(page(vec![SqlValue::Integer(-1)], false)).unwrap(),
        0
    );
    assert!(
        decode_oldest_assigned_transaction_age(page(vec![SqlValue::Integer(1)], true)).is_err()
    );
    assert!(decode_oldest_assigned_transaction_age(page(vec![SqlValue::Null], false)).is_err());
    assert!(decode_oldest_assigned_transaction_age(page(vec![], false)).is_err());
    assert!(decode_oldest_assigned_transaction_age(SqlQueryPage::new()).is_err());
}
