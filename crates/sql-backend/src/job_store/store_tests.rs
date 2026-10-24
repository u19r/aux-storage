use bytes::Bytes;
use scheduled_bg_jobs::store::StoreKey;
use storage_provider::RawOrderedJobStoreError;

use super::store::decode_many;
use crate::{SqlQueryPage, SqlRow, SqlValue};

#[test]
fn given_unrequested_point_row_when_decoded_then_preserve_corrupt_error_category() {
    let error = decode_many::<Bytes>(
        page([(b"other".as_slice(), b"value".as_slice())]),
        &[key(b"first")],
        |value| Ok(value.clone()),
    )
    .expect_err("unexpected point row");
    assert!(matches!(error, RawOrderedJobStoreError::Corrupt(_)));
}

#[test]
fn given_duplicate_point_rows_when_decoded_then_preserve_corrupt_error_category() {
    let keys = [key(b"first"), key(b"second")];
    let error = decode_many::<Bytes>(
        page([(b"first".as_slice(), b"one".as_slice()), (b"first", b"two")]),
        &keys,
        |value| Ok(value.clone()),
    )
    .expect_err("duplicate physical point row");
    assert!(matches!(error, RawOrderedJobStoreError::Corrupt(_)));
}

#[test]
fn given_truncated_point_page_when_decoded_then_reject_missing_native_results() {
    let mut page = page([(b"first".as_slice(), b"one".as_slice())]);
    page.more = true;
    let error = decode_many::<Bytes>(page, &[key(b"first"), key(b"second")], |value| {
        Ok(value.clone())
    })
    .expect_err("truncated native page");
    assert!(matches!(error, RawOrderedJobStoreError::Corrupt(_)));
}

fn key(bytes: &[u8]) -> StoreKey {
    StoreKey::try_from(bytes).expect("valid test key")
}

fn page(rows: impl IntoIterator<Item = (&'static [u8], &'static [u8])>) -> SqlQueryPage {
    let mut page = SqlQueryPage::new();
    page.rows.extend(rows.into_iter().map(|(key, value)| {
        SqlRow::new(vec![
            SqlValue::Blob(key.to_vec()),
            SqlValue::Blob(value.to_vec()),
        ])
    }));
    page
}
