use std::num::NonZeroUsize;

use storage_provider::{TtlIndexRead, maintenance::ExpirationCandidateCursor};
use storage_types::TableHistoryId;

use super::sql_statements::*;
use crate::{SqlEngine, SqlValue};

#[test]
fn given_signed_cutoffs_and_continuation_when_rendering_then_bounds_select_exact_expiry_range() {
    for engine in [SqlEngine::Sqlite, SqlEngine::Turso, SqlEngine::Postgres] {
        for cutoff in [i64::MIN, -1, 0, 1, i64::MAX] {
            let cursor = ExpirationCandidateCursor::try_from((cutoff, "a".to_owned())).unwrap();
            let read = TtlIndexRead {
                table_name: storage_types::TableName::new("table"),
                cutoff_seconds: cutoff,
                cursor: Some(cursor.into()),
                limit: NonZeroUsize::MIN,
            };
            let (sql, binds) = expiration_index_page(engine, &read, TableHistoryId::from([1; 16]))
                .unwrap()
                .into_parts();
            assert!(sql.contains("IS NOT NULL") && sql.contains("\"__aux-ttl-key\" >"));
            assert_eq!(sql.contains("\"__aux-ttl-key\" <"), cutoff != i64::MAX);
            let lower_index = usize::from(cutoff != i64::MAX);
            let SqlValue::Blob(lower) = &binds[lower_index] else {
                panic!("cursor must be a BLOB")
            };
            assert_eq!(
                *lower,
                storage_common::TtlIndexKey::new(cutoff, "a".to_owned().into())
                    .encode_ordered_bytes()
                    .unwrap()
            );
            for seconds in [i64::MIN, -1, 0, 1, i64::MAX] {
                for token in ["a", "aa", "b", "é"] {
                    let key = storage_common::TtlIndexKey::new(seconds, token.to_owned().into())
                        .encode_ordered_bytes()
                        .unwrap();
                    let below_upper = if lower_index == 0 {
                        true
                    } else {
                        let SqlValue::Blob(upper) = &binds[0] else {
                            panic!("cutoff must be a BLOB")
                        };
                        key < *upper
                    };
                    assert_eq!(below_upper, seconds <= cutoff);
                    assert_eq!(key > *lower, (seconds, token) > (cutoff, "a"));
                }
            }
        }
    }
}
