use std::num::NonZeroUsize;

use bytes::Bytes;
use scheduled_bg_jobs::store::{RangeEntry, RangePage, RangeRequest, StoreKey};
use storage_provider::{RawOrderedJobStoreError, job_store::finish_range_page};

use super::{SqlRawOrderedJobStore, sql_statements};
use crate::{ReadProtection, SqlAttempt, SqlBackend, SqlValue};

pub(super) async fn read<B: SqlBackend + 'static>(
    store: &SqlRawOrderedJobStore<B>,
    request: RangeRequest,
) -> Result<RangePage, RawOrderedJobStoreError> {
    let statement = sql_statements::range(store.engine, &request)?;
    let budget = request
        .byte_limit()
        .get()
        .checked_add(store.capabilities.max_value_bytes().get())
        .and_then(|bytes| bytes.checked_add(store.capabilities.max_key_bytes().get()))
        .ok_or_else(|| {
            RawOrderedJobStoreError::Backend(
                storage_types::InfrastructureResultKind::NativeCapacityRejected.into(),
            )
        })?;
    let max_bytes = NonZeroUsize::new(budget).ok_or_else(|| {
        RawOrderedJobStoreError::Backend(storage_types::StorageError::internal(
            "SQL scheduled job range budget is zero",
        ))
    })?;
    let page = store
        .read(move |attempt| {
            Box::pin(async move {
                attempt
                    .query(statement, ReadProtection::Snapshot, max_bytes)
                    .await
            })
        })
        .await?;
    let rows = page
        .rows
        .into_iter()
        .map(|row| decode_entry(row, store.capabilities.max_value_bytes().get()));
    finish_range_page(&request, rows, page.more)
}

fn decode_entry(
    row: crate::SqlRow,
    max_value_bytes: usize,
) -> Result<RangeEntry, RawOrderedJobStoreError> {
    let mut values = row.into_values().into_iter();
    let (Some(SqlValue::Blob(key)), Some(SqlValue::Blob(value)), None) =
        (values.next(), values.next(), values.next())
    else {
        return Err(RawOrderedJobStoreError::Corrupt("scheduled job range row"));
    };
    let key = StoreKey::try_from(key.as_slice())
        .map_err(|_| RawOrderedJobStoreError::Corrupt("scheduled job range key"))?;
    if value.len() > max_value_bytes {
        return Err(RawOrderedJobStoreError::Corrupt(
            "scheduled job range value bytes",
        ));
    }
    Ok(RangeEntry {
        key,
        value: Bytes::from(value),
    })
}
