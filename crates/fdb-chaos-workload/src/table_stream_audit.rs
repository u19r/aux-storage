//! Physical pointer reciprocity checks on a quiescent simulator database.

use kv_backend::{
    AttemptMode, KvAttempt, KvBackend, KvRange, ReadProtection,
    stream_write::key_layout::{
        item_key, item_row_prefix, pointer_owner_key, pointer_reference_key,
    },
};
use storage_types::{StorageError, StorageResult, StreamItemId, StreamName};
use stream_provider::{PointerOwner, StoredStreamPointer, StreamPointerReference};

use crate::common::storage_error_detail;

pub(crate) async fn audit_stream_pointers(
    driver: kv_backend::foundationdb::FoundationDbBackend,
    table_stream: StreamName,
) -> Result<(u64, u64), String> {
    driver
        .with_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move { audit_pointer_rows(attempt, &table_stream).await })
        })
        .await
        .map_err(|error| storage_error_detail(&error))
}

async fn audit_pointer_rows(
    attempt: &impl KvAttempt,
    table: &StreamName,
) -> StorageResult<(u64, u64)> {
    let system = StreamName::system_table_stream();
    let mut owner_prefix = pointer_owner_key(table, StreamItemId::default())?;
    owner_prefix.truncate(owner_prefix.len() - StreamItemId::default().as_bytes().len());
    let ranges = vec![
        bounded_range(item_row_prefix(table)?)?,
        bounded_range(owner_prefix)?,
        bounded_range(item_row_prefix(&system)?)?,
    ];
    let pages = attempt.scan_batch(ranges, ReadProtection::Snapshot).await?;
    let [rows, owners, system_rows]: [_; 3] = pages
        .try_into()
        .map_err(|_| invalid("pointer audit returned another page count"))?;
    if rows.more || owners.more || system_rows.more {
        return Err(invalid("pointer audit exceeded complete-scan bound"));
    }
    let mut checks = Vec::new();
    let mut decoupled = 0;
    for row in &rows.entries {
        let Some(is_decoupled) = append_pointer_checks(&mut checks, row, table)? else {
            return Err(invalid("table pointer targets another table history"));
        };
        decoupled += u64::from(is_decoupled);
    }
    for row in &system_rows.entries {
        append_pointer_checks(&mut checks, row, table)?;
    }
    for owner in owners.entries {
        checks.push((item_key(table, decode_row_id(&owner.key)?)?, None));
        checks.push((owner.value, Some(Vec::new())));
    }
    verify_checks(attempt, checks).await?;
    Ok((rows.entries.len() as u64, decoupled))
}

fn append_pointer_checks(
    checks: &mut Vec<(Vec<u8>, Option<Vec<u8>>)>,
    row: &kv_backend::KvEntry,
    table: &StreamName,
) -> StorageResult<Option<bool>> {
    let id = decode_row_id(&row.key)?;
    let item = stream_provider::decode_stream_item_row(id, &row.value)?;
    let pointer: StoredStreamPointer = storage_types::storage_serde::from_bytes(&item.data)?;
    let (StoredStreamPointer::Pointer { table_name, .. }
    | StoredStreamPointer::Embedded { table_name, .. }) = &pointer;
    let pointer_table = storage_types::ItemHistoryAddress {
        history_id: pointer.history_address().history_id,
        stream_name: StreamName::table_stream(table_name),
    }
    .physical_stream_name();
    if &pointer_table != table {
        return Ok(None);
    }
    let system = StreamName::system_table_stream();
    if item.data_type != stream_provider::StreamDataType::StreamPointer
        || ![Some(table), Some(&system)].contains(&item.stream_name.as_ref())
    {
        return Err(invalid(
            "stream pointer envelope has another identity or data type",
        ));
    }
    let item_id = StreamItemId::from(pointer.target_item_stream_version());
    checks.push((
        item_key(&pointer.history_address().physical_stream_name(), item_id)?,
        None,
    ));
    for (owner, stream) in [
        (PointerOwner::Table, table),
        (PointerOwner::System, &system),
    ] {
        checks.push((item_key(stream, id)?, Some(row.value.clone())));
        append_reference_checks(
            checks,
            StreamPointerReference {
                owner,
                owner_stream: stream.clone(),
                owner_item_id: id,
                item_stream: pointer.history_address().physical_stream_name(),
                item_item_id: matches!(pointer, StoredStreamPointer::Pointer { .. })
                    .then_some(item_id),
            },
        )?;
    }
    Ok(Some(id != item_id))
}

fn append_reference_checks(
    checks: &mut Vec<(Vec<u8>, Option<Vec<u8>>)>,
    reference: StreamPointerReference,
) -> StorageResult<()> {
    let reverse = pointer_reference_key(&reference)?;
    checks.push((
        pointer_owner_key(&reference.owner_stream, reference.owner_item_id)?,
        Some(reverse.clone()),
    ));
    checks.push((reverse, Some(Vec::new())));
    Ok(())
}

async fn verify_checks(
    attempt: &impl KvAttempt,
    checks: Vec<(Vec<u8>, Option<Vec<u8>>)>,
) -> StorageResult<()> {
    let keys = checks.iter().map(|(key, _)| key.clone()).collect();
    let values = attempt.get_batch(keys, ReadProtection::Snapshot).await?;
    if values.len() != checks.len() {
        return Err(invalid("pointer audit returned another value count"));
    }
    for ((_, expected), value) in checks.into_iter().zip(values) {
        let Some(value) = value else {
            return Err(invalid("stream pointer has a missing row or index"));
        };
        if expected.is_some_and(|expected| value != expected) {
            return Err(invalid("stream pointer row or reciprocal index differs"));
        }
    }
    Ok(())
}

fn decode_row_id(key: &[u8]) -> StorageResult<StreamItemId> {
    let length = StreamItemId::default().as_bytes().len();
    let suffix = key
        .get(key.len().saturating_sub(length)..)
        .ok_or_else(|| invalid("stream row key is truncated"))?;
    StreamItemId::try_from(suffix).map_err(|_| invalid("stream row ID is invalid"))
}

fn bounded_range(begin: Vec<u8>) -> StorageResult<KvRange> {
    KvRange::try_for_prefix(
        begin,
        std::num::NonZeroU32::new(10_001).expect("nonzero audit bound"),
        std::num::NonZeroUsize::new(64 * 1024 * 1024).expect("nonzero audit bytes"),
    )
}

fn invalid(message: &str) -> StorageError {
    StorageError::internal(message)
}
