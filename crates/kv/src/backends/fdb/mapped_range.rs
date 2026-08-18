//! FoundationDB mapped-range execution and owned decoding.
//!
//! The native binding borrows nested key/value slices from the future that
//! owns the result.  This module copies those slices at the read boundary so
//! a graph response cannot retain a native buffer across a retry or task
//! cancellation.  It also treats an incomplete secondary page as a first
//! class result; callers must either carry the page continuation or fall back
//! to the ordinary range path.

use std::time::Instant;

use foundationdb::{RangeOption, Transaction, mapped_key_values::FdbMappedKeyValue};
use storage_provider::{
    ReadSequenceMappedEntry, ReadSequenceMappedKeyValue, ReadSequenceMappedRangePage,
};
use storage_types::{StorageError, StorageResult};

use crate::backends::fdb::metrics::{
    record_fdb_operation, record_fdb_operation_bytes, record_fdb_operation_latency,
};

const MAPPER_BAD_INDEX: i32 = 2030;

pub(super) fn is_mapper_bad_index(error: &foundationdb::FdbError) -> bool {
    error.code() == MAPPER_BAD_INDEX
}

/// Errors from one mapped-range transaction attempt.  Keeping the native
/// FoundationDB error separate from request-validation failures lets the owner
/// rebuild the whole transaction on retryable conflicts without retrying a
/// malformed request.
#[derive(Debug)]
pub(crate) enum MappedRangeAttemptError {
    Fdb(foundationdb::FdbError),
    Storage(StorageError),
}

pub(crate) async fn get_mapped_range_attempt(
    trx: &Transaction,
    range: &RangeOption<'_>,
    mapper: Option<&[u8]>,
    iteration: usize,
    physical_prefix: &[u8],
) -> Result<ReadSequenceMappedRangePage, MappedRangeAttemptError> {
    let started = Instant::now();
    let (page, read_key_bytes, read_bytes) = match mapper {
        Some(mapper) => {
            // The pinned FoundationDB 7.4 server rejects the snapshot=true
            // mapped-range flag.  Mapped ReadSequence attempts are fresh
            // read-only transactions, so snapshot=false cannot expose
            // in-transaction writes; changing this requires a live server/API
            // compatibility proof, not just a binding-doc update.
            let native = trx
                .get_mapped_range(range, mapper, iteration, false)
                .await
                .map_err(MappedRangeAttemptError::Fdb)?;
            let read_key_bytes = native
                .iter()
                .map(|entry| entry.parent_key().len() as u64)
                .sum();
            let read_bytes = native
                .iter()
                .map(|entry| {
                    entry
                        .parent_key()
                        .len()
                        .saturating_add(entry.parent_value().len())
                        .saturating_add(
                            entry
                                .key_values()
                                .iter()
                                .map(|value| value.key().len().saturating_add(value.value().len()))
                                .sum::<usize>(),
                        ) as u64
                })
                .sum();
            (
                decode_native(native, physical_prefix)?,
                read_key_bytes,
                read_bytes,
            )
        }
        None => {
            let native = trx
                .get_range(range, iteration, false)
                .await
                .map_err(MappedRangeAttemptError::Fdb)?;
            let read_key_bytes = native.iter().map(|value| value.key().len() as u64).sum();
            let read_bytes = native
                .iter()
                .map(|value| value.key().len().saturating_add(value.value().len()) as u64)
                .sum();
            (
                decode_primary(native, physical_prefix)?,
                read_key_bytes,
                read_bytes,
            )
        }
    };
    record_fdb_operation("read_context", "range_read", 1);
    record_fdb_operation_bytes("read_context", "read_key", read_key_bytes);
    record_fdb_operation_bytes("read_context", "read", read_bytes);
    record_fdb_operation_latency("read_context", "range_read", started.elapsed());
    Ok(page)
}

fn decode_primary(
    native: foundationdb::future::FdbValues,
    physical_prefix: &[u8],
) -> Result<ReadSequenceMappedRangePage, MappedRangeAttemptError> {
    let mut entries = Vec::with_capacity(native.len());
    for key_value in &native {
        let key = relative_key(key_value.key(), physical_prefix)?.to_vec();
        entries.push(ReadSequenceMappedEntry {
            parent_key: key.clone(),
            parent_value: key_value.value().to_vec(),
            begin: key,
            end: Vec::new(),
            key_values: Vec::new(),
        });
    }
    Ok(ReadSequenceMappedRangePage {
        entries,
        more: native.more(),
    })
}

pub(super) fn validate_request(range: &RangeOption<'_>) -> StorageResult<()> {
    if range.begin.key().is_empty() || range.end.key().is_empty() {
        return Err(StorageError::validation(
            "mapped range requires explicit non-empty range selectors",
        ));
    }
    Ok(())
}

fn decode_native(
    native: foundationdb::future::MappedKeyValues,
    physical_prefix: &[u8],
) -> Result<ReadSequenceMappedRangePage, MappedRangeAttemptError> {
    let mut entries = Vec::with_capacity(native.len());
    for entry in &native {
        let parent_key = relative_key(entry.parent_key(), physical_prefix)?.to_vec();
        let parent_value = entry.parent_value().to_vec();
        let begin = relative_key(entry.begin_range(), physical_prefix)?.to_vec();
        let end = if entry.end_range().is_empty() {
            Vec::new()
        } else {
            relative_key(entry.end_range(), physical_prefix)?.to_vec()
        };
        let key_values = decode_mapped_key_values(entry, physical_prefix)?;
        entries.push(ReadSequenceMappedEntry {
            parent_key,
            parent_value,
            begin,
            end,
            key_values,
        });
    }
    Ok(ReadSequenceMappedRangePage {
        entries,
        more: native.more(),
    })
}

fn decode_mapped_key_values(
    entry: &FdbMappedKeyValue,
    physical_prefix: &[u8],
) -> Result<Vec<ReadSequenceMappedKeyValue>, MappedRangeAttemptError> {
    entry
        .key_values()
        .iter()
        .map(|key_value| {
            Ok(ReadSequenceMappedKeyValue {
                key: relative_key(key_value.key(), physical_prefix)?.to_vec(),
                value: key_value.value().to_vec(),
            })
        })
        .collect()
}

fn relative_key<'a>(
    key: &'a [u8],
    physical_prefix: &[u8],
) -> Result<&'a [u8], MappedRangeAttemptError> {
    key.strip_prefix(physical_prefix).ok_or_else(|| {
        MappedRangeAttemptError::Storage(StorageError::internal(
            "mapped range returned a key outside the configured FoundationDB prefix",
        ))
    })
}
