//! Complete canonical originals use the standard bounded record envelope.

use std::sync::Arc;

use infrastructure_provider::record::{DurableRecord, DurableRecordKey, RecordChange};
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use storage_types::{Revision, StorageResult, TableHistoryId};

use super::JournalStore;
use crate::native_record::{NativeRecords, corrupt_record};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OriginalPayload<'a> {
    pub history: TableHistoryId,
    #[serde(borrow)]
    original: &'a RawValue,
}

impl OriginalPayload<'_> {
    /// A raw one-element array retains whitespace outside the canonical JSON
    /// value.
    pub(super) fn original_bytes(&self) -> StorageResult<&[u8]> {
        self.original
            .get()
            .as_bytes()
            .strip_prefix(b"[")
            .and_then(|bytes| bytes.strip_suffix(b"]"))
            .filter(|bytes| !bytes.is_empty())
            .ok_or_else(corrupt_record)
    }
}

impl NativeRecords {
    pub(crate) fn prepare_original(
        &self,
        key: DurableRecordKey,
        bytes: &[u8],
        history: TableHistoryId,
    ) -> StorageResult<RecordChange> {
        let mut framed = Vec::new();
        framed
            .try_reserve_exact(bytes.len().checked_add(2).ok_or_else(corrupt_record)?)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        framed.push(b'[');
        framed.extend_from_slice(bytes);
        framed.push(b']');
        let original = serde_json::from_slice(&framed).map_err(|_| corrupt_record())?;
        self.prepare(key, None, &OriginalPayload { history, original })
    }

    pub(crate) fn original_payload_bound(
        &self,
        length: usize,
    ) -> StorageResult<std::num::NonZeroUsize> {
        let original = serde_json::from_str("[0]").map_err(|_| corrupt_record())?;
        let overhead = serde_json::to_vec(&OriginalPayload {
            history: TableHistoryId::from([u8::MAX; 16]),
            original,
        })
        .map_err(|_| corrupt_record())?
        .len()
        .checked_sub(1)
        .ok_or_else(corrupt_record)?;
        let length = length.checked_add(overhead).ok_or_else(corrupt_record)?;
        std::num::NonZeroUsize::new(length).ok_or_else(corrupt_record)
    }
}

impl JournalStore {
    pub(super) fn decode_original_payload<'row>(
        &self,
        row: &'row DurableRecord,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<OriginalPayload<'row>> {
        if row.revision != Revision::INITIAL {
            return Err(corrupt_record());
        }
        let payload: OriginalPayload<'_> = self.records.decode(row)?;
        codec
            .require_event_length(payload.original_bytes()?.len())
            .map_err(|_| corrupt_record())?;
        Ok(payload)
    }

    pub(in crate::journal) fn decode_original_bytes(
        &self,
        row: &DurableRecord,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Arc<[u8]>> {
        Ok(Arc::from(
            self.decode_original_payload(row, codec)?.original_bytes()?,
        ))
    }
}
