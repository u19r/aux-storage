use infrastructure_provider::record::{DurableRecord, RecordChange};
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::event::content::CanonicalEvent;
use storage_types::{Revision, StorageResult, TableHistoryId};

use crate::{
    canonical_original::CanonicalOriginalRef, constants::EVENT_SUFFIX, journal::JournalStore,
    native_record::corrupt_record,
};

#[derive(Default)]
pub(in crate::journal) struct PreparedOriginalBatch {
    changes: Vec<RecordChange>,
}

impl PreparedOriginalBatch {
    pub(in crate::journal) fn include(
        &mut self,
        journal: &JournalStore,
        event: CanonicalOriginalRef<'_>,
        history: TableHistoryId,
    ) -> StorageResult<()> {
        let key = journal.table_identity_key(EVENT_SUFFIX, history, event.identity())?;
        self.changes
            .try_reserve(1)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        self.changes.push(
            journal
                .records
                .prepare_original(key, event.bytes(), history)?,
        );
        Ok(())
    }

    pub(in crate::journal) fn into_changes(self) -> Vec<RecordChange> {
        self.changes
    }
}

impl JournalStore {
    pub(in crate::journal) fn decode_retained_event(
        &self,
        row: &DurableRecord,
        bytes: &[u8],
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<CanonicalEvent> {
        let event = codec.decode_event(bytes).map_err(|_| corrupt_record())?;
        if row.revision != Revision::INITIAL
            || row.key
                != self.table_identity_key(
                    EVENT_SUFFIX,
                    self.decode_original_payload(row, codec)?.history,
                    &event.as_ref().id,
                )?
        {
            return Err(corrupt_record());
        }
        Ok(event)
    }

    pub(in crate::journal) fn prepare_original_batch(
        &self,
        input: &[(&CanonicalEvent, TableHistoryId)],
    ) -> StorageResult<Vec<RecordChange>> {
        let mut originals = PreparedOriginalBatch::default();
        for (event, history) in input {
            originals.include(self, (*event).into(), *history)?;
        }
        Ok(originals.into_changes())
    }
}
