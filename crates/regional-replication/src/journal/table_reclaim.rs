//! One durable page cursor owns regional and external table erasure.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange},
};
use replication_types::identity::origin::JournalGeneration;
use serde::{Deserialize, Serialize};
use storage_provider::{
    TableReclaimAttempt, TableReclaimPhase, TableReclaimStep, TableReclaimTarget,
};
use storage_types::{ReadProtection, StorageResult, TableHistoryId};

use super::{
    JournalStore, table_metadata_reclaim::TableMetadataFamily,
    table_owned_reclaim::TableOwnedFamily, table_workflow_reclaim::PendingTableFamily,
};
use crate::{constants, native_record::corrupt_record};

#[derive(Serialize, Deserialize)]
enum ExternalTableState {
    Discover {
        minimum: u64,
    },
    Generation {
        generation: JournalGeneration,
        phase: GenerationPhase,
    },
}

#[derive(Serialize, Deserialize)]
enum GenerationPhase {
    TrimReservation,
    Pending {
        family: PendingTableFamily,
        after: Option<DurableRecordKey>,
    },
    Metadata {
        family: TableMetadataFamily,
        after: Option<DurableRecordKey>,
    },
    Owned(TableOwnedFamily),
    Receipts {
        after: Option<DurableRecordKey>,
    },
    Catalog,
}

pub(crate) enum TableReclaimWork {
    Archive(TableHistoryId),
    Settled(TableReclaimStep),
}

impl JournalStore {
    pub(crate) async fn reclaim_external_table_page<
        A: PhysicalRecordAttempt + TableReclaimAttempt,
    >(
        &self,
        attempt: &mut A,
        target: &TableReclaimTarget,
    ) -> StorageResult<TableReclaimWork> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            if target.phase != TableReclaimPhase::ExternalData {
                return Err(corrupt_record());
            }
            let (key, observation) = self
                .load_external_table_cursor(operation.as_mut(), target.history_id)
                .await?;
            let Some((row, state)) = observation else {
                return Ok(TableReclaimWork::Archive(target.history_id));
            };
            let next = self
                .advance_external_table_cursor(operation.as_mut(), target.history_id, state)
                .await?;
            match next {
                Some(next) => {
                    self.store_external_table_cursor(operation.as_mut(), (key, Some(&row)), &next)
                        .await?
                }
                None => {
                    self.finish_external_table_reclaim(operation.as_mut(), target, row)
                        .await?
                }
            }
            Ok(TableReclaimWork::Settled(TableReclaimStep::Advanced))
        }
        .await;
        operation.complete(result)
    }

    pub(crate) async fn acknowledge_table_archive_erasure<
        A: PhysicalRecordAttempt + TableReclaimAttempt,
    >(
        &self,
        attempt: &mut A,
        history: TableHistoryId,
    ) -> StorageResult<()> {
        let Some(target) = attempt.load_reclaim_target().await? else {
            return Ok(());
        };
        if target.history_id != history || target.phase != TableReclaimPhase::ExternalData {
            return Ok(());
        }
        let (key, observation) = self.load_external_table_cursor(attempt, history).await?;
        if observation.is_none() {
            self.store_external_table_cursor(
                attempt,
                (key, None),
                &ExternalTableState::Discover { minimum: 1 },
            )
            .await?;
        }
        Ok(())
    }

    async fn load_external_table_cursor<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        history: TableHistoryId,
    ) -> StorageResult<(
        DurableRecordKey,
        Option<(DurableRecord, ExternalTableState)>,
    )> {
        let key = self.table_identity_key(constants::TABLE_RECLAIM_SUFFIX, history, &())?;
        let row = self
            .records
            .load(
                attempt,
                std::slice::from_ref(&key),
                ReadProtection::Conflict,
            )
            .await?
            .pop()
            .flatten();
        let observation = row
            .map(|row| -> StorageResult<_> {
                let state = self.records.decode(&row)?;
                Ok((row, state))
            })
            .transpose()?;
        Ok((key, observation))
    }

    async fn finish_external_table_reclaim<A: PhysicalRecordAttempt + TableReclaimAttempt>(
        &self,
        attempt: &mut A,
        target: &TableReclaimTarget,
        row: DurableRecord,
    ) -> StorageResult<()> {
        self.records
            .store(
                attempt,
                &[RecordChange::Delete {
                    key: row.key,
                    expected: row.revision,
                }],
            )
            .await?;
        attempt
            .store_reclaim_phase(target, Some(TableReclaimPhase::MetadataCleanup))
            .await
    }

    async fn store_external_table_cursor<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        observed: (DurableRecordKey, Option<&DurableRecord>),
        state: &ExternalTableState,
    ) -> StorageResult<()> {
        let change = self
            .records
            .prepare(observed.0, observed.1.map(|row| row.revision), state)?;
        self.records.store(attempt, &[change]).await
    }

    async fn advance_external_table_cursor<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        history: TableHistoryId,
        state: ExternalTableState,
    ) -> StorageResult<Option<ExternalTableState>> {
        match state {
            ExternalTableState::Discover { minimum } => {
                if minimum == 0 {
                    return Err(corrupt_record());
                }
                let resource = self.record_key(constants::ORIGIN_LAYOUT_SUFFIX.to_vec())?;
                let generation = attempt
                    .find_record_generation(&resource, minimum, ReadProtection::Conflict)
                    .await?;
                Ok(generation.map(|generation| ExternalTableState::Generation {
                    generation: generation.into(),
                    phase: GenerationPhase::TrimReservation,
                }))
            }
            ExternalTableState::Generation { generation, phase } => {
                let owner = self.load_reclaim_generation(attempt, generation).await?;
                let phase = owner
                    .reclaim_table_generation_page(attempt, history, phase)
                    .await?;
                Ok(match phase {
                    Some(phase) => Some(ExternalTableState::Generation { generation, phase }),
                    None => u64::from(generation)
                        .checked_add(1)
                        .map(|minimum| ExternalTableState::Discover { minimum }),
                })
            }
        }
    }

    async fn reclaim_table_generation_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        history: TableHistoryId,
        phase: GenerationPhase,
    ) -> StorageResult<Option<GenerationPhase>> {
        let next = match phase {
            GenerationPhase::TrimReservation => {
                self.reclaim_table_trim_reservation(attempt, history)
                    .await?;
                GenerationPhase::Pending {
                    family: PendingTableFamily::Enable,
                    after: None,
                }
            }
            GenerationPhase::Pending { family, after } => {
                self.reclaim_pending_generation_page(attempt, history, (family, after))
                    .await?
            }
            GenerationPhase::Metadata { family, after } => {
                self.reclaim_metadata_generation_page(attempt, history, (family, after))
                    .await?
            }
            GenerationPhase::Owned(family) => {
                self.reclaim_owned_generation_page(attempt, history, family)
                    .await?
            }
            GenerationPhase::Receipts { after } => {
                self.reclaim_receipt_generation_page(attempt, history, after)
                    .await?
            }
            GenerationPhase::Catalog => {
                self.reclaim_table_catalog(attempt, history).await?;
                return Ok(None);
            }
        };
        Ok(Some(next))
    }

    async fn reclaim_pending_generation_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        history: TableHistoryId,
        cursor: (PendingTableFamily, Option<DurableRecordKey>),
    ) -> StorageResult<GenerationPhase> {
        let (family, after) = cursor;
        let after = self
            .reclaim_pending_table_work(attempt, history, (family, after))
            .await?;
        Ok(match (after, family.next()) {
            (Some(after), _) => GenerationPhase::Pending {
                family,
                after: Some(after),
            },
            (None, Some(family)) => GenerationPhase::Pending {
                family,
                after: None,
            },
            (None, None) => GenerationPhase::Metadata {
                family: TableMetadataFamily::CausalStream,
                after: None,
            },
        })
    }

    async fn reclaim_metadata_generation_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        history: TableHistoryId,
        cursor: (TableMetadataFamily, Option<DurableRecordKey>),
    ) -> StorageResult<GenerationPhase> {
        let (family, after) = cursor;
        let after = self
            .reclaim_table_metadata(attempt, history, (family, after))
            .await?;
        Ok(match (after, family.next()) {
            (Some(after), _) => GenerationPhase::Metadata {
                family,
                after: Some(after),
            },
            (None, Some(family)) => GenerationPhase::Metadata {
                family,
                after: None,
            },
            (None, None) => GenerationPhase::Owned(TableOwnedFamily::SourceHold),
        })
    }

    async fn reclaim_owned_generation_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        history: TableHistoryId,
        family: TableOwnedFamily,
    ) -> StorageResult<GenerationPhase> {
        Ok(
            match (
                self.reclaim_table_owned_family(attempt, history, family)
                    .await?,
                family.next(),
            ) {
                (TableReclaimStep::Advanced, _) => GenerationPhase::Owned(family),
                (TableReclaimStep::Complete, Some(family)) => GenerationPhase::Owned(family),
                (TableReclaimStep::Complete, None) => GenerationPhase::Receipts { after: None },
            },
        )
    }

    async fn reclaim_receipt_generation_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        history: TableHistoryId,
        after: Option<DurableRecordKey>,
    ) -> StorageResult<GenerationPhase> {
        Ok(
            match self
                .reclaim_operation_receipts(attempt, history, after)
                .await?
            {
                Some(after) => GenerationPhase::Receipts { after: Some(after) },
                None => GenerationPhase::Catalog,
            },
        )
    }
}
