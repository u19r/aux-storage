//! Persist native authority faults, then restore exact bytes after rejection
//! proof.
use std::sync::Arc;

use infrastructure_provider::record::{DurableRecord, RecordChange};
use replication_types::identity::journal_partition::JournalPartition;
use storage_provider::native_admission::NativeWork;
use storage_types::{AttemptMode, ReadProtection};

use super::{JournalStore, NativeAttempt};
use crate::test_support as fixture;

#[derive(Clone, Copy, Debug)]
pub(crate) enum PartitionFault {
    Missing,
    CorruptFloor,
    ClockExhausted,
    PositionExhausted,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum TableFault {
    MissingSchema,
    IncarnationMismatch,
}

pub(crate) async fn damage_table(
    store: &fixture::Store,
    journal: &Arc<JournalStore>,
    table: replication_types::identity::resource::TableIdentity,
    fault: TableFault,
) -> DurableRecord {
    let native = Arc::clone(journal);
    Arc::clone(journal).with_admitted_attempt((store, NativeWork::Recovery), NativeAttempt::Items(AttemptMode::ReadWrite), move |attempt, _| Box::pin(async move {
        let key = native.identity_key(crate::constants::TABLE_STATE_SUFFIX, &table)?;
        let original = native.records.load(attempt, &[key], ReadProtection::Conflict).await?.pop().unwrap().unwrap();
        let mut state: replication_types::configuration::installed::InstalledTableState = native.records.decode(&original)?;
        match fault {
            TableFault::MissingSchema => {
                state.configuration.schema = (u64::from(state.configuration.schema) + 1).into();
                state.schemas = vec![state.configuration.schema];
            }
            TableFault::IncarnationMismatch => { state.physical = storage_types::TableHistoryId::from([99; 16]); }
        }
        state.validate().unwrap();
        let change = native.records.prepare(original.key.clone(), Some(original.revision), &state)?;
        native.records.store(attempt, &[change]).await?;
        Ok(original)
    })).await.unwrap()
}

pub(crate) async fn damage(
    store: &fixture::Store,
    journal: &Arc<JournalStore>,
    partition: JournalPartition,
    fault: PartitionFault,
) -> DurableRecord {
    let native = Arc::clone(journal);
    Arc::clone(journal).with_admitted_attempt((store, NativeWork::Recovery), NativeAttempt::Items(AttemptMode::ReadWrite), move |attempt, _| Box::pin(async move {
        let key = native.partition_key(partition)?;
        let original = native.records.load(attempt, &[key], ReadProtection::Conflict).await?.pop().unwrap().unwrap();
        let change = if matches!(fault, PartitionFault::Missing) {
            RecordChange::Delete { key: original.key.clone(), expected: original.revision }
        } else {
            let mut value: serde_json::Value = native.records.decode(&original)?;
            match fault {
                PartitionFault::CorruptFloor => { value.as_object_mut().unwrap().remove("floor"); }
                PartitionFault::ClockExhausted => { value["floor"] = serde_json::json!({"physical_ms": u64::MAX, "logical": u64::MAX}); }
                PartitionFault::PositionExhausted => { value["position"] = u64::MAX.into(); }
                PartitionFault::Missing => unreachable!(),
            }
            native.records.prepare(original.key.clone(), Some(original.revision), &value)?
        };
        native.records.store(attempt, &[change]).await?;
        Ok(original)
    })).await.unwrap()
}

pub(crate) async fn restore(
    store: &fixture::Store,
    journal: &Arc<JournalStore>,
    original: DurableRecord,
) {
    let native = Arc::clone(journal);
    Arc::clone(journal)
        .with_admitted_attempt(
            (store, NativeWork::Recovery),
            NativeAttempt::Items(AttemptMode::ReadWrite),
            move |attempt, _| {
                Box::pin(async move {
                    let current = native
                        .records
                        .load(
                            attempt,
                            std::slice::from_ref(&original.key),
                            ReadProtection::Conflict,
                        )
                        .await?
                        .pop()
                        .unwrap();
                    let change = native.records.prepare_bytes(
                        original.key,
                        current.map(|row| row.revision),
                        original.bytes,
                    )?;
                    native.records.store(attempt, &[change]).await
                })
            },
        )
        .await
        .unwrap();
}
