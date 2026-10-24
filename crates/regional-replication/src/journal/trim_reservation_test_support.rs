//! Damage actual native custody inside an attempt that must roll back in full.
use std::sync::Arc;

use infrastructure_provider::{
    batch::RecordBatch,
    physical_attempt::PhysicalRecordAttempt,
    record::{
        ConditionalWriteResult, DurableRecord, DurableRecordKey, RecordChange, RecordExpectation,
    },
};
use storage_provider::{DynamoPort, native_admission::NativeWork};
use storage_types::{AttemptMode, InfrastructureResultKind, ReadProtection, StorageEnum};

use super::{JournalStore, NativeAttempt};
use crate::{
    constants, test_support as fixture,
    trim::{
        executor::{TrimRequest, execute_trim},
        proposal::PublicTrimProposal,
    },
};

pub(crate) async fn verify_counted_custody<P: DynamoPort>(
    store: &P,
    journal: &Arc<JournalStore>,
    proposal: &Arc<PublicTrimProposal>,
) where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let request = Arc::new(TrimRequest::Observe(Arc::clone(proposal)));
    let before = execute_trim(
        store,
        Arc::clone(journal),
        Arc::clone(&request),
        (fixture::limits(), fixture::codec()),
    )
    .await
    .unwrap();
    let keys = custody_keys(journal, proposal);
    let native = Arc::clone(journal);
    let captured = Arc::clone(journal)
        .with_admitted_attempt(
            (store, NativeWork::Recovery),
            NativeAttempt::Items(AttemptMode::ReadOnly),
            move |attempt, _| {
                Box::pin(async move {
                    native
                        .records
                        .load(attempt, &keys, ReadProtection::Snapshot)
                        .await
                })
            },
        )
        .await
        .unwrap();
    let mut faults = captured
        .iter()
        .flatten()
        .map(|record| {
            vec![RecordChange::Delete {
                key: record.key.clone(),
                expected: record.revision,
            }]
        })
        .collect::<Vec<_>>();
    let barrier = captured[0].as_ref().unwrap();
    for count in [0, 2] {
        let mut value: serde_json::Value = journal.records.decode(barrier).unwrap();
        value["reservations"] = count.into();
        value["reservation_revision"] = count.into();
        faults.push(vec![
            journal
                .records
                .prepare(barrier.key.clone(), Some(barrier.revision), &value)
                .unwrap(),
        ]);
    }
    let index = captured[1].as_ref().unwrap();
    let mut extra = index.key.as_ref().clone();
    extra.suffix.push(0);
    faults.push(vec![RecordChange::Store {
        expected: RecordExpectation::Absent,
        record: DurableRecord {
            key: extra.try_into().unwrap(),
            revision: 0.into(),
            bytes: Arc::clone(&index.bytes),
        },
    }]);
    for record in captured
        .iter()
        .skip(1)
        .flatten()
        .filter(|record| record.revision == 0.into())
    {
        let mut value: serde_json::Value = journal.records.decode(record).unwrap();
        value["unexpected_authority"] = true.into();
        faults.push(vec![
            RecordChange::Delete {
                key: record.key.clone(),
                expected: record.revision,
            },
            journal
                .records
                .prepare(record.key.clone(), None, &value)
                .unwrap(),
        ]);
    }
    for (index, changes) in faults.into_iter().enumerate() {
        let request = Arc::clone(&request);
        let native = Arc::clone(journal);
        let rejected = Arc::clone(journal)
            .with_admitted_attempt(
                (store, NativeWork::Recovery),
                NativeAttempt::Items(AttemptMode::ReadWrite),
                move |attempt, _| {
                    Box::pin(async move {
                        for change in &changes {
                            let batch = RecordBatch::try_new(
                                std::slice::from_ref(change),
                                fixture::physical_limits(),
                            )?;
                            assert_eq!(
                                attempt.store_records(&batch).await?,
                                ConditionalWriteResult::Applied
                            );
                        }
                        native
                            .execute_trim_operation(
                                attempt,
                                (&request, None),
                                (&fixture::limits(), &fixture::codec()),
                            )
                            .await
                    })
                },
            )
            .await
            .err()
            .unwrap_or_else(|| panic!("native custody fault {index} accepted a trim observation"));
        assert!(
            matches!(
                rejected.as_ref(),
                StorageEnum::InfrastructureResult(InfrastructureResultKind::Encoding { .. })
            ),
            "native custody fault {index} returned {rejected:?}"
        );
        let intact = execute_trim(
            store,
            Arc::clone(journal),
            Arc::new(TrimRequest::Observe(Arc::clone(proposal))),
            (fixture::limits(), fixture::codec()),
        )
        .await
        .unwrap();
        assert_eq!(
            serde_json::to_vec(&intact).unwrap(),
            serde_json::to_vec(&before).unwrap(),
            "native custody fault {index} changed settled trim authority"
        );
    }
}

fn custody_keys(journal: &JournalStore, proposal: &PublicTrimProposal) -> Vec<DurableRecordKey> {
    let local = &proposal.participant(&journal.region).unwrap().state;
    let identity = &(proposal.candidate.lineage, proposal.operation);
    let mut keys = vec![
        journal.journal_epoch_key().unwrap(),
        journal
            .identity_key(constants::TRIM_RESERVATION_INDEX_SUFFIX, identity)
            .unwrap(),
        journal
            .identity_key(constants::TRIM_RESERVATION_SUFFIX, identity)
            .unwrap(),
    ];
    for prefix in [
        constants::PUBLIC_TRIM_OPERATION_SUFFIX,
        constants::PUBLIC_TRIM_PROPOSAL_SUFFIX,
        constants::CONTROL_PENDING_SUFFIX,
        constants::PUBLIC_TRIM_SOURCE_SCAN_SUFFIX,
        constants::PUBLIC_TRIM_PREPARATION_SUFFIX,
        constants::PUBLIC_TRIM_COMMIT_SUFFIX,
        constants::PUBLIC_TRIM_COMPLETION_SUFFIX,
    ] {
        keys.push(journal.identity_key(prefix, &proposal.operation).unwrap());
    }
    keys.extend([
        journal
            .identity_key(constants::CONTROL_SLOT_SUFFIX, &local.configuration.table)
            .unwrap(),
        journal.public_trim_grant_turn_key(proposal).unwrap(),
        journal
            .public_trim_head_key(local.physical, proposal.candidate.lineage)
            .unwrap(),
        journal.public_trim_installation_key(proposal).unwrap(),
    ]);
    keys
}
