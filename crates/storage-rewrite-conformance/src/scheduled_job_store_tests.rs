//! The initialized Dynamo owner also supplies the durable generic job store.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    panic::{AssertUnwindSafe, resume_unwind},
};

use futures::FutureExt as _;
use scheduled_bg_jobs::store::{
    CommitOperationId, CommitOutcome, Mutation, StoreKey, StoreTransaction, StoreTransactionInput,
    TransactionLimits,
};

use super::test_support::TestStore;

#[tokio::test]
async fn given_initialized_owner_when_job_store_conformance_runs_then_ordered_transactions_hold() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        store.scheduled_job_store.bootstrap().await.unwrap();
        let report = scheduled_bg_jobs_conformance::run_basic(
            &store.scheduled_job_store,
            &scheduled_bg_jobs_conformance::ConformanceConfig::isolated(),
        )
        .await
        .unwrap_or_else(|error| panic!("job store conformance: {error:?}"));
        assert_eq!(report.assertions, 22);
        assert_eq!(report.reads, 3);
        assert_eq!(report.get_many, 4);
        assert_eq!(report.ranges, 4);
        assert_eq!(report.commits, 9);
        assert_eq!(report.conflicts, 4);
        assert_eq!(report.unknowns, 0);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[tokio::test]
async fn given_initialized_owner_when_job_commit_outcome_is_unknown_then_reconciliation_holds() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        store.scheduled_job_store.bootstrap().await.unwrap();
        let decorated =
            scheduled_bg_jobs_conformance::FaultStore::new(store.scheduled_job_store.clone());
        let report = scheduled_bg_jobs_conformance::run_faults(
            &decorated,
            &scheduled_bg_jobs_conformance::ConformanceConfig::isolated(),
        )
        .await
        .unwrap_or_else(|error| panic!("job fault conformance: {error:?}"));
        assert_eq!(report.unknowns, 2);
        assert_eq!(report.conflicts, 1);
        assert_eq!(decorated.pending_faults().unwrap(), 0);
        let rerun = scheduled_bg_jobs_conformance::run_basic(
            &store.scheduled_job_store,
            &scheduled_bg_jobs_conformance::ConformanceConfig::isolated(),
        )
        .await
        .unwrap_or_else(|error| panic!("job conformance after faults: {error:?}"));
        assert_eq!(rerun.assertions, 22);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[tokio::test]
async fn given_initialized_owner_when_job_profile_runs_then_work_is_bounded() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        store.scheduled_job_store.bootstrap().await.unwrap();
        let counted =
            scheduled_bg_jobs_conformance::CountingStore::new(store.scheduled_job_store.clone());
        let profile = scheduled_bg_jobs_conformance::run_operation_profile(
            &counted,
            &scheduled_bg_jobs_conformance::ConformanceConfig::isolated(),
        )
        .await
        .unwrap_or_else(|error| panic!("job operation profile: {error:?}"));
        assert_eq!(profile.report.assertions, 22);
        assert_eq!(profile.counts.commits, 12);
        assert_eq!(profile.counts.get_many, 6);
        assert_eq!(profile.counts.ranges, 4);
        assert_eq!(profile.counts.gets, 3);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[tokio::test]
async fn given_initialized_owner_when_isolated_job_runs_overlap_then_both_commit() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        store.scheduled_job_store.bootstrap().await.unwrap();
        let first_config = scheduled_bg_jobs_conformance::ConformanceConfig::isolated();
        let second_config = scheduled_bg_jobs_conformance::ConformanceConfig::isolated();
        let (first, second) = tokio::join!(
            scheduled_bg_jobs_conformance::run_basic(&store.scheduled_job_store, &first_config),
            scheduled_bg_jobs_conformance::run_basic(&store.scheduled_job_store, &second_config),
        );
        assert_eq!(first.unwrap().assertions, 22);
        assert_eq!(second.unwrap().assertions, 22);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[tokio::test]
async fn given_initialized_owner_when_operation_id_is_reused_then_effect_is_applied_once() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        store.scheduled_job_store.bootstrap().await.unwrap();
        let operation_id = CommitOperationId::from([9; 16]);
        let key = StoreKey::try_from(b"p-jobs-same-op/k1".to_vec()).unwrap();
        let first = job_transaction(operation_id, key.clone(), b"v-a");
        assert_eq!(
            store.scheduled_job_store.commit(&first).await.unwrap(),
            CommitOutcome::Committed
        );
        assert_eq!(
            store.scheduled_job_store.commit(&first).await.unwrap(),
            CommitOutcome::Committed
        );
        assert_eq!(
            store
                .scheduled_job_store
                .get(key.clone())
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"v-a"
        );
        let conflicting = job_transaction(operation_id, key.clone(), b"v-b");
        assert!(!matches!(
            store.scheduled_job_store.commit(&conflicting).await,
            Ok(CommitOutcome::Committed)
        ));
        assert_eq!(
            store
                .scheduled_job_store
                .get(key)
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"v-a"
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

fn job_transaction(
    operation_id: CommitOperationId,
    key: StoreKey,
    value: &[u8],
) -> StoreTransaction {
    StoreTransaction::try_from(StoreTransactionInput {
        operation_id,
        checks: Vec::new(),
        mutations: vec![Mutation::Put(key, value.to_vec().into())],
        limits: TransactionLimits {
            max_checks: NonZeroU32::new(100).unwrap(),
            max_mutations: NonZeroU32::new(100).unwrap(),
            max_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        },
    })
    .unwrap()
}
