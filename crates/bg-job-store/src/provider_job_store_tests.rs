use std::{
    collections::BTreeMap,
    num::{NonZeroU32, NonZeroUsize},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use scheduled_bg_jobs::store::{
    Check, CommitOperationId, CommitOutcome, Mutation, RangeDirection, RangeEntry, RangePage,
    RangeRequest, RangeRequestInput, StoreCapabilities, StoreCapabilitiesInput, StoreDataError,
    StoreFailure, StoreKey, StoreLevel, StoreTransaction, StoreTransactionInput, TransactionLimits,
};
use storage_provider::{RawOrderedJobStore, RawOrderedJobStoreError};
use storage_types::StorageError;

use crate::{ScheduledJobStore, ScheduledJobStoreError};

#[derive(Clone)]
struct RecordingRaw {
    capabilities: StoreCapabilities,
    commit_behavior: CommitBehavior,
    state: Arc<Mutex<RecordingState>>,
}

#[derive(Clone)]
enum CommitBehavior {
    Committed,
    Unknown,
    BackendFailure,
    Replay(Arc<Mutex<ReplayState>>),
}

struct RecordingState {
    values: BTreeMap<StoreKey, Bytes>,
    get_calls: usize,
    get_many_calls: usize,
}

struct ReplayState {
    transaction: Option<StoreTransaction>,
    outcome: CommitOutcome,
    applications: usize,
}

impl Default for ReplayState {
    fn default() -> Self {
        Self {
            transaction: None,
            outcome: CommitOutcome::Committed,
            applications: 0,
        }
    }
}

impl RecordingRaw {
    fn new() -> Self {
        Self {
            capabilities: StoreCapabilities::try_from(StoreCapabilitiesInput {
                level: StoreLevel::Local,
                max_key_bytes: nonzero_usize(128),
                max_value_bytes: nonzero_usize(1_024),
                max_get_many_keys: nonzero_u32(16),
                max_get_many_key_bytes: nonzero_usize(512),
                max_transaction_bytes: nonzero_usize(2_048),
                max_transaction_keys: nonzero_u32(16),
                max_range_items: nonzero_u32(16),
                max_range_bytes: nonzero_usize(2_048),
                max_transaction_duration: Duration::from_secs(1),
            })
            .expect("test capabilities"),
            commit_behavior: CommitBehavior::Committed,
            state: Arc::new(Mutex::new(RecordingState {
                values: BTreeMap::new(),
                get_calls: 0,
                get_many_calls: 0,
            })),
        }
    }

    fn replaying() -> Self {
        Self::replaying_with(CommitOutcome::Committed)
    }

    fn replaying_with(outcome: CommitOutcome) -> Self {
        let mut raw = Self::new();
        raw.commit_behavior = CommitBehavior::Replay(Arc::new(Mutex::new(ReplayState {
            outcome,
            ..ReplayState::default()
        })));
        raw
    }

    fn replay_state(&self) -> Arc<Mutex<ReplayState>> {
        match &self.commit_behavior {
            CommitBehavior::Replay(state) => Arc::clone(state),
            _ => panic!("replaying test raw"),
        }
    }

    fn unknown() -> Self {
        let mut raw = Self::new();
        raw.commit_behavior = CommitBehavior::Unknown;
        raw
    }

    fn backend_failure() -> Self {
        let mut raw = Self::new();
        raw.commit_behavior = CommitBehavior::BackendFailure;
        raw
    }

    fn insert(&self, key: &[u8], value: &[u8]) {
        self.state.lock().expect("state lock").values.insert(
            StoreKey::try_from(key).expect("test key"),
            Bytes::copy_from_slice(value),
        );
    }

    fn value(&self, key: &[u8]) -> Option<Bytes> {
        self.state
            .lock()
            .expect("state lock")
            .values
            .get(&StoreKey::try_from(key).expect("test key"))
            .cloned()
    }

    fn apply_mutations(&self, transaction: &StoreTransaction) {
        let mut state = self.state.lock().expect("state lock");
        for mutation in transaction.mutations() {
            match mutation {
                Mutation::Put(key, value) => {
                    state.values.insert(key.clone(), value.clone());
                }
                Mutation::Delete(key) => {
                    state.values.remove(key);
                }
            }
        }
    }
}

#[async_trait]
impl RawOrderedJobStore for RecordingRaw {
    fn bind_native_admission(
        &self,
        _: Arc<dyn storage_provider::native_admission::NativeAdmission>,
    ) -> storage_types::StorageResult<()> {
        // This adapter fixture has no native provider or settlement lifecycle.
        Err(StorageError::internal(
            "adapter-only fixture cannot bind native admission",
        ))
    }

    fn capabilities(&self) -> StoreCapabilities {
        self.capabilities
    }

    async fn bootstrap(&self) -> Result<(), RawOrderedJobStoreError> {
        Ok(())
    }

    async fn get(&self, key: StoreKey) -> Result<Option<Bytes>, RawOrderedJobStoreError> {
        let mut state = self.state.lock().expect("state lock");
        state.get_calls += 1;
        Ok(state.values.get(&key).cloned())
    }

    async fn get_many(
        &self,
        keys: &[StoreKey],
    ) -> Result<Vec<Option<Bytes>>, RawOrderedJobStoreError> {
        let mut state = self.state.lock().expect("state lock");
        state.get_many_calls += 1;
        Ok(keys
            .iter()
            .map(|key| state.values.get(key).cloned())
            .collect())
    }

    async fn read_range(
        &self,
        _request: &RangeRequest,
    ) -> Result<RangePage, RawOrderedJobStoreError> {
        RangePage::new(Vec::new(), None)
            .map_err(|_| RawOrderedJobStoreError::Backend(StorageError::internal("test page")))
    }

    async fn commit(
        &self,
        transaction: &StoreTransaction,
    ) -> Result<CommitOutcome, RawOrderedJobStoreError> {
        match &self.commit_behavior {
            CommitBehavior::Committed => Ok(CommitOutcome::Committed),
            CommitBehavior::Unknown => Ok(CommitOutcome::Unknown),
            CommitBehavior::BackendFailure => Err(RawOrderedJobStoreError::Backend(
                StorageError::internal("secret-key commit backend detail"),
            )),
            CommitBehavior::Replay(state) => {
                let mut state = state.lock().expect("state lock");
                match state.transaction.as_ref() {
                    Some(recorded) if recorded == transaction => Ok(state.outcome),
                    Some(_) => Err(RawOrderedJobStoreError::OperationReuseMismatch),
                    None => {
                        state.transaction = Some(transaction.clone());
                        state.applications += 1;
                        let outcome = state.outcome;
                        drop(state);
                        if outcome == CommitOutcome::Committed {
                            self.apply_mutations(transaction);
                        }
                        Ok(outcome)
                    }
                }
            }
        }
    }
}

fn key(bytes: &[u8]) -> StoreKey {
    StoreKey::try_from(bytes).expect("test key")
}

fn nonzero_usize(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("non-zero test limit")
}

fn nonzero_u32(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).expect("non-zero test limit")
}

#[test]
fn given_raw_backend_when_constructing_provider_job_store_then_it_is_object_safe() {
    let raw = Arc::new(RecordingRaw::new());
    let _: Arc<dyn RawOrderedJobStore> = raw.clone();
    let store = ScheduledJobStore::new(raw.clone());
    assert_eq!(store.capabilities().level(), StoreLevel::Local);
}

#[tokio::test]
async fn given_empty_batch_when_reading_many_then_backend_io_is_skipped() {
    let raw = Arc::new(RecordingRaw::new());
    let state = Arc::clone(&raw.state);
    let store = ScheduledJobStore::new(raw);

    assert_eq!(store.get_many(&[]).await.expect("empty batch"), Vec::new());
    assert_eq!(state.lock().expect("state lock").get_many_calls, 0);
}

#[tokio::test]
async fn given_duplicate_keys_when_reading_many_then_order_and_missing_values_are_preserved() {
    let raw = Arc::new(RecordingRaw::new());
    raw.insert(b"B", b"value");
    let store = ScheduledJobStore::new(raw.clone());

    let values = store
        .get_many(&[key(b"B"), key(b"A"), key(b"B")])
        .await
        .expect("ordered batch");
    assert_eq!(
        values,
        vec![
            Some(Bytes::from_static(b"value")),
            None,
            Some(Bytes::from_static(b"value")),
        ]
    );
    assert_eq!(raw.state.lock().expect("state lock").get_many_calls, 1);
}

#[tokio::test]
async fn given_nonempty_batch_when_reading_many_then_adapter_uses_only_one_native_batch() {
    let raw = Arc::new(RecordingRaw::new());
    raw.insert(b"key", b"value");
    let store = ScheduledJobStore::new(raw.clone());

    assert_eq!(
        store
            .get_many(&[key(b"key")])
            .await
            .expect("single-key batch"),
        vec![Some(Bytes::from_static(b"value"))]
    );
    let state = raw.state.lock().expect("state lock");
    assert_eq!(state.get_many_calls, 1);
    assert_eq!(state.get_calls, 0);
}

#[tokio::test]
async fn given_wrong_native_batch_cardinality_when_reading_many_then_adapter_fails_closed() {
    let store = ScheduledJobStore::new(Arc::new(FailingRaw));

    let error = store
        .get_many(&[key(b"key")])
        .await
        .expect_err("cardinality mismatch");
    assert_eq!(error.to_string(), "corrupt store data");
}

#[tokio::test]
async fn given_backend_failure_when_reading_then_error_text_is_fixed_and_redacted() {
    let store = ScheduledJobStore::new(Arc::new(FailingRaw));

    let error = store
        .get(key(b"secret-key"))
        .await
        .expect_err("backend failure");
    assert!(!error.to_string().contains("secret-key"));
    assert!(!format!("{error:?}").contains("secret-key"));
}

#[test]
fn given_raw_backend_error_when_formatting_then_diagnostics_are_redacted() {
    let error = RawOrderedJobStoreError::Backend(StorageError::internal("secret-key details"));

    assert_eq!(format!("{error:?}"), "Backend(<redacted>)");
    assert_eq!(error.to_string(), "ordered store backend failure");
    assert!(std::error::Error::source(&error).is_none());
}

#[test]
fn given_raw_corruption_when_mapping_then_adapter_preserves_corrupt_category_and_redaction() {
    let error = ScheduledJobStoreError::from(RawOrderedJobStoreError::Corrupt("secret-key"));

    assert_eq!(error.to_string(), "corrupt store data");
    assert!(!format!("{error:?}").contains("secret-key"));
}

#[test]
fn given_each_raw_error_variant_when_mapping_then_adapter_uses_fixed_categories() {
    let cases = [
        (
            RawOrderedJobStoreError::OperationReuseMismatch,
            ScheduledJobStoreError::OperationReuseMismatch,
        ),
        (
            RawOrderedJobStoreError::Timeout,
            ScheduledJobStoreError::Backend(StoreFailure::Timeout),
        ),
        (
            RawOrderedJobStoreError::Corrupt("secret-key"),
            ScheduledJobStoreError::Backend(StoreFailure::Corrupt(StoreDataError::Malformed(
                "secret-key",
            ))),
        ),
        (
            RawOrderedJobStoreError::Backend(StorageError::service_unavailable(1)),
            ScheduledJobStoreError::Backend(StoreFailure::Unavailable),
        ),
        (
            RawOrderedJobStoreError::Backend(StorageError::unsupported("secret-key")),
            ScheduledJobStoreError::Backend(StoreFailure::Unsupported),
        ),
        (
            RawOrderedJobStoreError::Backend(StorageError::internal("secret-key")),
            ScheduledJobStoreError::Backend(StoreFailure::Permanent),
        ),
    ];

    for (raw, expected) in cases {
        assert_eq!(ScheduledJobStoreError::from(raw), expected);
    }
}

#[test]
fn given_mapped_backend_error_when_formatting_then_source_preserves_only_the_redacted_category() {
    let error = ScheduledJobStoreError::from(RawOrderedJobStoreError::Backend(
        StorageError::internal("secret-key details"),
    ));

    let source = std::error::Error::source(&error).expect("redacted category remains observable");
    assert_eq!(
        source.downcast_ref::<StoreFailure>(),
        Some(&StoreFailure::Permanent)
    );
    assert!(source.source().is_none());
    assert_eq!(source.to_string(), "permanent store failure");
    assert_eq!(format!("{source:?}"), "Permanent");
    assert_eq!(error.to_string(), "permanent store failure");
    assert_eq!(format!("{error:?}"), "Backend(Permanent)");
}

#[test]
fn given_operation_reuse_mismatch_when_formatting_then_error_is_fixed_and_redacted() {
    let raw = RawOrderedJobStoreError::OperationReuseMismatch;
    let adapter = ScheduledJobStoreError::from(raw);

    assert_eq!(
        adapter.to_string(),
        "commit operation ID was reused with different content"
    );
    assert_eq!(format!("{adapter:?}"), "OperationReuseMismatch");

    let raw = RawOrderedJobStoreError::OperationReuseMismatch;
    assert_eq!(
        raw.to_string(),
        "commit operation ID was reused with different content"
    );
    assert_eq!(format!("{raw:?}"), "OperationReuseMismatch");
}

#[tokio::test]
async fn given_same_operation_and_transaction_when_retried_then_established_outcome_is_replayed() {
    let raw = Arc::new(RecordingRaw::replaying());
    let state = raw.replay_state();
    let store = ScheduledJobStore::new(raw.clone());
    let transaction = transaction(7, b"replay", b"value");

    assert_eq!(
        store.commit(&transaction).await.expect("first commit"),
        CommitOutcome::Committed
    );
    assert_eq!(
        store.commit(&transaction).await.expect("replayed commit"),
        CommitOutcome::Committed
    );
    let state = state.lock().expect("state lock");
    assert_eq!(state.transaction.as_ref(), Some(&transaction));
    assert_eq!(state.applications, 1);
    drop(state);
    assert_eq!(raw.value(b"replay"), Some(Bytes::from_static(b"value")));
}

#[tokio::test]
async fn given_prior_conflict_when_same_transaction_is_retried_then_conflict_is_replayed() {
    let raw = Arc::new(RecordingRaw::replaying_with(CommitOutcome::Conflict));
    let state = raw.replay_state();
    let store = ScheduledJobStore::new(raw.clone());
    let transaction = transaction(10, b"conflict", b"value");

    assert_eq!(
        store.commit(&transaction).await.expect("first conflict"),
        CommitOutcome::Conflict
    );
    assert_eq!(
        store.commit(&transaction).await.expect("replayed conflict"),
        CommitOutcome::Conflict
    );
    assert_eq!(state.lock().expect("state lock").applications, 1);
    assert_eq!(raw.value(b"conflict"), None);
}

#[tokio::test]
async fn given_reused_operation_with_different_transaction_when_committing_then_adapter_is_redacted_and_no_effect_is_applied()
 {
    let raw = Arc::new(RecordingRaw::replaying());
    let state = raw.replay_state();
    raw.insert(b"second", b"original");
    let store = ScheduledJobStore::new(raw.clone());
    let first = transaction(8, b"first", b"value");
    let mismatch = transaction(8, b"second", b"value");

    store.commit(&first).await.expect("first commit");
    let error = store
        .commit(&mismatch)
        .await
        .expect_err("operation reuse mismatch");

    assert_eq!(
        error.to_string(),
        "commit operation ID was reused with different content"
    );
    assert!(!format!("{error:?}").contains("second"));
    assert_eq!(state.lock().expect("state lock").applications, 1);
    assert_eq!(raw.value(b"first"), Some(Bytes::from_static(b"value")));
    assert_eq!(raw.value(b"second"), Some(Bytes::from_static(b"original")));
}

#[tokio::test]
async fn given_raw_unknown_commit_outcome_when_committing_then_adapter_preserves_unknown() {
    let store = ScheduledJobStore::new(Arc::new(RecordingRaw::unknown()));
    let transaction = transaction(9, b"unknown", b"value");

    assert_eq!(
        scheduled_bg_jobs::store::JobStore::commit(&store, &transaction)
            .await
            .expect("unknown commit"),
        CommitOutcome::Unknown
    );
}

#[tokio::test]
async fn given_known_backend_commit_error_when_committing_then_unknown_is_not_invented() {
    let store = ScheduledJobStore::new(Arc::new(RecordingRaw::backend_failure()));
    let transaction = transaction(11, b"known-error", b"value");

    let error = store
        .commit(&transaction)
        .await
        .expect_err("known backend error");
    assert_eq!(
        error,
        ScheduledJobStoreError::Backend(StoreFailure::Permanent)
    );
    assert_ne!(
        error,
        ScheduledJobStoreError::Backend(StoreFailure::Timeout)
    );
    assert!(!format!("{error:?}").contains("secret-key"));
}

fn transaction(operation_id: u8, key_bytes: &[u8], value: &[u8]) -> StoreTransaction {
    StoreTransaction::try_from(StoreTransactionInput {
        operation_id: CommitOperationId::from([operation_id; 16]),
        checks: vec![Check::Absent(key(key_bytes))],
        mutations: vec![Mutation::Put(key(key_bytes), Bytes::copy_from_slice(value))],
        limits: TransactionLimits {
            max_checks: nonzero_u32(1),
            max_mutations: nonzero_u32(1),
            max_bytes: nonzero_usize(128),
        },
    })
    .expect("test transaction")
}

#[tokio::test]
async fn given_oversized_native_value_when_reading_then_adapter_fails_closed() {
    let raw = Arc::new(RecordingRaw::new());
    raw.insert(b"oversized", &vec![b'v'; 1_025]);
    let store = ScheduledJobStore::new(raw);

    let error = store
        .get(key(b"oversized"))
        .await
        .expect_err("oversized value");
    assert_eq!(error.to_string(), "corrupt store data");
}

#[tokio::test]
async fn given_out_of_bounds_native_range_when_reading_then_adapter_fails_closed() {
    let store = ScheduledJobStore::new(Arc::new(OutOfBoundsRangeRaw));
    let request = RangeRequest::try_from(RangeRequestInput {
        start: Some(key(b"A")),
        end: Some(key(b"C")),
        direction: RangeDirection::Forward,
        item_limit: nonzero_u32(1),
        byte_limit: nonzero_usize(64),
        continuation: None,
    })
    .expect("valid range");

    let error = store
        .read_range(request)
        .await
        .expect_err("out-of-bounds page");
    assert_eq!(error.to_string(), "corrupt store data");
}

struct FailingRaw;

#[async_trait]
impl RawOrderedJobStore for FailingRaw {
    fn bind_native_admission(
        &self,
        _: Arc<dyn storage_provider::native_admission::NativeAdmission>,
    ) -> storage_types::StorageResult<()> {
        // This adapter fixture has no native provider or settlement lifecycle.
        Err(StorageError::internal(
            "adapter-only fixture cannot bind native admission",
        ))
    }

    fn capabilities(&self) -> StoreCapabilities {
        RecordingRaw::new().capabilities
    }

    async fn bootstrap(&self) -> Result<(), RawOrderedJobStoreError> {
        Ok(())
    }

    async fn get(&self, _key: StoreKey) -> Result<Option<Bytes>, RawOrderedJobStoreError> {
        Err(RawOrderedJobStoreError::Backend(StorageError::internal(
            "secret-key backend detail",
        )))
    }

    async fn get_many(
        &self,
        _keys: &[StoreKey],
    ) -> Result<Vec<Option<Bytes>>, RawOrderedJobStoreError> {
        Ok(Vec::new())
    }

    async fn read_range(
        &self,
        _request: &RangeRequest,
    ) -> Result<RangePage, RawOrderedJobStoreError> {
        Ok(RangePage {
            entries: Vec::new(),
            continuation: None,
        })
    }

    async fn commit(
        &self,
        _transaction: &StoreTransaction,
    ) -> Result<CommitOutcome, RawOrderedJobStoreError> {
        Ok(CommitOutcome::Committed)
    }
}

struct OutOfBoundsRangeRaw;

#[async_trait]
impl RawOrderedJobStore for OutOfBoundsRangeRaw {
    fn bind_native_admission(
        &self,
        _: Arc<dyn storage_provider::native_admission::NativeAdmission>,
    ) -> storage_types::StorageResult<()> {
        // This adapter fixture has no native provider or settlement lifecycle.
        Err(StorageError::internal(
            "adapter-only fixture cannot bind native admission",
        ))
    }

    fn capabilities(&self) -> StoreCapabilities {
        RecordingRaw::new().capabilities
    }

    async fn bootstrap(&self) -> Result<(), RawOrderedJobStoreError> {
        Ok(())
    }

    async fn get(&self, _key: StoreKey) -> Result<Option<Bytes>, RawOrderedJobStoreError> {
        Ok(None)
    }

    async fn get_many(
        &self,
        keys: &[StoreKey],
    ) -> Result<Vec<Option<Bytes>>, RawOrderedJobStoreError> {
        Ok(vec![None; keys.len()])
    }

    async fn read_range(
        &self,
        _request: &RangeRequest,
    ) -> Result<RangePage, RawOrderedJobStoreError> {
        RangePage::new(
            vec![RangeEntry {
                key: key(b"outside"),
                value: Bytes::from_static(b"value"),
            }],
            None,
        )
        .map_err(|_| RawOrderedJobStoreError::Backend(StorageError::internal("test page")))
    }

    async fn commit(
        &self,
        _transaction: &StoreTransaction,
    ) -> Result<CommitOutcome, RawOrderedJobStoreError> {
        Ok(CommitOutcome::Committed)
    }
}
