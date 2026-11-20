//! A native abort between stamp and metadata must not release request identity.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, Ordering},
};

use storage_provider::{
    DynamoOperations, RequestTableBindings,
    maintenance::{
        DirectExpirationTransaction, ExpirationAttempt, ExpirationPort, ExpirationTargetRead,
        ExpirationTransaction,
    },
};
use storage_types::{
    AttemptMode, AttemptPlan, Clock, DeadlineClass, StorageEnum, StorageError, StorageResult,
    TableName, TimestampMillis, context::WrappedError as _,
};

struct MetadataAbort(AtomicBool);
impl crate::SqlFaultProbe for MetadataAbort {
    fn fail(&self, call: crate::SqlFaultCall<'_>) -> Option<StorageError> {
        if call.sql.starts_with("SELECT id, table_name, table_status,")
            && self.0.swap(false, Ordering::SeqCst)
        {
            return Some(
                StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(
                    storage_types::ConflictCause::Logical("injected metadata abort"),
                ))
                .into(),
            );
        }
        None
    }
}

struct TestClock(AtomicI64);
impl Clock for TestClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::from_timestamp(self.0.load(Ordering::SeqCst))
    }
}

async fn read_target<A: ExpirationPort>(
    attempt: &mut A,
    bindings: RequestTableBindings,
    scheduled: bool,
) -> StorageResult<()> {
    let mut expiration = ExpirationAttempt::new(attempt, bindings);
    if scheduled {
        expiration
            .read_expiration_target(&ExpirationTargetRead::First)
            .await
            .map(|_| ())
    } else {
        expiration
            .read_direct_policy(&TableName::new("target"))
            .await
            .map(|_| ())
    }
}

#[tokio::test]
async fn given_native_ttl_metadata_abort_when_name_is_recreated_then_direct_and_scheduled_retries_reject_it()
 {
    for scheduled in [false, true] {
        let probe = Arc::new(MetadataAbort(AtomicBool::new(false)));
        let clock = Arc::new(TestClock(AtomicI64::new(
            TimestampMillis::now().timestamp_millis(),
        )));
        let backend = crate::sqlite::SqliteBackend::initialize(
            ":memory:",
            storage_types::AttemptDeadlines::default(),
        )
        .await
        .unwrap()
        .with_fault_probe(probe.clone());
        let store = crate::SqlStore::initialize_schema(
            backend,
            storage_types::InitialStreamLineages::ONE,
            true,
        )
        .await
        .unwrap()
        .with_clock(clock.clone());
        let create: storage_types::CreateTableRequest = serde_json::from_value(serde_json::json!({
            "TableName": "target", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
        }))
        .unwrap();
        let ttl: storage_types::UpdateTimeToLiveRequest = serde_json::from_value(serde_json::json!({
            "TableName": "target", "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": true}
        })).unwrap();
        store.create_table(create.clone()).await.unwrap();
        store.update_time_to_live(ttl.clone()).await.unwrap();
        let bindings = RequestTableBindings::default();
        probe.0.store(true, Ordering::SeqCst);
        let first = bindings.clone();
        let error = storage_provider::DynamoPort::with_attempt(
            &store,
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
                storage_types::AttemptDeadlineSource::attempt_deadlines(&store),
            ),
            move |attempt| Box::pin(read_target(attempt, first, scheduled)),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error.to_enum(),
            StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(_))
        ));
        store
            .delete_table(storage_types::DeleteTableRequest {
                table_name: TableName::new("target"),
            })
            .await
            .unwrap();
        // Delete this advance with phase04's immediate alias detachment.
        clock.0.fetch_add(30_000, Ordering::SeqCst);
        store.create_table(create).await.unwrap();
        store.update_time_to_live(ttl).await.unwrap();
        store.put_item(serde_json::from_value(serde_json::json!({"TableName": "target", "Item": {"pk": {"S": "new"}, "ttl": {"N": "1"}}})).unwrap()).await.unwrap();
        let error = storage_provider::DynamoPort::with_attempt(
            &store,
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
                storage_types::AttemptDeadlineSource::attempt_deadlines(&store),
            ),
            move |attempt| Box::pin(read_target(attempt, bindings, scheduled)),
        )
        .await
        .unwrap_err();
        assert!(matches!(error.to_enum(), StorageEnum::TableNotFound { .. }));
        let item = store
            .get_item(
                serde_json::from_value(
                    serde_json::json!({"TableName": "target", "Key": {"pk": {"S": "new"}}}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            item.item.is_some(),
            "retry crossed the immutable table boundary"
        );
    }
}
