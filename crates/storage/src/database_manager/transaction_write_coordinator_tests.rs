use std::{num::NonZeroU16, time::Duration};

use scheduled_bg_jobs::{
    DriveBudget, EffectMode, EnqueueOptions, ExecutionContext, FairnessClass, FairnessPolicy,
    HandlerFuture, JobCatalog, JobHandler, JobId, JobKind, JobOutcome, JobSpec, Namespace, OwnerId,
    QueueName, QueueSelection, RetryPreset, RetrySelection, Timestamp, runtime::JobSystem,
};
use storage_types::{
    AttributeDefinition, AttributeMap, AttributeValue, CreateTableRequest, KeyAttributeType,
    KeyAttributes, KeySchemaElement, KeyType, StorageEnum, TableName, TransactPutRequest,
    TransactWriteItem, TransactWriteItemsRequest, WireItem, context::WrappedError as _,
};

use super::DatabaseManager;

struct UnusedHandler;
impl JobHandler for UnusedHandler {
    fn execute<'a>(&'a self, _: ExecutionContext<'a>, _: &'a [u8]) -> HandlerFuture<'a> {
        Box::pin(async { Ok(JobOutcome::Cancelled) })
    }
}

fn system(
    db: &DatabaseManager,
    owner: u8,
    table: &TableName,
) -> std::sync::Arc<JobSystem<crate::ScheduledJobStore>> {
    let mut catalog = JobCatalog::new();
    catalog
        .register(
            JobSpec::unit(
                JobKind::try_new("destination-effect").unwrap(),
                NonZeroU16::MIN,
                QueueName::try_new("effects").unwrap(),
                UnusedHandler,
            )
            .policy(
                EffectMode::Fenced,
                FairnessPolicy::low_volume(),
                RetrySelection::Preset(RetryPreset::Network),
                Duration::from_secs(30),
            ),
        )
        .unwrap();
    bg_jobs_tokio::JobRuntime::builder(
        db.scheduled_job_store().unwrap(),
        catalog,
        DriveBudget::try_tiny(QueueSelection::all()).unwrap(),
    )
    .namespace(Namespace::try_new(format!("effect-authority-tests:{}", table.as_ref())).unwrap())
    .owner(OwnerId::from([owner; 16]))
    .build()
    .unwrap()
    .test_system()
}

async fn fixture(
    backend: storage_provider::StorageBackend,
) -> (DatabaseManager, TableName, Timestamp) {
    let mut config = native_config(backend.clone());
    match backend {
        #[cfg(feature = "postgres")]
        storage_provider::StorageBackend::Postgres => {
            config.postgres = Some(storage_provider::PostgresSettings {
                dsn: std::env::var("TEST_JOB_EFFECT_POSTGRES_DSN")
                    .expect("owned Postgres fixture required"),
                tls: false,
                ..Default::default()
            });
        }
        #[cfg(feature = "foundationdb")]
        storage_provider::StorageBackend::FoundationDb => {
            config.foundationdb = Some(storage_provider::FoundationDbSettings {
                cluster_file: Some(
                    std::env::var("TEST_JOB_EFFECT_FDB_CLUSTER_FILE")
                        .expect("owned FDB fixture required"),
                ),
                subspace_prefix: Some(
                    crate::storage_test_support_tests::unique_path("job-effect-scope")
                        .to_string_lossy()
                        .into_owned(),
                ),
                ..Default::default()
            });
        }
        _ => {}
    }
    let db = DatabaseManager::new_with_config_and_runtime_options(
        config,
        super::DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
            .build(),
    )
    .await
    .unwrap();
    let table = TableName::new(&format!(
        "job_destination_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    db.create_table(&table_request(&table)).await.unwrap();
    (
        db,
        table,
        Timestamp::from_millis(storage_types::TimestampMillis::now().timestamp_millis()),
    )
}

fn local_backends() -> Vec<storage_provider::StorageBackend> {
    vec![
        storage_provider::StorageBackend::SQLite,
        #[cfg(feature = "turso")]
        storage_provider::StorageBackend::Turso,
        #[cfg(feature = "rocksdb")]
        storage_provider::StorageBackend::RocksDB,
    ]
}

fn native_config(backend: storage_provider::StorageBackend) -> storage_provider::StorageConfig {
    storage_provider::StorageConfig {
        backend_type: backend,
        connection_string: Some(
            crate::storage_test_support_tests::unique_path("job-effects")
                .to_string_lossy()
                .into_owned(),
        ),
        file_path: None,
        sqlite: None,
        turso: None,
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

fn table_request(table: &TableName) -> CreateTableRequest {
    CreateTableRequest::new(
        table.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    )
}

async fn enqueue(system: &JobSystem<crate::ScheduledJobStore>, id: u8, at: Timestamp) {
    let seed = scheduled_bg_jobs::JobSeed::new(
        JobKind::try_new("destination-effect").unwrap(),
        NonZeroU16::MIN,
        bytes::Bytes::new(),
        EnqueueOptions::new(FairnessClass::low_volume())
            .job_id(JobId::from([id; 16]))
            .scheduled_at(at),
        at,
    )
    .unwrap();
    system.ensure_seed(seed, at).await.unwrap();
}

fn request(table: &TableName, token: &str, value: &str) -> TransactWriteItemsRequest {
    TransactWriteItemsRequest {
        transact_items: ["effect", "checkpoint"]
            .into_iter()
            .map(|key| TransactWriteItem {
                put: Some(TransactPutRequest {
                    table_name: table.clone(),
                    item: WireItem::try_from(&AttributeMap::from_iter([
                        ("pk".to_string(), AttributeValue::S(key.to_string())),
                        ("payload".to_string(), AttributeValue::S(value.to_string())),
                        (
                            storage_types::single_table_entity::UPDATED_AT_ATTR.to_string(),
                            AttributeValue::N("1700000000000".to_string()),
                        ),
                    ]))
                    .unwrap(),
                    indexers: None,
                    condition_expression: None,
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: None,
                }),
                update: None,
                delete: None,
                condition_check: None,
            })
            .collect(),
        client_request_token: Some(token.to_string()),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    }
}

async fn read(db: &DatabaseManager, table: &TableName, key: &str) -> Option<AttributeMap> {
    db.get_item_decode::<_, AttributeMap>(
        table.clone(),
        KeyAttributes::from_iter([("pk".to_string(), AttributeValue::S(key.to_string()))]),
    )
    .await
    .unwrap()
}

fn encode_effect_request(
    table: &TableName,
    value: &str,
) -> storage_types::TransactWriteItemsEncodeRequest {
    let transact_items = request(table, "fixture", value)
        .transact_items
        .into_iter()
        .map(|item| {
            let put = item.put.unwrap();
            storage_types::TransactEncodeItem {
                put: Some(storage_types::TransactEncodePutRequest {
                    table_name: put.table_name,
                    item: storage_types::WireEntity::unindexed(put.item),
                    condition_expression: put.condition_expression,
                    expression_attribute_names: put.expression_attribute_names,
                    expression_attribute_values: put.expression_attribute_values,
                    return_values_on_condition_check_failure: put
                        .return_values_on_condition_check_failure,
                    aux_item_stream_ttl_hours: put.aux_item_stream_ttl_hours,
                }),
                ..Default::default()
            }
        })
        .collect();
    storage_types::TransactWriteItemsEncodeRequest {
        transact_items,
        ..Default::default()
    }
}

fn business_effect_guard(
    table: &TableName,
    expected: &str,
) -> storage_types::TransactConditionCheckRequest {
    storage_types::TransactConditionCheckRequest {
        table_name: table.clone(),
        key: KeyAttributes::from_iter([("pk".to_owned(), AttributeValue::S("effect".to_owned()))]),
        condition_expression: "#business_payload = :business_expected".to_owned(),
        expression_attribute_names: Some(storage_types::VecMap::from_iter([(
            "#business_payload".to_owned(),
            "payload".to_owned(),
        )])),
        expression_attribute_values: Some(AttributeMap::from_iter([(
            ":business_expected".to_owned(),
            AttributeValue::S(expected.to_owned()),
        )])),
        return_values_on_condition_check_failure: None,
    }
}

#[tokio::test]
async fn given_current_native_claim_when_same_key_business_guard_is_stale_then_entire_effect_is_rejected()
 {
    for backend in local_backends() {
        let (db, table, now) = fixture(backend).await;
        db.transact_write_items(request(&table, "initial-business-state", "current"))
            .await
            .unwrap();
        let runtime = system(&db, 1, &table);
        enqueue(&runtime, 1, now).await;
        let claim = runtime.claim(JobId::from([1; 16]), now).await.unwrap();
        let authority = claim.context().unwrap().effect_authority().unwrap();
        let stale = super::DomainEffectAuthorization::for_job(
            authority.clone(),
            business_effect_guard(&table, "previous"),
        );
        let failure = db
            .transact_write_authorized_effect(encode_effect_request(&table, "rejected"), &stale)
            .await
            .unwrap_err();
        assert!(
            failure.is_canceled_condition_check_failed(0),
            "the first business write must reject its stale guard: {:?}",
            failure.to_enum()
        );
        assert!(failure.is_conditional_write_contention());
        for key in ["effect", "checkpoint"] {
            assert_eq!(
                read(&db, &table, key).await.unwrap().get("payload"),
                Some(&AttributeValue::S("current".to_owned()))
            );
        }
        let current = super::DomainEffectAuthorization::for_job(
            authority.clone(),
            business_effect_guard(&table, "current"),
        );
        db.transact_write_authorized_effect(encode_effect_request(&table, "accepted"), &current)
            .await
            .unwrap();
        let stale_again = db
            .transact_write_authorized_effect(
                encode_effect_request(&table, "stale-second-write"),
                &current,
            )
            .await
            .unwrap_err();
        assert!(
            stale_again.is_canceled_condition_check_failed(0),
            "a committed business revision invalidates the same guard: {:?}",
            stale_again.to_enum()
        );
        assert!(stale_again.is_conditional_write_contention());
        for key in ["effect", "checkpoint"] {
            assert_eq!(
                read(&db, &table, key).await.unwrap().get("payload"),
                Some(&AttributeValue::S("accepted".to_owned()))
            );
        }
    }
}

#[tokio::test]
async fn given_replacement_claim_when_old_owner_writes_then_effect_and_checkpoint_remain_absent() {
    for backend in local_backends() {
        verify_replacement_claim_when_old_owner_writes_then_effect_and_checkpoint_remain_absent(
            backend,
        )
        .await;
    }
}

async fn verify_replacement_claim_when_old_owner_writes_then_effect_and_checkpoint_remain_absent(
    backend: storage_provider::StorageBackend,
) {
    let (db, table, now) = fixture(backend.clone()).await;
    let first = system(&db, 1, &table);
    let second = system(&db, 2, &table);
    let past = Timestamp::from_millis(now.as_millis() - 60_000);
    enqueue(&first, 1, past).await;
    let old = first.claim(JobId::from([1; 16]), past).await.unwrap();
    let authority = old.context().unwrap().effect_authority().unwrap();
    let current = second.claim(JobId::from([1; 16]), now).await.unwrap();
    let error = db
        .transact_write_job_effect(request(&table, "old-owner", "stale"), authority)
        .await
        .unwrap_err();
    assert!(matches!(
        error.to_enum(),
        StorageEnum::ConditionalCheckFailed
    ));
    assert!(read(&db, &table, "effect").await.is_none());
    assert!(read(&db, &table, "checkpoint").await.is_none());
    db.transact_write_job_effect(
        request(&table, "current-owner", "accepted"),
        current.context().unwrap().effect_authority().unwrap(),
    )
    .await
    .unwrap();
    for key in ["effect", "checkpoint"] {
        assert_eq!(
            read(&db, &table, key).await.unwrap().get("payload"),
            Some(&AttributeValue::S("accepted".to_string()))
        );
    }
}

#[tokio::test]
async fn given_large_effects_and_a_prior_format_receipt_when_the_completed_execution_replays_then_payload_copies_are_compacted_without_rewriting_the_destination()
 {
    let config = native_config(storage_provider::StorageBackend::SQLite);
    let path = config.connection_string.clone().unwrap();
    let db = DatabaseManager::new_with_config_and_runtime_options(
        config,
        super::DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
            .build(),
    )
    .await
    .unwrap();
    let table = TableName::new("compact_execution_receipt");
    db.create_table(&table_request(&table)).await.unwrap();
    let now = Timestamp::from_millis(storage_types::TimestampMillis::now().timestamp_millis());
    let system = system(&db, 1, &table);
    enqueue(&system, 2, now).await;
    let claim = system.claim(JobId::from([2; 16]), now).await.unwrap();
    let authority = claim.context().unwrap().effect_authority().unwrap();
    let payload = (0_u64..10_000)
        .map(|index| format!("{:x}", md5::compute(index.to_be_bytes())))
        .collect::<String>();
    let input = request(&table, "large-effect", &payload);
    db.transact_write_job_effect(input.clone(), authority.clone())
        .await
        .unwrap();
    assert_eq!(
        read(&db, &table, "effect").await.unwrap().get("payload"),
        Some(&AttributeValue::S(payload))
    );

    let connection = rusqlite::Connection::open(&path).unwrap();
    let (token, bytes): (String, Vec<u8>) = connection
        .query_row(
            "SELECT token, receipt_blob FROM sys_transact_receipts",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(
        bytes.len() < 4096,
        "a native execution receipt must not retain staged bytes: {}",
        bytes.len()
    );
    let mut receipt: serde_json::Value = storage_types::storage_serde::from_bytes(&bytes).unwrap();
    let mut binding = Vec::new();
    authority.bind_request_identity(&mut binding);
    let mut request_identity = serde_json::to_value(&input).unwrap();
    request_identity
        .as_object_mut()
        .unwrap()
        .remove("ClientRequestToken");
    receipt["request_identity"] =
        serde_json::to_value(serde_json::to_vec(&(binding, request_identity)).unwrap()).unwrap();
    let previous = storage_types::storage_serde::to_bytes(&receipt).unwrap();
    assert!(previous.len() > 100_000);
    connection
        .execute(
            "UPDATE sys_transact_receipts SET receipt_blob = ?1 WHERE token = ?2",
            rusqlite::params![previous, token],
        )
        .unwrap();
    drop(connection);
    system
        .complete(
            claim,
            JobOutcome::Complete(scheduled_bg_jobs::OutcomeCode::try_new("done").unwrap()),
            now,
        )
        .await
        .unwrap();
    assert!(
        db.find_job_effect_receipt("large-effect", authority.clone())
            .await
            .unwrap()
            .is_some()
    );
    db.transact_write_items(request(&table, "new-value", "replacement"))
        .await
        .unwrap();
    db.transact_write_job_effect(input, authority.clone())
        .await
        .unwrap();
    assert_eq!(
        read(&db, &table, "effect").await.unwrap().get("payload"),
        Some(&AttributeValue::S("replacement".to_owned()))
    );
    let error = db
        .transact_write_job_effect(request(&table, "large-effect", "changed"), authority)
        .await
        .unwrap_err();
    assert!(matches!(
        error.to_enum(),
        StorageEnum::IdempotentParameterMismatch
    ));
    let connection = rusqlite::Connection::open(&path).unwrap();
    let compacted: Vec<u8> = connection
        .query_row(
            "SELECT receipt_blob FROM sys_transact_receipts WHERE token = ?1",
            [&token],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        compacted.len() < 4096,
        "canonical replay must remove the prior receipt's copied payload"
    );
    drop(connection);
    drop(system);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn given_committed_effect_when_replayed_after_completion_then_receipt_wins_without_rewriting()
{
    for backend in local_backends() {
        verify_committed_effect_when_replayed_after_completion_then_receipt_wins_without_rewriting(
            backend,
        )
        .await;
    }
}

async fn verify_committed_effect_when_replayed_after_completion_then_receipt_wins_without_rewriting(
    backend: storage_provider::StorageBackend,
) {
    let (db, table, now) = fixture(backend.clone()).await;
    let system = system(&db, 1, &table);
    enqueue(&system, 2, now).await;
    let claim = system.claim(JobId::from([2; 16]), now).await.unwrap();
    let authority = claim.context().unwrap().effect_authority().unwrap();
    assert!(
        db.find_job_effect_receipt("lost-ack", authority.clone())
            .await
            .unwrap_or_else(|error| panic!("{backend:?} missing receipt: {:?}", error.to_enum()))
            .is_none()
    );
    let accepted = request(&table, "lost-ack", "accepted");
    db.transact_write_job_effect(accepted.clone(), authority.clone())
        .await
        .unwrap();
    system
        .complete(
            claim,
            JobOutcome::Complete(scheduled_bg_jobs::OutcomeCode::try_new("done").unwrap()),
            now,
        )
        .await
        .unwrap();
    assert!(
        db.find_job_effect_receipt("lost-ack", authority.clone())
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        db.find_job_effect_receipt("other-target", authority.clone())
            .await
            .unwrap()
            .is_none()
    );
    enqueue(&system, 3, now).await;
    let foreign = system.claim(JobId::from([3; 16]), now).await.unwrap();
    assert!(
        db.find_job_effect_receipt(
            "lost-ack",
            foreign.context().unwrap().effect_authority().unwrap()
        )
        .await
        .unwrap()
        .is_none()
    );
    let invalid = db
        .find_job_effect_receipt("", authority.clone())
        .await
        .unwrap_err();
    assert!(matches!(
        invalid.to_enum(),
        StorageEnum::RawValidation { .. }
    ));
    db.transact_write_job_effect(accepted, authority.clone())
        .await
        .unwrap();
    let error = db
        .transact_write_job_effect(request(&table, "lost-ack", "changed"), authority)
        .await
        .unwrap_err();
    assert!(
        matches!(error.to_enum(), StorageEnum::IdempotentParameterMismatch),
        "unexpected changed-request error: {error}"
    );
    for key in ["effect", "checkpoint"] {
        assert_eq!(
            read(&db, &table, key).await.unwrap().get("payload"),
            Some(&AttributeValue::S("accepted".to_string()))
        );
    }
}

#[tokio::test]
async fn given_renewed_claim_when_writing_effect_then_original_witness_remains_valid() {
    for backend in local_backends() {
        verify_renewed_claim_when_writing_effect_then_original_witness_remains_valid(backend).await;
    }
}

async fn verify_renewed_claim_when_writing_effect_then_original_witness_remains_valid(
    backend: storage_provider::StorageBackend,
) {
    let (db, table, now) = fixture(backend.clone()).await;
    let system = system(&db, 1, &table);
    enqueue(&system, 3, now).await;
    let mut claim = system.claim(JobId::from([3; 16]), now).await.unwrap();
    let authority = claim.context().unwrap().effect_authority().unwrap();
    system
        .heartbeat(&mut claim, Timestamp::from_millis(now.as_millis() + 1))
        .await
        .unwrap();
    db.transact_write_job_effect(request(&table, "renewed", "accepted"), authority)
        .await
        .unwrap();
    assert!(read(&db, &table, "effect").await.is_some());
}

#[tokio::test]
async fn given_cancelled_or_expired_claim_when_writing_then_neither_effect_nor_checkpoint_commits()
{
    for backend in local_backends() {
        verify_cancelled_or_expired_claim_when_writing_then_neither_effect_nor_checkpoint_commits(
            backend,
        )
        .await;
    }
}

async fn verify_cancelled_or_expired_claim_when_writing_then_neither_effect_nor_checkpoint_commits(
    backend: storage_provider::StorageBackend,
) {
    for cancelled in [false, true] {
        let (db, table, now) = fixture(backend.clone()).await;
        let system = system(&db, 1, &table);
        let claimed_at = if cancelled {
            now
        } else {
            Timestamp::from_millis(now.as_millis() - 60_000)
        };
        enqueue(&system, 4, claimed_at).await;
        let claim = system
            .claim(JobId::from([4; 16]), claimed_at)
            .await
            .unwrap();
        let authority = claim.context().unwrap().effect_authority().unwrap();
        if cancelled {
            system.request_job_cancellation(&claim, now).await.unwrap();
        }
        let error = db
            .transact_write_job_effect(request(&table, "inactive", "forbidden"), authority)
            .await
            .unwrap_err();
        assert!(matches!(
            error.to_enum(),
            StorageEnum::ConditionalCheckFailed
        ));
        assert!(read(&db, &table, "effect").await.is_none());
        assert!(read(&db, &table, "checkpoint").await.is_none());
    }
}

#[tokio::test]
async fn given_foreign_execution_when_reusing_receipt_token_then_no_destination_is_rewritten() {
    for backend in local_backends() {
        verify_foreign_execution_when_reusing_receipt_token_then_no_destination_is_rewritten(
            backend,
        )
        .await;
    }
}

async fn verify_foreign_execution_when_reusing_receipt_token_then_no_destination_is_rewritten(
    backend: storage_provider::StorageBackend,
) {
    let (db, table, now) = fixture(backend.clone()).await;
    let system = system(&db, 1, &table);
    enqueue(&system, 5, now).await;
    let expired_at = Timestamp::from_millis(now.as_millis() - 60_000);
    enqueue(&system, 6, expired_at).await;
    let first = system.claim(JobId::from([5; 16]), now).await.unwrap();
    let second = system
        .claim(JobId::from([6; 16]), expired_at)
        .await
        .unwrap();
    let input = request(&table, "execution-bound", "accepted");
    db.transact_write_job_effect(
        input.clone(),
        first.context().unwrap().effect_authority().unwrap(),
    )
    .await
    .unwrap();
    let error = db
        .transact_write_job_effect(input, second.context().unwrap().effect_authority().unwrap())
        .await
        .unwrap_err();
    assert!(matches!(
        error.to_enum(),
        StorageEnum::ConditionalCheckFailed
    ));
    assert_eq!(
        read(&db, &table, "effect").await.unwrap().get("payload"),
        Some(&AttributeValue::S("accepted".to_string()))
    );
}

#[tokio::test]
async fn given_rejected_checkpoint_when_writing_effect_then_entire_destination_transaction_rolls_back()
 {
    for backend in local_backends() {
        verify_rejected_checkpoint_when_writing_effect_then_entire_destination_transaction_rolls_back(backend).await;
    }
}

async fn verify_rejected_checkpoint_when_writing_effect_then_entire_destination_transaction_rolls_back(
    backend: storage_provider::StorageBackend,
) {
    let (db, table, now) = fixture(backend.clone()).await;
    let system = system(&db, 1, &table);
    enqueue(&system, 7, now).await;
    let claim = system.claim(JobId::from([7; 16]), now).await.unwrap();
    let authority = claim.context().unwrap().effect_authority().unwrap();
    let mut rejected = request(&table, "atomic-effect", "accepted");
    rejected.transact_items[1]
        .put
        .as_mut()
        .unwrap()
        .condition_expression = Some("attribute_exists(pk)".to_string());
    let error = db
        .transact_write_job_effect(rejected, authority.clone())
        .await
        .unwrap_err();
    assert!(matches!(
        error.to_enum(),
        StorageEnum::TransactionCanceled { .. }
    ));
    assert!(read(&db, &table, "effect").await.is_none());
    assert!(read(&db, &table, "checkpoint").await.is_none());
    // A rejected transaction must not reserve the successful receipt token.
    db.transact_write_job_effect(request(&table, "atomic-effect", "accepted"), authority)
        .await
        .unwrap();
    assert!(read(&db, &table, "effect").await.is_some());
    assert!(read(&db, &table, "checkpoint").await.is_some());
}

#[tokio::test]
async fn given_legacy_stamping_enabled_when_replaying_job_intent_then_identity_and_explicit_metadata_are_preserved()
 {
    let options = super::DatabaseManagerRuntimeOptions::builder()
        .enable_single_table_mode(true)
        .build();
    let db = DatabaseManager::new_for_test_with_runtime_options(options)
        .await
        .unwrap();
    let table = TableName::new("job_metadata_identity");
    db.create_table(&table_request(&table)).await.unwrap();
    let now = Timestamp::from_millis(storage_types::TimestampMillis::now().timestamp_millis());
    let system = system(&db, 1, &table);
    enqueue(&system, 8, now).await;
    let claim = system.claim(JobId::from([8; 16]), now).await.unwrap();
    let authority = claim.context().unwrap().effect_authority().unwrap();
    let mut input = request(&table, "metadata", "accepted");
    for item in &mut input.transact_items {
        let put = item.put.as_mut().unwrap();
        put.item = put
            .item
            .with_attribute_updates(
                &[storage_types::single_table_entity::UPDATED_AT_ATTR],
                &AttributeMap::new(),
            )
            .unwrap();
    }
    db.transact_write_job_effect(input.clone(), authority.clone())
        .await
        .unwrap();
    system
        .complete(
            claim,
            JobOutcome::Complete(scheduled_bg_jobs::OutcomeCode::try_new("done").unwrap()),
            now,
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    db.transact_write_job_effect(input.clone(), authority.clone())
        .await
        .unwrap();
    input.transact_items[0].put.as_mut().unwrap().item = request(&table, "metadata", "accepted")
        .transact_items
        .remove(0)
        .put
        .unwrap()
        .item;
    let error = db
        .transact_write_job_effect(input, authority)
        .await
        .unwrap_err();
    assert!(matches!(
        error.to_enum(),
        StorageEnum::IdempotentParameterMismatch
    ));
}

#[tokio::test]
async fn given_old_job_receipt_when_ordinary_token_is_reused_then_durable_replay_still_wins() {
    use std::sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    };

    struct Clock(AtomicI64);
    impl storage_types::Clock for Clock {
        fn now(&self) -> storage_types::TimestampMillis {
            storage_types::TimestampMillis::from_timestamp(self.0.load(Ordering::SeqCst))
        }
    }
    let clock = Arc::new(Clock(AtomicI64::new(1_800_000_000_000)));
    let driver = sql_backend::sqlite::SqliteBackend::initialize(":memory:")
        .await
        .unwrap();
    let store = Arc::new(
        sql_backend::SqlStore::initialize_schema(driver)
            .await
            .unwrap()
            .with_clock(clock.clone()),
    );
    let bundle =
        crate::StorageProviderBundle::try_from_initialized_store(store.clone(), Default::default())
            .unwrap();
    let config = storage_provider::StorageConfig {
        backend_type: storage_provider::StorageBackend::SQLite,
        connection_string: Some(":memory:".to_string()),
        file_path: None,
        sqlite: None,
        turso: None,
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };
    let db = DatabaseManager::from_initialized_bundle(
        config,
        bundle,
        super::DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
            .build(),
    )
    .await
    .unwrap();
    let table = TableName::new("durable_effect_receipt");
    db.create_table(&table_request(&table)).await.unwrap();
    // A literal prior-format receipt has no retention field. It must remain
    // authoritative inside the ordinary window and expire at the boundary.
    #[derive(serde::Serialize)]
    struct LegacyReceipt {
        request_identity: Vec<u8>,
        response: Vec<u8>,
        completed_at: storage_types::TimestampMillis,
    }
    let legacy_input = request(&table, "legacy-token", "original");
    let mut identity = legacy_input.clone();
    identity.client_request_token = None;
    let legacy = LegacyReceipt {
        request_identity: serde_json::to_vec(&identity).unwrap(),
        response: serde_json::to_vec(&storage_types::TransactWriteItemsResponse {
            consumed_capacity: None,
            item_collection_metrics: None,
        })
        .unwrap(),
        completed_at: storage_types::TimestampMillis::from_timestamp(
            clock.0.load(Ordering::SeqCst),
        ),
    };
    let bytes = storage_types::storage_serde::to_bytes(&legacy).unwrap();
    use storage_provider::{DynamoPort as _, DynamoTransactWriteAttempt as _};
    store
        .with_dynamo_attempt(storage_types::AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move { attempt.write_transact_receipt("legacy-token", bytes).await })
        })
        .await
        .unwrap();
    db.transact_write_items(legacy_input).await.unwrap();
    assert!(read(&db, &table, "effect").await.is_none());
    let changed = request(&table, "legacy-token", "changed");
    let error = db.transact_write_items(changed.clone()).await.unwrap_err();
    assert!(matches!(
        error.to_enum(),
        StorageEnum::IdempotentParameterMismatch
    ));
    clock.0.fetch_add(
        storage_provider::TRANSACT_WRITE_IDEMPOTENCY_WINDOW_MILLIS,
        Ordering::SeqCst,
    );
    db.transact_write_items(changed).await.unwrap();
    assert_eq!(
        read(&db, &table, "effect").await.unwrap().get("payload"),
        Some(&AttributeValue::S("changed".to_string()))
    );
    let now = Timestamp::from_millis(clock.0.load(Ordering::SeqCst));
    let system = system(&db, 1, &table);
    enqueue(&system, 9, now).await;
    let claim = system.claim(JobId::from([9; 16]), now).await.unwrap();
    let authority = claim.context().unwrap().effect_authority().unwrap();
    let input = request(&table, "shared-token", "accepted");
    db.transact_write_job_effect(input.clone(), authority.clone())
        .await
        .unwrap();
    system
        .complete(
            claim,
            JobOutcome::Complete(scheduled_bg_jobs::OutcomeCode::try_new("done").unwrap()),
            now,
        )
        .await
        .unwrap();
    clock.0.fetch_add(24 * 60 * 60 * 1_000, Ordering::SeqCst);
    db.transact_write_items(request(&table, "shared-token", "ordinary"))
        .await
        .unwrap();
    db.transact_write_job_effect(input, authority.clone())
        .await
        .unwrap();
    assert_eq!(
        read(&db, &table, "effect").await.unwrap().get("payload"),
        Some(&AttributeValue::S("ordinary".to_string())),
        "receipt replay must not execute an old effect again"
    );
    let error = db
        .transact_write_job_effect(request(&table, "shared-token", "changed"), authority)
        .await
        .unwrap_err();
    assert!(matches!(
        error.to_enum(),
        StorageEnum::IdempotentParameterMismatch
    ));
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires an owned postgres fixture"]
async fn given_native_postgres_when_exercising_job_effects_then_fences_receipts_and_rollback_hold()
{
    verify_terminal_projection(storage_provider::StorageBackend::Postgres).await;
    verify_replacement_claim_when_old_owner_writes_then_effect_and_checkpoint_remain_absent(
        storage_provider::StorageBackend::Postgres,
    )
    .await;
    verify_committed_effect_when_replayed_after_completion_then_receipt_wins_without_rewriting(
        storage_provider::StorageBackend::Postgres,
    )
    .await;
    verify_renewed_claim_when_writing_effect_then_original_witness_remains_valid(
        storage_provider::StorageBackend::Postgres,
    )
    .await;
    verify_cancelled_or_expired_claim_when_writing_then_neither_effect_nor_checkpoint_commits(
        storage_provider::StorageBackend::Postgres,
    )
    .await;
    verify_foreign_execution_when_reusing_receipt_token_then_no_destination_is_rewritten(
        storage_provider::StorageBackend::Postgres,
    )
    .await;
    verify_rejected_checkpoint_when_writing_effect_then_entire_destination_transaction_rolls_back(
        storage_provider::StorageBackend::Postgres,
    )
    .await;
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires an owned foundationdb fixture"]
async fn given_native_foundationdb_when_exercising_job_effects_then_fences_receipts_and_rollback_hold()
 {
    verify_terminal_projection(storage_provider::StorageBackend::FoundationDb).await;
    verify_replacement_claim_when_old_owner_writes_then_effect_and_checkpoint_remain_absent(
        storage_provider::StorageBackend::FoundationDb,
    )
    .await;
    verify_committed_effect_when_replayed_after_completion_then_receipt_wins_without_rewriting(
        storage_provider::StorageBackend::FoundationDb,
    )
    .await;
    verify_renewed_claim_when_writing_effect_then_original_witness_remains_valid(
        storage_provider::StorageBackend::FoundationDb,
    )
    .await;
    verify_cancelled_or_expired_claim_when_writing_then_neither_effect_nor_checkpoint_commits(
        storage_provider::StorageBackend::FoundationDb,
    )
    .await;
    verify_foreign_execution_when_reusing_receipt_token_then_no_destination_is_rewritten(
        storage_provider::StorageBackend::FoundationDb,
    )
    .await;
    verify_rejected_checkpoint_when_writing_effect_then_entire_destination_transaction_rolls_back(
        storage_provider::StorageBackend::FoundationDb,
    )
    .await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires an owned PostgreSQL fixture"]
async fn given_native_postgres_cancellation_when_effect_waits_on_job_row_then_no_effect_commits() {
    use std::sync::Arc;

    let (db, table, now) = fixture(storage_provider::StorageBackend::Postgres).await;
    db.get_table_info(&table).await.unwrap();
    let system = system(&db, 1, &table);
    enqueue(&system, 10, now).await;
    let claim = system.claim(JobId::from([10; 16]), now).await.unwrap();
    let authority = claim.context().unwrap().effect_authority().unwrap();
    let job_key = authority.observation_keys()[0].as_bytes().to_vec();
    let cancellation_key = authority.observation_keys()[1].as_bytes().to_vec();
    // Obtain the cancellation bytes from the common lifecycle owner rather
    // than reconstructing its private persisted format in this native probe.
    system.request_job_cancellation(&claim, now).await.unwrap();
    let marker = db
        .scheduled_job_store()
        .unwrap()
        .get(authority.observation_keys()[1].clone())
        .await
        .unwrap()
        .unwrap();
    let (client, connection) = tokio_postgres::connect(
        &std::env::var("TEST_JOB_EFFECT_POSTGRES_DSN").unwrap(),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    let connection_task = tokio::spawn(async move {
        connection.await.unwrap();
    });
    client
        .execute(
            "DELETE FROM sys_scheduled_jobs_v2 WHERE key = $1",
            &[&cancellation_key],
        )
        .await
        .unwrap();
    client.batch_execute("BEGIN").await.unwrap();
    client
        .query_one(
            "SELECT value FROM sys_scheduled_jobs_v2 WHERE key = $1 FOR UPDATE",
            &[&job_key],
        )
        .await
        .unwrap();
    let db = Arc::new(db);
    let task_db = db.clone();
    let input = request(&table, "cancel-overlap", "forbidden");
    let effect =
        tokio::spawn(async move { task_db.transact_write_job_effect(input, authority).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            client
                .simple_query("SELECT pg_stat_clear_snapshot()")
                .await
                .unwrap();
            let blocked: bool = client
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = \
                     current_database() AND pid <> pg_backend_pid() AND wait_event_type = 'Lock' \
                     AND query LIKE '%sys_scheduled_jobs_v2%FOR UPDATE%')",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            if blocked {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "effect must reach protected job read: {error}; effect finished: {}",
            effect.is_finished()
        )
    });
    client
        .execute(
            "INSERT INTO sys_scheduled_jobs_v2 (key, value) VALUES ($1, $2)",
            &[&cancellation_key, &marker.as_ref()],
        )
        .await
        .unwrap();
    client.batch_execute("COMMIT").await.unwrap();
    let error = effect.await.unwrap().unwrap_err();
    assert!(
        matches!(error.to_enum(), StorageEnum::ConditionalCheckFailed),
        "unexpected effect error: {error}"
    );
    assert!(read(&db, &table, "effect").await.is_none());
    assert!(read(&db, &table, "checkpoint").await.is_none());
    drop(client);
    connection_task.await.unwrap();
}

#[tokio::test]
async fn given_terminal_projection_when_redrive_wins_then_stale_publication_is_rejected() {
    for backend in local_backends() {
        verify_terminal_projection(backend).await;
    }
}

async fn verify_terminal_projection(backend: storage_provider::StorageBackend) {
    use scheduled_bg_jobs::{
        OutcomeCode,
        test_support::raw::{RedriveExecution, RedriveRequest},
    };
    let (db, table, now) = fixture(backend).await;
    let system = system(&db, 1, &table);
    let id = JobId::from([19; 16]);
    enqueue(&system, 19, now).await;
    let claim = system.claim(id, now).await.unwrap();
    let live_authority = claim.context().unwrap().effect_authority().unwrap();
    system
        .apply_outcome(
            claim,
            JobOutcome::DeadLetter {
                code: OutcomeCode::try_new("permanent").unwrap(),
            },
            now,
        )
        .await
        .unwrap();
    let terminal = system.terminal_authority(id).await.unwrap().unwrap();
    let error = db
        .transact_write_job_effect(
            request(&table, "inactive-effect", "forbidden"),
            live_authority,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error.to_enum(),
        StorageEnum::ConditionalCheckFailed
    ));
    assert!(read(&db, &table, "effect").await.is_none());
    let mut absent = terminal_projection_request(&table, "missing-status", "forbidden");
    absent.transact_items[0]
        .update
        .as_mut()
        .unwrap()
        .condition_expression = Some("attribute_not_exists(pk)".to_string());
    absent.transact_items[0]
        .update
        .as_mut()
        .unwrap()
        .expression_attribute_values
        .as_mut()
        .unwrap()
        .remove(":pending");
    let error = db
        .transact_write_job_projection(
            storage_provider::JobTerminalProjection::try_from(absent).unwrap(),
            terminal.clone(),
        )
        .await
        .unwrap_err();
    assert!(
        error.is_canceled_condition_check_failed(0),
        "absent destination condition must reject natively: {error:?}"
    );
    assert!(read(&db, &table, "effect").await.is_none());
    db.transact_write_items(request(&table, "initial-status", "pending"))
        .await
        .unwrap();
    let projection = terminal_projection(&table, "terminal-status", "failed");
    db.transact_write_job_projection(projection.clone(), terminal.clone())
        .await
        .unwrap();
    let scheduled_bg_jobs::test_support::raw::LifecycleObservation::Current {
        record,
        operation_id: Some(operation),
        ..
    } = system.observe_lifecycle(id).await.unwrap()
    else {
        panic!("terminal record")
    };
    system
        .redrive_job(
            id,
            RedriveRequest::new(
                record.revision,
                operation,
                RedriveExecution::ReuseExecutionId,
                now,
            ),
        )
        .await
        .unwrap();
    let replacement = system.claim(id, now).await.unwrap();
    let error = db
        .transact_write_job_projection(
            terminal_projection(&table, "stale-terminal-status", "stale"),
            terminal.clone(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error.to_enum(),
        StorageEnum::ConditionalCheckFailed
    ));
    // Lost projection acknowledgement can still reconcile its receipt after
    // redrive. This returns the old response and never rewrites current state.
    db.transact_write_job_projection(projection, terminal)
        .await
        .unwrap();
    assert_eq!(
        read(&db, &table, "effect").await.unwrap().get("payload"),
        Some(&AttributeValue::S("failed".to_string()))
    );
    assert!(matches!(
        replacement.record().state,
        scheduled_bg_jobs::JobState::Leased(_)
    ));
}

fn terminal_projection(
    table: &TableName,
    token: &str,
    value: &str,
) -> storage_provider::JobTerminalProjection {
    storage_provider::JobTerminalProjection::try_from(terminal_projection_request(
        table, token, value,
    ))
    .unwrap()
}

fn terminal_projection_request(
    table: &TableName,
    token: &str,
    value: &str,
) -> TransactWriteItemsRequest {
    TransactWriteItemsRequest {
        transact_items: vec![TransactWriteItem {
            update: Some(storage_types::TransactUpdateRequest {
                table_name: table.clone(),
                key: KeyAttributes::from_iter([(
                    "pk".to_string(),
                    AttributeValue::S("effect".to_string()),
                )]),
                update_expression: "SET payload = :value".to_string(),
                condition_expression: Some("payload = :pending".to_string()),
                expression_attribute_values: Some(AttributeMap::from_iter([
                    (":value".to_string(), AttributeValue::S(value.to_string())),
                    (
                        ":pending".to_string(),
                        AttributeValue::S("pending".to_string()),
                    ),
                ])),
                expression_attribute_names: None,
                indexers: None,
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            ..Default::default()
        }],
        client_request_token: Some(token.to_string()),
        ..Default::default()
    }
}

#[derive(serde::Serialize, storage_derive::WireItemEncode)]
struct NativeCappedReservation {
    reservation_id: String,
}

impl storage_types::single_table_entity::SingleTableEntity for NativeCappedReservation {
    const STORAGE_ENTITY_TYPE: &'static str = "NATIVE_CAPPED_RESERVATION";
    const ENTITY_TYPE: &'static str = "NATIVE_CAPPED_RESERVATION";
    fn pk(&self) -> String {
        "RESERVATION".into()
    }
    fn sk(&self) -> String {
        self.reservation_id.clone()
    }
}

#[tokio::test]
async fn given_native_claim_when_capped_business_guard_rejects_then_entity_counter_and_side_effect_roll_back()
 {
    use storage_types::single_table_entity::SingleTableEntity as _;
    for backend in local_backends() {
        let (db, table, now) = fixture(backend).await;
        let capped = TableName::new(&format!("{}_capped", table.as_ref()));
        let mut definition = table_request(&capped);
        definition.key_schema.push(KeySchemaElement {
            attribute_name: "sk".into(),
            key_type: KeyType::Range,
        });
        definition.attribute_definitions.push(AttributeDefinition {
            attribute_name: "sk".into(),
            attribute_type: KeyAttributeType::S,
        });
        db.create_table(&definition).await.unwrap();
        let runtime = system(&db, 1, &table);
        enqueue(&runtime, 10, now).await;
        let claim = runtime.claim(JobId::from([10; 16]), now).await.unwrap();
        let authority = claim.context().unwrap().effect_authority().unwrap();
        let one = NativeCappedReservation {
            reservation_id: "one".into(),
        };
        let two = NativeCappedReservation {
            reservation_id: "two".into(),
        };
        let mut side_effect = encode_effect_request(&table, "accepted").transact_items;
        side_effect[0].put.as_mut().unwrap().condition_expression =
            Some("attribute_exists(pk)".into());
        assert!(
            db.create_capped_entity(
                crate::CreateCappedEntityInput::builder()
                    .table_name(capped.clone())
                    .item(&one)
                    .counted_entity_type(NativeCappedReservation::ENTITY_TYPE.to_owned())
                    .max_value(1_u64)
                    .additional_transact_items(side_effect)
                    .job_effect_authority(Some(authority.clone()))
                    .build()
            )
            .await
            .is_err()
        );
        assert!(
            db.get_item_decode::<_, AttributeMap>(capped.clone(), one.table_key_map())
                .await
                .unwrap()
                .is_none()
        );
        let count_key = std::collections::HashMap::from([
            ("pk".to_owned(), AttributeValue::S("COUNT".into())),
            (
                "sk".to_owned(),
                AttributeValue::S(NativeCappedReservation::ENTITY_TYPE.into()),
            ),
        ]);
        assert!(
            db.get_item_decode::<_, AttributeMap>(capped.clone(), count_key.clone())
                .await
                .unwrap()
                .is_none()
        );
        assert!(read(&db, &table, "effect").await.is_none());
        assert!(read(&db, &table, "checkpoint").await.is_none());
        db.create_capped_entity(
            crate::CreateCappedEntityInput::builder()
                .table_name(capped.clone())
                .item(&one)
                .counted_entity_type(NativeCappedReservation::ENTITY_TYPE.to_owned())
                .max_value(1_u64)
                .additional_transact_items(encode_effect_request(&table, "accepted").transact_items)
                .job_effect_authority(Some(authority.clone()))
                .build(),
        )
        .await
        .unwrap();
        let rejected = db
            .create_capped_entity(
                crate::CreateCappedEntityInput::builder()
                    .table_name(capped.clone())
                    .item(&two)
                    .counted_entity_type(NativeCappedReservation::ENTITY_TYPE.to_owned())
                    .max_value(1_u64)
                    .job_effect_authority(Some(authority))
                    .build(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            rejected,
            crate::CappedStorageError::CapacityExceededError
        ));
        assert!(
            db.get_item_decode::<_, AttributeMap>(capped.clone(), two.table_key_map())
                .await
                .unwrap()
                .is_none()
        );
        let count = db
            .get_item_decode::<_, AttributeMap>(capped, count_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(count.get("value"), Some(&AttributeValue::N("1".into())));
        assert_eq!(
            read(&db, &table, "effect").await.unwrap().get("payload"),
            Some(&AttributeValue::S("accepted".into()))
        );
        assert_eq!(
            read(&db, &table, "checkpoint")
                .await
                .unwrap()
                .get("payload"),
            Some(&AttributeValue::S("accepted".into()))
        );
    }
}
