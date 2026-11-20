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

#[cfg(feature = "postgres")]
static OWNED_POSTGRES_SCHEMAS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

/// The base DSN of the owned Postgres fixture; it names a database, never a
/// schema.
#[cfg(feature = "postgres")]
fn base_postgres_dsn() -> String {
    std::env::var("TEST_JOB_EFFECT_POSTGRES_DSN").expect("owned Postgres fixture required")
}

/// The DSN whose `search_path` is the schema owned by the fixture of `table`.
/// The schema is named after the table, which is unique per fixture.
#[cfg(feature = "postgres")]
fn owned_postgres_dsn(table: &TableName) -> String {
    let dsn = base_postgres_dsn();
    let schema = table.as_ref();
    if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        let separator = if dsn.contains('?') { '&' } else { '?' };
        format!("{dsn}{separator}options=-csearch_path%3D{schema}&application_name={schema}")
    } else {
        format!("{dsn} options='-c search_path={schema}' application_name={schema}")
    }
}

#[cfg(feature = "postgres")]
async fn schema_admin_client(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[cfg(feature = "postgres")]
async fn create_owned_postgres_schema(table: &TableName) {
    let schema = table.as_ref().to_string();
    schema_admin_client(&base_postgres_dsn())
        .await
        .execute(&format!("CREATE SCHEMA \"{schema}\""), &[])
        .await
        .unwrap();
    eprintln!("OWNED_FIXTURE created postgres schema={schema}");
    OWNED_POSTGRES_SCHEMAS.lock().unwrap().push(schema);
}

/// Drops every schema this process's fixtures created. Call at the end of a
/// Postgres test.
#[cfg(feature = "postgres")]
async fn drop_owned_postgres_schemas() {
    let schemas = std::mem::take(&mut *OWNED_POSTGRES_SCHEMAS.lock().unwrap());
    let client = schema_admin_client(&base_postgres_dsn()).await;
    for schema in schemas {
        client
            .execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"), &[])
            .await
            .unwrap();
        eprintln!("OWNED_FIXTURE deleted postgres schema={schema}");
    }
}

async fn fixture(
    backend: storage_provider::StorageBackend,
) -> (DatabaseManager, TableName, Timestamp) {
    let table = TableName::new(&format!(
        "job_destination_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    #[cfg_attr(
        not(any(feature = "postgres", feature = "foundationdb")),
        expect(unused_mut, reason = "only external providers add settings")
    )]
    let mut config = native_config(backend.clone());
    match backend {
        #[cfg(feature = "postgres")]
        storage_provider::StorageBackend::Postgres => {
            create_owned_postgres_schema(&table).await;
            config.postgres = Some(storage_provider::PostgresSettings {
                dsn: owned_postgres_dsn(&table),
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
        initial_stream_lineages: None,
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
async fn given_committed_effect_when_replayed_after_completion_then_receipt_is_gone_and_replay_is_rejected()
 {
    for backend in local_backends() {
        verify_committed_effect_when_replayed_after_completion_then_receipt_is_gone_and_replay_is_rejected(
            backend,
        )
        .await;
    }
}

async fn verify_committed_effect_when_replayed_after_completion_then_receipt_is_gone_and_replay_is_rejected(
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
    assert!(
        db.find_job_effect_receipt("lost-ack", authority.clone())
            .await
            .unwrap()
            .is_some()
    );
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
            .is_none(),
        "completion removes the execution's receipts"
    );
    {
        use scheduled_bg_jobs::store::{RangeDirection, RangeRequest, RangeRequestInput};
        let group = authority.receipt_group().unwrap();
        let page = db
            .scheduled_job_store()
            .unwrap()
            .read_range(
                RangeRequest::try_from(RangeRequestInput {
                    start: Some(group.prefix().clone()),
                    end: Some(group.end().clone()),
                    direction: RangeDirection::Forward,
                    item_limit: std::num::NonZeroU32::new(16).unwrap(),
                    byte_limit: std::num::NonZeroUsize::new(1_048_576).unwrap(),
                    continuation: None,
                })
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(page.entries.is_empty());
    }
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
    let error = db
        .transact_write_job_effect(accepted, authority)
        .await
        .unwrap_err();
    assert!(
        matches!(error.to_enum(), StorageEnum::ConditionalCheckFailed),
        "a completed execution rejects the replay: {error}"
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
    system
        .complete(
            claim,
            JobOutcome::Complete(scheduled_bg_jobs::OutcomeCode::try_new("done").unwrap()),
            now,
        )
        .await
        .unwrap();
    assert!(read(&db, &table, "effect").await.is_some());
}

#[tokio::test]
async fn given_job_effect_receipt_when_ordinary_token_is_reused_then_request_receipts_are_separate()
{
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
    let driver = sql_backend::sqlite::SqliteBackend::initialize(
        ":memory:",
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap();
    let store = Arc::new(
        sql_backend::SqlStore::initialize_schema(
            driver,
            storage_types::InitialStreamLineages::ONE,
            true,
        )
        .await
        .unwrap()
        .with_clock(clock.clone()),
    );
    let config = storage_provider::StorageConfig {
        initial_stream_lineages: None,
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
    let db = DatabaseManager::from_initialized_store(
        config,
        crate::InitializedStore::native(store.clone()),
        super::DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
            .build(),
    )
    .await
    .unwrap();
    let table = TableName::new("durable_effect_receipt");
    db.create_table(&table_request(&table)).await.unwrap();
    let now = Timestamp::from_millis(clock.0.load(Ordering::SeqCst));
    let system = system(&db, 1, &table);
    enqueue(&system, 9, now).await;
    let claim = system.claim(JobId::from([9; 16]), now).await.unwrap();
    let authority = claim.context().unwrap().effect_authority().unwrap();
    db.transact_write_job_effect(
        request(&table, "shared-token", "accepted"),
        authority.clone(),
    )
    .await
    .unwrap();
    // Far past the ten-minute request-receipt window.
    clock.0.fetch_add(24 * 60 * 60 * 1_000, Ordering::SeqCst);
    assert!(
        db.find_job_effect_receipt("shared-token", authority.clone())
            .await
            .unwrap()
            .is_some(),
        "a job effect receipt has no time expiry"
    );
    let request_receipt = || async {
        use storage_provider::DynamoTransactWriteAttempt as _;
        storage_provider::DynamoPort::with_attempt(
            store.as_ref(),
            storage_types::AttemptPlan::new(
                storage_types::AttemptMode::ReadWrite,
                storage_types::DeadlineClass::Write,
            )
            .start(storage_types::AttemptDeadlineSource::attempt_deadlines(
                store.as_ref(),
            )),
            move |attempt| {
                Box::pin(async move {
                    attempt
                        .read_transact_receipt(storage_provider::ReceiptSlot::Request(
                            "shared-token",
                        ))
                        .await
                })
            },
        )
        .await
        .unwrap()
    };
    assert!(
        request_receipt().await.is_none(),
        "a job effect writes no request receipt"
    );
    db.transact_write_items(request(&table, "shared-token", "ordinary"))
        .await
        .unwrap();
    assert_eq!(
        read(&db, &table, "effect").await.unwrap().get("payload"),
        Some(&AttributeValue::S("ordinary".to_string())),
        "an ordinary request with the same token executes as a new request"
    );
    assert!(
        request_receipt().await.is_some(),
        "the ordinary request created its own receipt in sys_transact_receipts"
    );
    assert!(
        db.find_job_effect_receipt("shared-token", authority)
            .await
            .unwrap()
            .is_some()
    );
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
    verify_committed_effect_when_replayed_after_completion_then_receipt_is_gone_and_replay_is_rejected(
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
    drop_owned_postgres_schemas().await;
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
    verify_committed_effect_when_replayed_after_completion_then_receipt_is_gone_and_replay_is_rejected(
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
    let (client, connection) =
        tokio_postgres::connect(&owned_postgres_dsn(&table), tokio_postgres::NoTls)
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
                     AND query LIKE '%UPDATE sys_scheduled_jobs_v2 SET value = value%')",
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
    drop_owned_postgres_schemas().await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires an owned postgres fixture"]
async fn given_native_postgres_effect_racing_completion_when_both_commit_then_no_receipt_survives()
{
    use std::sync::Arc;

    use sha2::{Digest as _, Sha256};

    let (db, table, now) = fixture(storage_provider::StorageBackend::Postgres).await;
    db.get_table_info(&table).await.unwrap();
    let system = system(&db, 1, &table);
    enqueue(&system, 11, now).await;
    let claim = system.claim(JobId::from([11; 16]), now).await.unwrap();
    let authority = claim.context().unwrap().effect_authority().unwrap();
    let group = authority.receipt_group().unwrap();
    let receipt_key = authority
        .receipt_key(&Sha256::digest("race").into())
        .unwrap()
        .as_bytes()
        .to_vec();
    let (client, connection) =
        tokio_postgres::connect(&owned_postgres_dsn(&table), tokio_postgres::NoTls)
            .await
            .unwrap();
    let connection_task = tokio::spawn(async move {
        connection.await.unwrap();
    });
    // The open admin insert holds the effect's receipt key.
    client.batch_execute("BEGIN").await.unwrap();
    client
        .execute(
            "INSERT INTO sys_scheduled_jobs_v2 (key, value) VALUES ($1, $2)",
            &[&receipt_key, &b"x".as_slice()],
        )
        .await
        .unwrap();
    let db = Arc::new(db);
    let effect = {
        let db = db.clone();
        let input = request(&table, "race", "effect");
        let authority = authority.clone();
        tokio::spawn(async move { db.transact_write_job_effect(input, authority).await })
    };
    let completion = {
        let system = system.clone();
        tokio::spawn(async move {
            system
                .complete(
                    claim,
                    JobOutcome::Complete(scheduled_bg_jobs::OutcomeCode::try_new("done").unwrap()),
                    now,
                )
                .await
                .map(|_| ())
        })
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            client
                .simple_query("SELECT pg_stat_clear_snapshot()")
                .await
                .unwrap();
            let blocked: bool = client
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = \
                     current_database() AND pid <> pg_backend_pid() AND wait_event_type = 'Lock' \
                     AND query LIKE '%FROM sys_scheduled_jobs_v2 WHERE key IN%FOR UPDATE%')",
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
            "completion must reach its protected read: {error}; effect finished: {}; completion \
             finished: {}",
            effect.is_finished(),
            completion.is_finished()
        )
    });
    client.batch_execute("ROLLBACK").await.unwrap();
    effect.await.unwrap().unwrap();
    completion.await.unwrap().unwrap();
    let prefix = group.prefix().as_bytes().to_vec();
    let end = group.end().as_bytes().to_vec();
    let remaining: i64 = client
        .query_one(
            "SELECT count(*) FROM sys_scheduled_jobs_v2 WHERE key >= $1 AND key < $2",
            &[&prefix, &end],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(remaining, 0);
    drop(client);
    connection_task.await.unwrap();
    drop_owned_postgres_schemas().await;
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

#[derive(Clone, Copy, Debug)]
enum InvalidJobWrite {
    ResultCondition,
    IndexCondition,
    CounterCondition,
    RepeatedTarget,
    OversizedItem,
    BadExpression,
}

fn terminal_related_writes(
    table: &TableName,
    token: &str,
    invalid_write: Option<InvalidJobWrite>,
) -> storage_provider::JobTerminalProjection {
    let mut result = terminal_projection_request(table, token, "failed")
        .transact_items
        .remove(0)
        .update
        .unwrap();
    let mut index = request(table, token, "indexed")
        .transact_items
        .remove(0)
        .put
        .unwrap();
    index.item = WireItem::try_from(&AttributeMap::from_iter([
        ("pk".to_string(), AttributeValue::S("index".to_string())),
        (
            "payload".to_string(),
            AttributeValue::S("indexed".to_string()),
        ),
    ]))
    .unwrap();
    index.condition_expression = Some("attribute_not_exists(pk)".to_string());
    let mut counter = result.clone();
    counter.key =
        KeyAttributes::from_iter([("pk".to_string(), AttributeValue::S("counter".to_string()))]);
    counter.update_expression = "ADD result_count :one".to_string();
    counter.condition_expression = Some("attribute_not_exists(blocked)".to_string());
    counter.expression_attribute_values = Some(AttributeMap::from_iter([(
        ":one".to_string(),
        AttributeValue::N("1".to_string()),
    )]));
    match invalid_write {
        Some(InvalidJobWrite::ResultCondition) => {
            result.condition_expression = Some("payload = :value".to_string());
            result
                .expression_attribute_values
                .as_mut()
                .unwrap()
                .remove(":pending");
        }
        Some(InvalidJobWrite::IndexCondition) => {
            index.condition_expression = Some("attribute_exists(pk)".to_string());
        }
        Some(InvalidJobWrite::CounterCondition) => {
            counter.condition_expression = Some("attribute_exists(blocked)".to_string());
        }
        Some(InvalidJobWrite::RepeatedTarget) => counter.key = result.key.clone(),
        Some(InvalidJobWrite::OversizedItem) => {
            index.item = WireItem::try_from(&AttributeMap::from_iter([
                ("pk".to_string(), AttributeValue::S("index".to_string())),
                (
                    "payload".to_string(),
                    AttributeValue::S("x".repeat(401 * 1024)),
                ),
            ]))
            .unwrap();
        }
        Some(InvalidJobWrite::BadExpression) => {
            counter.update_expression = "not an update".to_string();
        }
        None => {}
    }
    storage_provider::JobTerminalProjection::try_with_index_and_counter(
        result,
        index,
        counter,
        token.to_string(),
    )
    .unwrap()
}

#[tokio::test]
async fn given_terminal_jobs_when_related_writes_retry_then_rows_commit_once_and_conditions_roll_back()
 {
    for backend in local_backends() {
        for cancelled in [false, true] {
            for failed_write in [
                InvalidJobWrite::ResultCondition,
                InvalidJobWrite::IndexCondition,
                InvalidJobWrite::CounterCondition,
            ] {
                verify_terminal_related_writes(backend.clone(), cancelled, failed_write).await;
            }
        }
    }
}

async fn verify_terminal_related_writes(
    backend: storage_provider::StorageBackend,
    cancelled: bool,
    failed_write: InvalidJobWrite,
) {
    use scheduled_bg_jobs::{
        OutcomeCode,
        test_support::raw::{LifecycleObservation, RedriveExecution, RedriveRequest},
    };
    let (db, table, now) = fixture(backend.clone()).await;
    let system = system(&db, 1, &table);
    let id = JobId::from([31; 16]);
    enqueue(&system, 31, now).await;
    let claim = system.claim(id, now).await.unwrap();
    let live = claim.context().unwrap().effect_authority().unwrap();
    let outcome = if cancelled {
        JobOutcome::Cancelled
    } else {
        JobOutcome::DeadLetter {
            code: OutcomeCode::try_new("permanent").unwrap(),
        }
    };
    system.apply_outcome(claim, outcome, now).await.unwrap();
    let terminal = system.terminal_authority(id).await.unwrap().unwrap();
    db.transact_write_items(request(&table, "initial", "pending"))
        .await
        .unwrap();
    assert!(
        db.transact_write_job_effect(request(&table, "inactive", "forbidden"), live,)
            .await
            .is_err()
    );
    for (invalid_write, position) in [
        (InvalidJobWrite::ResultCondition, 0),
        (InvalidJobWrite::IndexCondition, 1),
        (InvalidJobWrite::CounterCondition, 2),
    ] {
        let token = format!("failed-{invalid_write:?}");
        let error = db
            .transact_write_job_projection(
                terminal_related_writes(&table, &token, Some(invalid_write)),
                terminal.clone(),
            )
            .await
            .unwrap_err();
        assert!(
            error.is_canceled_condition_check_failed(position),
            "expected {invalid_write:?} at position {position}, got {error:?}",
        );
        assert_eq!(
            read(&db, &table, "effect").await.unwrap().get("payload"),
            Some(&AttributeValue::S("pending".to_string())),
        );
        assert!(read(&db, &table, "index").await.is_none());
        assert!(read(&db, &table, "counter").await.is_none());
        // A rejected attempt must not save a request result under its token.
        assert!(
            db.transact_write_job_projection(
                terminal_related_writes(&table, &token, Some(invalid_write)),
                terminal.clone(),
            )
            .await
            .is_err()
        );
    }
    for invalid_write in [
        InvalidJobWrite::RepeatedTarget,
        InvalidJobWrite::OversizedItem,
        InvalidJobWrite::BadExpression,
    ] {
        assert!(
            db.transact_write_job_projection(
                terminal_related_writes(
                    &table,
                    &format!("invalid-{invalid_write:?}"),
                    Some(invalid_write)
                ),
                terminal.clone(),
            )
            .await
            .is_err()
        );
        assert!(read(&db, &table, "index").await.is_none());
        assert!(read(&db, &table, "counter").await.is_none());
    }
    assert!(
        db.transact_write_job_projection(
            terminal_related_writes(&table, &"x".repeat(37), None),
            terminal.clone(),
        )
        .await
        .is_err()
    );
    // A failed condition does not reserve its token: the corrected request can
    // commit.
    let token = format!("failed-{failed_write:?}");
    let writes = terminal_related_writes(&table, &token, None);
    db.transact_write_job_projection(writes.clone(), terminal.clone())
        .await
        .unwrap();
    db.transact_write_job_projection(writes.clone(), terminal.clone())
        .await
        .unwrap();
    let changed = db
        .transact_write_job_projection(
            terminal_related_writes(&table, &token, Some(InvalidJobWrite::CounterCondition)),
            terminal.clone(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        changed.to_enum(),
        StorageEnum::IdempotentParameterMismatch
    ));
    assert!(read(&db, &table, "index").await.is_some());
    assert_eq!(
        read(&db, &table, "counter")
            .await
            .unwrap()
            .get("result_count"),
        Some(&AttributeValue::N("1".to_string())),
    );
    if cancelled {
        system.settle_terminal(terminal.clone()).await.unwrap();
    } else {
        let LifecycleObservation::Current {
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
    }
    // Make every destination condition true before testing the old job token.
    db.transact_write_items(request(&table, "restore-pending", "pending"))
        .await
        .unwrap();
    db.delete_item(
        crate::DeleteItemInput::builder()
            .table_name(table.clone())
            .key(KeyAttributes::from_iter([(
                "pk".to_string(),
                AttributeValue::S("index".to_string()),
            )]))
            .build(),
    )
    .await
    .unwrap();
    let stale = db
        .transact_write_job_projection(
            terminal_related_writes(&table, "fresh-stale", None),
            terminal.clone(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        stale.to_enum(),
        StorageEnum::ConditionalCheckFailed
    ));
    assert_eq!(
        read(&db, &table, "effect").await.unwrap().get("payload"),
        Some(&AttributeValue::S("pending".to_string())),
    );
    assert!(read(&db, &table, "index").await.is_none());
    // Retrying the saved request does not overwrite later changes to its rows.
    // Settlement deletes the job and its saved results, so the retry is stale;
    // a reused execution keeps the saved result.
    let retry = db.transact_write_job_projection(writes, terminal).await;
    if cancelled {
        assert!(matches!(
            retry.unwrap_err().to_enum(),
            StorageEnum::ConditionalCheckFailed
        ));
    } else {
        retry.unwrap();
    }
    assert_eq!(
        read(&db, &table, "effect").await.unwrap().get("payload"),
        Some(&AttributeValue::S("pending".to_string())),
    );
    assert!(read(&db, &table, "index").await.is_none());
    assert_eq!(
        read(&db, &table, "counter")
            .await
            .unwrap()
            .get("result_count"),
        Some(&AttributeValue::N("1".to_string())),
    );
}
