use std::sync::Arc;

#[cfg(any(
    feature = "native-turso",
    feature = "native-rocksdb",
    feature = "native-foundationdb"
))]
use infrastructure_provider::physical_store::PhysicalRecordStore;
use infrastructure_provider::{
    batch::RecordBatch,
    physical_attempt::PhysicalRecordAttempt,
    record::{
        ConditionalWriteResult, DurableRecord, DurableRecordKey, RecordAddress, RecordChange,
        RecordExpectation,
    },
};
use storage_provider::DynamoPort;
use storage_types::{
    AttemptMode, InfrastructureContext, InfrastructureResultKind, StorageEnum, StorageError,
    StorageResult,
};

use super::fixture;
use crate::HarnessResult;

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_ordinary_file_sqlite_when_initialized_then_retained_image_infrastructure_layout_is_present()
-> HarnessResult<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("ordinary.sqlite");
    let backend = sql_backend::sqlite::SqliteBackend::initialize(&path).await?;
    let _ordinary = sql_backend::SqlStore::initialize_schema(backend).await?;
    let connection = rusqlite::Connection::open(path)?;
    let count: i64 = connection.query_row(
        "SELECT count(*) FROM sqlite_master WHERE name LIKE 'sys_infrastructure_%'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(2, count);
    Ok(())
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_file_sqlite_unsigned_records_when_native_columns_are_read_then_eight_byte_golden_order_and_maximum_revision_hold()
-> HarnessResult<()> {
    let fixture = fixture::FileSqlite::try_new().await?;
    seed_golden_records(&fixture.store).await?;
    let connection = rusqlite::Connection::open(&fixture.path)?;
    let observed = connection
        .prepare(
            "SELECT generation, revision, resource FROM sys_infrastructure_records WHERE \
             namespace = 'native-golden' ORDER BY generation, suffix",
        )?
        .query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(golden_columns(), observed);
    connection.execute(
        "UPDATE sys_infrastructure_records SET revision = ?1 WHERE namespace = 'native-golden' \
         AND generation = ?2",
        rusqlite::params![
            (u64::MAX - 1).to_be_bytes().to_vec(),
            u64::MAX.to_be_bytes().to_vec()
        ],
    )?;
    assert!(
        connection
            .execute(
                "UPDATE sys_infrastructure_records SET revision = ?1 WHERE namespace = \
                 'native-golden' AND generation = ?2",
                rusqlite::params![vec![0_u8; 7], u64::MAX.to_be_bytes().to_vec()],
            )
            .is_err()
    );
    verify_maximum_revision(&fixture.store).await
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_postgres_unsigned_records_when_native_columns_are_read_then_eight_byte_golden_order_and_maximum_revision_hold()
-> HarnessResult<()> {
    let fixture = fixture::Postgres::try_new().await?;
    let result = async {
        seed_golden_records(&fixture.store).await?;
        let connection = fixture.connect().await?;
        let observed = connection
            .query(
                "SELECT generation, revision, resource FROM sys_infrastructure_records WHERE \
                 namespace = 'native-golden' ORDER BY generation, suffix",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| {
                (
                    row.get::<_, Vec<u8>>(0),
                    row.get::<_, Vec<u8>>(1),
                    row.get::<_, Vec<u8>>(2),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(golden_columns(), observed);
        connection
            .execute(
                "UPDATE sys_infrastructure_records SET revision = $1 WHERE namespace = \
                 'native-golden' AND generation = $2",
                &[
                    &(u64::MAX - 1).to_be_bytes().to_vec(),
                    &u64::MAX.to_be_bytes().to_vec(),
                ],
            )
            .await?;
        assert!(
            connection
                .execute(
                    "UPDATE sys_infrastructure_records SET revision = $1 WHERE namespace = \
                     'native-golden' AND generation = $2",
                    &[&vec![0_u8; 7], &u64::MAX.to_be_bytes().to_vec()],
                )
                .await
                .is_err()
        );
        verify_maximum_revision(&fixture.store).await
    }
    .await;
    fixture.finish().await?;
    result
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_file_sqlite_marker_when_reopened_or_corrupted_then_identity_persists_and_invalid_layout_is_rejected()
-> HarnessResult<()> {
    let fixture = fixture::FileSqlite::try_new().await?;
    seed_golden_records(&fixture.store).await?;
    let connection = rusqlite::Connection::open(&fixture.path)?;
    let identity: Vec<u8> = connection.query_row(
        "SELECT store_identity FROM sys_infrastructure_format WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    assert_identity(&identity);
    drop(fixture.store);
    let reopened = fixture::FileSqlite::open_store(&fixture.path).await?;
    let persisted: Vec<u8> = connection.query_row(
        "SELECT store_identity FROM sys_infrastructure_format WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(identity, persisted);
    verify_golden_record_present(&reopened).await?;
    drop(reopened);
    assert!(
        connection
            .execute(
                "UPDATE sys_infrastructure_format SET store_identity = ?1",
                [vec![0_u8; 15]]
            )
            .is_err()
    );
    connection.execute(
        "UPDATE sys_infrastructure_format SET store_identity = ?1",
        [vec![0_u8; 16]],
    )?;
    assert_layout_rejected(fixture::FileSqlite::open_store(&fixture.path).await);
    connection.execute(
        "UPDATE sys_infrastructure_format SET store_identity = ?1, version = 2",
        [&identity],
    )?;
    assert_layout_rejected(fixture::FileSqlite::open_store(&fixture.path).await);
    connection.execute("DELETE FROM sys_infrastructure_format", [])?;
    assert_layout_rejected(fixture::FileSqlite::open_store(&fixture.path).await);
    connection.execute(
        "INSERT INTO sys_infrastructure_format (singleton, version, store_identity) VALUES (1, 1, \
         ?1)",
        [&identity],
    )?;
    connection.execute("DROP TABLE sys_infrastructure_records", [])?;
    require_native_layout_failure(fixture::FileSqlite::open_store(&fixture.path).await)?;
    let relations: i64 = connection.query_row(
        "SELECT count(*) FROM sqlite_master WHERE name = 'sys_infrastructure_records'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(0, relations);
    let marker: (i64, Vec<u8>) = connection.query_row(
        "SELECT version, store_identity FROM sys_infrastructure_format WHERE singleton = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!((1, identity), marker);
    Ok(())
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_postgres_marker_when_independent_pool_opens_or_layout_is_corrupt_then_identity_persists_and_invalid_layout_is_rejected()
-> HarnessResult<()> {
    let fixture = fixture::Postgres::try_new().await?;
    let result = async {
        seed_golden_records(&fixture.store).await?;
        let connection = fixture.connect().await?;
        let identity: Vec<u8> = connection
            .query_one(
                "SELECT store_identity FROM sys_infrastructure_format WHERE singleton = 1",
                &[],
            )
            .await?
            .get(0);
        assert_identity(&identity);
        let independent = fixture.open_independent_store().await?;
        let persisted: Vec<u8> = connection
            .query_one(
                "SELECT store_identity FROM sys_infrastructure_format WHERE singleton = 1",
                &[],
            )
            .await?
            .get(0);
        assert_eq!(identity, persisted);
        verify_golden_record_present(&independent).await?;
        drop(independent);
        assert!(
            connection
                .execute(
                    "UPDATE sys_infrastructure_format SET store_identity = $1",
                    &[&vec![0_u8; 15]]
                )
                .await
                .is_err()
        );
        connection
            .execute(
                "UPDATE sys_infrastructure_format SET store_identity = $1",
                &[&vec![0_u8; 16]],
            )
            .await?;
        assert_layout_rejected(fixture.open_independent_store().await);
        connection
            .execute(
                "UPDATE sys_infrastructure_format SET store_identity = $1, version = 2",
                &[&identity],
            )
            .await?;
        assert_layout_rejected(fixture.open_independent_store().await);
        connection
            .execute("DELETE FROM sys_infrastructure_format", &[])
            .await?;
        assert_layout_rejected(fixture.open_independent_store().await);
        connection
            .execute(
                "INSERT INTO sys_infrastructure_format (singleton, version, store_identity) \
                 VALUES (1, 1, $1)",
                &[&identity],
            )
            .await?;
        connection
            .execute("DROP TABLE sys_infrastructure_records", &[])
            .await?;
        require_native_layout_failure(fixture.open_independent_store().await)?;
        let absent: bool = connection
            .query_one(
                "SELECT to_regclass('sys_infrastructure_records') IS NULL",
                &[],
            )
            .await?
            .get(0);
        assert!(absent);
        let marker = connection
            .query_one(
                "SELECT version, store_identity FROM sys_infrastructure_format WHERE singleton = 1",
                &[],
            )
            .await?;
        assert_eq!(1, marker.get::<_, i64>(0));
        assert_eq!(identity, marker.get::<_, Vec<u8>>(1));
        Ok(())
    }
    .await;
    fixture.finish().await?;
    result
}

#[cfg(feature = "native-turso")]
#[tokio::test]
async fn given_turso_marker_when_reopened_or_corrupted_then_identity_persists_and_invalid_layout_is_rejected()
-> HarnessResult<()> {
    use sql_backend::{SqlStatement, SqlValue};

    let fixture = super::turso::FileTurso::try_new().await?;
    seed_golden_records(&fixture.store).await?;
    let identity = fixture.store.initialize_records(fixture::limits()?).await?;
    assert_identity(&identity);
    drop(fixture.store);
    let reopened = super::turso::FileTurso::open_store(&fixture.path).await?;
    assert_eq!(
        identity,
        reopened.initialize_records(fixture::limits()?).await?
    );
    verify_golden_record_present(&reopened).await?;
    drop(reopened);
    assert!(
        execute_turso_statement(
            &fixture.path,
            SqlStatement::new(
                "UPDATE sys_infrastructure_format SET store_identity = ?1",
                vec![SqlValue::Blob(vec![0; 15])],
            )
        )
        .await
        .is_err()
    );
    for (version, marker) in [(1, vec![0_u8; 16]), (2, identity.to_vec())] {
        execute_turso_statement(
            &fixture.path,
            SqlStatement::new(
                "UPDATE sys_infrastructure_format SET version = ?1, store_identity = ?2",
                vec![SqlValue::Integer(version), SqlValue::Blob(marker.clone())],
            ),
        )
        .await?;
        assert_layout_rejected(super::turso::FileTurso::open_store(&fixture.path).await);
        let observed = query_turso_statement(
            &fixture.path,
            SqlStatement::new(
                "SELECT version, store_identity FROM sys_infrastructure_format",
                vec![],
            ),
        )
        .await?;
        assert_eq!(1, observed.len());
        assert!(matches!(observed[0].as_slice(),
            [SqlValue::Integer(actual_version), SqlValue::Blob(actual_marker)]
                if *actual_version == version && actual_marker == &marker));
    }
    execute_turso_statement(
        &fixture.path,
        SqlStatement::new("DELETE FROM sys_infrastructure_format", vec![]),
    )
    .await?;
    assert_layout_rejected(super::turso::FileTurso::open_store(&fixture.path).await);
    assert!(
        query_turso_statement(
            &fixture.path,
            SqlStatement::new(
                "SELECT store_identity FROM sys_infrastructure_format",
                vec![],
            )
        )
        .await?
        .is_empty()
    );
    execute_turso_statement(
        &fixture.path,
        SqlStatement::new(
            "INSERT INTO sys_infrastructure_format (singleton, version, store_identity) VALUES \
             (1, 1, ?1)",
            vec![SqlValue::Blob(identity.to_vec())],
        ),
    )
    .await?;
    execute_turso_statement(
        &fixture.path,
        SqlStatement::new("DROP TABLE sys_infrastructure_records", vec![]),
    )
    .await?;
    require_native_layout_failure(super::turso::FileTurso::open_store(&fixture.path).await)?;
    assert!(
        query_turso_statement(
            &fixture.path,
            SqlStatement::new(
                "SELECT name FROM sqlite_master WHERE name = 'sys_infrastructure_records'",
                vec![],
            )
        )
        .await?
        .is_empty()
    );
    let observed = query_turso_statement(
        &fixture.path,
        SqlStatement::new(
            "SELECT version, store_identity FROM sys_infrastructure_format",
            vec![],
        ),
    )
    .await?;
    assert_eq!(1, observed.len());
    assert!(matches!(observed[0].as_slice(),
        [SqlValue::Integer(1), SqlValue::Blob(marker)] if marker.as_slice() == identity));
    Ok(())
}

#[cfg(feature = "native-turso")]
#[tokio::test]
async fn given_turso_unsigned_records_when_native_columns_are_read_then_eight_byte_golden_order_and_maximum_revision_hold()
-> HarnessResult<()> {
    use sql_backend::{SqlStatement, SqlValue};

    let fixture = super::turso::FileTurso::try_new().await?;
    seed_golden_records(&fixture.store).await?;
    drop(fixture.store);
    let rows = query_turso_statement(
        &fixture.path,
        SqlStatement::new(
            "SELECT generation, revision, resource FROM sys_infrastructure_records WHERE \
             namespace = 'native-golden' ORDER BY generation, suffix",
            vec![],
        ),
    )
    .await?;
    let expected = golden_columns();
    assert_eq!(expected.len(), rows.len());
    for (row, (generation, revision, resource)) in rows.iter().zip(expected) {
        assert!(matches!(row.as_slice(),
            [SqlValue::Blob(actual_generation), SqlValue::Blob(actual_revision), SqlValue::Blob(actual_resource)]
                if actual_generation == &generation && actual_revision == &revision && actual_resource == &resource));
    }
    execute_turso_statement(
        &fixture.path,
        SqlStatement::new(
            "UPDATE sys_infrastructure_records SET revision = ?1 WHERE namespace = \
             'native-golden' AND generation = ?2",
            vec![
                SqlValue::Blob((u64::MAX - 1).to_be_bytes().to_vec()),
                SqlValue::Blob(u64::MAX.to_be_bytes().to_vec()),
            ],
        ),
    )
    .await?;
    assert!(
        execute_turso_statement(
            &fixture.path,
            SqlStatement::new(
                "UPDATE sys_infrastructure_records SET revision = ?1 WHERE namespace = \
                 'native-golden' AND generation = ?2",
                vec![
                    SqlValue::Blob(vec![0; 7]),
                    SqlValue::Blob(u64::MAX.to_be_bytes().to_vec())
                ],
            )
        )
        .await
        .is_err()
    );
    verify_maximum_revision(&super::turso::FileTurso::open_store(&fixture.path).await?).await
}

#[cfg(feature = "native-turso")]
async fn execute_turso_statement(
    path: &std::path::Path,
    statement: sql_backend::SqlStatement,
) -> HarnessResult<()> {
    use sql_backend::{SqlAttempt as _, SqlBackend as _};

    let backend = sql_backend::turso::TursoBackend::initialize(
        path.to_str().ok_or("Turso fixture path is not UTF-8")?,
    )
    .await?;
    backend
        .with_schema_attempt(move |attempt| {
            Box::pin(async move { attempt.execute(statement).await.map(|_| ()) })
        })
        .await?;
    Ok(())
}

#[cfg(feature = "native-turso")]
async fn query_turso_statement(
    path: &std::path::Path,
    statement: sql_backend::SqlStatement,
) -> HarnessResult<Vec<Vec<sql_backend::SqlValue>>> {
    use sql_backend::{SqlAttempt as _, SqlBackend as _};

    let backend = sql_backend::turso::TursoBackend::initialize(
        path.to_str().ok_or("Turso fixture path is not UTF-8")?,
    )
    .await?;
    Ok(backend
        .with_schema_attempt(move |attempt| {
            Box::pin(async move {
                let page = attempt
                    .query(
                        statement,
                        storage_types::ReadProtection::Snapshot,
                        std::num::NonZeroUsize::new(1024).expect("fixture query bound"),
                    )
                    .await?;
                assert!(!page.more);
                Ok(page
                    .rows
                    .into_iter()
                    .map(sql_backend::SqlRow::into_values)
                    .collect())
            })
        })
        .await?)
}

#[cfg(feature = "native-rocksdb")]
#[tokio::test]
async fn given_rocksdb_marker_when_reopened_or_corrupted_then_identity_persists_and_invalid_layout_is_rejected()
-> HarnessResult<()> {
    let fixture = super::rocksdb::RocksDb::try_new().await?;
    drop(fixture.store);
    verify_kv_layout(|| {
        std::future::ready(kv_backend::rocksdb::RocksDbBackend::initialize(
            fixture.path.clone(),
        ))
    })
    .await
}

#[cfg(feature = "native-foundationdb")]
#[tokio::test]
async fn given_foundationdb_marker_when_independent_client_opens_or_layout_is_corrupt_then_identity_persists_and_invalid_layout_is_rejected()
-> HarnessResult<()> {
    let fixture = super::foundationdb::FoundationDb::try_new().await?;
    let result = verify_kv_layout(|| {
        kv_backend::foundationdb::FoundationDbBackend::initialize(
            kv_backend::foundationdb::FoundationDbOptions {
                cluster_file: fixture.cluster_file.to_str(),
                subspace_prefix: Some(&fixture.prefix),
                ..Default::default()
            },
        )
    })
    .await;
    fixture.finish().await?;
    result
}

#[cfg(any(feature = "native-rocksdb", feature = "native-foundationdb"))]
async fn verify_kv_layout<B, F, Fut>(open_backend: F) -> HarnessResult<()>
where
    B: kv_backend::KvBackend + 'static,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = StorageResult<B>>,
{
    let store = kv_backend::KvStore::initialize_format(open_backend().await?).await?;
    let identity = store.initialize_records(fixture::limits()?).await?;
    assert_identity(&identity);
    seed_golden_records(&store).await?;
    drop(store);
    let store = kv_backend::KvStore::initialize_format(open_backend().await?).await?;
    assert_eq!(
        identity,
        store.initialize_records(fixture::limits()?).await?
    );
    verify_golden_record_present(&store).await?;
    drop(store);
    let valid = load_kv_marker(&open_backend().await?)
        .await?
        .ok_or("missing initialized marker")?;
    assert_eq!(identity.as_slice(), &valid[1..]);
    let version = valid[0];
    let mut markers = (0..valid.len())
        .map(|length| valid[..length].to_vec())
        .collect::<Vec<_>>();
    markers.push([valid.as_slice(), &[0]].concat());
    markers.push([&[version], &[0; 16][..]].concat());
    markers.push([&[version + 1], identity.as_slice()].concat());
    for marker in markers {
        let backend = open_backend().await?;
        store_kv_marker(&backend, Some(marker.clone())).await?;
        let error = async {
            let store = kv_backend::KvStore::initialize_format(backend).await?;
            store.initialize_records(fixture::limits()?).await
        }
        .await
        .unwrap_err();
        if marker.first().is_some_and(|actual| *actual != version) {
            assert!(matches!(
                error.as_ref(),
                StorageEnum::InfrastructureResult(
                    InfrastructureResultKind::UnsupportedVersion { .. }
                )
            ));
        } else if marker.len() > valid.len() {
            assert!(
                matches!(
                    error.as_ref(),
                    StorageEnum::InfrastructureResult(
                        InfrastructureResultKind::NativeCapacityRejected
                    )
                ),
                "over-bound marker: {:?}",
                error.as_ref()
            );
        } else {
            assert!(
                matches!(
                    error.as_ref(),
                    StorageEnum::InfrastructureResult(InfrastructureResultKind::Encoding {
                        context: storage_types::InfrastructureContext::PhysicalLayout
                    })
                ),
                "malformed marker: {:?}",
                error.as_ref()
            );
        }
        assert_eq!(Some(marker), load_kv_marker(&open_backend().await?).await?);
    }
    let backend = open_backend().await?;
    store_kv_marker(&backend, Some(valid.clone())).await?;
    let store = kv_backend::KvStore::initialize_format(backend).await?;
    assert_eq!(
        identity,
        store.initialize_records(fixture::limits()?).await?
    );
    drop(store);
    let backend = open_backend().await?;
    store_kv_marker(&backend, None).await?;
    let error = async {
        let store = kv_backend::KvStore::initialize_format(backend).await?;
        store.initialize_records(fixture::limits()?).await
    }
    .await
    .unwrap_err();
    assert!(matches!(
        error.as_ref(),
        StorageEnum::InfrastructureResult(InfrastructureResultKind::Encoding { .. })
    ));
    assert!(load_kv_marker(&open_backend().await?).await?.is_none());
    let backend = open_backend().await?;
    store_kv_marker(&backend, Some(valid)).await?;
    let store = kv_backend::KvStore::initialize_format(backend).await?;
    store.initialize_records(fixture::limits()?).await?;
    verify_golden_record_present(&store).await?;
    drop(store);
    Ok(())
}

#[cfg(any(feature = "native-rocksdb", feature = "native-foundationdb"))]
async fn store_kv_marker<B: kv_backend::KvBackend>(
    backend: &B,
    marker: Option<Vec<u8>>,
) -> StorageResult<()> {
    use kv_backend::KvAttempt as _;

    backend
        .with_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                // Fixture corruption uses the persisted V1 marker grammar,
                // never a fake lowerer.
                let key = b"\x7fi/v".to_vec();
                let change = match marker {
                    Some(value) => kv_backend::KvMutation::Put { key, value },
                    None => kv_backend::KvMutation::Delete { key },
                };
                attempt.stage(vec![change])
            })
        })
        .await
}

#[cfg(any(feature = "native-rocksdb", feature = "native-foundationdb"))]
async fn load_kv_marker<B: kv_backend::KvBackend>(backend: &B) -> StorageResult<Option<Vec<u8>>> {
    use kv_backend::KvAttempt as _;

    backend
        .with_attempt(AttemptMode::ReadOnly, |attempt| {
            Box::pin(async move {
                attempt
                    .get(b"\x7fi/v", storage_types::ReadProtection::Snapshot)
                    .await
            })
        })
        .await
}

fn assert_identity(identity: &[u8]) {
    assert_eq!(16, identity.len());
    assert!(identity.iter().any(|byte| *byte != 0));
}

fn assert_layout_rejected<T>(result: HarnessResult<T>) {
    let error = match result {
        Ok(_) => panic!("corrupt physical layout qualified"),
        Err(error) => error,
    };
    assert!(matches!(
        error
            .downcast_ref::<StorageError>()
            .map(StorageError::as_ref),
        Some(StorageEnum::InfrastructureResult(
            InfrastructureResultKind::Encoding { .. }
        ))
    ));
}

fn require_native_layout_failure<T>(result: HarnessResult<T>) -> HarnessResult<()> {
    let error = result
        .err()
        .ok_or("established marker accepted a missing records relation")?;
    assert!(
        matches!(
            error
                .downcast_ref::<StorageError>()
                .map(StorageError::as_ref),
            Some(StorageEnum::InfrastructureResult(
                InfrastructureResultKind::Encoding {
                    context: InfrastructureContext::PhysicalLayout
                }
            ))
        ),
        "missing records relation returned {error:?}"
    );
    Ok(())
}

async fn verify_golden_record_present<P>(port: &P) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let generations = [0, 1, i64::MAX as u64, 1_u64 << 63, u64::MAX];
    let result = fixture::with_records(port, AttemptMode::ReadOnly, move |attempt| {
        Box::pin(async move {
            let keys = generations
                .into_iter()
                .map(key)
                .collect::<StorageResult<Vec<_>>>()?;
            attempt
                .load_records(
                    &keys,
                    storage_types::ReadProtection::Snapshot,
                    fixture::limits()?,
                )
                .await
        })
    })
    .await?;
    assert_eq!(generations.len(), result.len());
    for (generation, record) in generations.into_iter().zip(result) {
        let record = record.ok_or("native reopen lost record")?;
        assert_eq!(key(generation)?, record.key);
        assert_eq!(0, u64::from(record.revision));
        assert_eq!(b"golden", record.bytes.as_ref());
    }
    Ok(())
}

async fn seed_golden_records<P>(port: &P) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let changes = [0, 1, i64::MAX as u64, 1_u64 << 63, u64::MAX]
        .into_iter()
        .map(|generation| {
            Ok(RecordChange::Store {
                expected: RecordExpectation::Absent,
                record: DurableRecord {
                    key: key(generation)?,
                    revision: 0.into(),
                    bytes: Arc::from(&b"golden"[..]),
                },
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let result = fixture::with_records(port, AttemptMode::ReadWrite, move |attempt| {
        Box::pin(async move {
            attempt
                .store_records(&RecordBatch::try_new(&changes, fixture::limits()?)?)
                .await
        })
    })
    .await?;
    assert_eq!(ConditionalWriteResult::Applied, result);
    Ok(())
}

async fn verify_maximum_revision<P>(port: &P) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let target = key(u64::MAX)?;
    let original = target.clone();
    let revision = fixture::with_records(port, AttemptMode::ReadWrite, move |attempt| {
        Box::pin(async move {
            let changes = [RecordChange::Store {
                expected: RecordExpectation::Revision((u64::MAX - 1).into()),
                record: DurableRecord {
                    key: target.clone(),
                    revision: u64::MAX.into(),
                    bytes: Arc::from(&b"maximum"[..]),
                },
            }];
            assert_eq!(
                ConditionalWriteResult::Applied,
                attempt
                    .store_records(&RecordBatch::try_new(&changes, fixture::limits()?)?)
                    .await?
            );
            let record = attempt
                .load_records(
                    &[target],
                    storage_types::ReadProtection::Conflict,
                    fixture::limits()?,
                )
                .await?
                .pop()
                .flatten()
                .ok_or::<StorageError>(
                    InfrastructureResultKind::Encoding {
                        context: InfrastructureContext::RecordEncoding,
                    }
                    .into(),
                )?;
            Ok(record.revision)
        })
    })
    .await?;
    assert_eq!(u64::MAX, u64::from(revision));
    assert!(
        matches!((revision.checked_next()).as_ref(), Err(error) if matches!(error.as_ref(), StorageEnum::InfrastructureResult(InfrastructureResultKind::InvalidBound { .. })))
    );
    let persisted = fixture::with_records(port, AttemptMode::ReadOnly, move |attempt| {
        Box::pin(async move {
            attempt
                .load_records(
                    &[original],
                    storage_types::ReadProtection::Snapshot,
                    fixture::limits()?,
                )
                .await
        })
    })
    .await?;
    let record = persisted[0]
        .as_ref()
        .ok_or("maximum revision disappeared")?;
    assert_eq!(u64::MAX, u64::from(record.revision));
    assert_eq!(b"maximum", record.bytes.as_ref());
    Ok(())
}

fn golden_columns() -> Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    [0, 1, i64::MAX as u64, 1_u64 << 63, u64::MAX]
        .map(|generation| {
            (
                generation.to_be_bytes().to_vec(),
                vec![0; 8],
                b"binary\0resource".to_vec(),
            )
        })
        .to_vec()
}

fn key(generation: u64) -> StorageResult<DurableRecordKey> {
    RecordAddress {
        namespace: "native-golden".into(),
        resource: b"binary\0resource".to_vec(),
        generation,
        suffix: vec![0, 255],
    }
    .try_into()
}
