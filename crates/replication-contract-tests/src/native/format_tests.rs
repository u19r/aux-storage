use std::{num::NonZeroUsize, sync::Arc};

use infrastructure_provider::{
    batch::RecordBatch,
    physical_attempt::PhysicalRecordAttempt,
    record::{
        ConditionalWriteResult, DurableRecord, DurableRecordKey, RecordAddress, RecordChange,
        RecordExpectation,
    },
};
use replication_provider::{error::RecordError, record::RecordCodec};
use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_provider::DynamoPort;
use storage_types::{
    AttemptMode, InfrastructureContext, InfrastructureResultKind, StorageEnum, StorageError,
    StorageResult,
};

use super::fixture;
use crate::HarnessResult;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CounterRecord {
    counter: u64,
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_memory_sqlite_envelopes_when_corrupt_or_over_bound_then_typed_rejection_precedes_effects()
-> HarnessResult<()> {
    verify_envelopes(&fixture::memory_sqlite().await?).await
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_file_sqlite_envelopes_when_corrupt_or_over_bound_then_typed_rejection_precedes_effects()
-> HarnessResult<()> {
    let fixture = fixture::FileSqlite::try_new().await?;
    verify_envelopes(&fixture.store).await
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_postgres_envelopes_when_corrupt_or_over_bound_then_typed_rejection_precedes_effects()
-> HarnessResult<()> {
    let fixture = fixture::Postgres::try_new().await?;
    let result = verify_envelopes(&fixture.store).await;
    fixture.finish().await?;
    result
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_memory_sqlite_aggregate_load_when_returned_bytes_exceed_qualification_then_no_effect_commits()
-> HarnessResult<()> {
    verify_aggregate_load_bound(&fixture::memory_sqlite().await?).await
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_file_sqlite_aggregate_load_when_returned_bytes_exceed_qualification_then_no_effect_commits()
-> HarnessResult<()> {
    let fixture = fixture::FileSqlite::try_new().await?;
    verify_aggregate_load_bound(&fixture.store).await
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_postgres_aggregate_load_when_returned_bytes_exceed_qualification_then_no_effect_commits()
-> HarnessResult<()> {
    let fixture = fixture::Postgres::try_new().await?;
    let result = verify_aggregate_load_bound(&fixture.store).await;
    fixture.finish().await?;
    result
}

#[cfg(feature = "native-turso")]
#[tokio::test]
async fn given_turso_envelopes_when_corrupt_or_over_bound_then_typed_rejection_precedes_effects()
-> HarnessResult<()> {
    let fixture = super::turso::FileTurso::try_new().await?;
    verify_envelopes(&fixture.store).await
}

#[cfg(feature = "native-turso")]
#[tokio::test]
async fn given_turso_aggregate_load_when_returned_bytes_exceed_qualification_then_no_effect_commits()
-> HarnessResult<()> {
    let fixture = super::turso::FileTurso::try_new().await?;
    verify_aggregate_load_bound(&fixture.store).await
}

#[cfg(feature = "native-rocksdb")]
#[tokio::test]
async fn given_rocksdb_envelopes_when_corrupt_or_over_bound_then_typed_rejection_precedes_effects()
-> HarnessResult<()> {
    let fixture = super::rocksdb::RocksDb::try_new().await?;
    verify_envelopes(&fixture.store).await
}

#[cfg(feature = "native-rocksdb")]
#[tokio::test]
async fn given_rocksdb_aggregate_load_when_returned_bytes_exceed_qualification_then_no_effect_commits()
-> HarnessResult<()> {
    let fixture = super::rocksdb::RocksDb::try_new().await?;
    verify_aggregate_load_bound(&fixture.store).await
}

#[cfg(feature = "native-foundationdb")]
#[tokio::test]
async fn given_foundationdb_envelopes_when_corrupt_or_over_bound_then_typed_rejection_precedes_effects()
-> HarnessResult<()> {
    let fixture = super::foundationdb::FoundationDb::try_new().await?;
    let result = verify_envelopes(&fixture.store).await;
    fixture.finish().await?;
    result
}

#[cfg(feature = "native-foundationdb")]
#[tokio::test]
async fn given_foundationdb_aggregate_load_when_returned_bytes_exceed_qualification_then_no_effect_commits()
-> HarnessResult<()> {
    let fixture = super::foundationdb::FoundationDb::try_new().await?;
    let result = verify_aggregate_load_bound(&fixture.store).await;
    fixture.finish().await?;
    result
}

// N14: production envelope decoding consumes bytes loaded from the actual
// store.
async fn verify_envelopes<P>(port: &P) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let codec = codec()?;
    let encoded = codec.encode(&CounterRecord { counter: 1 })?;
    assert_eq!(golden_record(), encoded.as_ref());
    let clean = key(b"clean".to_vec())?;
    let encoded_key = codec.encode(&clean)?;
    assert_eq!(codec.encode(clean.as_ref())?, encoded_key);
    assert_eq!(
        clean,
        codec.decode::<DurableRecordKey>(encoded_key.as_ref())?
    );
    for (namespace, resource) in [("", vec![1]), ("bad\0scope", vec![1]), ("valid", vec![])] {
        let invalid = RecordAddress {
            namespace: namespace.to_owned(),
            resource,
            generation: 0,
            suffix: vec![],
        };
        assert!(
            codec
                .decode::<DurableRecordKey>(codec.encode(&invalid)?.as_ref())
                .is_err()
        );
    }
    store_envelope(port, clean.clone(), encoded.as_ref().to_vec()).await?;
    assert_eq!(
        Ok(CounterRecord { counter: 1 }),
        decode_with_effect(port, clean, key(b"clean-effect".to_vec())?)
            .await?
            .map_err(|error| error.to_string())
    );
    for (index, (bytes, expected)) in invalid_envelopes(encoded.as_ref()).into_iter().enumerate() {
        let stored = key(format!("invalid-{index}").into_bytes())?;
        let effect = key(format!("effect-{index}").into_bytes())?;
        store_envelope(port, stored.clone(), bytes).await?;
        let error = decode_with_effect(port, stored, effect.clone())
            .await?
            .expect_err("invalid envelope decoded");
        expected.assert_matches(&error);
        assert!(
            fixture::with_records(port, AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    Ok(attempt
                        .load_records(
                            &[effect],
                            storage_types::ReadProtection::Snapshot,
                            fixture::limits()?,
                        )
                        .await?[0]
                        .is_none())
                })
            })
            .await?
        );
    }
    let oversized = vec![RecordChange::Store {
        expected: RecordExpectation::Absent,
        record: DurableRecord {
            key: key(b"physical-overflow".to_vec())?,
            revision: 0.into(),
            bytes: Arc::from(vec![0; 4097]),
        },
    }];
    assert!(
        matches!((RecordBatch::try_new(&oversized, fixture::limits()?)).as_ref(), Err(error) if matches!(error.as_ref(), StorageEnum::InfrastructureResult(InfrastructureResultKind::InvalidBound { .. })))
    );
    assert!(RecordCodec::try_new(NonZeroUsize::new(38).expect("header length")).is_err());
    Ok(())
}

async fn decode_with_effect<P>(
    port: &P,
    stored: DurableRecordKey,
    effect: DurableRecordKey,
) -> HarnessResult<Result<CounterRecord, RecordError>>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    Ok(
        fixture::with_records(port, AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let record = attempt
                    .load_records(
                        &[stored],
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
                let decoded = codec()
                    .map_err(|_| {
                        StorageError::from(InfrastructureResultKind::Encoding {
                            context: InfrastructureContext::PhysicalLayout,
                        })
                    })?
                    .decode::<CounterRecord>(&record.bytes);
                if decoded.is_err() {
                    return Ok(decoded);
                }
                let changes = [RecordChange::Store {
                    expected: RecordExpectation::Absent,
                    record: DurableRecord {
                        key: effect,
                        revision: 0.into(),
                        bytes: Arc::from(&b"decoded"[..]),
                    },
                }];
                assert_eq!(
                    ConditionalWriteResult::Applied,
                    attempt
                        .store_records(&RecordBatch::try_new(&changes, fixture::limits()?)?)
                        .await?
                );
                Ok(decoded)
            })
        })
        .await?,
    )
}

async fn verify_aggregate_load_bound<P>(port: &P) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let keys = (0..5)
        .map(|index| key(vec![index]))
        .collect::<StorageResult<Vec<_>>>()?;
    for key in &keys {
        store_envelope(port, key.clone(), vec![0; 3500]).await?;
    }
    let result = fixture::with_records(port, AttemptMode::ReadWrite, move |attempt| {
        Box::pin(async move {
            let _ = attempt
                .load_records(
                    &keys,
                    storage_types::ReadProtection::Conflict,
                    fixture::limits()?,
                )
                .await?;
            let changes = [RecordChange::Store {
                expected: RecordExpectation::Absent,
                record: DurableRecord {
                    key: key(b"aggregate-effect".to_vec())?,
                    revision: 0.into(),
                    bytes: Arc::from(&b"wrong"[..]),
                },
            }];
            attempt
                .store_records(&RecordBatch::try_new(&changes, fixture::limits()?)?)
                .await
        })
    })
    .await;
    assert!(
        matches!((result).as_ref(), Err(error) if matches!(error.as_ref(), StorageEnum::InfrastructureResult(InfrastructureResultKind::NativeCapacityRejected | InfrastructureResultKind::InvalidBound { context: storage_types::InfrastructureContext::Bounds }))),
        "aggregate result: {result:?}"
    );
    let target = key(b"aggregate-effect".to_vec())?;
    assert!(
        fixture::with_records(port, AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                Ok(attempt
                    .load_records(
                        &[target],
                        storage_types::ReadProtection::Snapshot,
                        fixture::limits()?,
                    )
                    .await?[0]
                    .is_none())
            })
        })
        .await?
    );
    Ok(())
}

fn invalid_envelopes(valid: &[u8]) -> Vec<(Vec<u8>, ExpectedFailure)> {
    let mut wrong_magic = valid.to_vec();
    wrong_magic[0] = b'X';
    let mut wrong_version = valid.to_vec();
    wrong_version[5] = 2;
    let mut wrong_digest = valid.to_vec();
    wrong_digest[6] ^= 1;
    let mut cases = vec![
        (wrong_magic, ExpectedFailure::Corrupt),
        (wrong_version, ExpectedFailure::Version),
        (wrong_digest, ExpectedFailure::Corrupt),
        (valid[..valid.len() - 1].to_vec(), ExpectedFailure::Corrupt),
        (vec![0; 513], ExpectedFailure::Bound),
        (
            envelope(br#"{"counter":1,"unexpected":true}"#),
            ExpectedFailure::Encoding,
        ),
        (
            envelope(br#"{"counter":18446744073709551616}"#),
            ExpectedFailure::Encoding,
        ),
        (
            envelope(br#"{"counter":1,"declared_bytes":18446744073709551615}"#),
            ExpectedFailure::Encoding,
        ),
    ];
    cases.extend((0..38).map(|length| (valid[..length].to_vec(), ExpectedFailure::Corrupt)));
    cases
}

enum ExpectedFailure {
    Corrupt,
    Version,
    Bound,
    Encoding,
}

impl ExpectedFailure {
    fn assert_matches(&self, error: &RecordError) {
        assert!(matches!(
            (self, error),
            (Self::Corrupt, RecordError::Corrupt)
                | (Self::Version, RecordError::IncompatibleVersion)
                | (Self::Bound, RecordError::TooLarge)
                | (Self::Encoding, RecordError::Encoding)
        ));
    }
}

fn golden_record() -> Vec<u8> {
    let mut bytes = b"ARPL\0\x01".to_vec();
    bytes.extend([
        0x55, 0x45, 0x9e, 0x86, 0x3d, 0x1c, 0x8f, 0xcd, 0xaf, 0xf7, 0xac, 0x18, 0x54, 0x9f, 0x96,
        0xdb, 0x44, 0x95, 0xea, 0x3a, 0x74, 0xc4, 0xd6, 0x59, 0xe4, 0x10, 0xcd, 0x55, 0x4d, 0x65,
        0xff, 0x43,
    ]);
    bytes.extend(br#"{"counter":1}"#);
    bytes
}

fn envelope(payload: &[u8]) -> Vec<u8> {
    let mut bytes = b"ARPL\0\x01".to_vec();
    bytes.extend(CanonicalDigest::of(payload).as_ref());
    bytes.extend(payload);
    bytes
}

fn codec() -> HarnessResult<RecordCodec> {
    Ok(RecordCodec::try_new(
        NonZeroUsize::new(512).expect("codec bound"),
    )?)
}

fn key(suffix: Vec<u8>) -> StorageResult<DurableRecordKey> {
    RecordAddress {
        namespace: "native-envelope".into(),
        resource: b"format\0one".to_vec(),
        generation: 1,
        suffix,
    }
    .try_into()
}

async fn store_envelope<P>(port: &P, key: DurableRecordKey, bytes: Vec<u8>) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let changes = [RecordChange::Store {
        expected: RecordExpectation::Absent,
        record: DurableRecord {
            key,
            revision: 0.into(),
            bytes: Arc::from(bytes),
        },
    }];
    let applied = fixture::with_records(port, AttemptMode::ReadWrite, move |attempt| {
        Box::pin(async move {
            attempt
                .store_records(&RecordBatch::try_new(&changes, fixture::limits()?)?)
                .await
        })
    })
    .await?;
    assert_eq!(ConditionalWriteResult::Applied, applied);
    Ok(())
}
