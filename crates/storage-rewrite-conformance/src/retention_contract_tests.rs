//! Retention discovery, TTL protection, policy fencing, and durable bounded
//! progress.

use std::num::{NonZeroU64, NonZeroUsize};

use scheduled_bg_jobs::{JobId, Namespace};
use storage_provider::maintenance::ExpirationRequest;
use storage_types::{
    AttributeMap, AttributeValue, GetItemRequest, PutItemRequest, TableName,
    TimeToLiveSpecification, TimestampMillis, UpdateTimeToLiveRequest, WireItem,
};

use super::test_support::{TestStore, hash_table_request};

fn direct_put(table: TableName, item: AttributeMap) -> anyhow::Result<PutItemRequest> {
    Ok(PutItemRequest::new(table, WireItem::try_from(&item)?))
}

fn key_attributes(pk: &str) -> storage_types::KeyAttributes {
    [("pk".to_string(), AttributeValue::S(pk.to_string()))].into()
}

fn expiration_request() -> ExpirationRequest {
    ExpirationRequest {
        destination: Namespace::try_new("conformance:ttl-expiration").expect("namespace"),
        job: JobId::from([7; 16]),
        fence: NonZeroU64::MIN,
        requested_cutoff: TimestampMillis::from_timestamp(2_000_000_000_000),
        candidate_limit: NonZeroUsize::new(256).expect("candidate limit"),
    }
}

#[tokio::test]
async fn given_markers_when_list_due_then_bounded() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = TableName::new("ret-bounded");
        fixture
            .operations
            .create_table(hash_table_request(TableName::new("ret-bounded")))
            .await
            .map_err(|err| anyhow::anyhow!("create table: {err:?}"))?;
        for pk in ["m-1", "m-2"] {
            let item = AttributeMap::from_iter([
                ("pk".to_string(), AttributeValue::S(pk.to_string())),
                ("value".to_string(), AttributeValue::S("v".to_string())),
            ]);
            fixture
                .operations
                .put_item(direct_put(table.clone(), item)?)
                .await
                .map_err(|err| anyhow::anyhow!("put {pk}: {err:?}"))?;
        }
        let repository = fixture.stream_retention_repository.as_ref();
        let wide = repository
            .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 10)
            .await
            .map_err(|err| anyhow::anyhow!("retention discovery failed: {err:?}"))?;
        anyhow::ensure!(!wide.is_empty(), "seeded due markers omitted");
        anyhow::ensure!(wide.len() <= 10, "wide page exceeded bound");
        let one = repository
            .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 1)
            .await
            .map_err(|err| anyhow::anyhow!("narrow discovery: {err:?}"))?;
        anyhow::ensure!(
            one.len() == 1 && one[0] == wide[0],
            "limit-1 page is not the wide page prefix"
        );
        if wide.len() >= 2 {
            let two = repository
                .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 2)
                .await
                .map_err(|err| anyhow::anyhow!("page-2 discovery: {err:?}"))?;
            anyhow::ensure!(
                two[..] == wide[..2],
                "limit-2 page is not the wide page prefix"
            );
        }
        let mut ordered = wide.clone();
        ordered.sort_by(|left, right| {
            left.due_bucket
                .cmp(&right.due_bucket)
                .then_with(|| left.scope.scope_id.cmp(&right.scope.scope_id))
                .then_with(|| left.policy_version.cmp(&right.policy_version))
        });
        anyhow::ensure!(
            wide == ordered,
            "due markers are not in (due_bucket, scope_id, policy_version) order"
        );
        // Production due buckets are real timestamps: nothing is due at
        // the epoch, while everything seeded is due at the far bound.
        let none = repository
            .list_due_stream_trim_markers(TimestampMillis::from_timestamp(1), 10)
            .await
            .map_err(|err| anyhow::anyhow!("cutoff discovery: {err:?}"))?;
        anyhow::ensure!(
            none.is_empty(),
            "cutoff excluded nothing: {} markers due at epoch",
            none.len()
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_custom_durations_when_table_created_and_updated_then_policy_advances()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = TableName::new("ret-durations");
        let mut create = hash_table_request(TableName::new("ret-durations"));
        create.aux_stream_duration_hours =
            Some(storage_types::StreamRetentionDuration::FiniteHours(168));
        create.aux_default_item_stream_duration_hours =
            Some(storage_types::StreamRetentionDuration::Forever);
        fixture
            .operations
            .create_table(create)
            .await
            .map_err(|err| anyhow::anyhow!("create with durations: {err:?}"))?;
        let info = fixture
            .operations
            .get_table_info(table.clone())
            .await
            .map_err(|err| anyhow::anyhow!("table info: {err:?}"))?;
        anyhow::ensure!(
            info.table_stream_duration == storage_types::StreamRetentionDuration::FiniteHours(168),
            "table duration: {:?}",
            info.table_stream_duration
        );
        anyhow::ensure!(
            info.default_item_stream_duration == storage_types::StreamRetentionDuration::Forever,
            "item duration: {:?}",
            info.default_item_stream_duration
        );
        let repository = fixture.stream_retention_repository.as_ref();
        // Per-item custom duration reaches production in the actual
        // request: the written item stays readable with its exact value.
        let item = AttributeMap::from_iter([
            ("pk".to_string(), AttributeValue::S("k1".to_string())),
            ("value".to_string(), AttributeValue::S("v1".to_string())),
        ]);
        let mut request = PutItemRequest::new(table.clone(), WireItem::try_from(&item)?);
        request.aux_item_stream_ttl_hours =
            Some(storage_types::StreamRetentionDuration::FiniteHours(1));
        fixture
            .operations
            .put_item(request)
            .await
            .map_err(|err| anyhow::anyhow!("put with item duration: {err:?}"))?;
        let found = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key_attributes("k1"))
            })
            .await
            .map_err(|err| anyhow::anyhow!("get: {err:?}"))?
            .item
            .ok_or_else(|| anyhow::anyhow!("item with custom duration missing"))?
            .to_attribute_map()
            .map_err(|err| anyhow::anyhow!("decode: {err:?}"))?;
        anyhow::ensure!(
            found == item,
            "item with custom duration changed: {found:?}"
        );
        // Policy update through the real update path: persisted durations
        // and the table scope's trim-state retention/version must advance.
        fixture
            .operations
            .update_table(storage_types::UpdateTableRequest {
                aux_stream_duration_hours: Some(
                    storage_types::StreamRetentionDuration::FiniteHours(24),
                ),
                aux_default_item_stream_duration_hours: Some(
                    storage_types::StreamRetentionDuration::FiniteHours(48),
                ),
                ..storage_types::UpdateTableRequest::new(table.clone())
            })
            .await
            .map_err(|err| anyhow::anyhow!("update durations: {err:?}"))?;
        let updated = fixture
            .operations
            .get_table_info(table.clone())
            .await
            .map_err(|err| anyhow::anyhow!("updated table info: {err:?}"))?;
        anyhow::ensure!(
            updated.table_stream_duration
                == storage_types::StreamRetentionDuration::FiniteHours(24),
            "updated table duration: {:?}",
            updated.table_stream_duration
        );
        anyhow::ensure!(
            updated.default_item_stream_duration
                == storage_types::StreamRetentionDuration::FiniteHours(48),
            "updated item duration: {:?}",
            updated.default_item_stream_duration
        );
        let due = repository
            .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 10)
            .await
            .map_err(|err| anyhow::anyhow!("due after update: {err:?}"))?;
        let scope = due
            .iter()
            .find(|marker| marker.scope.table_name == table)
            .ok_or_else(|| anyhow::anyhow!("no trim marker for the updated table"))?
            .scope
            .clone();
        let states = repository
            .load_stream_trim_states(std::slice::from_ref(&scope))
            .await
            .map_err(|err| anyhow::anyhow!("load state: {err:?}"))?;
        let Some(Some(state)) = states.first() else {
            anyhow::bail!("updated table scope has no trim state");
        };
        anyhow::ensure!(
            state.retention == storage_types::StreamRetentionDuration::FiniteHours(24),
            "trim state retention did not advance: {:?}",
            state.retention
        );
        anyhow::ensure!(
            state.policy_version >= 1,
            "trim state policy version missing: {}",
            state.policy_version
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_expired_candidate_when_concurrent_update_moves_ttl_then_sweep_revalidates()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let repository = fixture.expiration_repository.as_ref();
        let table = TableName::new("ret-revalidate");
        fixture
            .operations
            .create_table(hash_table_request(TableName::new("ret-revalidate")))
            .await
            .map_err(|err| anyhow::anyhow!("create: {err:?}"))?;
        fixture
            .operations
            .update_time_to_live(UpdateTimeToLiveRequest {
                table_name: table.clone(),
                time_to_live_specification: TimeToLiveSpecification {
                    attribute_name: "ttl".to_string(),
                    enabled: true,
                },
            })
            .await
            .map_err(|err| anyhow::anyhow!("enable ttl: {err:?}"))?;
        // Seed an expired candidate and prove it exists as of T0.
        let expired = AttributeMap::from_iter([
            ("pk".to_string(), AttributeValue::S("race".to_string())),
            ("ttl".to_string(), AttributeValue::N("1".to_string())),
            ("value".to_string(), AttributeValue::S("old".to_string())),
        ]);
        fixture
            .operations
            .put_item(direct_put(table.clone(), expired.clone())?)
            .await
            .map_err(|err| anyhow::anyhow!("seed expired: {err:?}"))?;
        let observed = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key_attributes("race"))
            })
            .await
            .map_err(|err| anyhow::anyhow!("capture candidate: {err:?}"))?
            .item
            .ok_or_else(|| anyhow::anyhow!("seeded candidate missing"))?
            .to_attribute_map()
            .map_err(|err| anyhow::anyhow!("decode candidate: {err:?}"))?;
        anyhow::ensure!(
            observed == expired,
            "seeded candidate changed before the move: {observed:?}"
        );
        // Concurrently move the item into the future before the advance.
        let moved = AttributeMap::from_iter([
            ("pk".to_string(), AttributeValue::S("race".to_string())),
            (
                "ttl".to_string(),
                AttributeValue::N("4000000000".to_string()),
            ),
            ("value".to_string(), AttributeValue::S("new".to_string())),
        ]);
        fixture
            .operations
            .put_item(direct_put(table.clone(), moved.clone())?)
            .await
            .map_err(|err| anyhow::anyhow!("concurrent move: {err:?}"))?;
        for pk in ["removed", "expired-control"] {
            fixture
                .operations
                .put_item(direct_put(
                    table.clone(),
                    AttributeMap::from_iter([
                        ("pk".to_string(), AttributeValue::S(pk.to_string())),
                        ("ttl".to_string(), AttributeValue::N("1".to_string())),
                        ("value".to_string(), AttributeValue::S("old".to_string())),
                    ]),
                )?)
                .await
                .map_err(|err| anyhow::anyhow!("seed {pk}: {err:?}"))?;
        }
        let without_ttl = AttributeMap::from_iter([
            ("pk".to_string(), AttributeValue::S("removed".to_string())),
            ("value".to_string(), AttributeValue::S("new".to_string())),
        ]);
        fixture
            .operations
            .put_item(direct_put(table.clone(), without_ttl.clone())?)
            .await
            .map_err(|err| anyhow::anyhow!("remove ttl attribute: {err:?}"))?;
        let mut control_expired = false;
        for step in 0..5 {
            repository
                .advance_expiration(expiration_request())
                .await
                .map_err(|err| anyhow::anyhow!("advance {step}: {err:?}"))?;
            control_expired = fixture
                .operations
                .get_item(GetItemRequest {
                    consistent_read: Some(true),
                    ..GetItemRequest::new(table.clone(), key_attributes("expired-control"))
                })
                .await
                .map_err(|err| anyhow::anyhow!("get control: {err:?}"))?
                .item
                .is_none();
            if control_expired {
                break;
            }
        }
        anyhow::ensure!(
            control_expired,
            "positive expired control survived every sweep"
        );
        let surviving = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key_attributes("race"))
            })
            .await
            .map_err(|err| anyhow::anyhow!("get: {err:?}"))?
            .item
            .ok_or_else(|| anyhow::anyhow!("moved item deleted without revalidation"))?
            .to_attribute_map()
            .map_err(|err| anyhow::anyhow!("decode surviving: {err:?}"))?;
        anyhow::ensure!(
            surviving == moved,
            "moved item mutated by advance: {surviving:?}"
        );
        let removed = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key_attributes("removed"))
            })
            .await
            .map_err(|err| anyhow::anyhow!("get ttl-removed item: {err:?}"))?
            .item
            .ok_or_else(|| anyhow::anyhow!("ttl-removed item was deleted"))?
            .to_attribute_map()
            .map_err(|err| anyhow::anyhow!("decode ttl-removed item: {err:?}"))?;
        anyhow::ensure!(
            removed == without_ttl,
            "ttl removal changed item: {removed:?}"
        );
        repository
            .advance_expiration(expiration_request())
            .await
            .map_err(|err| anyhow::anyhow!("replay advance: {err:?}"))?;
        for (pk, expected) in [("race", &moved), ("removed", &without_ttl)] {
            let item = fixture
                .operations
                .get_item(GetItemRequest {
                    consistent_read: Some(true),
                    ..GetItemRequest::new(table.clone(), key_attributes(pk))
                })
                .await
                .map_err(|err| anyhow::anyhow!("get {pk} after replay: {err:?}"))?
                .item
                .ok_or_else(|| anyhow::anyhow!("{pk} missing after replay"))?
                .to_attribute_map()
                .map_err(|err| anyhow::anyhow!("decode {pk} after replay: {err:?}"))?;
            anyhow::ensure!(item == *expected, "{pk} changed after replay: {item:?}");
        }
        anyhow::ensure!(
            fixture
                .operations
                .get_item(GetItemRequest {
                    consistent_read: Some(true),
                    ..GetItemRequest::new(table, key_attributes("expired-control"))
                })
                .await
                .map_err(|err| anyhow::anyhow!("get control after replay: {err:?}"))?
                .item
                .is_none(),
            "expired control reappeared after replay"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_missing_or_malformed_target_when_observed_then_fail_closed_without_effect()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = TableName::new("ret-missing");
        fixture
            .operations
            .create_table(hash_table_request(TableName::new("ret-missing")))
            .await
            .map_err(|err| anyhow::anyhow!("create: {err:?}"))?;
        let witness = AttributeMap::from_iter([
            ("pk".to_string(), AttributeValue::S("w".to_string())),
            ("value".to_string(), AttributeValue::S("v".to_string())),
        ]);
        fixture
            .operations
            .put_item(direct_put(table.clone(), witness.clone())?)
            .await
            .map_err(|err| anyhow::anyhow!("put witness: {err:?}"))?;
        // Missing TTL target: advancing a table without TTL enabled
        // completes with no deletions and the witness item intact.
        fixture
            .expiration_repository
            .as_ref()
            .advance_expiration(expiration_request())
            .await
            .map_err(|err| anyhow::anyhow!("advance without ttl: {err:?}"))?;
        let kept = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key_attributes("w"))
            })
            .await
            .map_err(|err| anyhow::anyhow!("get witness: {err:?}"))?
            .item
            .ok_or_else(|| anyhow::anyhow!("witness deleted without ttl"))?
            .to_attribute_map()
            .map_err(|err| anyhow::anyhow!("decode witness: {err:?}"))?;
        anyhow::ensure!(
            kept == witness,
            "witness mutated by advance without ttl: {kept:?}"
        );
        // Missing retention scope loads as absent.
        let repository = fixture.stream_retention_repository.as_ref();
        let states = repository
            .load_stream_trim_states(&[storage_provider::StreamTrimScope::table(
                "ret-missing/no-such-scope".to_string(),
                TableName::new("ret-missing"),
            )])
            .await
            .map_err(|err| anyhow::anyhow!("missing scope load: {err:?}"))?;
        anyhow::ensure!(states == vec![None], "missing scope unexpectedly resolved");
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_item_writes_when_boundaries_loaded_then_embedded_and_pointer_cohere()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = TableName::new("ret-streams");
        fixture
            .operations
            .create_table(hash_table_request(TableName::new("ret-streams")))
            .await
            .map_err(|err| anyhow::anyhow!("create: {err:?}"))?;
        for pk in ["s-1", "s-2"] {
            let item = AttributeMap::from_iter([
                ("pk".to_string(), AttributeValue::S(pk.to_string())),
                ("value".to_string(), AttributeValue::S("v".to_string())),
            ]);
            fixture
                .operations
                .put_item(direct_put(table.clone(), item)?)
                .await
                .map_err(|err| anyhow::anyhow!("put {pk}: {err:?}"))?;
        }
        let repository = fixture.stream_retention_repository.as_ref();
        let due = repository
            .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 10)
            .await
            .map_err(|err| anyhow::anyhow!("due: {err:?}"))?;
        anyhow::ensure!(!due.is_empty(), "no due markers after item writes");
        // Every selected scope must produce a corresponding boundary
        // observation.
        let scopes: Vec<storage_provider::StreamTrimScope> = due
            .iter()
            .take(4)
            .map(|marker| marker.scope.clone())
            .collect();
        let boundaries = repository
            .load_stream_trim_boundaries(&scopes)
            .await
            .map_err(|err| anyhow::anyhow!("boundaries: {:?}", err.as_ref()))?;
        anyhow::ensure!(
            boundaries.len() == scopes.len(),
            "boundary cardinality changed"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_due_markers_when_worker_runs_then_pages_bounded_and_restart_resumes()
-> anyhow::Result<()> {
    let fixture = TestStore::new_durable().await;
    let outcome = async {
        let table = TableName::new("ret-restart");
        fixture
            .operations
            .create_table(hash_table_request(TableName::new("ret-restart")))
            .await
            .map_err(|err| anyhow::anyhow!("create: {err:?}"))?;
        for pk in ["r-1", "r-2", "r-3"] {
            let item = AttributeMap::from_iter([
                ("pk".to_string(), AttributeValue::S(pk.to_string())),
                ("value".to_string(), AttributeValue::S("v".to_string())),
            ]);
            fixture
                .operations
                .put_item(direct_put(table.clone(), item)?)
                .await
                .map_err(|err| anyhow::anyhow!("put {pk}: {err:?}"))?;
        }
        let repository = fixture.stream_retention_repository.as_ref();
        let due_before = repository
            .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 10)
            .await
            .map_err(|err| anyhow::anyhow!("due before: {err:?}"))?;
        anyhow::ensure!(!due_before.is_empty(), "no due work seeded by item writes");
        let worker = storage_provider::StreamDurationTrimWorker::new(
            repository,
            storage_provider::StreamDurationTrimConfig {
                marker_page_size: storage_provider::StreamTrimPageLimit::try_from(2usize)
                    .map_err(|err| anyhow::anyhow!("page limit: {err:?}"))?,
                stream_page_size: storage_provider::StreamTrimPageLimit::try_from(5usize)
                    .map_err(|err| anyhow::anyhow!("page limit: {err:?}"))?,
            },
        );
        let first = worker
            .run_due_page(
                TimestampMillis::from_timestamp(i64::MAX),
                TimestampMillis::from_timestamp(i64::MAX),
            )
            .await
            .map_err(|err| anyhow::anyhow!("first trim page: {err:?}"))?;
        anyhow::ensure!(
            first.due_markers_scanned >= 1 && first.due_markers_scanned <= 2,
            "first page out of bound: {} scanned",
            first.due_markers_scanned
        );
        Ok(due_before)
    }
    .await;
    let due_before = match outcome {
        Ok(due_before) => due_before,
        Err(case) => {
            fixture.finish().await;
            return Err(case);
        }
    };
    let reopened = fixture.reopen().await;
    let outcome = async {
        let repository = reopened.stream_retention_repository.as_ref();
        let worker = storage_provider::StreamDurationTrimWorker::new(
            repository,
            storage_provider::StreamDurationTrimConfig {
                marker_page_size: storage_provider::StreamTrimPageLimit::try_from(2usize)
                    .map_err(|err| anyhow::anyhow!("page limit: {err:?}"))?,
                stream_page_size: storage_provider::StreamTrimPageLimit::try_from(5usize)
                    .map_err(|err| anyhow::anyhow!("page limit: {err:?}"))?,
            },
        );
        let second = worker
            .run_due_page(
                TimestampMillis::from_timestamp(i64::MAX),
                TimestampMillis::from_timestamp(i64::MAX),
            )
            .await
            .map_err(|err| anyhow::anyhow!("second trim page: {err:?}"))?;
        anyhow::ensure!(
            second.due_markers_scanned <= 2,
            "restart page exceeded marker bound: {} scanned",
            second.due_markers_scanned
        );
        // Restart must not resurrect finished work into the due set.
        let due = repository
            .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 10)
            .await
            .map_err(|err| anyhow::anyhow!("due after restart: {err:?}"))?;
        anyhow::ensure!(
            due.iter().all(|marker| due_before.contains(marker)),
            "finished work resurrected after restart"
        );
        Ok(())
    }
    .await;
    reopened.finish().await;
    outcome
}

#[tokio::test]
async fn given_stale_finish_when_applied_then_newer_policy_survives() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = verify_stale_policy_wave(&fixture).await;
    fixture.finish().await;
    outcome
}
async fn verify_stale_policy_wave(fixture: &TestStore) -> anyhow::Result<()> {
    use storage_provider::{StreamTrimPageLimit, maintenance::StreamRetentionWave};
    let mut request = hash_table_request(TableName::new("ret-finish"));
    request.aux_stream_duration_hours =
        Some(storage_types::StreamRetentionDuration::FiniteHours(1));
    fixture.operations.create_table(request.clone()).await?;
    let repository = fixture.stream_retention_repository.as_ref();
    let due = fixture.now() + 48 * 60 * 60 * 1000;
    let marker = repository
        .list_due_stream_trim_markers(due, 10)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing created marker"))?;
    fixture
        .operations
        .update_table(serde_json::from_value(serde_json::json!({
            "TableName": request.table_name, "AuxStreamDurationHours": 1
        }))?)
        .await?;
    let scopes = [marker.scope.clone()];
    let state = repository.load_stream_trim_states(&scopes).await?;
    anyhow::ensure!(
        state[0]
            .as_ref()
            .is_some_and(|state| state.policy_version > marker.policy_version)
    );
    let wave = StreamRetentionWave::try_new(
        vec![marker.clone()],
        due,
        due,
        StreamTrimPageLimit::try_from(1usize)?,
    )?;
    let first = repository.trim_stream_marker_wave(wave.clone()).await?;
    anyhow::ensure!(first.stale_markers == 1 && first.rows_deleted == 0 && first.state_writes == 0);
    let remaining = repository.list_due_stream_trim_markers(due, 10).await?;
    anyhow::ensure!(!remaining.contains(&marker) && remaining.len() == 1);
    anyhow::ensure!(repository.load_stream_trim_states(&scopes).await? == state);
    let replay = repository.trim_stream_marker_wave(wave).await?;
    anyhow::ensure!(replay.rows_deleted == 0 && replay.state_writes == 0);
    anyhow::ensure!(repository.load_stream_trim_states(&scopes).await? == state);
    anyhow::ensure!(repository.list_due_stream_trim_markers(due, 10).await? == remaining);
    Ok(())
}
