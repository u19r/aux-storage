//! Scheduled retention policy and progress through real native transactions.

use std::collections::HashMap;

use anyhow::Context as _;
use storage_provider::{
    StreamTrimPageLimit, StreamTrimScopeKind, maintenance::StreamRetentionWave,
};
use storage_types::{
    AttributeValue, StreamName, StreamRetentionDuration, StreamSpecification, StreamViewType,
    TableName, TimestampMillis,
};

use super::test_support::{TestStore, hash_table_request};

async fn publish_history(fixture: &TestStore) -> anyhow::Result<TableName> {
    let table = TableName::new("scheduled-retention-transaction");
    let mut request = hash_table_request(table.clone());
    request.aux_stream_duration_hours = Some(StreamRetentionDuration::FiniteHours(1));
    request.aux_default_item_stream_duration_hours = Some(StreamRetentionDuration::FiniteHours(1));
    request.stream_specification = Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    });
    fixture.operations.create_table(request).await?;
    for revision in 0..3 {
        let item = HashMap::from([
            ("pk".to_owned(), AttributeValue::S("same-item".to_owned())),
            (
                "value".to_owned(),
                AttributeValue::S(
                    (0..96)
                        .map(|part| {
                            uuid::Uuid::new_v5(
                                &uuid::Uuid::NAMESPACE_OID,
                                format!("{revision}/{part}").as_bytes(),
                            )
                            .to_string()
                        })
                        .collect(),
                ),
            ),
        ]);
        let mut put = storage_types::PutItemRequest::new(
            table.clone(),
            storage_types::WireItem::try_from(&item)?,
        );
        put.aux_item_stream_ttl_hours = Some(StreamRetentionDuration::FiniteHours(1));
        fixture.operations.put_item(put).await?;
    }
    Ok(table)
}

async fn set_retention(
    fixture: &TestStore,
    table: &TableName,
    duration: StreamRetentionDuration,
) -> anyhow::Result<()> {
    fixture
        .operations
        .update_table(serde_json::from_value(serde_json::json!({
            "TableName": table,
            "AuxStreamDurationHours": duration,
        }))?)
        .await?;
    Ok(())
}

async fn discover_wave(
    fixture: &TestStore,
    table: &TableName,
    observed_at: TimestampMillis,
) -> anyhow::Result<StreamRetentionWave> {
    let marker = fixture
        .stream_retention_repository
        .as_ref()
        .list_due_stream_trim_markers(observed_at, 100)
        .await?
        .into_iter()
        .filter(|marker| {
            marker.scope.table_name == *table && marker.scope.kind == StreamTrimScopeKind::Table
        })
        .max_by_key(|marker| marker.policy_version)
        .ok_or_else(|| anyhow::anyhow!("finite table history has no due marker"))?;
    Ok(StreamRetentionWave::try_new(
        vec![marker],
        observed_at,
        observed_at,
        StreamTrimPageLimit::try_from(100_usize)?,
    )?)
}

async fn table_stream(
    fixture: &TestStore,
    table: &TableName,
) -> anyhow::Result<stream_provider::StreamRowKind> {
    let page = fixture
        .stream_operations
        .read_forward(
            stream_provider::StreamRowKind::System(StreamName::system_table_stream()),
            None,
            100,
        )
        .await?;
    for row in page.items {
        let pointer: stream_provider::StoredStreamPointer =
            storage_types::storage_serde::from_bytes(&row.data)?;
        if pointer.table_name() == table {
            return Ok(stream_provider::StreamRowKind::Table(
                storage_types::ItemHistoryAddress {
                    history_id: pointer.history_address().history_id,
                    stream_name: StreamName::table_stream(table),
                }
                .physical_stream_name(),
            ));
        }
    }
    anyhow::bail!("missing system pointer for seeded table")
}

async fn table_rows(fixture: &TestStore, table: &TableName) -> anyhow::Result<usize> {
    Ok(fixture
        .stream_operations
        .read_forward(table_stream(fixture, table).await?, None, 100)
        .await?
        .items
        .len())
}

async fn verify_duplicate_wave(fixture: &TestStore) -> anyhow::Result<()> {
    let table = publish_history(fixture).await?;
    let now = fixture.now() + 2 * 60 * 60 * 1000;
    let valid_wave = discover_wave(fixture, &table, now).await?;
    let marker = valid_wave.markers()[0].clone();
    let scopes = [marker.scope.clone()];
    let rows = table_rows(fixture, &table).await?;
    let state = fixture
        .stream_retention_repository
        .as_ref()
        .load_stream_trim_states(&scopes)
        .await?;
    let invalid_wave = StreamRetentionWave::try_new(
        vec![marker.clone(), marker],
        now,
        now,
        StreamTrimPageLimit::try_from(1_usize)?,
    );
    anyhow::ensure!(
        invalid_wave.is_err(),
        "duplicate scope reached provider execution"
    );
    anyhow::ensure!(
        table_rows(fixture, &table).await? == rows,
        "duplicate wave deleted rows"
    );
    anyhow::ensure!(
        fixture
            .stream_retention_repository
            .as_ref()
            .load_stream_trim_states(&scopes)
            .await?
            == state,
        "duplicate wave changed progress"
    );
    Ok(())
}

#[tokio::test]
async fn given_duplicate_scope_when_wave_is_requested_then_no_history_or_progress_changes()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = verify_duplicate_wave(&fixture).await;
    fixture.finish().await;
    outcome
}

async fn verify_policy_and_replay(fixture: &TestStore) -> anyhow::Result<()> {
    let table = publish_history(fixture).await?;
    let now = fixture.now() + 2 * 60 * 60 * 1000;
    let old_wave = discover_wave(fixture, &table, now).await?;
    let before = table_rows(fixture, &table).await?;
    anyhow::ensure!(before >= 3, "history fixture did not publish three rows");

    set_retention(fixture, &table, StreamRetentionDuration::Forever).await?;
    let stale = fixture
        .stream_retention_repository
        .as_ref()
        .trim_stream_marker_wave(old_wave)
        .await
        .context("stale marker wave")?;
    anyhow::ensure!(
        stale.rows_deleted == 0,
        "old marker deleted rows after policy extension"
    );
    anyhow::ensure!(
        table_rows(fixture, &table).await? == before,
        "old marker changed retained history"
    );

    set_retention(fixture, &table, StreamRetentionDuration::FiniteHours(1)).await?;
    let wave = discover_wave(fixture, &table, now).await?;
    let before_reclaim = table_rows(fixture, &table).await?;
    let first = fixture
        .stream_retention_repository
        .as_ref()
        .trim_stream_marker_wave(wave.clone())
        .await
        .context("current marker wave")?;
    let after_reclaim = table_rows(fixture, &table).await?;
    anyhow::ensure!(
        first.rows_deleted > 0,
        "released policy did not reclaim eligible rows"
    );
    anyhow::ensure!(
        before_reclaim - after_reclaim == first.rows_deleted,
        "reclaim count disagrees with real history"
    );
    let scopes = vec![wave.markers()[0].scope.clone()];
    let state = fixture
        .stream_retention_repository
        .as_ref()
        .load_stream_trim_states(&scopes)
        .await?;
    let replay = fixture
        .stream_retention_repository
        .as_ref()
        .trim_stream_marker_wave(wave)
        .await
        .context("replayed or combined marker wave")?;
    anyhow::ensure!(
        replay.rows_deleted == 0,
        "replayed wave deleted additional history"
    );
    anyhow::ensure!(
        table_rows(fixture, &table).await? == after_reclaim,
        "replayed wave changed physical rows"
    );
    anyhow::ensure!(
        fixture
            .stream_retention_repository
            .as_ref()
            .load_stream_trim_states(&scopes)
            .await?
            == state,
        "replayed wave changed progress"
    );
    Ok(())
}

#[tokio::test]
async fn given_discovered_marker_when_policy_changes_then_current_policy_and_atomic_progress_win()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = verify_policy_and_replay(&fixture).await;
    fixture.finish().await;
    outcome
}

async fn verify_policy_replacement(fixture: &TestStore) -> anyhow::Result<()> {
    let table = publish_history(fixture).await?;
    let now = fixture.now() + 2 * 60 * 60 * 1000;
    let old_wave = discover_wave(fixture, &table, now).await?;
    let marker = old_wave.markers()[0].clone();
    let scopes = [marker.scope.clone()];
    set_retention(fixture, &table, StreamRetentionDuration::Forever).await?;
    set_retention(fixture, &table, StreamRetentionDuration::FiniteHours(1)).await?;
    let states = fixture
        .stream_retention_repository
        .as_ref()
        .load_stream_trim_states(&scopes)
        .await?;
    let current = states[0].as_ref().context("missing replacement state")?;
    anyhow::ensure!(
        current.policy_version > marker.policy_version,
        "returning to the original duration reused an old policy generation"
    );
    let rows = table_rows(fixture, &table).await?;
    let replay = fixture
        .stream_retention_repository
        .as_ref()
        .trim_stream_marker_wave(old_wave)
        .await?;
    anyhow::ensure!(
        replay.rows_deleted == 0 && table_rows(fixture, &table).await? == rows,
        "old policy generation reclaimed current history"
    );
    Ok(())
}

#[tokio::test]
async fn given_replaced_table_policy_when_old_work_replays_then_generation_rejects_old_work()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = verify_policy_replacement(&fixture).await;
    fixture.finish().await;
    outcome
}

async fn verify_rejected_combined_update(fixture: &TestStore) -> anyhow::Result<()> {
    let table = publish_history(fixture).await?;
    let now = fixture.now() + 2 * 60 * 60 * 1000;
    let wave = discover_wave(fixture, &table, now).await?;
    let scopes = [wave.markers()[0].scope.clone()];
    let states = fixture
        .stream_retention_repository
        .as_ref()
        .load_stream_trim_states(&scopes)
        .await?;
    let metadata = fixture.operations.get_table_info(table.clone()).await?;
    let rejected = fixture
        .operations
        .update_table(serde_json::from_value(serde_json::json!({
            "TableName": table,
            "DeletionProtectionEnabled": true,
            "AuxStreamDurationHours": StreamRetentionDuration::Forever,
            "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "absent-index"}}],
        }))?)
        .await;
    anyhow::ensure!(rejected.is_err(), "invalid combined update succeeded");
    anyhow::ensure!(
        fixture
            .stream_retention_repository
            .as_ref()
            .load_stream_trim_states(&scopes)
            .await?
            == states,
        "rejected combined update committed its retention policy"
    );
    anyhow::ensure!(
        serde_json::to_value(
            fixture
                .operations
                .get_table_info(table.clone())
                .await?
                .as_ref()
        )? == serde_json::to_value(metadata.as_ref())?,
        "rejected combined update committed table metadata"
    );
    Ok(())
}

#[tokio::test]
async fn given_rejected_combined_update_when_gsi_is_missing_then_all_settings_remain_unchanged()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = verify_rejected_combined_update(&fixture).await;
    fixture.finish().await;
    outcome
}

async fn verify_consumed_marker(fixture: &TestStore) -> anyhow::Result<()> {
    let table = publish_history(fixture).await?;
    let now = fixture.now() + 2 * 60 * 60 * 1000;
    let repository = fixture.stream_retention_repository.as_ref();
    let marker = repository
        .list_due_stream_trim_markers(now, 100)
        .await?
        .into_iter()
        .filter(|marker| marker.scope.kind == StreamTrimScopeKind::Item)
        .max_by_key(|marker| marker.policy_version)
        .context("missing current item retention marker")?;
    let scopes = [marker.scope.clone()];
    let wave = StreamRetentionWave::try_new(
        vec![marker.clone()],
        now,
        now,
        StreamTrimPageLimit::try_from(1usize)?,
    )?;
    let protected = repository.trim_stream_marker_wave(wave.clone()).await?;
    anyhow::ensure!(
        protected.rows_deleted == 0 && protected.state_writes == 1,
        "protected wave deleted {} rows, wrote {} states, rejected {} stale markers",
        protected.rows_deleted,
        protected.state_writes,
        protected.stale_markers
    );
    let next_marker = marker.rescheduled_after(now, now)?;
    let pointers = fixture
        .stream_operations
        .read_backward(table_stream(fixture, &table).await?, None, 1)
        .await?;
    let pointer: stream_provider::StoredStreamPointer = storage_types::storage_serde::from_bytes(
        &pointers
            .items
            .first()
            .ok_or_else(|| anyhow::anyhow!("missing retained table pointer"))?
            .data,
    )?;
    let item_stream = pointer.history_address().physical_stream_name();
    // Release table and SystemStream pointer ownership through their real
    // operations, leaving eligible old images for the current item wave.
    repository
        .trim_stream_marker_wave(discover_wave(fixture, &table, now).await?)
        .await?;
    let stream = &fixture.stream_operations;
    let scope = stream.system_stream_scope().await?;
    stream
        .get_system_stream_page(stream_provider::SystemStreamPageRequest::try_new(
            scope.clone(),
            None,
            100,
        )?)
        .await?;
    let released = fixture
        .system_stream_trim_repository
        .trim_system_stream_page(stream_provider::SystemStreamTrimPageRequest::try_new(
            scope, now, 100, None,
        )?)
        .await?;
    anyhow::ensure!(released.reclaimed_records() >= 1);
    let markers = repository
        .list_due_stream_trim_markers(next_marker.due_bucket, 100)
        .await?;
    anyhow::ensure!(!markers.contains(&marker), "old marker was not consumed");
    anyhow::ensure!(markers.contains(&next_marker), "replacement marker missing");
    let state = repository.load_stream_trim_states(&scopes).await?;
    let rows = fixture
        .stream_operations
        .read_forward(
            stream_provider::StreamRowKind::Item(item_stream.clone()),
            None,
            100,
        )
        .await?
        .items;
    anyhow::ensure!(
        rows.len() >= 3,
        "fixture must retain multiple eligible rows"
    );
    let replay = repository.trim_stream_marker_wave(wave).await?;
    anyhow::ensure!(
        replay.rows_deleted == 0 && replay.state_writes == 0,
        "consumed marker reclaimed history or changed progress"
    );
    anyhow::ensure!(
        fixture
            .stream_operations
            .read_forward(
                stream_provider::StreamRowKind::Item(item_stream.clone()),
                None,
                100
            )
            .await?
            .items
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>()
            == rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        "replay changed rows"
    );
    anyhow::ensure!(
        repository.load_stream_trim_states(&scopes).await? == state,
        "replay changed current progress"
    );
    anyhow::ensure!(
        repository
            .list_due_stream_trim_markers(next_marker.due_bucket, 100)
            .await?
            == markers,
        "replay changed due markers"
    );
    let resumed = StreamRetentionWave::try_new(
        vec![next_marker.clone()],
        next_marker.due_bucket,
        next_marker.due_bucket,
        StreamTrimPageLimit::try_from(1_usize)?,
    )?;
    let reclaimed = repository.trim_stream_marker_wave(resumed).await?;
    anyhow::ensure!(
        reclaimed.rows_deleted == 1,
        "current marker did not reclaim one page"
    );
    anyhow::ensure!(
        fixture
            .stream_operations
            .read_forward(stream_provider::StreamRowKind::Item(item_stream), None, 100)
            .await?
            .items
            .len()
            == rows.len() - 1,
        "page count differs from history"
    );
    Ok(())
}

#[tokio::test]
async fn given_consumed_marker_when_replayed_then_remaining_history_and_progress_are_unchanged()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = verify_consumed_marker(&fixture).await;
    fixture.finish().await;
    outcome
}

async fn verify_combined_scope_progress(fixture: &TestStore) -> anyhow::Result<()> {
    let table = publish_history(fixture).await?;
    let pointers = fixture
        .stream_operations
        .read_forward(table_stream(fixture, &table).await?, None, 100)
        .await?;
    let pointer: stream_provider::StoredStreamPointer = storage_types::storage_serde::from_bytes(
        &pointers
            .items
            .last()
            .ok_or_else(|| anyhow::anyhow!("missing table pointer"))?
            .data,
    )?;
    let history = pointer.history_address();
    let before = fixture
        .stream_operations
        .read_backward(
            stream_provider::StreamRowKind::Item(history.physical_stream_name()),
            None,
            100,
        )
        .await?;
    anyhow::ensure!(
        before.items.len() == 3,
        "expected three physical item images"
    );
    let scope = fixture.stream_operations.system_stream_scope().await?;
    fixture
        .stream_operations
        .get_system_stream_page(stream_provider::SystemStreamPageRequest::try_new(
            scope.clone(),
            None,
            100,
        )?)
        .await?;
    let now = fixture.now() + 2 * 60 * 60 * 1000;
    let released = fixture
        .system_stream_trim_repository
        .trim_system_stream_page(stream_provider::SystemStreamTrimPageRequest::try_new(
            scope, now, 100, None,
        )?)
        .await?;
    anyhow::ensure!(
        released.reclaimed_records() > 0,
        "system history release did not reclaim old owners"
    );
    let mut markers = std::collections::BTreeMap::new();
    for marker in fixture
        .stream_retention_repository
        .as_ref()
        .list_due_stream_trim_markers(now, 100)
        .await?
    {
        if marker.scope.table_name != table {
            continue;
        }
        markers
            .entry(marker.scope.scope_id.clone())
            .and_modify(|old: &mut storage_provider::StreamTrimDueMarker| {
                if old.policy_version < marker.policy_version {
                    *old = marker.clone();
                }
            })
            .or_insert(marker);
    }
    let item_scope = markers
        .values()
        .find(|marker| marker.scope.kind == StreamTrimScopeKind::Item)
        .ok_or_else(|| anyhow::anyhow!("no item scope in combined wave"))?
        .scope
        .clone();
    anyhow::ensure!(
        markers
            .values()
            .any(|marker| marker.scope.kind == StreamTrimScopeKind::Table),
        "no table scope in combined wave"
    );
    let wave = StreamRetentionWave::try_new(
        markers.into_values().collect(),
        now,
        now,
        StreamTrimPageLimit::try_from(100_usize)?,
    )?;
    fixture
        .stream_retention_repository
        .as_ref()
        .trim_stream_marker_wave(wave)
        .await
        .context("replayed or combined marker wave")?;
    let after = fixture
        .stream_operations
        .read_backward(
            stream_provider::StreamRowKind::Item(history.physical_stream_name()),
            None,
            100,
        )
        .await?;
    let oldest = after
        .items
        .last()
        .ok_or_else(|| anyhow::anyhow!("latest item image was removed"))?;
    let states = fixture
        .stream_retention_repository
        .as_ref()
        .load_stream_trim_states(&[item_scope])
        .await?;
    let state = states
        .first()
        .and_then(Option::as_ref)
        .ok_or_else(|| anyhow::anyhow!("item progress was removed"))?;
    anyhow::ensure!(
        state.oldest_retained_version == Some(storage_types::ItemStreamVersion::from(oldest.id)),
        "durable item progress does not match physical history after combined cleanup"
    );
    Ok(())
}

#[tokio::test]
async fn given_table_and_item_scopes_when_wave_reclaims_then_progress_matches_final_history()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = verify_combined_scope_progress(&fixture).await;
    fixture.finish().await;
    outcome
}

async fn verify_policy_restart() -> anyhow::Result<()> {
    let fixture = TestStore::new_durable().await;
    let table = match publish_history(&fixture).await {
        Ok(table) => table,
        Err(error) => {
            fixture.finish().await;
            return Err(error);
        }
    };
    let now = fixture.now() + 2 * 60 * 60 * 1000;
    let old_wave = discover_wave(&fixture, &table, now).await?;
    let scopes = [old_wave.markers()[0].scope.clone()];
    let initial = old_wave.markers()[0].policy_version;
    let outcome = async {
        let (first, second, third) = tokio::join!(
            set_retention(&fixture, &table, StreamRetentionDuration::FiniteHours(1)),
            set_retention(&fixture, &table, StreamRetentionDuration::FiniteHours(1)),
            set_retention(&fixture, &table, StreamRetentionDuration::FiniteHours(1)),
        );
        first?;
        second?;
        third?;
        let states = fixture
            .stream_retention_repository
            .as_ref()
            .load_stream_trim_states(&scopes)
            .await?;
        anyhow::ensure!(
            states[0].as_ref().context("missing policy")?.policy_version == initial + 3,
            "concurrent replacements lost or reused a generation"
        );
        Ok(())
    }
    .await;
    if let Err(error) = outcome {
        fixture.finish().await;
        return Err(error);
    }
    let fixture = fixture.reopen().await;
    let outcome = async {
        set_retention(&fixture, &table, StreamRetentionDuration::FiniteHours(1)).await?;
        let states = fixture
            .stream_retention_repository
            .as_ref()
            .load_stream_trim_states(&scopes)
            .await?;
        anyhow::ensure!(
            states[0]
                .as_ref()
                .context("missing reopened policy")?
                .policy_version
                == initial + 4,
            "reopened replacement did not retain its generation"
        );
        let rows = table_rows(&fixture, &table).await?;
        let result = fixture
            .stream_retention_repository
            .as_ref()
            .trim_stream_marker_wave(old_wave)
            .await?;
        anyhow::ensure!(
            result.rows_deleted == 0 && table_rows(&fixture, &table).await? == rows,
            "reopened old work deleted replacement history"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_concurrent_same_duration_updates_when_reopened_then_each_generation_is_preserved()
-> anyhow::Result<()> {
    verify_policy_restart().await
}
