//! Scheduled retention policy and progress through real native transactions.

use std::collections::HashMap;

use anyhow::Context as _;
use storage_provider::{StreamTrimPageLimit, maintenance::StreamRetentionWave};
use storage_types::{
    AttributeValue, StreamRetentionDuration, StreamSpecification, StreamViewType, TableName,
    TimestampMillis,
};

use super::{
    image_retention_tests::table_stream,
    test_support::{TestStore, hash_table_request},
};

async fn publish_history(fixture: &TestStore) -> anyhow::Result<TableName> {
    let table = TableName::new("scheduled-retention-transaction");
    let mut request = hash_table_request(table.clone());
    request.aux_stream_duration_hours = Some(StreamRetentionDuration::from_hours(24).unwrap());
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
        let put = storage_types::PutItemRequest::new(
            table.clone(),
            storage_types::WireItem::try_from(&item)?,
        );
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
        .filter(|marker| marker.scope.table_name == *table)
        .max_by_key(|marker| marker.policy_version)
        .ok_or_else(|| anyhow::anyhow!("finite table history has no due marker"))?;
    Ok(StreamRetentionWave::try_new(
        vec![marker],
        observed_at,
        observed_at,
        StreamTrimPageLimit::try_from(100_usize)?,
    )?)
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
    let now = fixture.advance_time(std::time::Duration::from_secs(25 * 3600));
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
    let now = fixture.advance_time(std::time::Duration::from_secs(25 * 3600));
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

    set_retention(
        fixture,
        &table,
        StreamRetentionDuration::from_hours(24).unwrap(),
    )
    .await?;
    let wave = discover_wave(fixture, &table, now).await?;
    let first = fixture
        .stream_retention_repository
        .trim_stream_marker_wave(wave.clone())
        .await?;
    let next_due = fixture.advance_time(std::time::Duration::from_millis(1));
    let next_wave = discover_wave(fixture, &table, next_due).await?;
    let second = fixture
        .stream_retention_repository
        .trim_stream_marker_wave(next_wave)
        .await?;
    let reclaimed = first.rows_deleted + second.rows_deleted;
    anyhow::ensure!(
        reclaimed == 5,
        "three pointers and two obsolete images must be reclaimed"
    );
    let after_reclaim = table_rows(fixture, &table).await?;
    anyhow::ensure!(
        after_reclaim == 0,
        "pointer inventory must drain after bounded history cleanup"
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
    let now = fixture.advance_time(std::time::Duration::from_secs(25 * 3600));
    let old_wave = discover_wave(fixture, &table, now).await?;
    let marker = old_wave.markers()[0].clone();
    let scopes = [marker.scope.clone()];
    set_retention(fixture, &table, StreamRetentionDuration::Forever).await?;
    set_retention(
        fixture,
        &table,
        StreamRetentionDuration::from_hours(24).unwrap(),
    )
    .await?;
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
    let now = fixture.advance_time(std::time::Duration::from_secs(25 * 3600));
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

async fn verify_policy_restart() -> anyhow::Result<()> {
    let fixture = TestStore::new_durable().await;
    let table = match publish_history(&fixture).await {
        Ok(table) => table,
        Err(error) => {
            fixture.finish().await;
            return Err(error);
        }
    };
    let now = fixture.advance_time(std::time::Duration::from_secs(25 * 3600));
    let old_wave = discover_wave(&fixture, &table, now).await?;
    let scopes = [old_wave.markers()[0].scope.clone()];
    let initial = old_wave.markers()[0].policy_version;
    let outcome = async {
        let (first, second, third) = tokio::join!(
            set_retention(
                &fixture,
                &table,
                StreamRetentionDuration::from_hours(24).unwrap()
            ),
            set_retention(
                &fixture,
                &table,
                StreamRetentionDuration::from_hours(24).unwrap()
            ),
            set_retention(
                &fixture,
                &table,
                StreamRetentionDuration::from_hours(24).unwrap()
            ),
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
        set_retention(
            &fixture,
            &table,
            StreamRetentionDuration::from_hours(24).unwrap(),
        )
        .await?;
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
