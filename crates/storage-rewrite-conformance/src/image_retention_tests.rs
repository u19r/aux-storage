//! Pointer lifetime and image retention through real provider operations.

use std::collections::HashMap;

use storage_provider::{
    StreamTrimPageLimit, StreamTrimScopeKind, maintenance::StreamRetentionWave,
};
use storage_types::{
    AttributeValue, StreamName, StreamSpecification, StreamViewType, TableName, TimestampMillis,
};

use super::test_support::{TestStore, hash_table_request};

async fn publish_images(
    fixture: &TestStore,
    embedded: bool,
    incarnation: &str,
) -> anyhow::Result<TableName> {
    let table = TableName::new("pointer_owner_lifetime");
    let mut request = hash_table_request(table.clone());
    request.aux_stream_duration_hours =
        Some(storage_types::StreamRetentionDuration::FiniteHours(1));
    request.aux_default_item_stream_duration_hours =
        Some(storage_types::StreamRetentionDuration::FiniteHours(1));
    request.stream_specification = Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    });

    fixture.operations.create_table(request).await?;
    for generation in 0..3 {
        // Deterministic high-entropy bytes keep the large payload above the
        // embedding threshold even when the provider compresses it.
        let payload = if embedded {
            format!("{incarnation}/{generation}")
        } else {
            (0..128)
                .map(|part| {
                    uuid::Uuid::new_v5(
                        &uuid::Uuid::NAMESPACE_OID,
                        format!("{incarnation}/{generation}/{part}").as_bytes(),
                    )
                    .to_string()
                })
                .collect()
        };
        let item = HashMap::from([
            ("pk".to_owned(), AttributeValue::S("item".to_owned())),
            ("payload".to_owned(), AttributeValue::S(payload)),
        ]);
        let mut put = storage_types::PutItemRequest::new(
            table.clone(),
            storage_types::WireItem::try_from(&item)?,
        );
        put.aux_item_stream_ttl_hours =
            Some(storage_types::StreamRetentionDuration::FiniteHours(1));
        fixture.operations.put_item(put).await?;
    }
    fixture
        .operations
        .update_table(serde_json::from_value(serde_json::json!({
            "TableName": table, "AuxStreamDurationHours": 1
        }))?)
        .await?;
    let page = fixture
        .stream_operations
        .read_forward(table_stream(fixture, &table).await?, None, 10)
        .await?;
    anyhow::ensure!(
        page.items.len() == 3,
        "writes did not publish three new pointers"
    );
    for row in page.items {
        let pointer: stream_provider::StoredStreamPointer =
            storage_types::storage_serde::from_bytes(&row.data)?;
        anyhow::ensure!(
            matches!(
                pointer,
                stream_provider::StoredStreamPointer::Embedded { .. }
            ) == embedded,
            "fixture did not select the required pointer representation"
        );
    }
    Ok(table)
}

async fn trim_scope(fixture: &TestStore, kind: StreamTrimScopeKind) -> anyhow::Result<usize> {
    let retention = fixture.stream_retention_repository.as_ref();
    let marker = retention
        .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 100)
        .await?
        .into_iter()
        .filter(|marker| marker.scope.kind == kind)
        .max_by_key(|marker| marker.policy_version)
        .ok_or_else(|| anyhow::anyhow!("published history has no requested trim scope"))?;
    let state = retention
        .load_stream_trim_states(std::slice::from_ref(&marker.scope))
        .await?
        .pop()
        .flatten()
        .ok_or_else(|| anyhow::anyhow!("published history has no retention policy"))?;
    let storage_types::StreamRetentionDuration::FiniteHours(hours) = state.effective_retention
    else {
        anyhow::bail!("reclamation fixture requires finite retention")
    };
    let observed_at = fixture.now() + i64::from(hours) * 60 * 60 * 1000 + 1000;
    let wave = StreamRetentionWave::try_new(
        vec![marker],
        observed_at,
        observed_at,
        StreamTrimPageLimit::try_from(10_usize)?,
    )?;
    let result = retention.trim_stream_marker_wave(wave).await?;
    anyhow::ensure!(
        result.stale_markers == 0,
        "selected marker was stale despite loading its current state: version {}",
        state.policy_version
    );
    Ok(result.rows_deleted)
}

async fn verify_retention(
    fixture: &TestStore,
    embedded: bool,
    system_first: bool,
) -> anyhow::Result<()> {
    let table = publish_images(fixture, embedded, "original").await?;
    if system_first {
        fixture
            .stream_operations
            .get_system_stream_page(stream_provider::SystemStreamPageRequest::try_new(
                fixture.stream_operations.system_stream_scope().await?,
                None,
                100,
            )?)
            .await?;
        let result = fixture
            .system_stream_trim_repository
            .trim_system_stream_page(stream_provider::SystemStreamTrimPageRequest::try_new(
                fixture.stream_operations.system_stream_scope().await?,
                fixture.now() + 1000,
                10,
                None,
            )?)
            .await?;
        anyhow::ensure!(
            result.reclaimed_records() >= 2,
            "System trim must release the old owners; got {}",
            result.reclaimed_records()
        );
    } else if !embedded {
        anyhow::ensure!(
            trim_scope(fixture, StreamTrimScopeKind::Table).await? == 3,
            "table pointer lifetime must be independent of the system pointer"
        );
    }
    let removed = trim_scope(fixture, StreamTrimScopeKind::Item).await?;
    anyhow::ensure!(
        removed == if embedded { 2 } else { 0 },
        "item trim removed {removed} images with embedded={embedded}"
    );
    let records = if embedded || system_first {
        table_records(fixture, &table).await?
    } else {
        system_records(fixture).await?
    };
    anyhow::ensure!(records.len() == 3, "retained pointers must remain readable");
    anyhow::ensure!(
        records
            .iter()
            .map(|record| usize::from(record.new_image.is_some())
                + usize::from(record.old_image.is_some()))
            .collect::<Vec<_>>()
            == [1, 2, 2],
        "pointer images or predecessors were lost"
    );
    Ok(())
}

async fn verify_pointer_retention(embedded: bool, system_first: bool) -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = verify_retention(&fixture, embedded, system_first).await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_verified_head_when_table_stream_is_idle_then_polls_keep_the_same_frontier()
-> anyhow::Result<()> {
    const POLLS: usize = 64;
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = publish_images(&fixture, true, "idle_poll").await?;
        let initial = fixture
            .operations
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table.clone(),
                None,
                Some(10),
            ))
            .await?;
        let frontier = initial
            .table_frontier()
            .ok_or_else(|| anyhow::anyhow!("initial frontier is absent"))?;
        let head = frontier
            .head
            .ok_or_else(|| anyhow::anyhow!("initial head is absent"))?;
        let started = std::time::Instant::now();
        for _ in 0..POLLS {
            let page = fixture
                .operations
                .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                    table.clone(),
                    Some(head.to_string()),
                    Some(10),
                ))
                .await?;
            anyhow::ensure!(page.records().is_empty());
            anyhow::ensure!(page.last_evaluated_key().is_none());
            anyhow::ensure!(page.table_frontier() == Some(frontier));
        }
        eprintln!(
            "table_stream_idle_poll_report {}",
            serde_json::json!({
                "polls": POLLS,
                "total_ns": started.elapsed().as_nanos(),
                "scope": "public table-stream page at a verified head",
            })
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_trimmed_table_history_when_a_new_pointer_arrives_then_old_checkpoint_cannot_resume()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = publish_images(&fixture, false, "original").await?;
        let before = fixture
            .operations
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table.clone(),
                None,
                Some(10),
            ))
            .await?;
        anyhow::ensure!(before.records().len() == 3);
        let first = before
            .table_frontier()
            .and_then(|frontier| frontier.oldest)
            .ok_or_else(|| anyhow::anyhow!("initial table frontier is empty"))?;
        let history_id = before
            .table_frontier()
            .ok_or_else(|| anyhow::anyhow!("initial table frontier is absent"))?
            .table_history_id;

        anyhow::ensure!(trim_scope(&fixture, StreamTrimScopeKind::Table).await? == 3);
        let item = HashMap::from([
            ("pk".to_owned(), AttributeValue::S("fresh".to_owned())),
            (
                "payload".to_owned(),
                AttributeValue::S("after trim".to_owned()),
            ),
        ]);
        fixture
            .operations
            .put_item(storage_types::PutItemRequest::new(
                table.clone(),
                storage_types::WireItem::try_from(&item)?,
            ))
            .await?;
        let after = fixture
            .operations
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table,
                None,
                Some(10),
            ))
            .await?;
        let frontier = after
            .table_frontier()
            .ok_or_else(|| anyhow::anyhow!("trimmed table frontier is absent"))?;
        anyhow::ensure!(frontier.table_history_id == history_id);
        anyhow::ensure!(after.records().len() == 1);
        anyhow::ensure!(!frontier.can_resume_from(history_id, first));
        let head = frontier
            .head
            .ok_or_else(|| anyhow::anyhow!("new pointer omitted from frontier"))?;
        anyhow::ensure!(frontier.oldest == Some(head) && first < head);
        anyhow::ensure!(frontier.can_resume_from(history_id, head));
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_new_publication_between_table_pages_then_pagination_keeps_a_complete_prefix()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = publish_images(&fixture, true, "original").await?;
        let first = fixture
            .operations
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table.clone(),
                None,
                Some(2),
            ))
            .await?;
        anyhow::ensure!(first.records().len() == 2);
        let cursor = first
            .last_evaluated_key()
            .ok_or_else(|| anyhow::anyhow!("bounded first page omitted its cursor"))?;
        let initial = fixture
            .operations
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table.clone(),
                None,
                Some(10),
            ))
            .await?;
        anyhow::ensure!(initial.records().len() == 3);
        anyhow::ensure!(
            first
                .records()
                .iter()
                .map(|record| &record.sequence_number)
                .collect::<Vec<_>>()
                == initial.records()[..2]
                    .iter()
                    .map(|record| &record.sequence_number)
                    .collect::<Vec<_>>()
        );
        let initial_frontier = first
            .table_frontier()
            .ok_or_else(|| anyhow::anyhow!("first page omitted its frontier"))?;
        anyhow::ensure!(initial_frontier == initial.table_frontier().unwrap());

        let item = HashMap::from([
            ("pk".to_owned(), AttributeValue::S("fourth".to_owned())),
            (
                "payload".to_owned(),
                AttributeValue::S("after first page".to_owned()),
            ),
        ]);
        fixture
            .operations
            .put_item(storage_types::PutItemRequest::new(
                table.clone(),
                storage_types::WireItem::try_from(&item)?,
            ))
            .await?;
        let second = fixture
            .operations
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table.clone(),
                Some(cursor.to_owned()),
                Some(2),
            ))
            .await?;
        anyhow::ensure!(second.records().len() == 2);
        anyhow::ensure!(second.last_evaluated_key().is_none());
        let complete = fixture
            .operations
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table,
                None,
                Some(10),
            ))
            .await?;
        anyhow::ensure!(complete.records().len() == 4);
        let paged = first
            .records()
            .iter()
            .chain(second.records())
            .map(|record| &record.sequence_number)
            .collect::<Vec<_>>();
        let expected = complete
            .records()
            .iter()
            .map(|record| &record.sequence_number)
            .collect::<Vec<_>>();
        anyhow::ensure!(
            paged == expected,
            "publication escaped or duplicated a page"
        );
        let later_frontier = second
            .table_frontier()
            .ok_or_else(|| anyhow::anyhow!("second page omitted its frontier"))?;
        anyhow::ensure!(later_frontier == complete.table_frontier().unwrap());
        anyhow::ensure!(
            initial_frontier.table_history_id == later_frontier.table_history_id
                && initial_frontier.head < later_frontier.head
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_embedded_pointers_when_old_images_expire_then_pointer_reads_remain_complete()
-> anyhow::Result<()> {
    verify_pointer_retention(true, false).await
}

#[tokio::test]
async fn given_referenced_pointers_when_table_history_expires_then_system_owner_retains_images()
-> anyhow::Result<()> {
    verify_pointer_retention(false, false).await
}

#[tokio::test]
async fn given_referenced_pointers_when_system_history_expires_then_table_owner_retains_images()
-> anyhow::Result<()> {
    verify_pointer_retention(false, true).await
}

async fn verify_recreated_history(fixture: &TestStore, replacement: &str) -> anyhow::Result<()> {
    let table = publish_images(fixture, false, "original").await?;
    let before = system_records(fixture).await?;
    anyhow::ensure!(before.len() == 3, "original system pointers missing");
    fixture
        .operations
        .delete_table(storage_types::DeleteTableRequest {
            table_name: table.clone(),
        })
        .await?;
    fixture.advance_time(std::time::Duration::from_secs(30));
    publish_images(fixture, false, replacement).await?;
    let after = system_records(fixture).await?;
    anyhow::ensure!(
        after.len() == 6,
        "delete/recreate must retain both incarnations"
    );
    for original in &before {
        let retained = after
            .iter()
            .find(|record| record.sequence_number == original.sequence_number)
            .ok_or_else(|| anyhow::anyhow!("old system pointer was removed"))?;
        anyhow::ensure!(
            retained.new_image == original.new_image && retained.old_image == original.old_image,
            "old pointer resolved an image from the replacement table"
        );
    }
    let current = table_records(fixture, &table).await?;
    anyhow::ensure!(
        current.len() == 3,
        "replacement table did not isolate its own history"
    );
    anyhow::ensure!(
        current.iter().all(|record| before
            .iter()
            .all(|old| old.sequence_number != record.sequence_number)),
        "replacement reused an old effect identity"
    );
    Ok(())
}

async fn verify_recreation(replacement: &str) -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = verify_recreated_history(&fixture, replacement).await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_retained_system_history_when_table_is_recreated_then_old_images_remain_isolated()
-> anyhow::Result<()> {
    verify_recreation("replacement").await
}

#[tokio::test]
async fn given_identical_writes_when_table_is_recreated_then_effects_have_distinct_incarnations()
-> anyhow::Result<()> {
    verify_recreation("original").await
}

async fn system_records(fixture: &TestStore) -> anyhow::Result<Vec<storage_types::StreamRecord>> {
    let scope = fixture.stream_operations.system_stream_scope().await?;
    let page = fixture
        .stream_operations
        .get_system_stream_page(stream_provider::SystemStreamPageRequest::try_new(
            scope, None, 100,
        )?)
        .await?;
    anyhow::ensure!(page.terminal(), "fixture exceeded its bounded system page");
    Ok(page.records().to_vec())
}

pub(super) async fn table_records(
    fixture: &TestStore,
    table: &TableName,
) -> anyhow::Result<Vec<storage_types::StreamRecord>> {
    let page = fixture
        .operations
        .get_stream_records(storage_types::GetStreamRecordsRequest::table(
            table.clone(),
            None,
            Some(100),
        ))
        .await?;
    Ok(page.into_parts().0)
}

pub(super) async fn table_stream(
    fixture: &TestStore,
    table: &TableName,
) -> anyhow::Result<stream_provider::StreamRowKind> {
    let page = fixture
        .stream_operations
        .read_backward(
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
    anyhow::bail!("missing seeded table pointer")
}

async fn release_gsi_system_history(
    fixture: &TestStore,
    issue_checkpoint: bool,
) -> anyhow::Result<usize> {
    let scope = fixture.stream_operations.system_stream_scope().await?;
    // Issue the first checkpoint only after all three writes; an earlier public
    // checkpoint would independently retain the predecessor under test.
    if issue_checkpoint {
        fixture
            .stream_operations
            .get_system_stream_page(stream_provider::SystemStreamPageRequest::try_new(
                scope.clone(),
                None,
                100,
            )?)
            .await?;
    }
    let result = fixture
        .system_stream_trim_repository
        .trim_system_stream_page(stream_provider::SystemStreamTrimPageRequest::try_new(
            scope,
            fixture.now() + 1000,
            100,
            None,
        )?)
        .await?;
    Ok(result.reclaimed_records())
}

async fn assert_gsi_predecessor(
    fixture: &TestStore,
    current: &str,
    predecessor: &str,
) -> anyhow::Result<()> {
    // After trimming the origin, a fresh public SystemStream checkpoint must
    // fail closed. Inspect the retained physical pointers through the same
    // initialized stream capability to prove their exact referenced images.
    let rows = fixture
        .stream_operations
        .read_forward(
            stream_provider::StreamRowKind::System(StreamName::system_table_stream()),
            None,
            100,
        )
        .await?;
    for row in rows.items {
        let pointer: stream_provider::StoredStreamPointer =
            storage_types::storage_serde::from_bytes(&row.data)?;
        let stream_provider::StoredStreamPointer::Pointer {
            item_stream_version,
            ..
        } = pointer
        else {
            continue;
        };
        let images = fixture
            .stream_operations
            .read_backward(
                stream_provider::StreamRowKind::Item(
                    pointer.history_address().physical_stream_name(),
                ),
                None,
                100,
            )
            .await?;
        let groups = images
            .items
            .iter()
            .filter(|image| image.id <= storage_types::StreamItemId::from(item_stream_version))
            .take(2)
            .map(|image| {
                let item: storage_types::AttributeMap =
                    storage_types::storage_serde::from_bytes(&image.data)?;
                match item.get("gsi_pk") {
                    Some(AttributeValue::S(group)) => Ok(group.clone()),
                    _ => anyhow::bail!("referenced image has no group"),
                }
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        if groups.first().is_some_and(|group| group == current) {
            anyhow::ensure!(
                groups == [current, predecessor],
                "referenced predecessor image was reclaimed"
            );
            return Ok(());
        }
    }
    anyhow::bail!("retained referenced update {current} was not readable")
}

#[tokio::test]
async fn given_pending_gsi_history_when_pointer_owners_expire_then_predecessor_survives_until_replay()
-> anyhow::Result<()> {
    use anyhow::Context;

    use super::{
        gsi_lifecycle_tests::{create_index, finish_backfill, put_replay_item},
        gsi_workflow_tests::{group_keys, table_request},
    };
    let fixture = TestStore::new().await;
    let outcome = async {
        let mut request = table_request("revision_replay");
        request.global_secondary_indexes = None;
        request.aux_stream_duration_hours =
            Some(storage_types::StreamRetentionDuration::FiniteHours(1));
        request.aux_default_item_stream_duration_hours =
            Some(storage_types::StreamRetentionDuration::FiniteHours(1));
        request.stream_specification = Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        });
        fixture.operations.create_table(request).await?;
        put_replay_item(&fixture, "move", "A", true).await?;
        create_index(&fixture, "revision_replay").await?;
        anyhow::ensure!(finish_backfill(&fixture, 8).await? > 0);
        anyhow::ensure!(group_keys(&fixture, "revision_replay", "A").await? == ["move"]);
        put_replay_item(&fixture, "move", "B", false).await?;
        put_replay_item(&fixture, "move", "C", true).await?;
        // The canonical route projects foreground writes immediately. The GSI
        // worker still owns its unconsumed history and predecessor image hold.
        anyhow::ensure!(
            group_keys(&fixture, "revision_replay", "A")
                .await?
                .is_empty()
        );
        anyhow::ensure!(
            group_keys(&fixture, "revision_replay", "B")
                .await?
                .is_empty()
        );
        anyhow::ensure!(group_keys(&fixture, "revision_replay", "C").await? == ["move"]);
        fixture
            .operations
            .update_table(serde_json::from_value(serde_json::json!({
                "TableName": "revision_replay", "AuxStreamDurationHours": 1
            }))?)
            .await?;
        anyhow::ensure!(
            trim_scope(&fixture, StreamTrimScopeKind::Table)
                .await
                .context("release table owners")?
                == 3
        );
        anyhow::ensure!(
            release_gsi_system_history(&fixture, true)
                .await
                .context("release system owners")?
                >= 1
        );
        anyhow::ensure!(
            trim_scope(&fixture, StreamTrimScopeKind::Item)
                .await
                .context("trim item images")?
                == 0,
            "pending GSI replay lost its predecessor"
        );
        assert_gsi_predecessor(&fixture, "B", "A")
            .await
            .context("read B/A")?;
        anyhow::ensure!(finish_backfill(&fixture, 8).await? > 0);
        // Use a fresh trim operation and the next due retention bucket rather
        // than replaying the preceding successful trim receipt.
        fixture.advance_time(std::time::Duration::from_secs(60));
        anyhow::ensure!(
            release_gsi_system_history(&fixture, false)
                .await
                .context("release consumed system owners")?
                >= 1
        );
        anyhow::ensure!(
            trim_scope(&fixture, StreamTrimScopeKind::Item)
                .await
                .context("trim item images")?
                >= 1,
            "consumed GSI history left a permanent image hold"
        );
        assert_gsi_predecessor(&fixture, "C", "B")
            .await
            .context("read C/B")?;
        anyhow::ensure!(
            group_keys(&fixture, "revision_replay", "A")
                .await?
                .is_empty()
        );
        anyhow::ensure!(
            group_keys(&fixture, "revision_replay", "B")
                .await?
                .is_empty()
        );
        anyhow::ensure!(group_keys(&fixture, "revision_replay", "C").await? == ["move"]);
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}
