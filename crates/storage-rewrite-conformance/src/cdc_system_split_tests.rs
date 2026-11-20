//! Split/replay and continued writes through one initialized stream owner.

use serde_json::json;
use storage_types::{PutItemRequest, TableName};
use stream_provider::{
    SystemStreamLineage, SystemStreamPageRequest, SystemStreamSplitChild,
    SystemStreamSplitOperationId, SystemStreamSplitRequest,
};

use super::test_support::TestStore;

#[tokio::test]
async fn given_issued_checkpoint_when_split_replays_and_writes_continue_then_all_records_are_readable()
-> anyhow::Result<()> {
    let fixture = TestStore::new_durable().await;
    let outcome = async {
        let table = TableName::new("split_writes");
        super::cdc_stream_tests::create_named_table(&fixture, table.clone()).await?;
        let scope = fixture.stream_operations.system_stream_scope().await?;
        for key in ["before-1", "before-2"] {
            fixture
                .operations
                .put_item(
                    PutItemRequest::try_from(json!({
                        "TableName": table, "Item": {"pk": {"S": key}}
                    }))
                    .map_err(anyhow::Error::msg)?,
                )
                .await?;
        }
        fixture.wait_until_changes_are_readable().await;
        let first = fixture
            .stream_operations
            .get_system_stream_page(SystemStreamPageRequest::try_new(scope.clone(), None, 100)?)
            .await?;
        anyhow::ensure!(first.records().len() == 2 && first.terminal());
        let parent = fixture
            .stream_operations
            .system_stream_lineage_catalog(scope.clone())
            .await?
            .into_iter()
            .find(|lineage| !lineage.sealed() && lineage.parent_lineage_id().is_none())
            .ok_or_else(|| anyhow::anyhow!("active root lineage is absent"))?;
        let children = [
            SystemStreamSplitChild::try_new(
                SystemStreamLineage::try_new(
                    "split-left",
                    "split-left-lineage",
                    Some(parent.lineage_id().as_str().to_owned()),
                    false,
                )?,
                1,
            )?,
            SystemStreamSplitChild::try_new(
                SystemStreamLineage::try_new(
                    "split-right",
                    "split-right-lineage",
                    Some(parent.lineage_id().as_str().to_owned()),
                    false,
                )?,
                1,
            )?,
        ];
        let request = SystemStreamSplitRequest::try_new(
            scope.clone(),
            parent,
            children,
            SystemStreamSplitOperationId::try_new("conformance-split")?,
        )?;
        let overlapping_write = PutItemRequest::try_from(json!({
            "TableName": table, "Item": {"pk": {"S": "during-split"}}
        }))
        .map_err(anyhow::Error::msg)?;
        let (left, right, write) = tokio::join!(
            fixture
                .stream_operations
                .split_system_stream(request.clone()),
            fixture
                .stream_operations
                .split_system_stream(request.clone()),
            fixture.operations.put_item(overlapping_write)
        );
        write?;
        let (left, right) = (left?, right?);
        anyhow::ensure!(
            left.source_scope() == right.source_scope()
                && left.operation_id() == right.operation_id()
                && left.sealed_parent() == right.sealed_parent()
                && left.children() == right.children(),
            "same operation produced different durable topology"
        );
        use stream_provider::SystemStreamSplitDisposition::{Applied, Replayed};
        anyhow::ensure!(
            matches!(
                (left.disposition(), right.disposition()),
                (Applied, Replayed) | (Replayed, Applied)
            ),
            "concurrent split must apply once and replay once"
        );
        for key in ["after-1", "after-2"] {
            fixture
                .operations
                .put_item(
                    PutItemRequest::try_from(json!({
                        "TableName": table, "Item": {"pk": {"S": key}}
                    }))
                    .map_err(anyhow::Error::msg)?,
                )
                .await?;
        }
        fixture.wait_until_changes_are_readable().await;
        let child_records = read_records_after(
            &fixture,
            scope.clone(),
            first.issued_checkpoint().token().clone(),
        )
        .await?;
        let written_child = child_records
            .iter()
            .find(|record| {
                record.keys.get("pk")
                    == Some(&storage_types::AttributeValue::S("after-1".to_owned()))
            })
            .and_then(|record| record.system_position.as_ref())
            .ok_or_else(|| anyhow::anyhow!("child write position is absent"))?;
        let child_parent = fixture
            .stream_operations
            .system_stream_lineage_catalog(scope.clone())
            .await?
            .into_iter()
            .find(|lineage| lineage.lineage_id() == written_child.lineage_id())
            .ok_or_else(|| anyhow::anyhow!("written child is absent"))?;
        anyhow::ensure!(child_parent.parent_lineage_id() == Some(request.parent().lineage_id()));
        let grandchildren = ["left", "right"].map(|side| {
            SystemStreamSplitChild::try_new(
                SystemStreamLineage::try_new(
                    format!("grandchild-{side}"),
                    format!("grandchild-{side}-lineage"),
                    Some(child_parent.lineage_id().as_str().to_owned()),
                    false,
                )?,
                1,
            )
        });
        let [grandchild_left, grandchild_right] = grandchildren;
        let child_request = SystemStreamSplitRequest::try_new(
            scope.clone(),
            child_parent,
            [grandchild_left?, grandchild_right?],
            SystemStreamSplitOperationId::try_new("conformance-child-split")?,
        )?;
        fixture
            .stream_operations
            .split_system_stream(child_request.clone())
            .await?;
        fixture
            .operations
            .put_item(
                PutItemRequest::try_from(json!({
                    "TableName": table, "Item": {"pk": {"S": "after-child-split"}}
                }))
                .map_err(anyhow::Error::msg)?,
            )
            .await?;
        let initial_lineage_count = first.issued_checkpoint().checkpoint().frontier().len();
        Ok::<_, anyhow::Error>((
            scope,
            first.issued_checkpoint().token().clone(),
            request,
            child_request,
            left,
            initial_lineage_count,
        ))
    }
    .await;
    let (scope, checkpoint, request, child_request, original, initial_lineage_count) = match outcome
    {
        Ok(value) => value,
        Err(error) => {
            fixture.finish().await;
            return Err(error);
        }
    };
    let fixture = fixture.reopen().await;
    let outcome = async {
        let replay = fixture
            .stream_operations
            .split_system_stream(request.clone())
            .await?;
        anyhow::ensure!(
            replay.disposition() == stream_provider::SystemStreamSplitDisposition::Replayed
        );
        anyhow::ensure!(
            replay.sealed_parent() == original.sealed_parent()
                && replay.children() == original.children()
        );
        let catalog_request = request.clone();
        anyhow::ensure!(
            replay
                == fixture
                    .stream_operations
                    .split_system_stream(request)
                    .await?
        );
        fixture.wait_until_changes_are_readable().await;
        let records = read_records_after(&fixture, scope.clone(), checkpoint).await?;
        let mut keys = records
            .iter()
            .map(|record| match record.keys.get("pk") {
                Some(storage_types::AttributeValue::S(key)) => Ok(key.clone()),
                _ => anyhow::bail!("stream record key is missing"),
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        keys.sort_unstable();
        anyhow::ensure!(
            keys == ["after-1", "after-2", "after-child-split", "during-split"],
            "split lost or duplicated post-checkpoint writes"
        );
        let catalog = fixture
            .stream_operations
            .system_stream_lineage_catalog(scope)
            .await?;
        anyhow::ensure!(
            catalog.len() == initial_lineage_count + 4
                && catalog.iter().filter(|lineage| lineage.sealed()).count() == 2
        );
        for split in [&catalog_request, &child_request] {
            let parent = catalog
                .iter()
                .find(|lineage| lineage.lineage_id() == split.parent().lineage_id())
                .ok_or_else(|| anyhow::anyhow!("sealed parent is absent"))?;
            anyhow::ensure!(parent.sealed());
            for child in split.children() {
                let actual = catalog
                    .iter()
                    .find(|lineage| lineage.lineage_id() == child.lineage().lineage_id())
                    .ok_or_else(|| anyhow::anyhow!("split child is absent"))?;
                anyhow::ensure!(actual.parent_lineage_id() == Some(parent.lineage_id()));
            }
        }
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

async fn read_records_after(
    fixture: &TestStore,
    scope: storage_types::SystemStreamScope,
    checkpoint: storage_types::SystemStreamCheckpointToken,
) -> anyhow::Result<Vec<storage_types::StreamRecord>> {
    let mut checkpoint = checkpoint;
    let mut records = Vec::new();
    for _ in 0..16 {
        let page = fixture
            .stream_operations
            .get_system_stream_page(SystemStreamPageRequest::try_new(
                scope.clone(),
                Some(checkpoint),
                100,
            )?)
            .await?;
        records.extend_from_slice(page.records());
        if page.terminal() {
            return Ok(records);
        }
        checkpoint = page.issued_checkpoint().token().clone();
    }
    anyhow::bail!("split descendants exceeded bounded pagination")
}
