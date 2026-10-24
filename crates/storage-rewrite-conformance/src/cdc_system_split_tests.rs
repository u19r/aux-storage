//! Split/replay and continued writes through one initialized stream owner.

use serde_json::json;
use storage_types::{PutItemRequest, TableName};
use stream_provider::{
    SystemStreamLineage, SystemStreamPageRequest, SystemStreamSplitChild,
    SystemStreamSplitOperationId, SystemStreamSplitRequest,
};

use super::test_support::{TestStore, hash_table_request};

#[tokio::test]
async fn given_issued_checkpoint_when_split_replays_and_writes_continue_then_all_records_are_readable()
-> anyhow::Result<()> {
    let fixture = TestStore::new_durable().await;
    let outcome = async {
        let table = TableName::new("split_writes");
        fixture
            .operations
            .create_table(hash_table_request(table.clone()))
            .await?;
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
        Ok::<_, anyhow::Error>((scope, first.issued_checkpoint().token().clone(), request))
    }
    .await;
    let (scope, checkpoint, request) = match outcome {
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
            replay
                == fixture
                    .stream_operations
                    .split_system_stream(request)
                    .await?
        );
        let records = fixture
            .stream_operations
            .get_system_stream_page(SystemStreamPageRequest::try_new(
                scope.clone(),
                Some(checkpoint),
                100,
            )?)
            .await?;
        let mut keys = records
            .records()
            .iter()
            .map(|record| match record.keys.get("pk") {
                Some(storage_types::AttributeValue::S(key)) => Ok(key.clone()),
                _ => anyhow::bail!("stream record key is missing"),
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        keys.sort_unstable();
        anyhow::ensure!(
            keys == ["after-1", "after-2", "during-split"] && records.terminal(),
            "split lost or duplicated post-checkpoint writes"
        );
        let catalog = fixture
            .stream_operations
            .system_stream_lineage_catalog(scope)
            .await?;
        anyhow::ensure!(
            catalog.len() == 3 && catalog.iter().filter(|lineage| lineage.sealed()).count() == 1
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}
