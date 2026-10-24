use std::num::{NonZeroU64, NonZeroUsize};

use scheduled_bg_jobs::{JobId, Namespace};
use serde_json::json;
use storage_provider::maintenance::ExpirationRequest;
use storage_types::{
    GetItemRequest, PutItemRequest, TableName, TimeToLiveSpecification, TimestampMillis,
    UpdateTimeToLiveRequest, context::WrappedError, maintenance::StepResult,
};

use super::test_support::TestStore;

fn request(job: u8, fence: u64) -> ExpirationRequest {
    ExpirationRequest {
        destination: Namespace::try_new("conformance:expiration-authority").expect("namespace"),
        job: JobId::from([job; 16]),
        fence: NonZeroU64::new(fence).expect("fence"),
        requested_cutoff: TimestampMillis::from_timestamp(2_000_000_000_000),
        candidate_limit: NonZeroUsize::MIN,
    }
}

async fn seed_due_items(fixture: &TestStore) -> anyhow::Result<()> {
    let table = TableName::new("expiration_authority");
    fixture.create_hash_table(table.clone()).await;
    fixture
        .operations
        .update_time_to_live(UpdateTimeToLiveRequest {
            table_name: table.clone(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: "ttl".into(),
                enabled: true,
            },
        })
        .await?;
    for key in ["a", "b", "c"] {
        fixture
            .operations
            .put_item(
                PutItemRequest::try_from(json!({
                    "TableName": table.as_ref(),
                    "Item": {"pk": {"S": key}, "ttl": {"N": "1"}}
                }))
                .map_err(anyhow::Error::msg)?,
            )
            .await?;
    }
    Ok(())
}

async fn count_survivors(fixture: &TestStore) -> anyhow::Result<usize> {
    let mut count = 0;
    for key in ["a", "b", "c"] {
        let response = fixture
            .operations
            .get_item(
                GetItemRequest::try_from(json!({
                    "TableName": "expiration_authority", "Key": {"pk": {"S": key}}
                }))
                .map_err(anyhow::Error::msg)?,
            )
            .await?;
        count += usize::from(response.item.is_some());
    }
    Ok(count)
}

#[tokio::test]
async fn given_bound_expiration_when_older_fence_runs_then_items_survive() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        fixture
            .expiration_repository
            .advance_expiration(request(7, 2))
            .await
            .map_err(|error| anyhow::anyhow!("bind: {error:?}"))?;
        seed_due_items(&fixture).await?;
        let result = fixture
            .expiration_repository
            .advance_expiration(request(7, 1))
            .await
            .map_err(|error| anyhow::anyhow!("stale: {error:?}"))?;
        let remaining = count_survivors(&fixture).await?;
        anyhow::ensure!(
            matches!(result, StepResult::Superseded) && remaining == 3,
            "stale fence returned {result:?}, {remaining}/3 items survived"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_bound_expiration_when_foreign_job_runs_then_items_survive() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        fixture
            .expiration_repository
            .advance_expiration(request(7, 2))
            .await
            .map_err(|error| anyhow::anyhow!("bind: {error:?}"))?;
        seed_due_items(&fixture).await?;
        let result = fixture
            .expiration_repository
            .advance_expiration(request(8, 3))
            .await;
        let remaining = count_survivors(&fixture).await?;
        let rejected = result.as_ref().is_err_and(|error| {
            matches!(
                error.to_enum(),
                storage_types::StorageEnum::Validation { .. }
            )
        });
        anyhow::ensure!(
            rejected && remaining == 3,
            "foreign job returned {result:?}, {remaining}/3 items survived"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

async fn direct_pages(fixture: &TestStore) -> anyhow::Result<()> {
    let repository = &fixture.ttl_sweep_repository;
    seed_due_items(fixture).await?;
    let table = TableName::new("expiration_authority");
    let mut old_command = None;
    for expected_remaining in [2, 1, 0] {
        let policy = repository
            .read_ttl_sweep_policy(&table)
            .await?
            .expect("enabled TTL");
        let captured_at = TimestampMillis::from_timestamp(2_000_000_000_000);
        let input = storage_provider::TtlSweepPageInput::try_new(
            "ttl",
            captured_at,
            1,
            policy.checkpoint.cursor,
        )?;
        let command = storage_provider::TtlSweepPageCommand::try_new(
            table.clone(),
            input,
            policy.policy_generation,
            captured_at,
        )?;
        let page = repository.execute_ttl_sweep_page(command.clone()).await?;
        anyhow::ensure!(
            page.deleted_items == 1 && count_survivors(fixture).await? == expected_remaining,
            "direct page did not consume exactly one item"
        );
        anyhow::ensure!(
            page.next_cursor.is_some() == (expected_remaining != 0),
            "direct page continuation differs from remaining work"
        );
        if let Some(stale) = old_command {
            let Err(error) = repository.execute_ttl_sweep_page(stale).await else {
                anyhow::bail!("old cursor must reject");
            };
            anyhow::ensure!(
                matches!(
                    error.to_enum(),
                    storage_types::StorageEnum::ConditionalCheckFailed
                ),
                "unexpected stale cursor error: {error:?}"
            );
            anyhow::ensure!(
                count_survivors(fixture).await? == expected_remaining,
                "stale cursor changed items"
            );
        }
        old_command = Some(command);
    }
    let policy = repository
        .read_ttl_sweep_policy(&table)
        .await?
        .expect("enabled policy");
    let captured_at = TimestampMillis::from_timestamp(2_000_000_000_000);
    let stale = storage_provider::TtlSweepPageCommand::try_new(
        table.clone(),
        storage_provider::TtlSweepPageInput::try_new("ttl", captured_at, 1, None)?,
        policy.policy_generation,
        captured_at,
    )?;
    for enabled in [false, true] {
        fixture
            .operations
            .update_time_to_live(UpdateTimeToLiveRequest {
                table_name: table.clone(),
                time_to_live_specification: TimeToLiveSpecification {
                    attribute_name: "ttl".into(),
                    enabled,
                },
            })
            .await?;
    }
    let Err(error) = repository.execute_ttl_sweep_page(stale).await else {
        anyhow::bail!("old generation must reject after re-enable");
    };
    anyhow::ensure!(
        matches!(
            error.to_enum(),
            storage_types::StorageEnum::ConditionalCheckFailed
        ),
        "unexpected old generation error: {error:?}"
    );
    Ok(())
}

#[tokio::test]
async fn given_direct_expiration_when_page_commits_then_cursor_is_persisted_and_stale_commands_reject()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = direct_pages(&fixture).await;
    fixture.finish().await;
    outcome
}

async fn consume_one(fixture: &TestStore, expected_remaining: usize) -> anyhow::Result<()> {
    let result = fixture
        .expiration_repository
        .advance_expiration(request(7, 2))
        .await
        .map_err(|error| anyhow::anyhow!("advance: {error:?}"))?;
    let remaining = count_survivors(fixture).await?;
    anyhow::ensure!(
        matches!(result, StepResult::Advanced(())) && remaining == expected_remaining,
        "one-candidate page returned {result:?} and left {remaining}/3 items; expected \
         {expected_remaining}"
    );
    Ok(())
}

async fn resume_equal_deadlines() -> anyhow::Result<()> {
    let mut fixture = TestStore::new_durable().await;
    if let Err(error) = seed_due_items(&fixture).await {
        fixture.finish().await;
        return Err(error);
    }
    for remaining in [2, 1, 0] {
        if let Err(error) = consume_one(&fixture, remaining).await {
            fixture.finish().await;
            return Err(error);
        }
        fixture = fixture.reopen().await;
    }
    let outcome = async {
        let result = fixture
            .expiration_repository
            .advance_expiration(request(7, 2))
            .await
            .map_err(|error| anyhow::anyhow!("complete: {error:?}"))?;
        anyhow::ensure!(
            matches!(result, StepResult::Complete),
            "expected complete, got {result:?}"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

async fn replace_active_target(fixture: &TestStore, recreate: bool) -> anyhow::Result<usize> {
    let table = TableName::new("expiration_authority");
    if recreate {
        fixture
            .operations
            .delete_table(storage_types::DeleteTableRequest {
                table_name: table.clone(),
            })
            .await?;
        // The public route reserves deleted names for 30 seconds.
        fixture.advance_time(std::time::Duration::from_secs(30));
        seed_due_items(fixture).await?;
        return Ok(3);
    }
    for enabled in [false, true] {
        fixture
            .operations
            .update_time_to_live(UpdateTimeToLiveRequest {
                table_name: table.clone(),
                time_to_live_specification: TimeToLiveSpecification {
                    attribute_name: "ttl".into(),
                    enabled,
                },
            })
            .await
            .map_err(|error| anyhow::anyhow!("replace TTL enabled={enabled}: {error:?}"))?;
    }
    // Write a new due value after replacement so even deferred KV backfill has
    // real candidates. An empty new index must not make this fence test pass.
    for key in ["a", "b", "c"] {
        let response = fixture
            .operations
            .get_item(
                GetItemRequest::try_from(json!({
                    "TableName": table.as_ref(), "Key": {"pk": {"S": key}}
                }))
                .map_err(anyhow::Error::msg)?,
            )
            .await?;
        let Some(item) = response.item else {
            continue;
        };
        let mut item = item.into_attribute_map()?;
        item.insert("ttl", storage_types::AttributeValue::N("2".into()));
        fixture
            .operations
            .put_item(PutItemRequest::new(
                table.clone(),
                storage_types::WireItem::try_from(&item).map_err(anyhow::Error::msg)?,
            ))
            .await?;
    }
    Ok(2)
}

async fn changed_target_survives(recreate: bool) -> anyhow::Result<()> {
    let fixture = TestStore::new_durable().await;
    let setup = async {
        seed_due_items(&fixture).await?;
        consume_one(&fixture, 2).await?;
        replace_active_target(&fixture, recreate).await
    }
    .await;
    let expected = match setup {
        Ok(expected) => expected,
        Err(error) => {
            fixture.finish().await;
            return Err(error);
        }
    };
    let fixture = fixture.reopen().await;
    let outcome = async {
        let result = fixture
            .expiration_repository
            .advance_expiration(request(7, 2))
            .await?;
        anyhow::ensure!(
            matches!(result, StepResult::Advanced(())),
            "changed target must advance past old target: {result:?}"
        );
        anyhow::ensure!(
            count_survivors(&fixture).await? == expected,
            "old cursor consumed a changed target"
        );
        let result = fixture
            .expiration_repository
            .advance_expiration(request(7, 2))
            .await?;
        anyhow::ensure!(
            matches!(result, StepResult::Complete),
            "cycle must complete: {result:?}"
        );
        if recreate {
            consume_one(&fixture, expected - 1).await?;
        }
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_equal_deadlines_when_limit_is_one_then_one_candidate_is_consumed()
-> anyhow::Result<()> {
    resume_equal_deadlines().await
}

#[tokio::test]
async fn given_active_expiration_when_policy_or_table_changes_then_old_cursor_cannot_delete_new_target()
-> anyhow::Result<()> {
    for recreate in [false, true] {
        changed_target_survives(recreate).await?;
    }
    Ok(())
}
