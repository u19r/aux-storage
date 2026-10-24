//! Public TTL configuration and error responses through the replacement store.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    panic::{AssertUnwindSafe, resume_unwind},
};

use futures::FutureExt as _;
use scheduled_bg_jobs::{JobId, Namespace};
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_provider::maintenance::ExpirationRequest;
use storage_types::{
    DescribeTimeToLiveRequest, GetItemRequest, PutItemRequest, TimeToLiveStatus,
    UpdateTimeToLiveRequest, context::WrappedError as _, maintenance::StepResult,
};

use super::test_support::{ConformanceFixtures, TestStore, assert_dynamo_error_response_exact};

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

// Business rule: TTL configuration changes are visible without hiding a live
// item.
#[tokio::test]
async fn given_table_with_item_when_ttl_is_enabled_then_disabled_then_state_and_item_are_exact() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let item = json!({"pk": {"S": "customer-1"}, "ttl": {"N": "4102444800"}});
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(), "Item": item
        }))
        .unwrap();
        store.operations.put_item(put).await.unwrap();

        for enabled in [true, false] {
            let update: UpdateTimeToLiveRequest = serde_json::from_value(json!({
                "TableName": table.as_ref(),
                "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": enabled}
            }))
            .unwrap();
            let changed = observe(store.operations.update_time_to_live(update)).await;
            changed.assert_no_warning_or_error();
            let span =
                changed.one_span_with("dynamo_operation", &[("operation", "UpdateTimeToLive")]);
            assert_eq!(span.fields.len(), 1);
            assert_eq!(
                serde_json::to_value(changed.result.unwrap()).unwrap(),
                json!({"TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": enabled}})
            );

            let described = observe(store.operations.describe_time_to_live(
                DescribeTimeToLiveRequest {
                    table_name: table.clone(),
                },
            ))
            .await;
            described.assert_no_warning_or_error();
            let span =
                described.one_span_with("dynamo_operation", &[("operation", "DescribeTimeToLive")]);
            assert_eq!(span.fields.len(), 1);
            let description = described.result.unwrap().time_to_live_description.unwrap();
            if enabled {
                assert_eq!(description.attribute_name.as_deref(), Some("ttl"));
                assert!(matches!(
                    description.time_to_live_status,
                    TimeToLiveStatus::Enabling | TimeToLiveStatus::Enabled
                ));
            } else {
                assert_eq!(description.attribute_name, None);
                assert_eq!(description.time_to_live_status, TimeToLiveStatus::Disabled);
            }

            let get = GetItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Key": {"pk": {"S": "customer-1"}}
            }))
            .unwrap();
            let read = observe(store.operations.get_item(get)).await;
            read.assert_no_warning_or_error();
            assert_eq!(
                serde_json::to_value(read.result.unwrap()).unwrap(),
                json!({"Item": item})
            );
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business rule: replacing an expired item must leave the new live value
// readable.
#[tokio::test]
async fn given_expired_item_when_live_version_replaces_it_then_latest_item_is_readable() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let update: UpdateTimeToLiveRequest = serde_json::from_value(json!({
            "TableName": table.as_ref(),
            "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": true}
        }))
        .unwrap();
        let enabled = observe(store.operations.update_time_to_live(update)).await;
        enabled.assert_no_warning_or_error();
        let span = enabled.one_span_with("dynamo_operation", &[("operation", "UpdateTimeToLive")]);
        assert_eq!(span.fields.len(), 1);
        assert_eq!(
            serde_json::to_value(enabled.result.unwrap()).unwrap(),
            json!({
                "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": true}
            })
        );

        let now_seconds = store.now().timestamp_millis() / 1_000;
        let expired = now_seconds - 60;
        let live = now_seconds + 60 * 60;
        let old_item = json!({
            "pk": {"S": "one"}, "ttl": {"N": expired.to_string()}, "value": {"S": "old"}
        });
        let new_item = json!({
            "pk": {"S": "one"}, "ttl": {"N": live.to_string()}, "value": {"S": "new"}
        });
        for item in [old_item, new_item.clone()] {
            let request = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Item": item
            }))
            .unwrap();
            let written = observe(store.operations.put_item(request)).await;
            written.assert_no_warning_or_error();
            let span = written.one_span_with("dynamo_operation", &[("operation", "PutItem")]);
            assert_eq!(span.fields.len(), 1);
            assert_eq!(
                serde_json::to_value(written.result.unwrap()).unwrap(),
                json!({})
            );
        }

        let request = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(), "Key": {"pk": {"S": "one"}}
        }))
        .unwrap();
        let read = observe(store.operations.get_item(request)).await;
        read.assert_no_warning_or_error();
        let span = read.one_span_with("dynamo_operation", &[("operation", "GetItem")]);
        assert_eq!(span.fields.len(), 1);
        assert_eq!(
            serde_json::to_value(read.result.unwrap()).unwrap(),
            json!({"Item": new_item})
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business and boundary rules: the sweep deletes a due item but preserves a
// live replacement.
#[tokio::test]
async fn given_expired_and_replaced_items_when_expiration_runs_then_only_expired_item_is_deleted() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let update: UpdateTimeToLiveRequest = serde_json::from_value(json!({
            "TableName": table.as_ref(),
            "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": true}
        }))
        .unwrap();
        store.operations.update_time_to_live(update).await.unwrap();

        let now_seconds = store.now().timestamp_millis() / 1_000;
        let expired = (now_seconds - 60).to_string();
        let live = (now_seconds + 3_600).to_string();
        let due_item = json!({
            "pk": {"S": "due"}, "ttl": {"N": expired}, "value": {"S": "old"}
        });
        let new_item = json!({
            "pk": {"S": "replaced"}, "ttl": {"N": live}, "value": {"S": "new"}
        });
        for item in [
            due_item.clone(),
            json!({"pk": {"S": "replaced"}, "ttl": {"N": expired}, "value": {"S": "old"}}),
            new_item.clone(),
        ] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Item": item
            }))
            .unwrap();
            let written = observe(store.operations.put_item(put)).await;
            written.assert_no_warning_or_error();
            written.result.unwrap();
        }
        let due = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(), "Key": {"pk": {"S": "due"}}
        }))
        .unwrap();
        let before = store.operations.get_item(due).await.unwrap();
        assert_eq!(
            serde_json::to_value(before).unwrap(),
            json!({"Item": due_item})
        );

        let request = ExpirationRequest {
            destination: Namespace::try_new("rewrite-conformance-expiration").unwrap(),
            job: JobId::from([8; 16]),
            fence: NonZeroU64::MIN,
            requested_cutoff: store.now(),
            candidate_limit: NonZeroUsize::new(16).unwrap(),
        };
        let mut complete = false;
        let mut advanced = 0;
        for _ in 0..16 {
            let step = observe(
                store
                    .expiration_repository
                    .advance_expiration(request.clone()),
            )
            .await;
            step.assert_no_warning_or_error();
            match step.result.unwrap() {
                StepResult::Advanced(()) => advanced += 1,
                StepResult::Complete => {
                    complete = true;
                    break;
                }
                StepResult::Superseded => panic!("expiration job lost its fence"),
            }
        }
        assert!(
            advanced > 0,
            "expiration sweep must advance through the due page"
        );
        assert!(complete, "expiration sweep must finish without waiting");

        for replay in [false, true] {
            if replay {
                let step = observe(
                    store
                        .expiration_repository
                        .advance_expiration(request.clone()),
                )
                .await;
                step.assert_no_warning_or_error();
                step.result
                    .expect("completed expiration job can be replayed");
            }
            for (key, expected) in [
                ("due", json!({})),
                ("replaced", json!({"Item": new_item.clone()})),
            ] {
                let get = GetItemRequest::try_from(json!({
                    "TableName": table.as_ref(), "Key": {"pk": {"S": key}}
                }))
                .unwrap();
                let read = observe(store.operations.get_item(get)).await;
                read.assert_no_warning_or_error();
                let span = read.one_span_with("dynamo_operation", &[("operation", "GetItem")]);
                assert_eq!(span.fields.len(), 1);
                assert_eq!(
                    serde_json::to_value(read.result.unwrap()).unwrap(),
                    expected
                );
            }
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Negative rule: the exact missing-table message comes from dynamo-conformance.
#[tokio::test]
async fn given_missing_table_when_ttl_is_described_then_aws_error_and_log_level_are_exact() {
    let case = FIXTURES.case(
        include_str!("../fixtures/describe_time_to_live_cases.jsonl"),
        "400/missing_table.json",
    );
    assert_eq!(case["operation"], "DescribeTimeToLive");
    let request: DescribeTimeToLiveRequest =
        serde_json::from_value(case["request"].clone()).unwrap();
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let observed = observe(store.operations.describe_time_to_live(request)).await;
        observed.assert_no_warning_or_error();
        let span =
            observed.one_span_with("dynamo_operation", &[("operation", "DescribeTimeToLive")]);
        assert_eq!(span.fields.len(), 1);
        assert_dynamo_error_response_exact(
            observed.result.unwrap_err(),
            axum::http::StatusCode::from_u16(case["expected"]["status"].as_u64().unwrap() as u16)
                .unwrap(),
            &case["expected"]["body"].to_string(),
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business rule: a bounded direct sweep deletes only current expired values;
// stale cursor and policy commands cannot make further changes.
#[tokio::test]
async fn direct_ttl_sweep_pages_and_rejects_stale_commands() {
    use storage_common::{TtlSweepPageCommand, TtlSweepPageInput};

    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let update = serde_json::from_value(json!({
            "TableName": table.as_ref(),
            "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": true}
        }))
        .unwrap();
        store.operations.update_time_to_live(update).await.unwrap();
        let cutoff = store.now();
        for (key, ttl) in [("a", 1), ("b", 2), ("c", 3), ("live", 4102444800i64)] {
            store
                .operations
                .put_item(
                    PutItemRequest::try_from(json!({
                        "TableName": table.as_ref(),
                        "Item": {"pk": {"S": key}, "ttl": {"N": ttl.to_string()}}
                    }))
                    .unwrap(),
                )
                .await
                .unwrap();
        }
        let config = store
            .ttl_sweep_repository
            .read_ttl_sweep_policy(&table)
            .await
            .unwrap()
            .unwrap();
        let first = TtlSweepPageCommand::try_new(
            table.clone(),
            TtlSweepPageInput::try_new("ttl".to_owned(), cutoff, 1, None).unwrap(),
            config.policy_generation,
            cutoff,
        )
        .unwrap();
        let page = store
            .ttl_sweep_repository
            .execute_ttl_sweep_page(first.clone())
            .await
            .unwrap();
        assert_eq!(page.deleted_items, 1);
        assert!(page.next_cursor.is_some());
        let stale = store
            .ttl_sweep_repository
            .execute_ttl_sweep_page(first.clone())
            .await;
        let Err(error) = stale else {
            panic!("stale TTL command succeeded");
        };
        assert!(matches!(
            error.to_enum(),
            storage_types::StorageEnum::ConditionalCheckFailed
        ));
        let mut cursor = page.next_cursor;
        let mut deleted = page.deleted_items;
        while let Some(next) = cursor {
            let command = TtlSweepPageCommand::try_new(
                table.clone(),
                TtlSweepPageInput::try_new("ttl".to_owned(), cutoff, 1, Some(next)).unwrap(),
                config.policy_generation,
                cutoff,
            )
            .unwrap();
            let page = store
                .ttl_sweep_repository
                .execute_ttl_sweep_request(storage_common::TtlSweepRequest {
                    table_name: command.table_name,
                    input: command.input,
                })
                .await
                .unwrap();
            deleted += page.deleted_items;
            cursor = page.next_cursor;
        }
        assert_eq!(deleted, 3);
        for key in ["a", "b", "c", "live"] {
            let response = store
                .operations
                .get_item(
                    GetItemRequest::try_from(json!({
                        "TableName": table.as_ref(), "Key": {"pk": {"S": key}}
                    }))
                    .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.item.is_some(), key == "live");
        }
        let disable = serde_json::from_value(json!({
            "TableName": table.as_ref(),
            "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": false}
        }))
        .unwrap();
        store.operations.update_time_to_live(disable).await.unwrap();
        let stale = store
            .ttl_sweep_repository
            .execute_ttl_sweep_page(first)
            .await;
        let Err(error) = stale else {
            panic!("stale TTL command succeeded");
        };
        assert!(matches!(
            error.to_enum(),
            storage_types::StorageEnum::ConditionalCheckFailed
        ));
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
