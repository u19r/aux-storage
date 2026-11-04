//! Stream discovery metadata through the replacement Dynamo catalog operation.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{CreateTableRequest, DescribeTableRequest, ListStreamsRequest};

use super::test_support::{TestStore, unique_table_name};

// Business and format rules: only a stream-enabled table publishes a coherent
// stream identity.
#[tokio::test]
async fn given_enabled_and_disabled_tables_when_created_and_described_then_only_enabled_table_has_stream_identity()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let enabled = unique_table_name();
        let enabled_request = CreateTableRequest::try_from(json!({
            "TableName": enabled.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }))
        .expect("valid stream-enabled CreateTable request");
        let created = observe(store.operations.create_table(enabled_request)).await;
        created.assert_no_warning_or_error();
        let created = created
            .result
            .expect("create enabled table")
            .table_description;
        let described = observe(store.operations.describe_table(DescribeTableRequest {
            table_name: enabled.clone(),
        }))
        .await;
        described.assert_no_warning_or_error();
        let described = described.result.expect("describe enabled table").table;
        assert_eq!(created.latest_stream_arn, described.latest_stream_arn);
        assert_eq!(created.latest_stream_label, described.latest_stream_label);
        for table_name in [Some(enabled.clone()), None] {
            let listed = observe(store.operations.list_streams(ListStreamsRequest {
                table_name,
                exclusive_start_stream_arn: None,
                limit: Some(100),
            }))
            .await;
            listed.assert_no_warning_or_error();
            let listed = listed.result.expect("list enabled table stream");
            let entry = listed
                .streams
                .iter()
                .find(|entry| entry.table_name == enabled)
                .expect("enabled stream is listed");
            assert_eq!(
                Some(entry.stream_arn.as_str()),
                created.latest_stream_arn.as_deref()
            );
            assert_eq!(
                Some(entry.stream_label.as_str()),
                created.latest_stream_label.as_deref()
            );
        }
        assert_eq!(
            serde_json::to_value(described.stream_specification).expect("serialize stream view"),
            json!({"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}),
        );
        let label = created
            .latest_stream_label
            .expect("enabled table has a stream label");
        assert_eq!(
            created.latest_stream_arn,
            Some(format!(
                "arn:aws:dynamodb:us-east-1:123456789012:table/{enabled}/stream/{label}"
            )),
        );

        let disabled = unique_table_name();
        let disabled_request = CreateTableRequest::try_from(json!({
            "TableName": disabled.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("valid stream-disabled CreateTable request");
        let created = observe(store.operations.create_table(disabled_request)).await;
        created.assert_no_warning_or_error();
        let created = created
            .result
            .expect("create disabled table")
            .table_description;
        let described = observe(store.operations.describe_table(DescribeTableRequest {
            table_name: disabled,
        }))
        .await;
        described.assert_no_warning_or_error();
        let described = described.result.expect("describe disabled table").table;
        assert!(created.latest_stream_arn.is_none());
        assert!(created.latest_stream_label.is_none());
        assert!(described.latest_stream_arn.is_none());
        assert!(described.latest_stream_label.is_none());
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[tokio::test]
async fn given_enabled_table_when_stream_read_requires_another_incarnation_then_no_page_or_tail_is_returned()
 {
    use storage_types::{
        GetStreamRecordsRequest, PutItemRequest, StorageEnum, TableHistoryId, TableHistoryTarget,
        context::WrappedError,
    };
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let name = unique_table_name();
        let request = CreateTableRequest::try_from(json!({
            "TableName": name.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
        }))
        .unwrap();
        let created = store
            .operations
            .create_table(request)
            .await
            .unwrap()
            .table_description;
        let label = created.latest_stream_label.unwrap();
        let (_, id) = label.rsplit_once('-').unwrap();
        let history_id = TableHistoryId::from(uuid::Uuid::parse_str(id).unwrap());
        let item: storage_types::AttributeMap =
            serde_json::from_value(json!({"pk": {"S": "one"}})).unwrap();
        let item = storage_types::WireItem::try_from(&item).unwrap();
        store
            .operations
            .put_item(PutItemRequest::new(name.clone(), item))
            .await
            .unwrap();
        let correct = TableHistoryTarget {
            table_name: name.clone(),
            history_id,
        };
        let page = store
            .operations
            .get_stream_records(GetStreamRecordsRequest::table_at_incarnation(
                correct.clone(),
                None,
                Some(10),
            ))
            .await
            .unwrap_or_else(|error| panic!("table page failed: {:?}", error.to_enum()));
        assert_eq!(page.records().len(), 1);
        let checkpoint = page.records()[0]
            .cursor
            .as_deref()
            .unwrap()
            .parse::<storage_types::TableStreamCheckpoint>()
            .unwrap();
        assert_eq!(checkpoint.history_id(), history_id);
        let mut other = *history_id.as_ref();
        // Counter-backed providers can allocate ID1; flipping its low bit
        // would produce the forbidden zero ID instead of a valid stale ID.
        other[0] ^= 0x80;
        let stale_token = storage_types::TableStreamCheckpoint::try_new(
            other.into(),
            checkpoint.source_scope().clone(),
            checkpoint.positions().to_vec(),
            storage_types::TableStreamReadProgress {
                next_lane: checkpoint.next_lane(),
                initial_visibility_cutoff: checkpoint.initial_visibility_cutoff(),
            },
        )
        .unwrap()
        .to_string();
        let error = store
            .operations
            .get_stream_records(GetStreamRecordsRequest::table(
                name.clone(),
                Some(stale_token),
                Some(10),
            ))
            .await
            .err()
            .unwrap();
        assert!(matches!(error.to_enum(), StorageEnum::TableNotFound { .. }));
        let resumed = store
            .operations
            .get_stream_records(GetStreamRecordsRequest::table(
                name.clone(),
                Some(checkpoint.to_string()),
                Some(10),
            ))
            .await
            .unwrap();
        assert!(resumed.records().is_empty());
        assert!(
            store
                .operations
                .table_stream_checkpoint(correct)
                .await
                .unwrap()
                .history_id()
                == history_id
        );
        let stale = TableHistoryTarget {
            table_name: name,
            history_id: TableHistoryId::from(uuid::Uuid::new_v4()),
        };
        for error in [
            store
                .operations
                .get_stream_records(GetStreamRecordsRequest::table_at_incarnation(
                    stale.clone(),
                    None,
                    Some(10),
                ))
                .await
                .err()
                .unwrap(),
            store
                .operations
                .table_stream_checkpoint(stale)
                .await
                .err()
                .unwrap(),
        ] {
            assert!(matches!(error.to_enum(), StorageEnum::TableNotFound { .. }));
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
