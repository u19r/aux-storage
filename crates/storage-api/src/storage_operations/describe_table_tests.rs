use storage_types::{
    AttributeDefinition, CreateGlobalSecondaryIndex, CreateTableRequest, DescribeTableRequest,
    DescribeTimeToLiveRequest, HIDDEN_TTL_INDEX_PREFIX, IndexName, KeyAttributeType,
    KeySchemaElement, KeyType, Projection, ProjectionType, TableName, TimeToLiveSpecification,
    UpdateTimeToLiveRequest,
};

use crate::{
    routes::routes_tests::{
        for_each_conformance_backend, handle_describe_table, handle_describe_time_to_live,
    },
    types::Response,
};

#[tokio::test]
async fn describe_table_omits_hidden_ttl_index() {
    for_each_conformance_backend(|_, db| async move {
        let table = TableName::new("DescribeTtlTable");

        let mut request = CreateTableRequest::new(
            table.clone(),
            vec![
                AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "sk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "ttl".to_string(),
                    attribute_type: KeyAttributeType::N,
                },
            ],
            vec![
                KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            storage_types::BillingMode::PayPerRequest,
        );
        request.global_secondary_indexes = Some(vec![CreateGlobalSecondaryIndex {
            index_name: IndexName::new("VisibleIndex"),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "ttl".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            projection: Projection {
                projection_type: Some(ProjectionType::All),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]);
        db.create_table(&request).await.expect("create table");

        db.update_time_to_live(UpdateTimeToLiveRequest {
            table_name: table.clone(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: "ttl".to_string(),
                enabled: true,
            },
        })
        .await
        .expect("enable ttl");

        let response = handle_describe_table(
            db.clone(),
            DescribeTableRequest {
                table_name: table.clone(),
            },
        )
        .await
        .expect("describe table");

        let Response::DescribeTable(resp) = response else {
            panic!("unexpected response variant");
        };
        let gsis = resp.table.global_secondary_indexes.unwrap();
        let hidden_index = IndexName::new(&format!(
            "{}{}",
            HIDDEN_TTL_INDEX_PREFIX,
            table.sanitized_name()
        ));
        assert!(
            gsis.iter().all(|g| g.index_name != hidden_index),
            "hidden TTL index should not be returned in DescribeTable"
        );
    })
    .await;
}

#[tokio::test]
async fn describe_table_uses_same_table_arn_shape_as_create_table() {
    for_each_conformance_backend(|_, db| async move {
        let table = TableName::new("DescribeArnTable");

        let request = CreateTableRequest::new(
            table.clone(),
            vec![AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            }],
            vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            }],
            storage_types::BillingMode::PayPerRequest,
        );
        db.create_table(&request).await.expect("create table");

        let response = handle_describe_table(
            db,
            DescribeTableRequest {
                table_name: table.clone(),
            },
        )
        .await
        .expect("describe table");

        let Response::DescribeTable(resp) = response else {
            panic!("unexpected response variant");
        };
        assert_eq!(
            resp.table.table_arn,
            "arn:aws:dynamodb:us-east-1:123456789012:table/DescribeArnTable"
        );
    })
    .await;
}

#[tokio::test]
async fn describe_table_includes_latest_stream_metadata_when_streams_are_enabled() {
    for_each_conformance_backend(|_, db| async move {
        let table = TableName::new("DescribeStreamTable");

        let mut request = CreateTableRequest::new(
            table.clone(),
            vec![AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            }],
            vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            }],
            storage_types::BillingMode::PayPerRequest,
        );
        request.stream_specification = Some(storage_types::StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
        });
        db.create_table(&request).await.expect("create table");

        let response = handle_describe_table(
            db,
            DescribeTableRequest {
                table_name: table.clone(),
            },
        )
        .await
        .expect("describe table");

        let Response::DescribeTable(resp) = response else {
            panic!("unexpected response variant");
        };
        let stream_label = resp
            .table
            .latest_stream_label
            .as_deref()
            .expect("latest stream label");
        let stream_arn = resp
            .table
            .latest_stream_arn
            .as_deref()
            .expect("latest stream arn");
        assert_eq!(
            stream_arn,
            format!(
                "arn:aws:dynamodb:us-east-1:123456789012:table/DescribeStreamTable/stream/\
                 {stream_label}"
            )
        );
    })
    .await;
}

#[tokio::test]
async fn describe_time_to_live_reflects_enabled_status() {
    for_each_conformance_backend(|_, db| async move {
        let table = TableName::new("DescribeTtlStatus");

        let request = CreateTableRequest::new(
            table.clone(),
            vec![
                AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "sk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "ttl".to_string(),
                    attribute_type: KeyAttributeType::N,
                },
            ],
            vec![
                KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            storage_types::BillingMode::PayPerRequest,
        );
        db.create_table(&request).await.expect("create table");

        db.update_time_to_live(UpdateTimeToLiveRequest {
            table_name: table.clone(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: "ttl".to_string(),
                enabled: true,
            },
        })
        .await
        .expect("enable ttl");

        let before_jobs = db
            .describe_time_to_live(&table)
            .await
            .expect("describe ttl before maintenance");
        let expected_status = before_jobs
            .time_to_live_description
            .as_ref()
            .expect("ttl description before maintenance")
            .time_to_live_status
            .clone();
        assert!(matches!(
            expected_status,
            storage_types::TimeToLiveStatus::Enabling | storage_types::TimeToLiveStatus::Enabled
        ));
        let response = handle_describe_time_to_live(
            db,
            DescribeTimeToLiveRequest {
                table_name: table.clone(),
            },
        )
        .await
        .expect("describe ttl");

        let Response::DescribeTimeToLive(resp) = response else {
            panic!("unexpected response variant");
        };
        let description = resp.time_to_live_description.expect("ttl description");
        assert_eq!(description.attribute_name.as_deref(), Some("ttl"));
        assert_eq!(description.time_to_live_status, expected_status);
    })
    .await;
}
