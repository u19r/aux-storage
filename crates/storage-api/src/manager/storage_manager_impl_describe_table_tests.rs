use storage_types::{
    AttributeDefinition, CreateGlobalSecondaryIndex, CreateReplicaAction, CreateTableRequest,
    DescribeTableRequest, DescribeTimeToLiveRequest, HIDDEN_TTL_INDEX_PREFIX, IndexName,
    KeyAttributeType, KeySchemaElement, KeyType, MultiRegionConsistency, Projection,
    ProjectionType, ReplicaStatus, ReplicaUpdate, TableName, TimeToLiveSpecification,
    UpdateTableRequest, UpdateTimeToLiveRequest,
};

use crate::{
    routes::routes_support_tests::{
        create_test_db, handle_describe_table, handle_describe_time_to_live,
    },
    types::Response,
};

#[tokio::test]
async fn describe_table_omits_hidden_ttl_index() {
    let db = create_test_db().await;
    let table = TableName::new("DescribeTtlTable");

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
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
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
    }]));
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
}

#[tokio::test]
async fn describe_table_uses_same_table_arn_shape_as_create_table() {
    let db = create_test_db().await;
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
}

#[tokio::test]
async fn describe_table_includes_latest_stream_metadata_when_streams_are_enabled() {
    let db = create_test_db().await;
    let table = TableName::new("DescribeStreamTable");

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
    )
    .with_stream_specification(Some(storage_types::StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
    }));
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
}

#[tokio::test]
async fn describe_time_to_live_reflects_enabled_status() {
    let db = create_test_db().await;
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
    assert_eq!(
        description.time_to_live_status,
        storage_types::TimeToLiveStatus::Enabled
    );
}

#[tokio::test]
async fn describe_table_includes_multi_region_replica_state() {
    let db = create_test_db().await;
    let table = TableName::new("DescribeReplicaTable");

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
    db.update_table(UpdateTableRequest {
        table_name: table.clone(),
        attribute_definitions: None,
        billing_mode: None,
        provisioned_throughput: None,
        on_demand_throughput: None,
        deletion_protection_enabled: None,
        aux_stream_duration_hours: None,
        aux_default_item_stream_duration_hours: None,
        global_secondary_index_updates: None,
        replica_updates: Some(vec![ReplicaUpdate {
            create: Some(CreateReplicaAction {
                region_name: "eu-west-1".to_string(),
            }),
            update: None,
            delete: None,
        }]),
        sse_specification: None,
        stream_specification: None,
        table_class: None,
    })
    .await
    .expect("seed replica state");

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
        resp.table.multi_region_consistency,
        Some(MultiRegionConsistency::Eventual)
    );
    assert_eq!(
        resp.table.replicas,
        Some(vec![storage_types::ReplicaDescription {
            region_name: "eu-west-1".to_string(),
            replica_status: ReplicaStatus::Creating,
            replica_status_description: Some("Replica creation requested".to_string()),
            replica_inaccessible_date_time: None,
        }])
    );
}
