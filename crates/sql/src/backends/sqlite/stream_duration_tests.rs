use std::collections::HashMap;

use storage_provider::{StorageProvider, StreamTrimDueMarker, StreamTrimScope, StreamTrimState};
use storage_types::{
    AttributeDefinition, AttributeValue, BatchWriteItemRequest, BillingMode, CreateTableRequest,
    ItemKey, KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType, PutRequest, StorageError,
    StorageResult, StreamName, StreamRetentionDuration, StreamSpecification, StreamViewType,
    TableName, TableStatus, TimestampMillis, TransactConditionCheckRequest, TransactPutRequest,
    TransactWriteItem, TransactWriteItemsRequest, UpdateItemRequest, UpdateTableRequest,
    WriteRequest,
};
use stream_provider::StreamProvider;

use crate::backends::sqlite::{
    SQLiteStorageProvider, provider_table_lifecycle::load_sqlite_table_scope_id,
};

async fn initialized_provider() -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new(":memory:")
        .await
        .expect("sqlite provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    provider
        .initialize_stream()
        .await
        .expect("initialize stream");
    provider
}

fn create_table_request(table_name: &str) -> CreateTableRequest {
    CreateTableRequest::new(
        TableName::new(table_name),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    )
}

fn update_table_request(table_name: &TableName) -> UpdateTableRequest {
    UpdateTableRequest {
        table_name: table_name.clone(),
        max_indexers: None,
        attribute_definitions: None,
        billing_mode: None,
        provisioned_throughput: None,
        on_demand_throughput: None,
        deletion_protection_enabled: None,
        aux_stream_duration_hours: None,
        aux_default_item_stream_duration_hours: None,
        global_secondary_index_updates: None,
        replica_updates: None,
        sse_specification: None,
        stream_specification: None,
        table_class: None,
    }
}

fn trim_state(scope: StreamTrimScope, policy_version: u64) -> StreamTrimState {
    StreamTrimState {
        scope,
        policy_version,
        retention: StreamRetentionDuration::FiniteHours(6),
        effective_retention: StreamRetentionDuration::FiniteHours(6),
        next_due_at: Some(TimestampMillis::from_timestamp(21_600_000)),
        oldest_retained_version: None,
        oldest_retained_timestamp: None,
        latest_version: None,
        latest_timestamp: None,
        updated_at: TimestampMillis::from_timestamp(1_000),
    }
}

fn item(pk: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        (
            "value".to_string(),
            AttributeValue::S("original".to_string()),
        ),
    ])
}

fn key(pk: &str) -> KeyAttributes {
    KeyAttributes::from(HashMap::from([(
        "pk".to_string(),
        AttributeValue::S(pk.to_string()),
    )]))
}

fn item_trim_scope(table_info: &storage_types::StoredTableInfo, pk: &str) -> StreamTrimScope {
    let key = key(pk);
    let item_key =
        ItemKey::from_key_schema(table_info.table_name.clone(), &table_info.key_schema, &key)
            .expect("item key");
    let stream =
        StreamName::table_item_stream(&table_info.table_name, &item_key).expect("item stream");
    let scope_id = String::from(&stream);
    let item_key_hash = sqlite_item_stream_key_hash(&stream);
    StreamTrimScope::item(scope_id, table_info.table_name.clone(), item_key_hash)
}

fn sqlite_item_stream_key_hash(stream_name: &StreamName) -> String {
    let digest = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, stream_name.as_ref())
        .as_hyphenated()
        .to_string();
    format!("sqlite-key:{digest}")
}

async fn create_duration_table(
    provider: &SQLiteStorageProvider,
    table_name: &str,
) -> StorageResult<TableName> {
    let table_name = TableName::new(table_name);
    let mut create = create_table_request(table_name.as_ref());
    create.aux_stream_duration_hours = Some(StreamRetentionDuration::FiniteHours(72));
    provider.create_table(&create).await?;
    Ok(table_name)
}

async fn create_stream_duration_table(
    provider: &SQLiteStorageProvider,
    table_name: &str,
) -> StorageResult<TableName> {
    let table_name = TableName::new(table_name);
    let mut create = create_table_request(table_name.as_ref()).with_stream_specification(Some(
        StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        },
    ));
    create.aux_stream_duration_hours = Some(StreamRetentionDuration::FiniteHours(1));
    provider.create_table(&create).await?;
    Ok(table_name)
}

async fn create_multi_region_control_table(provider: &SQLiteStorageProvider) -> StorageResult<()> {
    provider
        .create_table(&CreateTableRequest::new(
            TableName::new("sys_storage_replication"),
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
            BillingMode::PayPerRequest,
        ))
        .await
}

async fn force_stream_rows_created_at(
    provider: &SQLiteStorageProvider,
    created_at: TimestampMillis,
) {
    provider
        .connection
        .call_unwrap(move |conn| {
            conn.execute("UPDATE sys_stream_items SET created_at = ?1", [*created_at])
                .expect("update stream item created_at");
            conn.execute(
                "UPDATE sys_stream_pointer_index SET created_at = ?1",
                [*created_at],
            )
            .expect("update stream pointer index created_at");
        })
        .await;
}

async fn pointer_index_count(provider: &SQLiteStorageProvider) -> usize {
    provider
        .connection
        .call_unwrap(move |conn| {
            conn.query_row("SELECT COUNT(*) FROM sys_stream_pointer_index", [], |row| {
                row.get::<_, i64>(0)
            })
            .map(|count| usize::try_from(count).expect("pointer index count"))
            .expect("count pointer index")
        })
        .await
}

async fn table_stream_item_count(
    provider: &SQLiteStorageProvider,
    table_name: &TableName,
) -> StorageResult<usize> {
    let page =
        StreamProvider::read_forward(provider, StreamName::table_stream(table_name), None, 10)
            .await
            .map_err(|err| StorageError::internal(&format!("read table stream failed: {err}")))?;
    Ok(page.items.len())
}

#[tokio::test]
async fn sqlite_table_metadata_persists_custom_stream_durations() -> StorageResult<()> {
    let provider = initialized_provider().await;
    let table_name = TableName::new("duration_metadata");
    let mut create = create_table_request(table_name.as_ref());
    create.aux_stream_duration_hours = Some(StreamRetentionDuration::FiniteHours(168));
    create.aux_default_item_stream_duration_hours = Some(StreamRetentionDuration::Forever);

    provider.create_table(&create).await?;
    let table = provider.get_table_info(&table_name).await?;
    let table_scope_id = load_sqlite_table_scope_id(&provider, &table_name).await?;
    let table_scope = StreamTrimScope::table(table_scope_id.clone(), table_name.clone());
    let trim_state = provider
        .load_stream_trim_state_by_scope(&table_scope)
        .await?
        .expect("create table should write trim state");

    assert_eq!(
        table.table_stream_duration,
        StreamRetentionDuration::FiniteHours(168)
    );
    assert_eq!(
        table.default_item_stream_duration,
        StreamRetentionDuration::Forever
    );
    assert_eq!(trim_state.policy_version, 1);
    assert_eq!(
        trim_state.retention,
        StreamRetentionDuration::FiniteHours(168)
    );
    assert!(trim_state.next_due_at.is_some());

    let mut update = update_table_request(&table_name);
    update.aux_stream_duration_hours = Some(StreamRetentionDuration::FiniteHours(24));
    update.aux_default_item_stream_duration_hours = Some(StreamRetentionDuration::FiniteHours(48));
    provider.update_table(update).await?;
    let table = provider.get_table_info(&table_name).await?;
    let updated_trim_state = provider
        .load_stream_trim_state_by_scope(&table_scope)
        .await?
        .expect("update table should write trim state");

    assert_eq!(
        table.table_stream_duration,
        StreamRetentionDuration::FiniteHours(24)
    );
    assert_eq!(
        table.default_item_stream_duration,
        StreamRetentionDuration::FiniteHours(48)
    );
    assert_eq!(table.table_status, TableStatus::Active);
    assert_eq!(updated_trim_state.policy_version, 2);
    assert_eq!(
        updated_trim_state.retention,
        StreamRetentionDuration::FiniteHours(24)
    );

    let due_markers = provider
        .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 10)
        .await?;
    assert!(
        due_markers
            .iter()
            .any(|marker| marker.scope.scope_id == table_scope_id && marker.policy_version == 2),
        "update should write a due marker for the new table policy"
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_stream_trim_state_round_trips_by_scope() -> StorageResult<()> {
    let provider = initialized_provider().await;
    let scope = StreamTrimScope::table("table/orders", TableName::new("orders"));
    let state = trim_state(scope.clone(), 7);

    provider.write_stream_trim_state(state.clone()).await?;
    let loaded = provider
        .load_stream_trim_state_by_scope(&scope)
        .await?
        .expect("trim state should exist");

    assert_eq!(loaded, state);
    Ok(())
}

#[tokio::test]
async fn sqlite_due_markers_are_ordered_and_bounded() -> StorageResult<()> {
    let provider = initialized_provider().await;
    let late_scope = StreamTrimScope::table("table/late", TableName::new("late"));
    let early_scope = StreamTrimScope::table("table/early", TableName::new("early"));
    let late = StreamTrimDueMarker::new(TimestampMillis::from_timestamp(7_200_000), late_scope, 2);
    let early =
        StreamTrimDueMarker::new(TimestampMillis::from_timestamp(3_600_000), early_scope, 1);

    provider.upsert_stream_trim_due_marker(late.clone()).await?;
    provider
        .upsert_stream_trim_due_marker(early.clone())
        .await?;

    let first_page = provider
        .list_due_stream_trim_markers(TimestampMillis::from_timestamp(7_200_000), 1)
        .await?;
    assert_eq!(first_page, vec![early]);

    let all_due = provider
        .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 10)
        .await?;
    assert_eq!(all_due, vec![first_page[0].clone(), late]);
    Ok(())
}

#[tokio::test]
async fn sqlite_put_item_writes_item_stream_duration_state() -> StorageResult<()> {
    let provider = initialized_provider().await;
    let table_name = create_duration_table(&provider, "item_put_duration").await?;

    provider
        .put_item_with_stream_ttl(
            table_name.clone(),
            item("a"),
            None,
            None,
            None,
            None,
            Some(StreamRetentionDuration::FiniteHours(12)),
        )
        .await?;

    let table = provider.get_table_info(&table_name).await?;
    let scope = item_trim_scope(&table, "a");
    let state = provider
        .load_stream_trim_state_by_scope(&scope)
        .await?
        .expect("put item should write item trim state");
    assert_eq!(state.policy_version, ((12_u64) << 32) | u64::from(72_u32));
    assert_eq!(state.retention, StreamRetentionDuration::FiniteHours(12));
    assert!(
        state
            .scope
            .item_key_hash
            .as_deref()
            .is_some_and(|hash| hash.starts_with("sqlite-key:"))
    );
    assert_eq!(
        state.effective_retention,
        StreamRetentionDuration::FiniteHours(72)
    );
    assert!(state.next_due_at.is_some());
    Ok(())
}

#[tokio::test]
async fn sqlite_repeated_item_stream_duration_policy_does_not_churn_markers() -> StorageResult<()> {
    let provider = initialized_provider().await;
    let table_name = create_duration_table(&provider, "item_repeated_duration").await?;

    provider
        .put_item_with_stream_ttl(
            table_name.clone(),
            item("a"),
            None,
            None,
            None,
            None,
            Some(StreamRetentionDuration::FiniteHours(12)),
        )
        .await?;
    provider
        .put_item_with_stream_ttl(
            table_name.clone(),
            item("a"),
            None,
            None,
            None,
            None,
            Some(StreamRetentionDuration::FiniteHours(12)),
        )
        .await?;

    let table = provider.get_table_info(&table_name).await?;
    let scope = item_trim_scope(&table, "a");
    let state = provider
        .load_stream_trim_state_by_scope(&scope)
        .await?
        .expect("put item should write item trim state");
    let due_markers = provider
        .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 100)
        .await?;
    let item_due_markers = due_markers
        .iter()
        .filter(|marker| marker.scope.scope_id == scope.scope_id)
        .count();

    assert_eq!(state.policy_version, ((12_u64) << 32) | u64::from(72_u32));
    assert_eq!(
        item_due_markers, 1,
        "repeated identical item TTL should upsert one current marker"
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_update_item_writes_item_stream_duration_state() -> StorageResult<()> {
    let provider = initialized_provider().await;
    let table_name = create_duration_table(&provider, "item_update_duration").await?;
    provider
        .put_item(table_name.clone(), item("a"), None, None, None, None)
        .await?;

    provider
        .update_item(UpdateItemRequest {
            table_name: table_name.clone(),
            key: key("a"),
            update_expression: Some("SET value = :value".to_string()),
            indexers: None,
            attribute_updates: None,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: Some(HashMap::from([(
                ":value".to_string(),
                AttributeValue::S("updated".to_string()),
            )])),
            expected: None,
            conditional_operator: None,
            return_values: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: Some(StreamRetentionDuration::FiniteHours(96)),
        })
        .await?;

    let table = provider.get_table_info(&table_name).await?;
    let state = provider
        .load_stream_trim_state_by_scope(&item_trim_scope(&table, "a"))
        .await?
        .expect("update item should write item trim state");
    assert_eq!(state.retention, StreamRetentionDuration::FiniteHours(96));
    assert_eq!(
        state.effective_retention,
        StreamRetentionDuration::FiniteHours(96)
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_delete_item_writes_item_stream_duration_state() -> StorageResult<()> {
    let provider = initialized_provider().await;
    let table_name = create_duration_table(&provider, "item_delete_duration").await?;
    provider
        .put_item_with_stream_ttl(
            table_name.clone(),
            item("a"),
            None,
            None,
            None,
            None,
            Some(StreamRetentionDuration::FiniteHours(168)),
        )
        .await?;

    provider
        .delete_item_with_stream_ttl(
            table_name.clone(),
            key("a"),
            None,
            None,
            None,
            Some(StreamRetentionDuration::FiniteHours(24)),
        )
        .await?;

    let table = provider.get_table_info(&table_name).await?;
    let state = provider
        .load_stream_trim_state_by_scope(&item_trim_scope(&table, "a"))
        .await?
        .expect("delete item should write item trim state");
    assert_eq!(state.retention, StreamRetentionDuration::FiniteHours(24));
    assert_eq!(
        state.effective_retention,
        StreamRetentionDuration::FiniteHours(72)
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_failed_conditional_put_does_not_write_item_stream_duration_state()
-> StorageResult<()> {
    let provider = initialized_provider().await;
    let table_name = create_duration_table(&provider, "item_conditional_duration").await?;
    provider
        .put_item(table_name.clone(), item("a"), None, None, None, None)
        .await?;

    let result = provider
        .put_item_with_stream_ttl(
            table_name.clone(),
            item("a"),
            Some("attribute_not_exists(pk)".to_string()),
            None,
            None,
            None,
            Some(StreamRetentionDuration::FiniteHours(24)),
        )
        .await;
    assert!(result.is_err());

    let table = provider.get_table_info(&table_name).await?;
    assert!(
        provider
            .load_stream_trim_state_by_scope(&item_trim_scope(&table, "a"))
            .await?
            .is_none(),
        "failed conditional write must not leave item trim state"
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_batch_write_put_applies_item_stream_duration_state() -> StorageResult<()> {
    let provider = initialized_provider().await;
    let table_name = create_duration_table(&provider, "item_batch_duration").await?;

    provider
        .batch_write_item(
            BatchWriteItemRequest {
                request_items: HashMap::from([(
                    table_name.clone(),
                    vec![WriteRequest {
                        put_request: Some(PutRequest {
                            item: item("batch"),
                            indexers: None,
                            aux_item_stream_ttl_hours: Some(StreamRetentionDuration::FiniteHours(
                                120,
                            )),
                        }),
                        delete_request: None,
                    }],
                )]),
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
            },
            true,
        )
        .await?;

    let table = provider.get_table_info(&table_name).await?;
    let state = provider
        .load_stream_trim_state_by_scope(&item_trim_scope(&table, "batch"))
        .await?
        .expect("batch put should write item trim state");
    assert_eq!(state.retention, StreamRetentionDuration::FiniteHours(120));
    Ok(())
}

#[tokio::test]
async fn sqlite_transaction_rollback_removes_item_stream_duration_state() -> StorageResult<()> {
    let provider = initialized_provider().await;
    let table_name = create_duration_table(&provider, "item_txn_duration").await?;
    provider
        .put_item(table_name.clone(), item("existing"), None, None, None, None)
        .await?;

    let result = provider
        .transact_write_items(TransactWriteItemsRequest {
            transact_items: vec![
                TransactWriteItem {
                    put: Some(TransactPutRequest {
                        table_name: table_name.clone(),
                        item: item("new"),
                        indexers: None,
                        condition_expression: None,
                        expression_attribute_names: None,
                        expression_attribute_values: None,
                        return_values_on_condition_check_failure: None,
                        aux_item_stream_ttl_hours: Some(StreamRetentionDuration::FiniteHours(24)),
                    }),
                    update: None,
                    delete: None,
                    condition_check: None,
                },
                TransactWriteItem {
                    put: None,
                    update: None,
                    delete: None,
                    condition_check: Some(TransactConditionCheckRequest {
                        table_name: table_name.clone(),
                        key: key("existing"),
                        condition_expression: "attribute_not_exists(pk)".to_string(),
                        expression_attribute_names: None,
                        expression_attribute_values: None,
                        return_values_on_condition_check_failure: None,
                    }),
                },
            ],
            client_request_token: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        })
        .await;
    assert!(result.is_err());

    let table = provider.get_table_info(&table_name).await?;
    assert!(
        provider
            .load_stream_trim_state_by_scope(&item_trim_scope(&table, "new"))
            .await?
            .is_none(),
        "transaction rollback must remove item trim state"
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_custom_table_trim_deletes_bounded_table_stream_page() -> StorageResult<()> {
    let provider = initialized_provider().await;
    let table_name = create_stream_duration_table(&provider, "table_trim_duration").await?;
    provider
        .put_item(table_name.clone(), item("a"), None, None, None, None)
        .await
        .expect("put item for table trim");
    force_stream_rows_created_at(&provider, TimestampMillis::now() - (2 * 60 * 60 * 1000)).await;

    let table_scope_id = load_sqlite_table_scope_id(&provider, &table_name).await?;
    provider
        .upsert_stream_trim_due_marker(StreamTrimDueMarker::new(
            TimestampMillis::now(),
            StreamTrimScope::table(table_scope_id, table_name.clone()),
            1,
        ))
        .await
        .expect("upsert table trim marker");
    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .expect("run custom table trim");

    let table_page =
        StreamProvider::read_forward(&provider, StreamName::table_stream(&table_name), None, 10)
            .await
            .map_err(|err| StorageError::internal(&format!("read table stream failed: {err}")))?;
    assert!(table_page.items.is_empty());

    let item_stream = item_trim_scope(&provider.get_table_info(&table_name).await?, "a").scope_id;
    let item_page =
        StreamProvider::read_forward(&provider, StreamName::from(item_stream.as_str()), None, 10)
            .await
            .map_err(|err| StorageError::internal(&format!("read item stream failed: {err}")))?;
    assert_eq!(
        item_page.items.len(),
        1,
        "table trim must not delete item rows directly"
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_custom_item_trim_waits_for_retained_table_pointer() -> StorageResult<()> {
    let provider = initialized_provider().await;
    let table_name = create_stream_duration_table(&provider, "item_trim_duration").await?;
    provider
        .put_item_with_stream_ttl(
            table_name.clone(),
            item("a"),
            None,
            None,
            None,
            None,
            Some(StreamRetentionDuration::FiniteHours(1)),
        )
        .await
        .expect("put first item for item trim");
    provider
        .put_item(table_name.clone(), item("a"), None, None, None, None)
        .await
        .expect("put second item for item trim");
    force_stream_rows_created_at(&provider, TimestampMillis::now() - (2 * 60 * 60 * 1000)).await;

    let table = provider.get_table_info(&table_name).await?;
    let item_scope = item_trim_scope(&table, "a");
    provider
        .upsert_stream_trim_due_marker(StreamTrimDueMarker::new(
            TimestampMillis::now(),
            item_scope.clone(),
            provider
                .load_stream_trim_state_by_scope(&item_scope)
                .await?
                .expect("item trim state")
                .policy_version,
        ))
        .await
        .expect("upsert item trim marker");
    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .expect("run protected item trim");

    let item_page = StreamProvider::read_forward(
        &provider,
        StreamName::from(item_scope.scope_id.as_str()),
        None,
        10,
    )
    .await
    .map_err(|err| StorageError::internal(&format!("read item stream failed: {err}")))?;
    assert_eq!(
        item_page.items.len(),
        2,
        "item trim must wait while retained table pointers reference both rows"
    );

    let table_scope_id = load_sqlite_table_scope_id(&provider, &table_name).await?;
    provider
        .upsert_stream_trim_due_marker(StreamTrimDueMarker::new(
            TimestampMillis::now(),
            StreamTrimScope::table(table_scope_id, table_name.clone()),
            1,
        ))
        .await
        .expect("upsert table trim marker");
    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .expect("run table trim before item trim");
    assert_eq!(pointer_index_count(&provider).await, 0);
    provider
        .upsert_stream_trim_due_marker(StreamTrimDueMarker::new(
            TimestampMillis::now(),
            item_scope.clone(),
            provider
                .load_stream_trim_state_by_scope(&item_scope)
                .await?
                .expect("item trim state after table trim")
                .policy_version,
        ))
        .await
        .expect("upsert second item trim marker");
    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .expect("run item trim after table trim");

    let item_page = StreamProvider::read_forward(
        &provider,
        StreamName::from(item_scope.scope_id.as_str()),
        None,
        10,
    )
    .await
    .map_err(|err| StorageError::internal(&format!("read item stream failed: {err}")))?;
    assert_eq!(
        item_page.items.len(),
        1,
        "after table pointers are gone, item trim keeps only latest row"
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_table_duration_updates_cover_shrink_expansion_and_forever() -> StorageResult<()> {
    let provider = initialized_provider().await;
    let table_name = create_duration_table(&provider, "table_policy_changes").await?;
    let table_scope_id = load_sqlite_table_scope_id(&provider, &table_name).await?;
    let table_scope = StreamTrimScope::table(table_scope_id, table_name.clone());

    let mut shrink = update_table_request(&table_name);
    shrink.aux_stream_duration_hours = Some(StreamRetentionDuration::FiniteHours(1));
    provider.update_table(shrink).await?;
    let shrink_state = provider
        .load_stream_trim_state_by_scope(&table_scope)
        .await?
        .expect("shrink should write table trim state");
    assert_eq!(shrink_state.policy_version, 2);
    assert_eq!(
        shrink_state.retention,
        StreamRetentionDuration::FiniteHours(1)
    );
    assert!(shrink_state.next_due_at.is_some());

    let mut expansion = update_table_request(&table_name);
    expansion.aux_stream_duration_hours = Some(StreamRetentionDuration::FiniteHours(168));
    provider.update_table(expansion).await?;
    let expansion_state = provider
        .load_stream_trim_state_by_scope(&table_scope)
        .await?
        .expect("expansion should write table trim state");
    assert_eq!(expansion_state.policy_version, 3);
    assert_eq!(
        expansion_state.retention,
        StreamRetentionDuration::FiniteHours(168)
    );
    assert!(
        expansion_state.next_due_at > shrink_state.next_due_at,
        "expanding retention should push the next due time later"
    );

    let mut forever = update_table_request(&table_name);
    forever.aux_stream_duration_hours = Some(StreamRetentionDuration::Forever);
    provider.update_table(forever).await?;
    let forever_state = provider
        .load_stream_trim_state_by_scope(&table_scope)
        .await?
        .expect("forever should write table trim state");
    assert_eq!(forever_state.policy_version, 4);
    assert_eq!(forever_state.retention, StreamRetentionDuration::Forever);
    assert_eq!(forever_state.next_due_at, None);
    Ok(())
}

#[tokio::test]
async fn sqlite_custom_table_trim_respects_protected_replication_boundary() -> StorageResult<()> {
    let provider = initialized_provider().await;
    let table_name = create_stream_duration_table(&provider, "table_trim_protected").await?;
    provider
        .put_item(table_name.clone(), item("a"), None, None, None, None)
        .await?;
    provider
        .put_item(table_name.clone(), item("a"), None, None, None, None)
        .await?;
    force_stream_rows_created_at(&provider, TimestampMillis::now() - (2 * 60 * 60 * 1000)).await;

    let system_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .map_err(|err| StorageError::internal(&format!("read system stream failed: {err}")))?;
    assert_eq!(system_page.items.len(), 2);
    let protected_id = system_page.items[1].id;

    create_multi_region_control_table(&provider).await?;
    provider
        .put_item(
            TableName::new("sys_storage_replication"),
            HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::S("catchup#protected".to_string()),
                ),
                ("sk".to_string(), AttributeValue::S("session".to_string())),
                (
                    "payload".to_string(),
                    AttributeValue::S(
                        serde_json::json!({
                            "protected_stream_cursor": protected_id,
                            "updated_at": TimestampMillis::now(),
                        })
                        .to_string(),
                    ),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await?;

    let table_scope_id = load_sqlite_table_scope_id(&provider, &table_name).await?;
    provider
        .upsert_stream_trim_due_marker(StreamTrimDueMarker::new(
            TimestampMillis::now(),
            StreamTrimScope::table(table_scope_id, table_name.clone()),
            1,
        ))
        .await?;
    provider.run_job(storage_common::STREAM_TRIM_JOB).await?;

    assert_eq!(
        table_stream_item_count(&provider, &table_name).await?,
        1,
        "table trim should stop before the protected stream pointer"
    );
    Ok(())
}
