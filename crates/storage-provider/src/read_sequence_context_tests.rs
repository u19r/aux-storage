use std::collections::HashMap;

use async_trait::async_trait;
use storage_types::{
    AttributeValue, BatchGetItemRequest, BatchGetWireItemResponse, KeyAttributes,
    QueryTableRequest, StorageEnum, StorageResult, TableName, WireItem,
};

use crate::{
    ReadSequenceBatchGetResponse, ReadSequenceReadContext, ReadSequenceReadLimits,
    StorageProviderReadContext,
};

struct FixtureReadContext {
    item: Option<WireItem>,
    batch: BatchGetWireItemResponse,
    query: Vec<WireItem>,
}

#[async_trait]
impl StorageProviderReadContext for FixtureReadContext {
    async fn get_item(
        &self,
        _table_name: TableName,
        _key: KeyAttributes,
        _consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        Ok(self.item.clone())
    }

    async fn batch_get_item(
        &self,
        _request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        Ok(self.batch.clone())
    }

    async fn query_table(
        &self,
        _request: &QueryTableRequest,
    ) -> StorageResult<crate::RangeReadPage> {
        let mut progress = crate::RangeReadProgress::default();
        for item in &self.query {
            let attributes = item.to_attribute_map()?;
            progress.evaluated_count += 1;
            progress.evaluated_bytes += u64::from(item.logical_size()?);
            progress.last_evaluated_key =
                Some(storage_types::ItemKey::Table(storage_types::TableKey::new(
                    _request.table_name.clone(),
                    attributes.get("id").unwrap().clone(),
                    None,
                )));
        }
        crate::RangeReadPage {
            items: self.query.clone(),
            progress,
            continuation: None,
        }
        .finish()
    }
}

fn item(id: &str) -> WireItem {
    WireItem::try_from(&HashMap::from([(
        "id".to_string(),
        AttributeValue::S(id.to_string()),
    )]))
    .unwrap()
}

fn key() -> KeyAttributes {
    HashMap::from([("id".to_string(), AttributeValue::S("one".to_string()))]).into()
}

fn context(
    item: Option<WireItem>,
    batch: BatchGetWireItemResponse,
    query: Vec<WireItem>,
    max_total_items: u32,
) -> ReadSequenceReadContext {
    ReadSequenceReadContext::new(
        Box::new(FixtureReadContext { item, batch, query }),
        ReadSequenceReadLimits::new(max_total_items),
    )
}

#[tokio::test]
async fn given_wire_item_when_get_item_as_then_decodes_without_manual_mapping() {
    let context = context(
        Some(item("one")),
        BatchGetWireItemResponse::default(),
        vec![],
        1,
    );

    let decoded = context
        .get_item_as::<HashMap<String, AttributeValue>>(TableName::new("items"), key(), false)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        decoded.get("id"),
        Some(&AttributeValue::S("one".to_string()))
    );
    assert_eq!(context.items_read(), 1);
}

#[tokio::test]
async fn given_missing_item_when_get_item_as_then_returns_none_without_spending_budget() {
    let context = context(None, BatchGetWireItemResponse::default(), vec![], 1);

    let decoded = context
        .get_item_as::<HashMap<String, AttributeValue>>(TableName::new("items"), key(), false)
        .await
        .unwrap();

    assert!(decoded.is_none());
    assert_eq!(context.items_read(), 0);
}

#[tokio::test]
async fn given_item_budget_when_reads_exceed_it_then_context_rejects_the_suffix() {
    let context = context(
        Some(item("one")),
        BatchGetWireItemResponse::default(),
        vec![],
        1,
    );

    context
        .get_item_as::<HashMap<String, AttributeValue>>(TableName::new("items"), key(), false)
        .await
        .unwrap();
    let error = context
        .get_item_as::<HashMap<String, AttributeValue>>(TableName::new("items"), key(), false)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("total read item limit exceeded"));
    assert_eq!(context.items_read(), 1);
}

#[tokio::test]
async fn given_malformed_wire_item_when_get_item_as_then_returns_decode_error() {
    let context = context(
        Some(WireItem::dynamo_json(b"not-json".to_vec())),
        BatchGetWireItemResponse::default(),
        vec![],
        1,
    );

    let error = context
        .get_item_as::<HashMap<String, AttributeValue>>(TableName::new("items"), key(), false)
        .await
        .unwrap_err();

    assert!(matches!(
        error.as_ref(),
        StorageEnum::InternalServerError { .. }
    ));
    assert_eq!(context.items_read(), 1);
}

#[tokio::test]
async fn given_wire_batch_when_batch_get_item_as_then_preserves_table_shape_and_decodes() {
    let table = TableName::new("items");
    let batch = BatchGetWireItemResponse {
        responses: Some(storage_types::VecMap::from_iter([(
            table.clone(),
            vec![item("one"), item("two")],
        )])),
        ..BatchGetWireItemResponse::default()
    };
    let context = context(None, batch, vec![], 2);

    let decoded: ReadSequenceBatchGetResponse<HashMap<String, AttributeValue>> = context
        .batch_get_item_as(BatchGetItemRequest {
            request_items: storage_types::VecMap::default(),
            return_consumed_capacity: None,
        })
        .await
        .unwrap();

    assert_eq!(
        decoded
            .responses
            .as_ref()
            .map(|tables| tables.get(&table).unwrap().len()),
        Some(2)
    );
    assert_eq!(context.items_read(), 2);
}

#[tokio::test]
async fn given_wire_query_when_query_table_as_then_decodes_items_and_accounts_budget() {
    let context = context(
        None,
        BatchGetWireItemResponse::default(),
        vec![item("one"), item("two")],
        2,
    );

    let page = context
        .query_table_as::<HashMap<String, AttributeValue>>(&QueryTableRequest {
            table_name: TableName::new("items"),
            index_name: None,
            key_condition_expression: "id = :id".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: None,
            projection_expression: None,
            limit: None,
            exclusive_start_key: None,
            scan_index_forward: None,
            consistent_read: false,
        })
        .await
        .unwrap();

    assert_eq!(page.items.len(), 2);
    assert_eq!(
        page.items[0].get("id"),
        Some(&AttributeValue::S("one".to_string()))
    );
    assert!(page.continuation.is_none());
    assert_eq!(context.items_read(), 2);
}

#[tokio::test]
async fn given_read_sequence_context_when_reads_are_issued_then_database_calls_are_captured() {
    let snapshot = metrics_facade::begin_request_cost_collection(None, || async {
        let context = context(
            Some(item("one")),
            BatchGetWireItemResponse::default(),
            vec![],
            2,
        );

        context
            .get_item_as::<HashMap<String, AttributeValue>>(TableName::new("items"), key(), false)
            .await
            .unwrap();
        context
            .batch_get_item_as::<HashMap<String, AttributeValue>>(BatchGetItemRequest {
                request_items: storage_types::VecMap::default(),
                return_consumed_capacity: None,
            })
            .await
            .unwrap();
        context
            .query_table_as::<HashMap<String, AttributeValue>>(&QueryTableRequest {
                table_name: TableName::new("items"),
                index_name: None,
                key_condition_expression: "id = :id".to_string(),
                expression_attribute_names: None,
                expression_attribute_values: None,
                projection_expression: None,
                limit: None,
                exclusive_start_key: None,
                scan_index_forward: None,
                consistent_read: false,
            })
            .await
            .unwrap();

        metrics_facade::finish_request_cost_collection(None, 1.0, None).await
    })
    .await;

    assert_eq!(snapshot.db_calls, 3);
    assert_eq!(snapshot.db_serial_waves, 3);
    assert_eq!(snapshot.db_max_parallelism, 1);
    assert_eq!(
        snapshot
            .db_call_breakdown
            .iter()
            .map(|entry| (entry.operation.as_str(), entry.calls))
            .collect::<Vec<_>>(),
        vec![
            ("read_sequence.batch_get_item", 1),
            ("read_sequence.get_item", 1),
            ("read_sequence.query_table", 1),
        ]
    );
}
