use std::collections::BTreeMap;

use crate::{
    common::*,
    imports::*,
    read_sequence_dag::{
        FdbChaosProvider, NormalizedResult, ReadSequenceDagWorkload, constants::MAPPED_INDEX_NAME,
    },
};

impl ReadSequenceDagWorkload {
    pub(super) fn create_mapped_table(&self, table_name: TableName) -> CreateTableRequest {
        CreateTableRequest::new(
            table_name,
            vec![
                AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "gsi_pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
            ],
            vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            }],
            BillingMode::PayPerRequest,
        )
        .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
            index_name: IndexName::new(MAPPED_INDEX_NAME),
            key_schema: vec![KeySchemaElement {
                attribute_name: "gsi_pk".to_string(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: Some(ProjectionType::All),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]))
    }

    pub(super) async fn seed_mapped_table(
        &self,
        provider: &Arc<FdbChaosProvider>,
        table_name: &TableName,
    ) -> Result<(), String> {
        provider
            .create_table(&self.create_mapped_table(table_name.clone()))
            .await
            .map_err(|error| storage_error_detail(&error))?;
        self.seed_rows(provider, table_name, ["a", "b", "c"], "open")
            .await?;
        self.seed_overflow_rows(provider, table_name).await
    }

    async fn seed_rows<'a, I>(
        &self,
        provider: &Arc<FdbChaosProvider>,
        table_name: &TableName,
        keys: I,
        status: &str,
    ) -> Result<(), String>
    where
        I: IntoIterator<Item = &'a str>,
    {
        for pk in keys {
            let item = HashMap::from([
                ("pk".to_string(), AttributeValue::S(pk.to_string())),
                ("gsi_pk".to_string(), AttributeValue::S(status.to_string())),
                (
                    "payload".to_string(),
                    AttributeValue::S(format!("payload-{pk}")),
                ),
            ]);
            provider
                .put_item(table_name.clone(), item, None, None, None, None)
                .await
                .map_err(|error| storage_error_detail(&error))?;
        }
        Ok(())
    }

    async fn seed_overflow_rows(
        &self,
        provider: &Arc<FdbChaosProvider>,
        table_name: &TableName,
    ) -> Result<(), String> {
        // The mapped provider caps the physical page at 100 parent rows.
        for index in 0..101 {
            let pk = format!("overflow-{index:03}");
            let item = HashMap::from([
                ("pk".to_string(), AttributeValue::S(pk.clone())),
                (
                    "gsi_pk".to_string(),
                    AttributeValue::S("closed".to_string()),
                ),
                (
                    "payload".to_string(),
                    AttributeValue::S(format!("payload-{pk}")),
                ),
            ]);
            provider
                .put_item(table_name.clone(), item, None, None, None, None)
                .await
                .map_err(|error| storage_error_detail(&error))?;
        }
        Ok(())
    }

    pub(super) fn read_sequence_request(
        &self,
        table_name: &TableName,
        index_name: &IndexName,
        status: &str,
        permuted: bool,
    ) -> ReadSequenceRequest {
        let query_node = query_node(table_name, index_name, status);
        let child_node = child_node(table_name);
        let independent_node = independent_node(table_name);
        let nodes = if permuted {
            vec![child_node, query_node, independent_node]
        } else {
            vec![query_node, child_node, independent_node]
        };
        ReadSequenceRequest::new(nodes)
    }

    pub(super) fn expected_result(status: &str) -> NormalizedResult {
        let query_pks = query_keys(status);
        let item = |pk: &str| {
            let gsi_pk = if pk.starts_with("overflow-") {
                "closed"
            } else {
                "open"
            };
            HashMap::from([
                ("pk".to_string(), AttributeValue::S(pk.to_string())),
                ("gsi_pk".to_string(), AttributeValue::S(gsi_pk.to_string())),
                (
                    "payload".to_string(),
                    AttributeValue::S(format!("payload-{pk}")),
                ),
            ])
        };
        BTreeMap::from([
            (
                "query".to_string(),
                vec![query_pks.iter().map(|pk| item(pk)).collect()],
            ),
            (
                "child".to_string(),
                query_pks.iter().map(|pk| vec![item(pk)]).collect(),
            ),
            ("independent".to_string(), vec![vec![item("a")]]),
        ])
    }

    pub(super) fn normalize_mapped_response(
        request: &ReadSequenceRequest,
        rows: &[storage_provider::ReadSequenceFlatRow],
    ) -> NormalizedResult {
        let mut grouped = BTreeMap::new();
        for row in rows {
            let name = request.nodes[row.node.index()].name.clone();
            let items = normalize_row_items(&row.result);
            grouped
                .entry(name)
                .or_insert_with(Vec::new)
                .push((row.invocation_ordinal, items));
        }
        grouped
            .into_iter()
            .map(|(name, mut invocations)| {
                invocations.sort_by_key(|(ordinal, _)| *ordinal);
                (
                    name,
                    invocations.into_iter().map(|(_, items)| items).collect(),
                )
            })
            .collect()
    }
}

fn query_node(table_name: &TableName, index_name: &IndexName, status: &str) -> ReadSequenceNode {
    let mut query = QueryRequest::new(table_name.clone(), "gsi_pk = :gsi_pk".to_string())
        .with_index_name(Some(index_name.clone()));
    query.expression_attribute_values = Some(HashMap::from([(
        ":gsi_pk".to_string(),
        AttributeValue::S(status.to_string()),
    )]));
    ReadSequenceNode::new("query", ReadSequenceNodeOperation::Query(query))
}

fn child_node(table_name: &TableName) -> ReadSequenceNode {
    ReadSequenceNode::builder()
        .name("child")
        .operation(ReadSequenceNodeOperation::Get(GetItemRequest::new(
            table_name.clone(),
            HashMap::from([("pk".to_string(), read_sequence_input_marker("pk"))]),
        )))
        .inputs(
            [(
                "pk".to_string(),
                ReadSequenceNodeInput {
                    from: ReadSequenceFromInput {
                        node: "query".to_string(),
                        select: ReadSequenceSelector("$.Query.Items[*].pk".to_string()),
                    },
                    mapped_key_source: None,
                    cardinality: ReadSequenceInputCardinality::Many,
                    on_missing: ReadSequenceOnMissing::Skip,
                },
            )]
            .into_iter()
            .collect(),
        )
        .iterate("pk")
        .build()
}

fn independent_node(table_name: &TableName) -> ReadSequenceNode {
    ReadSequenceNode::new(
        "independent",
        ReadSequenceNodeOperation::Get(GetItemRequest::new(
            table_name.clone(),
            HashMap::from([("pk".to_string(), AttributeValue::S("a".to_string()))]),
        )),
    )
}

fn query_keys(status: &str) -> Vec<String> {
    if status == "closed" {
        (0..101)
            .map(|index| format!("overflow-{index:03}"))
            .collect()
    } else {
        ["a", "b", "c"].into_iter().map(str::to_string).collect()
    }
}

fn normalize_row_items(
    result: &storage_provider::ReadSequenceFlatResult,
) -> Vec<HashMap<String, AttributeValue>> {
    match result {
        ReadSequenceFlatResult::Get { item } => item
            .clone()
            .into_iter()
            .map(|item| item.to_hashmap())
            .collect(),
        ReadSequenceFlatResult::Query { items, .. } => {
            items.iter().map(|item| item.to_hashmap()).collect()
        }
        ReadSequenceFlatResult::BatchGet { responses } => responses
            .values()
            .flat_map(|items| items.iter().map(|item| item.to_hashmap()))
            .collect(),
    }
}
