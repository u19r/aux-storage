use std::collections::BTreeMap;

use storage_types::PutItemRequest;

use crate::{
    common::*,
    imports::*,
    read_sequence_dag::{NormalizedResult, ReadSequenceDagWorkload, constants::MAPPED_INDEX_NAME},
};

impl ReadSequenceDagWorkload {
    pub(super) fn create_mapped_table(&self, table_name: TableName) -> CreateTableRequest {
        let mut request = CreateTableRequest::new(
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
        );
        request.global_secondary_indexes = Some(vec![CreateGlobalSecondaryIndex {
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
        }]);
        request
    }

    pub(super) async fn seed_mapped_table(
        &self,
        provider: &Arc<dyn storage_provider::DynamoOperations>,
        table_name: &TableName,
    ) -> Result<(), String> {
        provider
            .create_table(self.create_mapped_table(table_name.clone()))
            .await
            .map_err(|error| storage_error_detail(&error))?;
        self.seed_rows(provider, table_name, ["a", "b", "c"], "open")
            .await?;
        self.seed_overflow_rows(provider, table_name).await
    }

    async fn seed_rows<'a, I>(
        &self,
        provider: &Arc<dyn storage_provider::DynamoOperations>,
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
                .put_item(PutItemRequest::new(
                    table_name.clone(),
                    WireItem::try_from(&item).map_err(|error| storage_error_detail(&error))?,
                ))
                .await
                .map_err(|error| storage_error_detail(&error))?;
        }
        Ok(())
    }

    async fn seed_overflow_rows(
        &self,
        provider: &Arc<dyn storage_provider::DynamoOperations>,
        table_name: &TableName,
    ) -> Result<(), String> {
        // Force a public continuation beyond a 100-row query page.
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
                .put_item(PutItemRequest::new(
                    table_name.clone(),
                    WireItem::try_from(&item).map_err(|error| storage_error_detail(&error))?,
                ))
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
}

fn query_node(table_name: &TableName, index_name: &IndexName, status: &str) -> ReadSequenceNode {
    let mut query = QueryRequest::builder()
        .table_name(table_name.clone())
        .key_condition_expression("gsi_pk = :gsi_pk".to_string())
        .index_name(Some(index_name.clone()))
        .build();
    query.limit = Some(100);
    query.expression_attribute_values = Some(
        HashMap::from([(":gsi_pk".to_string(), AttributeValue::S(status.to_string()))]).into(),
    );
    ReadSequenceNode::new("query", ReadSequenceNodeOperation::Query(Box::new(query)))
}

fn child_node(table_name: &TableName) -> ReadSequenceNode {
    ReadSequenceNode::builder()
        .name("child")
        .operation(ReadSequenceNodeOperation::Get(Box::new(
            GetItemRequest::new(
                table_name.clone(),
                HashMap::from([("pk".to_string(), read_sequence_input_marker("pk"))]),
            ),
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
        ReadSequenceNodeOperation::Get(Box::new(GetItemRequest::new(
            table_name.clone(),
            HashMap::from([("pk".to_string(), AttributeValue::S("a".to_string()))]),
        ))),
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
