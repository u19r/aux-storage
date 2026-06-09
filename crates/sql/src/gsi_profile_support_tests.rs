use std::collections::HashMap;

use storage_types::{
    AttributeDefinition, AttributeValue, BatchWriteItemRequest, BillingMode,
    CreateGlobalSecondaryIndex, CreateTableRequest, IndexName, KeyAttributeType, KeySchemaElement,
    KeyType, Projection, ProjectionType, PutRequest, TableName, WriteRequest,
};

pub(crate) const REALISTIC_GSI_PROFILE_ITEMS: usize = 512;
pub(crate) const REALISTIC_GSI_PROFILE_BATCH: usize = 25;
pub(crate) const REALISTIC_GSI_PROFILE_INDEXES: usize = 3;

pub(crate) fn realistic_gsi_profile_request(table_name: &TableName) -> CreateTableRequest {
    let mut attribute_definitions = vec![
        AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "sk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
    ];
    let mut indexes = Vec::with_capacity(REALISTIC_GSI_PROFILE_INDEXES);
    for index in 0..REALISTIC_GSI_PROFILE_INDEXES {
        let gsi_pk = format!("gsi{index}_pk");
        let gsi_sk = format!("gsi{index}_sk");
        attribute_definitions.push(AttributeDefinition {
            attribute_name: gsi_pk.clone(),
            attribute_type: KeyAttributeType::S,
        });
        attribute_definitions.push(AttributeDefinition {
            attribute_name: gsi_sk.clone(),
            attribute_type: KeyAttributeType::S,
        });
        indexes.push(CreateGlobalSecondaryIndex {
            index_name: IndexName::new(&format!("gsi{index}")),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: gsi_pk,
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: gsi_sk,
                    key_type: KeyType::Range,
                },
            ],
            projection: Projection {
                projection_type: Some(ProjectionType::All),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        });
    }

    CreateTableRequest::new(
        table_name.clone(),
        attribute_definitions,
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
    )
    .with_global_secondary_indexes(Some(indexes))
}

pub(crate) fn realistic_gsi_profile_item(index: usize) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::with_capacity(16);
    item.insert(
        "pk".to_string(),
        AttributeValue::S(format!("tenant#{:093}", index % 64)),
    );
    item.insert(
        "sk".to_string(),
        AttributeValue::S(format!(
            "item#{index:08}#sort-key-component-realistic-length"
        )),
    );
    item.insert(
        "ttl".to_string(),
        AttributeValue::N("2200000000".to_string()),
    );
    item.insert(
        "payload".to_string(),
        AttributeValue::S(format!("payload-{index}-{}", "x".repeat(1_100))),
    );
    for gsi in 0..REALISTIC_GSI_PROFILE_INDEXES {
        item.insert(
            format!("gsi{gsi}_pk"),
            AttributeValue::S(format!("group#{gsi}#{:091}", index % 128)),
        );
        item.insert(
            format!("gsi{gsi}_sk"),
            AttributeValue::S(format!("rank#{index:096}")),
        );
    }
    for attr in 0..6 {
        item.insert(
            format!("attr{attr}"),
            AttributeValue::S(format!("value-{attr}-{index}")),
        );
    }
    item
}

pub(crate) fn realistic_gsi_profile_batches(
    table_name: &TableName,
) -> impl Iterator<Item = BatchWriteItemRequest> + '_ {
    (0..REALISTIC_GSI_PROFILE_ITEMS)
        .step_by(REALISTIC_GSI_PROFILE_BATCH)
        .map(move |chunk_start| {
            let writes = (chunk_start
                ..REALISTIC_GSI_PROFILE_ITEMS.min(chunk_start + REALISTIC_GSI_PROFILE_BATCH))
                .map(|index| WriteRequest {
                    put_request: Some(PutRequest {
                        item: realistic_gsi_profile_item(index),
                        aux_item_stream_ttl_hours: None,
                    }),
                    delete_request: None,
                })
                .collect::<Vec<_>>();

            BatchWriteItemRequest {
                request_items: HashMap::from([(table_name.clone(), writes)]),
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
            }
        })
}

pub(crate) fn print_gsi_profile_counters(provider: &'static str, phase: &str) {
    for counter in storage_common::provider_perf::snapshot_provider(provider) {
        let avg = counter.total.as_secs_f64() * 1_000.0 / counter.calls as f64;
        println!(
            "gsi_update_job_profile provider={provider} phase={phase} hotspot={} calls={} \
             total_ms={:.3} avg_ms={:.3} max_ms={:.3} total_amount={} max_amount={}",
            counter.name,
            counter.calls,
            counter.total.as_secs_f64() * 1_000.0,
            avg,
            counter.max.as_secs_f64() * 1_000.0,
            counter.total_amount,
            counter.max_amount
        );
    }
}
