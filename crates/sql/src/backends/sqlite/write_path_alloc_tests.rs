use std::collections::HashMap;

use alloc_counter::AllocationGuard;
use storage_provider::StorageProvider as _;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, KeyAttributeType, KeySchemaElement,
    KeyType, StreamName, StreamSpecification, StreamViewType, TableName, TimeToLiveSpecification,
    UpdateTimeToLiveRequest, WireItem,
};
use stream_provider::StreamProvider as _;

use crate::{SQLiteStorageProvider, naming};

const ITEM_COUNT: usize = 96;
const STREAM_READ_LIMIT: u32 = 256;
const TTL_ATTRIBUTE: &str = "ttl";
const TABLE_NAME_ENCODE_BASELINE: &str = "alloc_write_encode_ttl_stream_sqlite";

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime")
}

fn create_table_request(table_name: &TableName) -> CreateTableRequest {
    CreateTableRequest::new(
        table_name.clone(),
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
    )
    .with_stream_specification(Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    }))
}

fn sample_item(index: usize) -> HashMap<String, AttributeValue> {
    let ttl = 2_200_000_000_u64 + u64::try_from(index).unwrap_or(0);
    HashMap::from([
        ("pk".to_string(), AttributeValue::S("ORG#ALLOC".to_string())),
        (
            "sk".to_string(),
            AttributeValue::S(format!("ITEM#{index:04}")),
        ),
        (
            "entity_type".to_string(),
            AttributeValue::S("ALLOC_PROFILE".to_string()),
        ),
        ("revision".to_string(), AttributeValue::N(index.to_string())),
        (
            TTL_ATTRIBUTE.to_string(),
            AttributeValue::N(ttl.to_string()),
        ),
        (
            "payload".to_string(),
            AttributeValue::M(HashMap::from([
                (
                    "status".to_string(),
                    AttributeValue::S("active".to_string()),
                ),
                (
                    "flags".to_string(),
                    AttributeValue::L(vec![
                        AttributeValue::S("stream".to_string()),
                        AttributeValue::S("ttl".to_string()),
                    ]),
                ),
            ])),
        ),
    ])
}

fn sample_items() -> Vec<HashMap<String, AttributeValue>> {
    (0..ITEM_COUNT).map(sample_item).collect()
}

fn sample_wire_items() -> Vec<WireItem> {
    sample_items()
        .into_iter()
        .map(|item| WireItem::from_attribute_map(&item).expect("wire item"))
        .collect()
}

async fn setup_provider(table_name: &TableName) -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new(":memory:")
        .await
        .expect("create sqlite provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    provider
        .initialize_stream()
        .await
        .expect("initialize stream");
    provider
        .create_table(&create_table_request(table_name))
        .await
        .expect("create table");
    provider
        .update_time_to_live(UpdateTimeToLiveRequest {
            table_name: table_name.clone(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: TTL_ATTRIBUTE.to_string(),
                enabled: true,
            },
        })
        .await
        .expect("enable ttl");
    provider
}

async fn assert_stream_entries(provider: &SQLiteStorageProvider) {
    let page = provider
        .read_forward(StreamName::system_table_stream(), None, STREAM_READ_LIMIT)
        .await
        .expect("read stream entries");
    assert!(
        page.items.len() >= ITEM_COUNT,
        "expected at least {ITEM_COUNT} stream entries, got {}",
        page.items.len()
    );
}

async fn assert_ttl_row_count(
    provider: &SQLiteStorageProvider,
    table_name: &TableName,
    expected_rows: usize,
) {
    let ttl_table = naming::physical_ttl_index_table_name(table_name);
    let row_count = provider
        .connection
        .call_unwrap(move |conn| {
            let sql = format!("SELECT COUNT(*) FROM \"{ttl_table}\"");
            let mut stmt = conn.prepare(&sql)?;
            let count: i64 = stmt.query_row([], |row| row.get(0))?;
            Ok::<_, rusqlite::Error>(count)
        })
        .await
        .expect("count ttl rows");
    assert_eq!(
        usize::try_from(row_count).unwrap_or(0),
        expected_rows,
        "unexpected ttl row count"
    );
}

fn measure_put_item_encode_stream_ttl_baseline() -> alloc_counter::AllocationReport<'static> {
    let runtime = runtime();
    let table_name = TableName::new(TABLE_NAME_ENCODE_BASELINE);
    let provider = runtime.block_on(setup_provider(&table_name));
    let write_items = sample_wire_items();

    let guard = AllocationGuard::start(
        module_path!(),
        "sqlite_put_item_encode_stream_ttl_baseline",
        file!(),
        line!(),
        Some("baseline"),
    );
    runtime.block_on(async {
        for item in write_items {
            provider
                .put_item_encode(table_name.clone(), item, None, None, None, None)
                .await
                .expect("put item encode");
        }
    });
    let report = guard.finish();

    runtime.block_on(async {
        assert_stream_entries(&provider).await;
        assert_ttl_row_count(&provider, &table_name, ITEM_COUNT).await;
    });
    report
}

#[test]
fn sqlite_put_item_encode_stream_ttl_allocation_baseline_tests() {
    // Snapshot (2026-02-18, `cargo test -p sqlite write_path_alloc_tests --
    // --nocapture`): allocation_count=16707, allocated_bytes=3144704.
    let report = measure_put_item_encode_stream_ttl_baseline();
    alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}
