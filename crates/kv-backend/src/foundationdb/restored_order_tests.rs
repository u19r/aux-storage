//! Native exported-key import proof; this does not exercise
//! fdbbackup/fdbrestore.

use foundationdb::Database;
use serde::{Deserialize, Serialize};
use serde_json::json;
use storage_provider::DynamoOperations as _;
use storage_types::{InitialStreamLineages, UserStreamName};
use stream_provider::{
    StreamOperations as _, StreamPartitioningMode, SystemStreamLineage, SystemStreamPageRequest,
    SystemStreamSplitChild, SystemStreamSplitOperationId, SystemStreamSplitRequest,
};

use super::{FoundationDbBackend, FoundationDbOptions};
use crate::KvStore;

const SOURCE_VERSION: u64 = 4_000_000_000_000_000;
const PREFIX: &[u8] = b"restored-order-proof";

#[derive(Serialize, Deserialize)]
struct ExportedRows {
    covered_version: u64,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    scope: storage_types::SystemStreamScope,
    records: serde_json::Value,
    user_items: serde_json::Value,
}

fn cluster_file() -> String {
    std::env::var("TEST_FDB_CLUSTER_FILE").expect("run just test-fdb-restored-order")
}

fn export_path() -> String {
    std::env::var("TEST_FDB_RESTORE_EXPORT").expect("run just test-fdb-restored-order")
}

async fn open_store() -> KvStore<FoundationDbBackend> {
    let cluster = cluster_file();
    let backend = FoundationDbBackend::initialize(FoundationDbOptions {
        cluster_file: Some(&cluster),
        subspace_prefix: Some(PREFIX),
        ..Default::default()
    })
    .await
    .unwrap();
    KvStore::initialize_format(backend, InitialStreamLineages::ONE)
        .await
        .unwrap()
}

fn advance_version(version: u64) {
    let output = std::process::Command::new("/usr/local/bin/fdbcli")
        .args([
            "-C",
            &cluster_file(),
            "--timeout",
            "30",
            "--exec",
            &format!("advanceversion {version}"),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "native version advance failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn read_raw(database: &Database) -> (u64, Vec<(Vec<u8>, Vec<u8>)>) {
    // advanceversion forces recovery; retry a stale native read version through
    // FDB's owner.
    database
        .run(|transaction, _| async move {
            transaction.set_option(foundationdb::options::TransactionOption::Timeout(30_000))?;
            let version = transaction.get_read_version().await? as u64;
            let mut range = foundationdb::RangeOption::from((&b""[..], &b"\xff"[..]));
            let mut rows = Vec::new();
            for iteration in 1..32 {
                let page = transaction.get_range(&range, iteration, false).await?;
                rows.extend(
                    page.iter()
                        .map(|row| (row.key().to_vec(), row.value().to_vec())),
                );
                if !page.more() {
                    return Ok::<_, foundationdb::FdbBindingError>((version, rows));
                }
                range.begin =
                    foundationdb::KeySelector::first_greater_than(rows.last().unwrap().0.clone());
            }
            panic!("fixture native export paging must finish");
        })
        .await
        .unwrap()
}

async fn write_batch(store: &KvStore<FoundationDbBackend>, label: &str) {
    store.transact_write_items(serde_json::from_value(json!({"TransactItems": [
        {"Put": {"TableName": "restore_order", "Item": {"pk": {"S": format!("{label}-first")}}}},
        {"Put": {"TableName": "restore_order", "Item": {"pk": {"S": format!("{label}-second")}}}}
    ]})).unwrap()).await.unwrap();
}

async fn read_records(store: &KvStore<FoundationDbBackend>) -> Vec<storage_types::StreamRecord> {
    let scope = store.system_stream_scope().await.unwrap();
    let mut records = Vec::new();
    let mut token = None;
    for _ in 0..32 {
        let page = store
            .get_system_stream_page(
                SystemStreamPageRequest::try_new(scope.clone(), token, 1).unwrap(),
            )
            .await
            .unwrap();
        assert!(page.records().len() <= 1);
        records.extend_from_slice(page.records());
        if page.terminal() {
            return records;
        }
        token = Some(page.issued_checkpoint().token().clone());
    }
    panic!("fixture paging must reach captured high");
}

async fn read_user_items(store: &KvStore<FoundationDbBackend>) -> serde_json::Value {
    let stream = store
        .get_stream(UserStreamName::new("restore_user"))
        .await
        .unwrap()
        .unwrap();
    let mut items = Vec::new();
    let mut token = None;
    for _ in 0..16 {
        let page = store
            .read_forward(stream.internal_id.clone().into(), token, 1)
            .await
            .unwrap();
        items.extend(page.items);
        if !page.has_more {
            return serde_json::to_value(items).unwrap();
        }
        token = page.last_evaluated_key;
    }
    panic!("user history paging must finish");
}

#[tokio::test]
#[ignore = "requires disposable source fixture: just test-fdb-restored-order"]
async fn given_high_native_source_when_exported_then_real_history_and_covering_version_are_saved() {
    let store = open_store().await;
    advance_version(SOURCE_VERSION);
    store.create_table(serde_json::from_value(json!({
        "TableName": "restore_order", "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    })).unwrap()).await.unwrap();
    write_batch(&store, "old").await;
    store
        .create_stream(
            UserStreamName::new("restore_user"),
            None,
            StreamPartitioningMode::Single,
        )
        .await
        .unwrap();
    for payload in [b"old-first", b"old-last!"] {
        store
            .append_item(UserStreamName::new("restore_user"), payload, None)
            .await
            .unwrap();
    }
    let scope = store.system_stream_scope().await.unwrap();
    let parent = store
        .system_stream_lineage_catalog(scope.clone())
        .await
        .unwrap()
        .remove(0);
    let children = ["left", "right"].map(|name| {
        SystemStreamSplitChild::try_new(
            SystemStreamLineage::try_new(
                name,
                format!("{name}-lineage"),
                Some(parent.lineage_id().as_str().to_owned()),
                false,
            )
            .unwrap(),
            1,
        )
        .unwrap()
    });
    store
        .split_system_stream(
            SystemStreamSplitRequest::try_new(
                scope.clone(),
                parent,
                children,
                SystemStreamSplitOperationId::try_new("restore-source-split").unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let records = read_records(&store).await;
    assert_eq!(records.len(), 2);
    let old_stamps: Vec<_> = records
        .iter()
        .map(|record| {
            record
                .system_position
                .as_ref()
                .unwrap()
                .offset()
                .versionstamp()
                .unwrap()
        })
        .collect();
    assert_eq!(&old_stamps[0][..10], &old_stamps[1][..10]);
    assert_ne!(&old_stamps[0][10..], &old_stamps[1][10..]);
    let user_items = read_user_items(&store).await;
    let database = Database::from_path(&cluster_file()).unwrap();
    let (covered_version, rows) = read_raw(&database).await;
    assert!(covered_version > SOURCE_VERSION);
    for record in &records {
        let stamp = record
            .system_position
            .as_ref()
            .unwrap()
            .offset()
            .versionstamp()
            .unwrap();
        assert!(u64::from_be_bytes(stamp[..8].try_into().unwrap()) < covered_version);
    }
    let exported = ExportedRows {
        covered_version,
        rows,
        scope,
        records: serde_json::to_value(records).unwrap(),
        user_items,
    };
    std::fs::write(export_path(), serde_json::to_vec(&exported).unwrap()).unwrap();
    println!(
        "native source export: covered_version={covered_version}, rows={}",
        exported.rows.len()
    );
}

#[tokio::test]
#[ignore = "requires fresh disposable destination: just test-fdb-restored-order"]
async fn given_lower_native_destination_when_imported_and_advanced_then_history_is_unchanged_and_writes_follow_it()
 {
    let exported: ExportedRows =
        serde_json::from_slice(&std::fs::read(export_path()).unwrap()).unwrap();
    // Initialize only the process network; no application state may be
    // published before import.
    super::network::ensure_network(super::network::NetworkPolicy {
        cache_read_version_ms: 0,
        reply_byte_limit: storage_types::FOUNDATIONDB_DEFAULT_REPLY_BYTE_LIMIT,
    })
    .unwrap();
    let database = Database::from_path(&cluster_file()).unwrap();
    let (lower_version, empty) = read_raw(&database).await;
    assert!(empty.is_empty());
    assert!(lower_version < exported.covered_version);
    let transaction = database.create_trx().unwrap();
    transaction
        .set_option(foundationdb::options::TransactionOption::AccessSystemKeys)
        .unwrap();
    transaction.update_metadata_version();
    for (key, value) in &exported.rows {
        assert_ne!(
            key.as_slice(),
            crate::constants::FOUNDATIONDB_METADATA_VERSION_KEY
        );
        transaction.set(key, value);
    }
    transaction.commit().await.unwrap();
    let (import_version, imported) = read_raw(&database).await;
    assert_eq!(imported, exported.rows);
    assert!(
        import_version < exported.covered_version,
        "raw copy alone leaves new native positions below saved history"
    );
    advance_version(exported.covered_version + 1);
    let (advanced_version, _) = read_raw(&database).await;
    assert!(advanced_version > exported.covered_version);
    let store = open_store().await;
    assert_eq!(store.system_stream_scope().await.unwrap(), exported.scope);
    assert_eq!(
        serde_json::to_value(read_records(&store).await).unwrap(),
        exported.records
    );
    assert_eq!(read_user_items(&store).await, exported.user_items);
    write_batch(&store, "later-one").await;
    write_batch(&store, "later-two").await;
    let old_user_last = store
        .get_stream(UserStreamName::new("restore_user"))
        .await
        .unwrap()
        .unwrap();
    let old_user = store
        .read_forward(old_user_last.internal_id.into(), None, 10)
        .await
        .unwrap();
    let later_user_id = store
        .append_item(UserStreamName::new("restore_user"), b"later", None)
        .await
        .unwrap();
    assert!(old_user.items.iter().all(|item| item.id < later_user_id));
    let records = read_records(&store).await;
    assert_eq!(records.len(), 6);
    assert_eq!(
        serde_json::to_value(&records[..2]).unwrap(),
        exported.records
    );
    let mut new_stamps = Vec::new();
    for record in &records[2..] {
        let stamp = record
            .system_position
            .as_ref()
            .unwrap()
            .offset()
            .versionstamp()
            .unwrap();
        assert!(u64::from_be_bytes(stamp[..8].try_into().unwrap()) > exported.covered_version);
        new_stamps.push(stamp);
    }
    new_stamps.sort();
    new_stamps.dedup();
    assert_eq!(new_stamps.len(), 4);
    assert_eq!(&new_stamps[0][..10], &new_stamps[1][..10]);
    assert_eq!(&new_stamps[2][..10], &new_stamps[3][..10]);
    assert!(new_stamps[1][..10] < new_stamps[2][..10]);
    let (_, after) = read_raw(&database).await;
    // Retained history and its physical addresses are immutable; metadata and
    // current items can change.
    let history_prefixes: &[&[u8]] = &[
        b"\x7fs/i/",
        b"\x7fs/r/",
        b"\x7fs/l/",
        b"\x7fs/s",
        b"\x7fs/sr/",
    ];
    let described = store
        .describe_table(serde_json::from_value(json!({"TableName": "restore_order"})).unwrap())
        .await
        .unwrap();
    let table_id = crate::catalog::table_id_from_history(
        storage_types::TableHistoryTarget::try_from(&described.table)
            .unwrap()
            .history_id,
    )
    .unwrap();
    let mut checked = 0;
    for row in &exported.rows {
        let stream_part_of_table = (0x80..=0x85u8).any(|tag| {
            let root = [&b"T"[..], &table_id.to_be_bytes(), &[tag]].concat();
            row.0
                .windows(root.len())
                .any(|bytes| bytes == root.as_slice())
        });
        if stream_part_of_table
            || history_prefixes
                .iter()
                .any(|prefix| row.0.windows(prefix.len()).any(|bytes| bytes == *prefix))
        {
            assert!(
                after.contains(row),
                "saved history address or payload changed"
            );
            checked += 1;
        }
    }
    assert!(checked > 6);
    println!(
        "native import proof: before={lower_version}, imported={import_version}, covered={}, \
         advanced={advanced_version}, unchanged_history_rows={checked}, canonical_records=6, \
         new_unique_positions=4",
        exported.covered_version
    );
}
