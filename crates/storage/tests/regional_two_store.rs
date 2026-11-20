#![cfg(all(feature = "test-support", feature = "sqlite"))]
//! Two regions, two managers, loopback TCP (REG-12 section 5.2). SQLite runs by
//! default.
//!
//! Assertions are on contents and states, never on elapsed time: convergence is
//! polled.

use std::{collections::HashMap, path::Path, time::Duration};

use regional_replication::message::{
    LineageRef, PairTable, RegionalRequest, RegionalResponse, ReplicationPage,
};
use serde_json::json;
use storage::DatabaseManager;
use storage_types::{
    AttributeValue, CreateTableRequest, KeyAttributes, PairState, ReplicaStatus,
    SystemStreamOffset, TableName, UpdateTableRequest,
};

const POLLS: usize = 150;

fn port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Region {
    name: &'static str,
    rank: u16,
    port: u16,
    database: std::path::PathBuf,
    regional_file: std::path::PathBuf,
}

impl Region {
    fn new(directory: &Path, name: &'static str, rank: u16, own: u16, peer: u16) -> Self {
        let (other_name, other_rank) = if name == "a" { ("b", 2) } else { ("a", 1) };
        let regional_file = directory.join(format!("{name}-regional.json"));
        let regions = json!([
            {"name": name, "rank": rank, "address": format!("127.0.0.1:{own}")},
            {"name": other_name, "rank": other_rank, "address": format!("127.0.0.1:{peer}")},
        ]);
        std::fs::write(
            &regional_file,
            json!({
                "local_region": name, "listen": format!("127.0.0.1:{own}"), "regions": regions,
                "send_interval_ms": 20, "page_entry_limit": 3, "request_timeout_ms": 2000
            })
            .to_string(),
        )
        .unwrap();
        Self {
            name,
            rank,
            port: own,
            database: directory.join(format!("{name}.db")),
            regional_file,
        }
    }

    async fn open(&self) -> DatabaseManager {
        let features = config::Features {
            backends: config::Backends {
                initial_stream_lineages: None,
                sqlite: Some(config::SqliteBackendConfig {
                    db_path: self.database.display().to_string(),
                }),
                turso: None,
                postgres: None,
                rocksdb: None,
                foundationdb: None,
                remote: None,
            },
            regional_connections: HashMap::from([(
                "default".to_owned(),
                self.regional_file.clone(),
            )]),
            ..config::Features::default()
        };
        let root = config::RootConfig {
            features,
            ..config::RootConfig::default()
        };
        let runtime = storage::startup::RuntimeConfig::from_file(&root).unwrap_or_else(|error| {
            panic!(
                "region {} ({}, port {}): {error:?}",
                self.name, self.rank, self.port
            )
        });
        runtime.open().await.unwrap_or_else(|error| {
            panic!(
                "region {} ({}, port {}): {error:?}",
                self.name, self.rank, self.port
            )
        })
    }
}

fn table() -> TableName {
    TableName::new("regional_orders")
}

fn key(name: &str) -> KeyAttributes {
    KeyAttributes::from_iter([("pk".into(), AttributeValue::S(name.into()))])
}

async fn put(manager: &DatabaseManager, name: &str, value: &str) {
    let item = HashMap::from([
        ("pk".to_owned(), AttributeValue::S(name.to_owned())),
        ("value".to_owned(), AttributeValue::S(value.to_owned())),
    ]);
    manager
        .put_item(
            storage::PutItemInput::builder()
                .table_name(table())
                .item(item)
                .return_old_on_condition_failure(false)
                .build(),
        )
        .await
        .unwrap_or_else(|error| panic!("put {name}: {error:?}"));
}

async fn delete(manager: &DatabaseManager, name: &str) {
    manager
        .delete_item(
            storage::DeleteItemInput::builder()
                .table_name(table())
                .key(key(name))
                .return_old_on_condition_failure(false)
                .build(),
        )
        .await
        .unwrap_or_else(|error| panic!("delete {name}: {error:?}"));
}

async fn read(manager: &DatabaseManager, name: &str) -> Result<Option<String>, ()> {
    match manager.get_item(table(), key(name)).await {
        Ok(item) => Ok(item.map(|item| item.to_dynamo_json_string().unwrap())),
        Err(_) => Err(()),
    }
}

async fn create_source(manager: &DatabaseManager) {
    let request: CreateTableRequest = serde_json::from_value(json!({
        "TableName": table().as_ref(),
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    }))
    .unwrap();
    manager.create_table(&request).await.unwrap();
}

async fn replica_update(
    manager: &DatabaseManager,
    action: &str,
    region: &str,
) -> storage_types::StorageResult<()> {
    let request: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": table().as_ref(),
        "ReplicaUpdates": [{action: {"RegionName": region}}]
    }))
    .unwrap();
    manager.update_table(request).await.map(|_| ())
}

/// One region with its manager.
type Side<'a> = (&'a Region, &'a DatabaseManager);

/// Rows of the regional marker relation of the region's database; the error
/// says which step failed (no item data).
fn marker_count(region: &Region) -> Result<i64, String> {
    let connection = rusqlite::Connection::open_with_flags(
        &region.database,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .map_err(|error| format!("open {error}"))?;
    connection
        .busy_timeout(Duration::from_secs(2))
        .map_err(|error| format!("busy {error}"))?;
    connection
        .query_row("SELECT COUNT(*) FROM regional_markers", [], |row| {
            row.get(0)
        })
        .map_err(|error| format!("query {error}"))
}

/// Pair state, replica status and marker count of one region, for a timeout
/// message (no item data).
async fn describe(side: Side<'_>) -> String {
    let state = side
        .1
        .regional_binding_for_test(&table())
        .await
        .map(|binding| binding.map(|binding| binding.state));
    let replicas = side.1.replicas_for_test(table()).await.map(|replicas| {
        replicas.map(|replicas| {
            replicas
                .iter()
                .map(|replica| (replica.region_name.clone(), replica.replica_status.clone()))
                .collect::<Vec<_>>()
        })
    });
    format!(
        "region {}: pair state {state:?}, replicas {replicas:?}, markers {:?}",
        side.0.name,
        marker_count(side.0)
    )
}

/// Poll until both regions return the same value for every key (B may still be
/// Joining: an error) and both marker lists are empty. The timeout message
/// counts, from the last poll, keys that errored at each side and keys that
/// differ (no item data).
async fn await_converged(a: Side<'_>, b: Side<'_>, keys: &[String]) {
    let (mut errors_a, mut errors_b, mut differing) = (0, 0, 0);
    for _ in 0..POLLS {
        (errors_a, errors_b, differing) = (0, 0, 0);
        for name in keys {
            let (left, right) = (read(a.1, name).await, read(b.1, name).await);
            errors_a += usize::from(left.is_err());
            errors_b += usize::from(right.is_err());
            differing += usize::from(left != right);
        }
        let equal = errors_a == 0 && errors_b == 0 && differing == 0;
        if equal && marker_count(a.0) == Ok(0) && marker_count(b.0) == Ok(0) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (left, right) = (describe(a).await, describe(b).await);
    panic!(
        "regions did not converge on {keys:?}: keys erroring at a {errors_a}, at b {errors_b}, \
         differing {differing}\n{left}\n{right}"
    );
}

fn names(prefix: &str, count: usize) -> Vec<String> {
    (0..count).map(|n| format!("{prefix}{n:02}")).collect()
}

/// The table's pair state in one region (`None`: unpaired).
async fn state(manager: &DatabaseManager) -> Option<PairState> {
    manager
        .regional_binding_for_test(&table())
        .await
        .unwrap()
        .map(|binding| binding.state)
}

async fn await_state(manager: &DatabaseManager, expected: Option<PairState>) {
    for _ in 0..POLLS {
        if state(manager).await == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "pair state is {:?}, expected {expected:?}",
        state(manager).await
    );
}

/// Records of the table's public stream.
async fn public_records(manager: &DatabaseManager) -> usize {
    let (mut count, mut token) = (0, None::<String>);
    loop {
        let page = manager
            .get_stream_records_for_table_name(&table(), token.as_deref(), Some(100))
            .await
            .unwrap();
        if page.records().is_empty() {
            return count;
        }
        count += page.records().len();
        token = page.last_evaluated_key().map(str::to_owned);
        if token.is_none() {
            return count;
        }
    }
}

struct Pair {
    _directory: tempfile::TempDir,
    a_region: Region,
    b_region: Region,
    a: DatabaseManager,
    b: DatabaseManager,
}

/// Warnings and errors of the pair jobs and listeners go to the test output,
/// once per process.
fn init_tracing() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("warn")
            .with_test_writer()
            .try_init();
    });
}

async fn pair() -> Pair {
    init_tracing();
    let directory = tempfile::tempdir().unwrap();
    let (port_a, port_b) = (port(), port());
    let a_region = Region::new(directory.path(), "a", 1, port_a, port_b);
    let b_region = Region::new(directory.path(), "b", 2, port_b, port_a);
    let (a, b) = (a_region.open().await, b_region.open().await);
    create_source(&a).await;
    Pair {
        _directory: directory,
        a_region,
        b_region,
        a,
        b,
    }
}

async fn close(manager: DatabaseManager) {
    manager.shutdown_background_services().await.unwrap();
}

#[tokio::test]
async fn given_items_before_and_during_enable_when_bootstrap_completes_then_target_converges() {
    let Pair {
        a_region,
        b_region,
        a,
        b,
        _directory,
    } = pair().await;
    for name in names("k", 20) {
        put(&a, &name, "before").await;
    }
    replica_update(&a, "Create", "b").await.unwrap();
    for name in &names("k", 25)[20..] {
        put(&a, name, "during").await;
    }
    for name in &names("k", 3) {
        put(&a, name, "updated").await;
    }
    delete(&a, "k03").await;
    delete(&a, "k04").await;
    delete(&a, "absent").await;
    let mut keys = names("k", 25);
    keys.push("absent".into());
    await_converged((&a_region, &a), (&b_region, &b), &keys).await;
    assert!(read(&b, "k05").await.unwrap().is_some());
    assert_eq!(read(&b, "k03").await.unwrap(), None);
    // The pair is Active on both sides, DescribeTable at A shows the replica,
    // and no marker is left.
    await_state(&a, Some(PairState::Active)).await;
    await_state(&b, Some(PairState::Active)).await;
    let replicas = a
        .replicas_for_test(table())
        .await
        .unwrap()
        .expect("paired table lists its replica");
    assert_eq!(replicas.len(), 1);
    assert_eq!(
        (
            replicas[0].region_name.as_str(),
            &replicas[0].replica_status
        ),
        ("b", &ReplicaStatus::Active)
    );
}

#[tokio::test]
async fn given_concurrent_writes_on_both_regions_when_delivered_then_same_winner() {
    let Pair {
        a_region,
        b_region,
        a,
        b,
        _directory,
    } = pair().await;
    replica_update(&a, "Create", "b").await.unwrap();
    let keys = names("c", 5);
    await_converged((&a_region, &a), (&b_region, &b), &keys).await;
    for round in 0..30 {
        let (manager, side) = if round % 2 == 0 { (&a, "a") } else { (&b, "b") };
        for name in &keys {
            put(manager, name, &format!("round{round}-{side}")).await;
        }
    }
    await_converged((&a_region, &a), (&b_region, &b), &keys).await;
}

#[tokio::test]
async fn given_absent_delete_at_b_and_older_put_at_a_when_delivered_then_absent_and_markers_cleared()
 {
    let Pair {
        a_region,
        b_region,
        a,
        b,
        _directory,
    } = pair().await;
    replica_update(&a, "Create", "b").await.unwrap();
    let keys = names("z", 1);
    await_converged((&a_region, &a), (&b_region, &b), &keys).await;
    // A delete of an absent key leaves a marker at B, then a put at A races
    // with it.
    delete(&b, "z00").await;
    put(&a, "z00", "older").await;
    delete(&b, "z00").await;
    // Whichever order wins, both regions agree and every marker is cleared
    // (convergence requires it).
    await_converged((&a_region, &a), (&b_region, &b), &keys).await;
    put(&b, "z00", "later").await;
    await_converged((&a_region, &a), (&b_region, &b), &keys).await;
    assert!(read(&a, "z00").await.unwrap().is_some());
}

#[tokio::test]
async fn given_target_restarted_mid_stream_when_resumed_then_converges_without_duplicate_public_entries()
 {
    let Pair {
        a_region,
        b_region,
        a,
        b,
        _directory,
    } = pair().await;
    replica_update(&a, "Create", "b").await.unwrap();
    let first = names("r", 10);
    for name in &first {
        put(&a, name, "one").await;
    }
    await_converged((&a_region, &a), (&b_region, &b), &first).await;
    close(b).await;
    let second = names("s", 30);
    for name in &second {
        put(&a, name, "two").await;
    }
    let b = b_region.open().await;
    let keys = [first, second].concat();
    await_converged((&a_region, &a), (&b_region, &b), &keys).await;
    // One public record at B per applied change: a resent page must not
    // duplicate entries.
    let at_a = public_records(&a).await;
    assert_eq!(at_a, 40, "every put at A is one public record");
    assert_eq!(public_records(&b).await, at_a);
}

#[tokio::test]
async fn given_peer_down_when_writes_continue_then_held_and_delivered_on_return() {
    let Pair {
        a_region,
        b_region,
        a,
        b,
        _directory,
    } = pair().await;
    replica_update(&a, "Create", "b").await.unwrap();
    await_converged((&a_region, &a), (&b_region, &b), &names("h", 1)).await;
    close(b).await;
    let keys = names("h", 50);
    for name in &keys {
        put(&a, name, "while-down").await;
    }
    // History is held while the peer is away (the stream keeps every record),
    // and the pair stays.
    assert_eq!(public_records(&a).await, 50);
    assert_eq!(state(&a).await, Some(PairState::Active));
    let b = b_region.open().await;
    await_converged((&a_region, &a), (&b_region, &b), &keys).await;
    assert_eq!(public_records(&b).await, 50);
}

#[tokio::test]
async fn given_page_dropped_when_next_page_arrives_then_gap_and_resend() {
    let Pair {
        a_region,
        b_region,
        a,
        b,
        _directory,
    } = pair().await;
    replica_update(&a, "Create", "b").await.unwrap();
    let first = names("p", 6);
    for name in &first {
        put(&a, name, "one").await;
    }
    await_converged((&a_region, &a), (&b_region, &b), &first).await;
    let pair_id = state_pair(&a).await;
    let frontier = a
        .get_stream_records_for_table_name(&table(), None, Some(1))
        .await
        .unwrap();
    let lane = &frontier.table_frontier().expect("table frontier").lanes[0];
    let lineage = LineageRef {
        shard: lane.shard_id.clone(),
        lineage: lane.lineage_id.clone(),
    };
    // B's cursor in its provider's offset form (SQL offsets are order-only,
    // KV offsets are versionstamps; the two do not compare): a page that
    // covers nothing is acknowledged with the cursor and changes nothing.
    let probe = ReplicationPage {
        pair: pair_id,
        table: table(),
        lineage: lineage.clone(),
        parents: Vec::new(),
        after: None,
        through: None,
        sealed: false,
        entries: Vec::new(),
    };
    let RegionalResponse::Ack(ack) = b
        .regional_request_for_test(RegionalRequest::Page(probe))
        .await
        .unwrap()
    else {
        panic!("a page that covers nothing is acknowledged with the cursor");
    };
    let cursor = ack
        .through
        .expect("B has applied pages, so its cursor is set");
    let suffix = cursor
        .as_str()
        .trim_start_matches(|c: char| c.is_ascii_digit())
        .to_owned();
    let offset = |n: u64| SystemStreamOffset::try_new(format!("{n:020}{suffix}")).unwrap();
    let skipping_page = || ReplicationPage {
        pair: pair_id,
        table: table(),
        lineage: lineage.clone(),
        parents: Vec::new(),
        after: Some(offset(u64::MAX - 1)),
        through: Some(offset(u64::MAX)),
        sealed: false,
        entries: Vec::new(),
    };
    // A page that starts above B's cursor is a gap that names the cursor;
    // nothing changes at B.
    let RegionalResponse::Gap { expected: before } = b
        .regional_request_for_test(RegionalRequest::Page(skipping_page()))
        .await
        .unwrap()
    else {
        panic!("a page above the cursor must be answered with a gap");
    };
    assert!(
        before.is_some(),
        "B has applied pages, so its cursor is set"
    );
    // The cursor moves with delivery: more writes, then the gap names the new
    // cursor.
    let second = names("q", 6);
    for name in &second {
        put(&a, name, "two").await;
    }
    let keys = [first, second].concat();
    await_converged((&a_region, &a), (&b_region, &b), &keys).await;
    let RegionalResponse::Gap { expected: after } = b
        .regional_request_for_test(RegionalRequest::Page(skipping_page()))
        .await
        .unwrap()
    else {
        panic!("a page above the cursor must be answered with a gap");
    };
    assert_ne!(after, before, "the gap names B's current cursor");
    assert_eq!(public_records(&b).await, 12, "the gap page applied nothing");
}

async fn state_pair(manager: &DatabaseManager) -> storage_types::PairId {
    manager
        .regional_binding_for_test(&table())
        .await
        .unwrap()
        .expect("paired")
        .pair
}

#[tokio::test]
async fn given_graceful_removal_when_finished_then_both_unpaired_and_delete_table_succeeds() {
    let Pair {
        a_region,
        b_region,
        a,
        b,
        _directory,
    } = pair().await;
    replica_update(&a, "Create", "b").await.unwrap();
    let keys = names("g", 8);
    for name in &keys {
        put(&a, name, "v").await;
    }
    await_converged((&a_region, &a), (&b_region, &b), &keys).await;
    replica_update(&a, "Delete", "b").await.unwrap();
    // Both regions end unpaired, with no marker and no replica listed.
    await_state(&a, None).await;
    await_state(&b, None).await;
    assert_eq!(marker_count(&a_region), Ok(0));
    assert_eq!(marker_count(&b_region), Ok(0));
    assert!(a.replicas_for_test(table()).await.unwrap().is_none());
    assert!(b.replicas_for_test(table()).await.unwrap().is_none());
    // A paired table cannot be deleted; once the pair ends on each side,
    // DeleteTable succeeds.
    for manager in [&a, &b] {
        let mut deleted = false;
        for _ in 0..POLLS {
            if manager.delete_table(&table()).await.is_ok() {
                deleted = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(deleted, "the pair did not end");
    }
}

#[tokio::test]
async fn given_peer_down_when_force_local_then_unpaired_here_and_peer_unpairs_on_force() {
    let Pair {
        a_region,
        b_region,
        a,
        b,
        _directory,
    } = pair().await;
    replica_update(&a, "Create", "b").await.unwrap();
    await_converged((&a_region, &a), (&b_region, &b), &names("f", 1)).await;
    let pair_id = state_pair(&a).await;
    close(b).await;
    // Force: Delete of the local region name ends the pair here at once.
    replica_update(&a, "Delete", "a").await.unwrap();
    assert_eq!(state(&a).await, None);
    assert_eq!(marker_count(&a_region), Ok(0));
    a.delete_table(&table()).await.unwrap();
    // B was unreachable, so the best-effort Force never arrived: B is still
    // paired.
    let b = b_region.open().await;
    assert_eq!(state(&b).await, Some(PairState::Active));
    let forced = b
        .regional_request_for_test(RegionalRequest::Force(PairTable {
            pair: pair_id,
            table: table(),
        }))
        .await
        .unwrap();
    assert!(matches!(forced, RegionalResponse::Forced), "{forced:?}");
    assert_eq!(state(&b).await, None);
    assert_eq!(marker_count(&b_region), Ok(0));
    assert!(b.replicas_for_test(table()).await.unwrap().is_none());
    // B is an ordinary unpaired table now.
    put(&b, "after", "v").await;
    assert!(read(&b, "after").await.unwrap().is_some());
}
