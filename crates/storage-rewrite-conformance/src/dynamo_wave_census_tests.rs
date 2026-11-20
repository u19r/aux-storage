//! Wave census: the native events of one warm DynamoDB operation on each
//! provider. Each W6F step that changes a count updates `CENSUS` in the same
//! change (N2: SQL writers read GSI phases from the catalog binding).
//!
//! Each case runs its operation once to warm the statement and binding
//! caches, resets the census, and runs it again. Configurations: `plain`
//! (no GSI, no stream) and `gsi1_stream` (one GSI, stream
//! `NEW_AND_OLD_IMAGES`, the item exists, and each write changes the GSI key;
//! W6F design section 1). `plain/QueryPage1MB` reads one page of about 1 MB
//! from a table with a sort key.
//!
//! Besides waves and rows each row pins: relations or key families read
//! (`r=`) and written (`w=`); Postgres lock statements by kind; the conflict
//! keys and ranges (`rck rcr wck wcr`: FoundationDB read and write
//! conflicts, RocksDB validated written keys); durable work: Postgres WAL
//! records of the pool backends (`wal_rec`, not counted when every attempt is
//! `READ ONLY`, see `DurableMeter::apply`; the printed row also shows
//! `wal_b`, which is not pinned), SQLite WAL frames, Turso logical log bytes
//! (`log_b`), RocksDB write batch bytes (`batch_b`), FoundationDB mutation
//! bytes (`mut_b`). Durable bytes match within a small band; Postgres WAL
//! records are an upper bound (`tolerate_durable_bytes`).

use std::{sync::Arc, time::Duration};

use serde_json::{Value, json};
use storage_types::{
    BatchWriteItemRequest, CreateTableRequest, DeleteItemRequest, GetItemRequest, PutItemRequest,
    QueryRequest, TransactWriteItemsRequest, UpdateItemRequest,
};

use super::{
    native_census_test_support::{CensusCounts, NativeCensus},
    test_support::TestStore,
};

/// (provider, case, counts as `CensusCounts` displays them). Postgres cold
/// prepares are left out: they depend on which pooled connection serves the
/// attempt.
#[rustfmt::skip]
const CENSUS: &[(&str, &str, &str)] = &[
    ("sqlite", "plain/GetItem", "waves=2 stmts=2 read=2 begin=1 commit=1 r=item"),
    ("sqlite", "plain/Query", "waves=2 stmts=2 read=2 begin=1 commit=1 r=item"),
    ("sqlite", "plain/PutItem", "waves=3 stmts=3 read=2 written=1 begin=1 commit=1 barrier=1 r=item w=item wal_frames=1"),
    ("sqlite", "plain/UpdateItem", "waves=3 stmts=3 read=2 written=1 begin=1 commit=1 barrier=1 r=item w=item wal_frames=1"),
    ("sqlite", "plain/DeleteItem", "waves=3 stmts=3 read=2 written=1 begin=1 commit=1 barrier=1 r=item w=item wal_frames=1"),
    ("sqlite", "plain/ConditionalPutFails", "waves=2 stmts=2 read=2 begin=1 commit=1 r=item"),
    ("sqlite", "plain/BatchWrite25", "waves=3 stmts=3 read=26 written=25 begin=1 commit=1 barrier=1 r=item w=item wal_frames=1"),
    ("sqlite", "plain/TransactWrite10", "waves=3 stmts=3 read=11 written=10 begin=1 commit=1 barrier=1 r=item w=item wal_frames=1"),
    ("sqlite", "plain/QueryPage1MB", "waves=3 stmts=3 read=301 begin=1 commit=1 r=item"),
    ("sqlite", "gsi1_stream/GetItem", "waves=2 stmts=2 read=2 begin=1 commit=1 r=item"),
    ("sqlite", "gsi1_stream/Query", "waves=2 stmts=2 read=2 begin=1 commit=1 r=item"),
    ("sqlite", "gsi1_stream/QueryGsi", "waves=2 stmts=2 read=4 begin=1 commit=1 r=item"),
    ("sqlite", "gsi1_stream/PutItem", "waves=4 stmts=6 read=3 written=3 begin=1 commit=1 barrier=1 r=item+ss_lineage+ss_scope+ss_sequences w=item+ss_sequences+stream_entries wal_frames=5"),
    ("sqlite", "gsi1_stream/UpdateItem", "waves=4 stmts=6 read=3 written=3 begin=1 commit=1 barrier=1 r=item+ss_lineage+ss_scope+ss_sequences w=item+ss_sequences+stream_entries wal_frames=8"),
    ("sqlite", "gsi1_stream/DeleteItem", "waves=4 stmts=6 read=3 written=3 begin=1 commit=1 barrier=1 r=item+ss_lineage+ss_scope+ss_sequences w=item+ss_sequences+stream_entries wal_frames=5"),
    ("sqlite", "gsi1_stream/ConditionalPutFails", "waves=3 stmts=3 read=3 begin=1 commit=1 r=item+ss_lineage+ss_scope+ss_sequences"),
    ("sqlite", "gsi1_stream/BatchWrite25", "waves=4 stmts=30 read=27 written=51 begin=1 commit=1 barrier=1 r=item+ss_lineage+ss_scope+ss_sequences w=item+ss_sequences+stream_entries wal_frames=14"),
    ("sqlite", "gsi1_stream/TransactWrite10", "waves=4 stmts=15 read=12 written=21 begin=1 commit=1 barrier=1 r=item+ss_lineage+ss_scope+ss_sequences w=item+ss_sequences+stream_entries wal_frames=12"),
    // A local item stores an empty provenance blob (was the 23-byte unreplicated marker).
    ("turso", "plain/GetItem", "waves=4 stmts=2 read=2 begin=1 commit=1 r=item"),
    ("turso", "plain/Query", "waves=4 stmts=2 read=2 begin=1 commit=1 r=item"),
    ("turso", "plain/PutItem", "waves=5 stmts=3 read=2 written=1 begin=1 commit=1 r=item w=item log_b=142"),
    ("turso", "plain/UpdateItem", "waves=5 stmts=3 read=2 written=1 begin=1 commit=1 r=item w=item log_b=148"),
    ("turso", "plain/DeleteItem", "waves=5 stmts=3 read=2 written=1 begin=1 commit=1 r=item w=item log_b=53"),
    ("turso", "plain/ConditionalPutFails", "waves=4 stmts=2 read=2 begin=1 commit=1 r=item"),
    ("turso", "plain/BatchWrite25", "waves=5 stmts=3 read=26 written=25 begin=1 commit=1 r=item w=item log_b=2832"),
    ("turso", "plain/TransactWrite10", "waves=5 stmts=3 read=11 written=10 begin=1 commit=1 r=item w=item log_b=1192"),
    ("turso", "plain/QueryPage1MB", "waves=5 stmts=3 read=301 begin=1 commit=1 r=item"),
    ("turso", "gsi1_stream/GetItem", "waves=4 stmts=2 read=2 begin=1 commit=1 r=item"),
    ("turso", "gsi1_stream/Query", "waves=4 stmts=2 read=2 begin=1 commit=1 r=item"),
    ("turso", "gsi1_stream/QueryGsi", "waves=4 stmts=2 read=4 begin=1 commit=1 r=item"),
    ("turso", "gsi1_stream/PutItem", "waves=8 stmts=6 read=3 written=3 begin=1 commit=1 r=item+ss_lineage+ss_scope+ss_sequences w=item+ss_sequences+stream_entries log_b=924"),
    ("turso", "gsi1_stream/UpdateItem", "waves=8 stmts=6 read=3 written=3 begin=1 commit=1 r=item+ss_lineage+ss_scope+ss_sequences w=item+ss_sequences+stream_entries log_b=951"),
    ("turso", "gsi1_stream/DeleteItem", "waves=8 stmts=6 read=3 written=3 begin=1 commit=1 r=item+ss_lineage+ss_scope+ss_sequences w=item+ss_sequences+stream_entries log_b=728"),
    ("turso", "gsi1_stream/ConditionalPutFails", "waves=5 stmts=3 read=3 begin=1 commit=1 r=item+ss_lineage+ss_scope+ss_sequences"),
    ("turso", "gsi1_stream/BatchWrite25", "waves=32 stmts=30 read=27 written=51 begin=1 commit=1 r=item+ss_lineage+ss_scope+ss_sequences w=item+ss_sequences+stream_entries log_b=21697"),
    ("turso", "gsi1_stream/TransactWrite10", "waves=17 stmts=15 read=12 written=21 begin=1 commit=1 r=item+ss_lineage+ss_scope+ss_sequences w=item+ss_sequences+stream_entries log_b=8922"),
    ("postgres", "plain/GetItem", "waves=1 stmts=1 read=1 begin=1 commit=1 r=item"),
    ("postgres", "plain/Query", "waves=1 stmts=2 read=2 begin=1 commit=1 r=item"),
    ("postgres", "plain/PutItem", "waves=2 stmts=5 read=5 written=1 begin=1 commit=1 r=item w=item adv_cat_sh=1 adv_item=1 lock_table=1 wal_rec=3"),
    ("postgres", "plain/UpdateItem", "waves=2 stmts=5 read=5 written=1 begin=1 commit=1 r=item w=item adv_cat_sh=1 adv_item=1 lock_table=1 wal_rec=3"),
    ("postgres", "plain/DeleteItem", "waves=2 stmts=5 read=5 written=1 begin=1 commit=1 r=item w=item adv_cat_sh=1 adv_item=1 lock_table=1 wal_rec=2"),
    ("postgres", "plain/ConditionalPutFails", "waves=2 stmts=4 read=4 begin=1 commit=1 r=item adv_cat_sh=1 adv_item=1 lock_table=1"),
    ("postgres", "plain/BatchWrite25", "waves=2 stmts=5 read=29 written=25 begin=1 commit=1 r=item w=item adv_cat_sh=1 adv_item=1 lock_table=1 wal_rec=52"),
    ("postgres", "plain/TransactWrite10", "waves=2 stmts=5 read=14 written=10 begin=1 commit=1 r=item w=item adv_cat_sh=1 adv_item=1 lock_table=1 wal_rec=22"),
    ("postgres", "plain/QueryPage1MB", "waves=1 stmts=2 read=301 begin=1 commit=1 r=item"),
    ("postgres", "gsi1_stream/GetItem", "waves=1 stmts=1 read=1 begin=1 commit=1 r=item"),
    ("postgres", "gsi1_stream/Query", "waves=1 stmts=2 read=2 begin=1 commit=1 r=item"),
    ("postgres", "gsi1_stream/QueryGsi", "waves=1 stmts=2 read=4 begin=1 commit=1 r=item"),
    ("postgres", "gsi1_stream/PutItem", "waves=2 stmts=7 read=10 written=2 begin=1 commit=1 r=item+ss_lineage+ss_scope w=item+stream_entries adv_cat_sh=1 adv_item=1 lock_table=1 wal_rec=8"),
    ("postgres", "gsi1_stream/UpdateItem", "waves=2 stmts=7 read=10 written=2 begin=1 commit=1 r=item+ss_lineage+ss_scope w=item+stream_entries adv_cat_sh=1 adv_item=1 lock_table=1 wal_rec=8"),
    ("postgres", "gsi1_stream/DeleteItem", "waves=2 stmts=7 read=10 written=2 begin=1 commit=1 r=item+ss_lineage+ss_scope w=item+stream_entries adv_cat_sh=1 adv_item=1 lock_table=1 wal_rec=5"),
    ("postgres", "gsi1_stream/ConditionalPutFails", "waves=2 stmts=5 read=8 begin=1 commit=1 r=item+ss_lineage+ss_scope adv_cat_sh=1 adv_item=1 lock_table=1"),
    ("postgres", "gsi1_stream/BatchWrite25", "waves=2 stmts=31 read=58 written=50 begin=1 commit=1 r=item+ss_lineage+ss_scope w=item+stream_entries adv_cat_sh=1 adv_item=1 lock_table=1 wal_rec=181"),
    ("postgres", "gsi1_stream/TransactWrite10", "waves=2 stmts=16 read=28 written=20 begin=1 commit=1 r=item+ss_lineage+ss_scope w=item+stream_entries adv_cat_sh=1 adv_item=1 lock_table=1 wal_rec=72"),
    // BatchWrite and TransactWrite bytes dropped by a constant per item: a local item no longer stores a provenance value (REG-2, off-cost rule).
    ("rocksdb", "plain/GetItem", "waves=3 keys=2 read=2 begin=1 commit=1 r=item+~m/catalog-version"),
    ("rocksdb", "plain/Query", "waves=3 keys=1 ranges=1 read=2 begin=1 commit=1 r=item+~m/catalog-version"),
    ("rocksdb", "plain/PutItem", "waves=3 keys=2 read=2 written=1 begin=1 commit=1 barrier=1 r=item+~m/catalog-version w=item wck=1 batch_b=145"),
    ("rocksdb", "plain/UpdateItem", "waves=3 keys=2 read=2 written=1 begin=1 commit=1 barrier=1 r=item+~m/catalog-version w=item wck=1 batch_b=151"),
    ("rocksdb", "plain/DeleteItem", "waves=3 keys=2 read=2 written=1 begin=1 commit=1 barrier=1 r=item+~m/catalog-version w=item wck=1 batch_b=23"),
    ("rocksdb", "plain/ConditionalPutFails", "waves=3 keys=2 read=2 begin=1 commit=1 r=item+~m/catalog-version batch_b=12"),
    ("rocksdb", "plain/BatchWrite25", "waves=3 keys=26 read=26 written=25 begin=1 commit=1 barrier=1 r=item+~m/catalog-version w=item wck=25 batch_b=2837"),
    ("rocksdb", "plain/TransactWrite10", "waves=3 keys=11 read=11 written=10 begin=1 commit=1 barrier=1 r=item+~m/catalog-version w=item wck=10 batch_b=1182"),
    ("rocksdb", "plain/QueryPage1MB", "waves=4 keys=1 ranges=2 read=301 begin=1 commit=1 r=item+~m/catalog-version"),
    ("rocksdb", "gsi1_stream/GetItem", "waves=3 keys=2 read=2 begin=1 commit=1 r=item+~m/catalog-version"),
    ("rocksdb", "gsi1_stream/Query", "waves=3 keys=1 ranges=1 read=2 begin=1 commit=1 r=item+~m/catalog-version"),
    ("rocksdb", "gsi1_stream/QueryGsi", "waves=3 keys=1 ranges=1 read=4 begin=1 commit=1 r=gsi+~m/catalog-version"),
    ("rocksdb", "gsi1_stream/PutItem", "waves=3 keys=2 read=2 written=5 begin=1 commit=1 barrier=1 r=item+~m/catalog-version w=gsi+item+stream_entry+~s/r wck=5 batch_b=937"),
    ("rocksdb", "gsi1_stream/UpdateItem", "waves=3 keys=2 read=2 written=5 begin=1 commit=1 barrier=1 r=item+~m/catalog-version w=gsi+item+stream_entry+~s/r wck=5 batch_b=961"),
    ("rocksdb", "gsi1_stream/DeleteItem", "waves=3 keys=2 read=2 written=4 begin=1 commit=1 barrier=1 r=item+~m/catalog-version w=gsi+item+stream_entry+~s/r wck=4 batch_b=602"),
    ("rocksdb", "gsi1_stream/ConditionalPutFails", "waves=3 keys=2 read=2 begin=1 commit=1 r=item+~m/catalog-version batch_b=12"),
    ("rocksdb", "gsi1_stream/BatchWrite25", "waves=3 keys=26 read=26 written=125 begin=1 commit=1 barrier=1 r=item+~m/catalog-version w=gsi+item+stream_entry+~s/r wck=125 batch_b=22837"),
    ("rocksdb", "gsi1_stream/TransactWrite10", "waves=3 keys=11 read=11 written=50 begin=1 commit=1 barrier=1 r=item+~m/catalog-version w=gsi+item+stream_entry+~s/r wck=50 batch_b=9302"),
    ("rocksdb", "gsi1_build/BackfillPage", "waves=7 keys=5 ranges=3 read=7 written=4 begin=1 commit=1 barrier=1 r=gsi_runnable+item+lookup+meta+~m/i+~m/p w=gsi+gsi_build+gsi_runnable+~m/p wck=4 batch_b=1688"),
    // PERF-FDB-1: the \xff/metadataVersion snapshot read rides in the first data wave; writes
    // conflict on the table lookup (and the route scope key and lineage range) instead.
    ("foundationdb", "plain/GetItem", "waves=2 keys=2 read=2 begin=1 r=item+xff"),
    ("foundationdb", "plain/Query", "waves=2 keys=1 ranges=1 read=2 begin=1 r=item+xff"),
    ("foundationdb", "plain/PutItem", "waves=3 keys=2 read=2 written=1 begin=1 commit=1 r=item+xff w=item rck=2 wck=1 mut_b=179"),
    ("foundationdb", "plain/UpdateItem", "waves=3 keys=2 read=2 written=1 begin=1 commit=1 r=item+xff w=item rck=2 wck=1 mut_b=185"),
    ("foundationdb", "plain/DeleteItem", "waves=3 keys=2 read=2 written=1 begin=1 commit=1 r=item+xff w=item rck=2 wck=1 mut_b=58"),
    ("foundationdb", "plain/ConditionalPutFails", "waves=3 keys=2 read=2 begin=1 commit=1 r=item+xff rck=2"),
    ("foundationdb", "plain/BatchWrite25", "waves=3 keys=26 read=26 written=25 begin=1 commit=1 r=item+xff w=item rck=26 wck=25 mut_b=3975"),
    ("foundationdb", "plain/TransactWrite10", "waves=3 keys=11 read=11 written=10 begin=1 commit=1 r=item+xff w=item rck=11 wck=10 mut_b=1630"),
    ("foundationdb", "plain/QueryPage1MB", "waves=5 keys=1 ranges=2 read=301 begin=1 r=item+xff"),
    ("foundationdb", "gsi1_stream/GetItem", "waves=2 keys=2 read=2 begin=1 r=item+xff"),
    ("foundationdb", "gsi1_stream/Query", "waves=2 keys=1 ranges=1 read=2 begin=1 r=item+xff"),
    ("foundationdb", "gsi1_stream/QueryGsi", "waves=3 keys=1 ranges=1 read=4 begin=1 r=gsi+xff"),
    ("foundationdb", "gsi1_stream/PutItem", "waves=3 keys=2 read=2 written=5 begin=1 commit=1 r=item+xff w=gsi+item+stream_entry+~s/r rck=3 rcr=1 wck=3 mut_b=1144"),
    ("foundationdb", "gsi1_stream/UpdateItem", "waves=3 keys=2 read=2 written=5 begin=1 commit=1 r=item+xff w=gsi+item+stream_entry+~s/r rck=3 rcr=1 wck=3 mut_b=1168"),
    ("foundationdb", "gsi1_stream/DeleteItem", "waves=3 keys=2 read=2 written=4 begin=1 commit=1 r=item+xff w=gsi+item+stream_entry+~s/r rck=3 rcr=1 wck=2 mut_b=763"),
    ("foundationdb", "gsi1_stream/ConditionalPutFails", "waves=3 keys=2 read=2 begin=1 commit=1 r=item+xff rck=3 rcr=1"),
    ("foundationdb", "gsi1_stream/BatchWrite25", "waves=3 keys=26 read=26 written=125 begin=1 commit=1 r=item+xff w=gsi+item+stream_entry+~s/r rck=27 rcr=1 wck=75 mut_b=28081"),
    ("foundationdb", "gsi1_stream/TransactWrite10", "waves=3 keys=11 read=11 written=50 begin=1 commit=1 r=item+xff w=gsi+item+stream_entry+~s/r rck=12 rcr=1 wck=30 mut_b=11386"),
    ("foundationdb", "gsi1_build/BackfillPage", "waves=8 keys=5 ranges=3 read=7 written=7 begin=1 commit=1 r=gsi_runnable+item+lookup+meta+~m/i+~m/p w=0:chunk+chunk+gsi+gsi_build+gsi_runnable+~m/p rck=5 rcr=1 wck=4 wcr=3 mut_b=3874"),
];

struct Config {
    name: &'static str,
    gsi_and_stream: bool,
}

const CONFIGS: [Config; 2] = [
    Config {
        name: "plain",
        gsi_and_stream: false,
    },
    Config {
        name: "gsi1_stream",
        gsi_and_stream: true,
    },
];

fn request<T>(value: Value) -> T
where
    T: TryFrom<Value>,
    T::Error: std::fmt::Debug,
{
    T::try_from(value).expect("valid census request")
}

fn create_request(table: &str, config: &Config) -> CreateTableRequest {
    let mut value = json!({
        "TableName": table,
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]
    });
    if config.gsi_and_stream {
        value["AttributeDefinitions"] = json!([
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"}
        ]);
        value["GlobalSecondaryIndexes"] = json!([{
            "IndexName": "by_g",
            "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }]);
        value["StreamSpecification"] =
            json!({"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"});
        value["AuxStreamDurationHours"] = json!(48);
    }
    request(value)
}

fn item(key: &str, version: &str) -> Value {
    json!({"pk": {"S": key}, "v": {"S": version}, "g": {"S": format!("g_{version}")}})
}

fn put(table: &str, key: &str, version: &str) -> PutItemRequest {
    request(json!({"TableName": table, "Item": item(key, version)}))
}

async fn put_ok(store: &TestStore, table: &str, key: &str, version: &str) {
    store
        .operations
        .put_item(put(table, key, version))
        .await
        .expect("census put");
}

/// Durable work that the census reads outside the drivers: the WAL
/// statistics of the Postgres pool backends (the store tags them with its
/// schema as `application_name`), and the size of the Turso logical log.
pub(crate) enum DurableMeter {
    None,
    #[cfg(feature = "postgres")]
    Postgres(tokio_postgres::Client),
    #[cfg(feature = "turso")]
    Turso(std::path::PathBuf),
}

impl DurableMeter {
    #[cfg_attr(
        not(any(feature = "postgres", feature = "turso")),
        expect(unused_variables)
    )]
    pub(crate) async fn open(store: &TestStore, provider: &str) -> Self {
        match provider {
            #[cfg(feature = "postgres")]
            "postgres" => {
                let (client, connection) =
                    tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls)
                        .await
                        .expect("census statistics connection");
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                Self::Postgres(client)
            }
            #[cfg(feature = "turso")]
            "turso" => Self::Turso(
                format!("{}-log", store.turso_path().expect("Turso database path")).into(),
            ),
            _ => Self::None,
        }
    }

    /// Postgres: WAL records, full-page images and bytes; Turso: log bytes.
    pub(crate) async fn read(&self) -> [u64; 3] {
        match self {
            Self::None => [0; 3],
            #[cfg(feature = "postgres")]
            Self::Postgres(client) => {
                let row = client
                    .query_one(
                        "SELECT coalesce(sum(w.wal_records), 0)::bigint, coalesce(sum(w.wal_fpi), \
                         0)::bigint, coalesce(sum(w.wal_bytes), 0)::bigint FROM pg_stat_activity \
                         AS a CROSS JOIN LATERAL pg_stat_get_backend_wal(a.pid) AS w WHERE \
                         a.application_name = current_setting('application_name') AND a.pid <> \
                         pg_backend_pid()",
                        &[],
                    )
                    .await
                    .expect("backend WAL statistics");
                [0, 1, 2].map(|column| u64::try_from(row.get::<_, i64>(column)).unwrap_or(0))
            }
            #[cfg(feature = "turso")]
            Self::Turso(path) => [0, 0, std::fs::metadata(path).map_or(0, |file| file.len())],
        }
    }

    #[cfg_attr(
        not(any(feature = "postgres", feature = "turso")),
        expect(unused_variables)
    )]
    pub(crate) fn apply(&self, before: [u64; 3], after: [u64; 3], counts: &mut CensusCounts) {
        match self {
            Self::None => {}
            // Every attempt began `READ ONLY`, where Postgres rejects any data
            // write: WAL in that window is page maintenance (on-access heap
            // pruning, hint-bit full-page images after a checkpoint), not work
            // of the operation, and it varies with the cluster horizon and
            // checkpoint timing. Writes stay visible in `written=` and `w=`.
            #[cfg(feature = "postgres")]
            Self::Postgres(_) if counts.write_begins == 0 => {}
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => {
                counts.durable.wal_records = after[0].saturating_sub(before[0]);
                counts.durable.wal_full_pages = after[1].saturating_sub(before[1]);
                counts.durable.wal_bytes = after[2].saturating_sub(before[2]);
            }
            #[cfg(feature = "turso")]
            Self::Turso(_) => counts.durable.log_bytes = after[2].saturating_sub(before[2]),
        }
    }
}

/// Runs one operation for the warm-up (`version` 0) and once measured
/// (`version` 1), and returns the measured counts.
pub(crate) async fn measure<F, Fut>(
    census: &NativeCensus,
    meter: &DurableMeter,
    operation: F,
) -> CensusCounts
where
    F: Fn(u8) -> Fut,
    Fut: Future<Output = ()>,
{
    operation(0).await;
    census.reset();
    let before = meter.read().await;
    census.start_native_trace();
    operation(1).await;
    let mut counts = census.finish_native_trace("original_census");
    println!(
        "N9_CENSUS_TRANSFER returned={:?} chunk_keys={} cold_prepares={}",
        census.returned_data(),
        census.chunk_key_reads(),
        counts.cold_prepares
    );
    meter.apply(before, meter.read().await, &mut counts);
    counts
}

/// Items of the 1 MB page case: about 3.5 KB each, 300 of them.
const PAGE_ITEMS: usize = 300;
const PAGE_PAYLOAD_BYTES: usize = 3_500;

/// `QueryPage1MB`: one Query page close to the 1 MB page limit, on a table
/// with a sort key and no GSI or stream.
async fn run_page_case(
    store: &TestStore,
    census: &NativeCensus,
    meter: &DurableMeter,
) -> CensusCounts {
    let table = "census_page";
    store
        .operations
        .create_table(request(json!({
            "TableName": table,
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ]
        })))
        .await
        .expect("create census page table");
    let payload = "x".repeat(PAGE_PAYLOAD_BYTES);
    let items = (0..PAGE_ITEMS)
        .map(|index| {
            json!({"PutRequest": {"Item": {
                "pk": {"S": "page"},
                "sk": {"S": format!("{index:03}")},
                "ord": {"S": format!("{index:03}")},
                "payload": {"S": payload}
            }}})
        })
        .collect::<Vec<_>>();
    for chunk in items.chunks(25) {
        let mut request_items = serde_json::Map::new();
        request_items.insert(table.to_owned(), Value::Array(chunk.to_vec()));
        let response = store
            .operations
            .batch_write_item(request::<BatchWriteItemRequest>(
                json!({"RequestItems": request_items}),
            ))
            .await
            .expect("census page seed");
        assert!(
            response
                .unprocessed_items
                .as_ref()
                .is_none_or(|items| items.values().all(Vec::is_empty))
        );
    }
    measure(census, meter, |_| async move {
        let response = store
            .operations
            .query(request::<QueryRequest>(json!({
                "TableName": table,
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "page"}},
                // Override the default100; the original explicit limit lets bytes end this page.
                "Limit": PAGE_ITEMS
            })))
            .await
            .expect("census page query");
        assert!(
            response.count >= 250 && response.last_evaluated_key.is_some(),
            "the page must end at the 1 MB limit: {} items",
            response.count
        );
        let wire = serde_json::to_value(&response).unwrap();
        let expected = (0..response.count).map(|index| json!({
            "pk": {"S": "page"}, "sk": {"S": format!("{index:03}")}, "ord": {"S": format!("{index:03}")},
            "payload": {"S": "x".repeat(PAGE_PAYLOAD_BYTES)}
        })).collect::<Vec<_>>();
        assert_eq!(wire, json!({"Items": expected, "Count": response.count, "ScannedCount": response.count,
            "LastEvaluatedKey": {"pk": {"S": "page"}, "sk": {"S": format!("{:03}", response.count - 1)}}}));
    })
    .await
}

async fn run_config(
    store: &TestStore,
    census: &NativeCensus,
    meter: &DurableMeter,
    config: &Config,
) -> Vec<(String, CensusCounts)> {
    let table = format!("census_{}", config.name);
    let table = table.as_str();
    store
        .operations
        .create_table(create_request(table, config))
        .await
        .expect("create census table");
    for key in ["item", "d0", "d1"] {
        put_ok(store, table, key, "seed").await;
    }
    let mut cases = Vec::new();
    let mut record = |name: &str, counts| cases.push((format!("{}/{name}", config.name), counts));

    record(
        "GetItem",
        measure(census, meter, |_| async move {
            let response = store
                .operations
                .get_item(request::<GetItemRequest>(
                    json!({"TableName": table, "Key": {"pk": {"S": "item"}}}),
                ))
                .await
                .expect("census get");
            assert_eq!(
                serde_json::to_value(response).unwrap(),
                json!({"Item": item("item", "seed")})
            );
        })
        .await,
    );
    record(
        "Query",
        measure(census, meter, |_| async move {
            let response = store
                .operations
                .query(request::<QueryRequest>(json!({
                    "TableName": table,
                    "KeyConditionExpression": "pk = :pk",
                    "ExpressionAttributeValues": {":pk": {"S": "item"}}
                })))
                .await
                .expect("census query");
            assert_eq!(
                serde_json::to_value(response).unwrap(),
                json!({"Items": [item("item", "seed")], "Count": 1, "ScannedCount": 1})
            );
        })
        .await,
    );
    if config.gsi_and_stream {
        record(
            "QueryGsi",
            measure(census, meter, |_| async move {
                let response = store
                    .operations
                    .query(request::<QueryRequest>(json!({
                        "TableName": table,
                        "IndexName": "by_g",
                        "KeyConditionExpression": "g = :g",
                        "ExpressionAttributeValues": {":g": {"S": "g_seed"}}
                    })))
                    .await
                    .expect("census GSI query");
                assert_eq!(serde_json::to_value(response).unwrap(), json!({"Items": [item("d0", "seed"), item("d1", "seed"), item("item", "seed")], "Count": 3, "ScannedCount": 3}));
            })
            .await,
        );
    }
    put_ok(store, table, "warm", "warmup").await;
    record(
        "PutItem",
        measure(census, meter, |run| async move {
            let response = store
                .operations
                .put_item(put(table, "item", &format!("put{run}")))
                .await
                .unwrap();
            assert_eq!(serde_json::to_value(response).unwrap(), json!({}));
        })
        .await,
    );
    put_ok(store, table, "warm", "warmup").await;
    record(
        "UpdateItem",
        measure(census, meter, |run| async move {
            let response = store
                .operations
                .update_item(request::<UpdateItemRequest>(json!({
                    "TableName": table,
                    "Key": {"pk": {"S": "item"}},
                    "UpdateExpression": "SET v = :v, g = :g",
                    "ExpressionAttributeValues": {
                        ":v": {"S": format!("update{run}")},
                        ":g": {"S": format!("g_update{run}")}
                    }
                })))
                .await
                .expect("census update");
            assert_eq!(serde_json::to_value(response).unwrap(), json!({}));
        })
        .await,
    );
    put_ok(store, table, "warm", "warmup").await;
    record(
        "DeleteItem",
        measure(census, meter, |run| async move {
            let response = store
                .operations
                .delete_item(request::<DeleteItemRequest>(
                    json!({"TableName": table, "Key": {"pk": {"S": format!("d{run}")}}}),
                ))
                .await
                .expect("census delete");
            assert_eq!(serde_json::to_value(response).unwrap(), json!({}));
        })
        .await,
    );
    put_ok(store, table, "warm", "warmup").await;
    record(
        "ConditionalPutFails",
        measure(census, meter, |run| async move {
            let conditional = json!({
                "TableName": table,
                "Item": item("item", &format!("conditional{run}")),
                "ConditionExpression": "attribute_not_exists(pk)"
            });
            let outcome = store
                .operations
                .put_item(request::<PutItemRequest>(conditional))
                .await;
            use storage_types::context::WrappedError as _;
            assert!(matches!(
                outcome.unwrap_err().to_enum(),
                storage_types::StorageEnum::ConditionalCheckFailed
            ));
        })
        .await,
    );
    put_ok(store, table, "warm", "warmup").await;
    record(
        "BatchWrite25",
        measure(census, meter, |run| async move {
            let puts = (0..25)
                .map(|index| {
                    json!({"PutRequest": {
                        "Item": item(&format!("b{index:02}"), &format!("batch{run}"))
                    }})
                })
                .collect::<Vec<_>>();
            let mut items = serde_json::Map::new();
            items.insert(table.to_owned(), Value::Array(puts));
            let response = store
                .operations
                .batch_write_item(request::<BatchWriteItemRequest>(
                    json!({"RequestItems": items}),
                ))
                .await
                .expect("census batch write");
            assert_eq!(
                serde_json::to_value(response).unwrap(),
                json!({"UnprocessedItems": {}})
            );
        })
        .await,
    );
    put_ok(store, table, "warm", "warmup").await;
    record(
        "TransactWrite10",
        measure(census, meter, |run| async move {
            let items = (0..10)
                .map(|index| {
                    json!({"Put": {
                        "TableName": table,
                        "Item": item(&format!("t{index}"), &format!("transact{run}"))
                    }})
                })
                .collect::<Vec<_>>();
            let response = store
                .operations
                .transact_write_items(request::<TransactWriteItemsRequest>(
                    json!({"TransactItems": items}),
                ))
                .await
                .expect("census transact write");
            assert_eq!(serde_json::to_value(response).unwrap(), json!({}));
        })
        .await,
    );
    if !config.gsi_and_stream {
        record("QueryPage1MB", run_page_case(store, census, meter).await);
    }
    cases
}

/// Audit all 25/10 members and every logical stream image only after the
/// original warm/measured sequence, so audit reads cannot warm its caches.
async fn audit_census_data(store: &TestStore) {
    use std::collections::BTreeMap;
    let scope = store.stream_operations.system_stream_scope().await.unwrap();
    let catalog = store
        .stream_operations
        .system_stream_lineage_catalog(scope.clone())
        .await
        .unwrap();
    let page = store
        .stream_operations
        .get_system_stream_page(
            stream_provider::SystemStreamPageRequest::try_new(scope, None, 1).unwrap(),
        )
        .await
        .unwrap();
    let checkpoint = page.issued_checkpoint().checkpoint();
    println!(
        "N9_CENSUS_TOPOLOGY provider={} lineages={} checkpoint_frontier={} \
         checkpoint_json_bytes={} offset_lengths={:?}",
        TestStore::selected_backend(),
        catalog.len(),
        checkpoint.frontier().len(),
        serde_json::to_vec(checkpoint).unwrap().len(),
        checkpoint
            .frontier()
            .iter()
            .map(|position| position.offset().as_str().len())
            .collect::<Vec<_>>()
    );
    for config in &CONFIGS {
        let table = format!("census_{}", config.name);
        let mut expected = BTreeMap::new();
        let mut events = Vec::new();
        let mut write = |key: &str, version: Option<&str>| {
            let next = version.map(|version| item(key, version));
            let previous = match &next {
                Some(value) => expected.insert(key.to_owned(), value.clone()),
                None => expected.remove(key),
            };
            events.push(json!({"key": {"pk": {"S": key}},
                "event": if next.is_none() { "REMOVE" } else if previous.is_none() { "INSERT" } else { "MODIFY" },
                "old": previous, "new": next}));
        };
        for key in ["item", "d0", "d1"] {
            write(key, Some("seed"));
        }
        for operation in ["put", "update", "delete", "condition", "batch", "transact"] {
            write("warm", Some("warmup"));
            for run in 0..2 {
                match operation {
                    "put" | "update" => write("item", Some(&format!("{operation}{run}"))),
                    "delete" => write(&format!("d{run}"), None),
                    "batch" => {
                        for index in 0..25 {
                            write(&format!("b{index:02}"), Some(&format!("batch{run}")));
                        }
                    }
                    "transact" => {
                        for index in 0..10 {
                            write(&format!("t{index}"), Some(&format!("transact{run}")));
                        }
                    }
                    _ => {}
                }
            }
        }
        for (key, image) in &expected {
            let response = store
                .operations
                .get_item(request::<GetItemRequest>(
                    json!({"TableName": table, "Key": {"pk": {"S": key}}}),
                ))
                .await
                .unwrap();
            assert_eq!(
                serde_json::to_value(response).unwrap(),
                json!({"Item": image})
            );
        }
        for key in ["d0", "d1"] {
            let response = store
                .operations
                .get_item(request::<GetItemRequest>(
                    json!({"TableName": table, "Key": {"pk": {"S": key}}}),
                ))
                .await
                .unwrap();
            assert_eq!(serde_json::to_value(response).unwrap(), json!({}));
        }
        if !config.gsi_and_stream {
            continue;
        }
        for version in [
            "seed",
            "put0",
            "put1",
            "update0",
            "update1",
            "warmup",
            "batch0",
            "batch1",
            "transact0",
            "transact1",
            "conditional0",
            "conditional1",
        ] {
            let mut images = expected
                .values()
                .filter(|image| image["v"]["S"] == version)
                .cloned()
                .collect::<Vec<_>>();
            let response = store.operations.query(request::<QueryRequest>(json!({"TableName": table, "IndexName": "by_g", "KeyConditionExpression": "g = :g", "ExpressionAttributeValues": {":g": {"S": format!("g_{version}")}}}))).await.unwrap();
            let mut actual = serde_json::to_value(response).unwrap();
            let actual_images = actual["Items"].as_array_mut().unwrap();
            images.sort_by_key(Value::to_string);
            actual_images.sort_by_key(Value::to_string);
            assert_eq!(
                actual,
                json!({"Items": images, "Count": images.len(), "ScannedCount": images.len()})
            );
        }
        store.wait_until_changes_are_readable().await;
        let described = store
            .operations
            .describe_table(request(json!({"TableName": table})))
            .await
            .unwrap();
        let arn = described.table.latest_stream_arn.unwrap();
        let records = super::table_history_retention_tests::read_standard(store, &arn)
            .await
            .unwrap();
        let mut actual = records.into_iter().map(|record| {
            let wire = serde_json::to_value(record.dynamodb).unwrap();
            json!({"key": wire["Keys"], "event": record.event_name, "old": wire["OldImage"], "new": wire["NewImage"]})
        }).collect::<Vec<_>>();
        events.sort_by_key(Value::to_string);
        actual.sort_by_key(Value::to_string);
        assert_eq!(
            actual, events,
            "every logical stream event and complete old/new image"
        );
        println!(
            "N9_CENSUS_AUDIT provider={} primary={} deleted=2 stream_events={}",
            TestStore::selected_backend(),
            expected.len(),
            actual.len()
        );
    }
}

/// Durable work varies a little between runs. Stream rows encode time values
/// with different lengths, so the Turso, RocksDB and FoundationDB durable bytes
/// match within 1% (at least 64 bytes). Postgres WAL records vary between
/// runs with page pruning (prune-on-access) and hint-bit and lock records, so
/// the measured `wal_rec` must not exceed its pin, which is the highest value
/// seen in three serial runs of that row; the census does not pin Postgres
/// WAL bytes or full-page images.
fn tolerate_durable_bytes(provider: &str, counts: &CensusCounts, expected: &str) -> Option<String> {
    let pinned = |name: &str| {
        expected
            .split(' ')
            .find_map(|field| field.strip_prefix(name)?.strip_prefix('='))
            .map_or(Some(0), |value| value.parse::<u64>().ok())
    };
    let close = |measured: u64, pinned: u64| measured.abs_diff(pinned) <= (pinned / 100).max(64);
    let mut pinned_counts = counts.clone();
    let durable = &mut pinned_counts.durable;
    if provider == "postgres" {
        let records = pinned("wal_rec")?;
        if durable.wal_records > records {
            return None;
        }
        durable.wal_records = records;
    }
    for (name, value) in [
        ("log_b", &mut durable.log_bytes),
        ("batch_b", &mut durable.write_batch_bytes),
        ("mut_b", &mut durable.mutation_bytes),
    ] {
        let expected = pinned(name)?;
        if !close(*value, expected) {
            return None;
        }
        *value = expected;
    }
    eprintln!(
        "census: durable work within the band: {}",
        counts.durable_summary()
    );
    Some(pinned_counts.to_string())
}

#[tokio::test]
async fn given_warm_table_when_each_dynamo_operation_runs_then_wave_census_matches_provider_table()
{
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let provider = TestStore::selected_backend();
    let meter = DurableMeter::open(&store, &provider).await;
    let mut actual = Vec::new();
    for config in &CONFIGS {
        actual.extend(run_config(&store, &census, &meter, config).await);
    }
    if matches!(provider.as_str(), "rocksdb" | "foundationdb") {
        let mut create = super::gsi_workflow_tests::table_request("census_build");
        let indexes = create.global_secondary_indexes.take().unwrap();
        store.operations.create_table(create).await.unwrap();
        for key in ["A", "B", "C"] {
            super::gsi_workflow_tests::put_group(&store, "census_build", key, "x")
                .await
                .unwrap();
        }
        store.operations.update_table(serde_json::from_value(json!({
            "TableName": "census_build",
            "GlobalSecondaryIndexUpdates": indexes.into_iter().map(|index| json!({"Create": index})).collect::<Vec<_>>()
        })).unwrap()).await.unwrap();
        let build = super::gsi_workflow_tests::build_request(1);
        store
            .maintenance
            .advance_gsi_build(build.clone())
            .await
            .unwrap();
        census.reset();
        census.start_native_trace();
        store.maintenance.advance_gsi_build(build).await.unwrap();
        let counts = census.finish_native_trace("original_backfill_census");
        println!(
            "N9_CENSUS_TRANSFER returned={:?} chunk_keys={} cold_prepares={}",
            census.returned_data(),
            census.chunk_key_reads(),
            counts.cold_prepares
        );
        assert!(
            !counts.touched_read.contains("gsi"),
            "no destination entry read: {counts}"
        );
        if provider == "foundationdb" {
            assert!(
                counts.conflicts.read_ranges >= 1,
                "source range is protected: {counts}"
            );
        }
        actual.push(("gsi1_build/BackfillPage".to_owned(), counts));
    }
    // All primary, index and stream audits follow the last measured window.
    audit_census_data(&store).await;
    drop(meter);
    store.finish().await;
    let expected = CENSUS
        .iter()
        .filter(|(row_provider, _, _)| *row_provider == provider)
        .map(|(_, case, counts)| ((*case).to_owned(), (*counts).to_owned()))
        .collect::<Vec<_>>();
    let actual = actual
        .into_iter()
        .map(|(case, counts)| {
            println!("N9_CENSUS_ROW provider={provider} case={case} {counts}");
            let mut counts = CensusCounts {
                cold_prepares: 0,
                ..counts
            };
            if provider == "postgres" {
                counts.durable.wal_bytes = 0;
                counts.durable.wal_full_pages = 0;
            }
            let measured = counts.to_string();
            let row = expected
                .iter()
                .find(|(expected_case, _)| *expected_case == case)
                .filter(|(_, pinned)| *pinned != measured)
                .and_then(|(_, pinned)| tolerate_durable_bytes(&provider, &counts, pinned))
                .unwrap_or(measured);
            (case, row)
        })
        .collect::<Vec<_>>();
    if expected != actual {
        for (position, (pinned, measured)) in expected.iter().zip(&actual).enumerate() {
            if pinned != measured {
                eprintln!(
                    "census mismatch at {position}: expected {pinned:?}; actual {measured:?}"
                );
            }
        }
        eprintln!(
            "census rows: expected {}; actual {}",
            expected.len(),
            actual.len()
        );
        let table = actual
            .iter()
            .map(|(case, counts)| format!("    (\"{provider}\", \"{case}\", \"{counts}\"),"))
            .collect::<Vec<_>>()
            .join("\n");
        panic!("wave census of {provider} differs from CENSUS; measured rows:\n{table}");
    }
}

/// The hold hook stops a whole wave before it is sent, and the operation
/// completes after the release.
#[tokio::test]
async fn given_held_wave_when_released_then_operation_completes() {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let table = "census_hold";
    store
        .operations
        .create_table(create_request(
            table,
            &Config {
                name: "hold",
                gsi_and_stream: false,
            },
        ))
        .await
        .expect("create hold table");
    put_ok(&store, table, "item", "seed").await;
    let get = || {
        let operations = Arc::clone(&store.operations);
        async move {
            operations
                .get_item(request::<GetItemRequest>(
                    json!({"TableName": table, "Key": {"pk": {"S": "item"}}}),
                ))
                .await
        }
    };
    get().await.expect("warm get");
    let held = census.hold(|wave| wave.is_data());
    let mut reader = tokio::spawn(get());
    held.reached().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut reader)
            .await
            .is_err(),
        "a held wave must stop its operation"
    );
    held.release();
    let response = tokio::time::timeout(Duration::from_secs(10), reader)
        .await
        .expect("released operation completes")
        .expect("reader task")
        .expect("released get");
    assert!(response.item.is_some());
    store.finish().await;
}

/// The KV family stores the binding in N2b; until then a KV TTL checkpoint
/// and a KV GSI phase change keep their old catalog effect.
/// A warm Put after `change` runs in one native attempt (no stamp mismatch,
/// no binding reload) or retries (`retried`).
async fn measured_put(
    store: &TestStore,
    census: &NativeCensus,
    table: &str,
    key: &str,
) -> CensusCounts {
    census.reset();
    put_ok(store, table, key, "measured").await;
    census.snapshot()
}

/// W6 A15: a TTL sweep page stores its checkpoint as job state. It leaves
/// the catalog stamp unchanged, so a writer with a cached binding does not
/// retry.
#[tokio::test]
async fn given_ttl_sweep_page_then_stamp_unchanged() {
    use storage_common::{TtlSweepPageCommand, TtlSweepPageInput};

    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let table = "census_ttl_page";
    store
        .operations
        .create_table(create_request(
            table,
            &Config {
                name: "ttl",
                gsi_and_stream: false,
            },
        ))
        .await
        .expect("create TTL table");
    store
        .operations
        .update_time_to_live(
            serde_json::from_value(json!({
                "TableName": table,
                "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": true}
            }))
            .expect("valid TTL request"),
        )
        .await
        .expect("enable TTL");
    let cutoff = store.now();
    for key in ["a", "b", "c"] {
        store
            .operations
            .put_item(request(json!({
                "TableName": table,
                "Item": {"pk": {"S": key}, "ttl": {"N": "1"}}
            })))
            .await
            .expect("expired put");
    }
    let policy = store
        .maintenance
        .read_ttl_sweep_policy(&storage_types::TableName::new(table))
        .await
        .expect("TTL policy read")
        .expect("TTL policy");
    put_ok(&store, table, "writer", "warm").await;
    let command = TtlSweepPageCommand::try_new(
        policy,
        TtlSweepPageInput::try_new("ttl".to_owned(), cutoff, 1, None).expect("page input"),
        cutoff,
    )
    .expect("page command");
    let page = store
        .maintenance
        .execute_ttl_sweep_page(command)
        .await
        .expect("sweep page");
    assert_eq!(page.deleted_items, 1);
    assert!(
        page.next_cursor.is_some(),
        "the page must leave a checkpoint"
    );
    let counts = measured_put(&store, &census, table, "writer").await;
    store.finish().await;
    assert_eq!(
        (counts.begins, counts.rollbacks),
        (1, 0),
        "a sweep page changed the catalog stamp: {counts}"
    );
}

/// A GSI phase change is a binding change: it changes the catalog stamp, and
/// a writer with the cached binding of the old phase retries with the new
/// one.
#[tokio::test]
async fn given_gsi_phase_change_then_cached_writer_retries() {
    use std::num::NonZeroU64;

    use storage_provider::maintenance::GsiBuildRequest;
    use storage_types::{TimestampMillis, maintenance::StepResult};

    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let table = "census_gsi_phase";
    let mut create = create_request(
        table,
        &Config {
            name: "gsi_phase",
            gsi_and_stream: true,
        },
    );
    create.stream_specification = None;
    let indexes = create
        .global_secondary_indexes
        .take()
        .expect("census GSI definition");
    store
        .operations
        .create_table(create)
        .await
        .expect("create table");
    put_ok(&store, table, "seed", "seed").await;
    let mut update: storage_types::UpdateTableRequest =
        serde_json::from_value(json!({"TableName": table})).expect("valid update");
    update.attribute_definitions = Some(vec![
        storage_types::AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: storage_types::KeyAttributeType::S,
        },
        storage_types::AttributeDefinition {
            attribute_name: "g".to_owned(),
            attribute_type: storage_types::KeyAttributeType::S,
        },
    ]);
    update.global_secondary_index_updates = Some(
        indexes
            .into_iter()
            .map(|create| storage_types::GlobalSecondaryIndexUpdate {
                create: Some(create),
                update: None,
                delete: None,
            })
            .collect(),
    );
    store
        .operations
        .update_table(update)
        .await
        .expect("add GSI");
    // The writer binding is cached while the new GSI builds.
    put_ok(&store, table, "writer", "warm").await;
    let build = GsiBuildRequest {
        destination: b"aux-storage:conformance:gsi:v1"
            .to_vec()
            .try_into()
            .expect("destination"),
        job: [7; 16].into(),
        fence: NonZeroU64::MIN,
        page_limit: storage_backfill::GsiPageLimit::try_from(8usize).expect("page limit"),
        captured_at: TimestampMillis::from_timestamp(1_800_000_000_000),
    };
    let mut complete = false;
    for _ in 0..32 {
        let step = store
            .maintenance
            .advance_gsi_build(build.clone())
            .await
            .expect("GSI build step");
        assert!(!matches!(step, StepResult::Superseded));
        if matches!(step, StepResult::Complete) {
            complete = true;
            break;
        }
    }
    assert!(complete, "the GSI build did not become active");
    let counts = measured_put(&store, &census, table, "writer").await;
    let indexed = store
        .operations
        .query(request::<QueryRequest>(json!({
            "TableName": table,
            "IndexName": "by_g",
            "KeyConditionExpression": "g = :g",
            "ExpressionAttributeValues": {":g": {"S": "g_measured"}}
        })))
        .await
        .expect("GSI query");
    store.finish().await;
    assert!(
        counts.rollbacks >= 1 && counts.begins >= 2,
        "the cached writer did not retry after the phase change: {counts}"
    );
    assert_eq!(
        indexed.count, 1,
        "the retried write must maintain the index"
    );
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_active_gsi_when_query_and_sequence_run_then_native_reads_are_counted() {
    use futures::FutureExt as _;
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let table = "n7_gsi_read";
        store.operations.create_table(create_request(table, &CONFIGS[1])).await.unwrap();
        for key in ["a", "b", "c"] { put_ok(&store, table, key, "seed").await; }
        let query = json!({"TableName": table, "IndexName": "by_g",
            "KeyConditionExpression": "g = :g", "ExpressionAttributeValues": {":g": {"S": "g_seed"}},
            "Limit": 2, "ReturnConsumedCapacity": "INDEXES"});
        for sequence in [false, true] {
            let mut expected = None;
            for run in 0..2 {
                census.reset();
                let result = if sequence {
                    serde_json::to_value(store.operations.read_sequence(request(json!({
                        "Nodes": [{"Name": "page", "Operation": {"Query": query}}], "Outputs": ["page"]
                    }))).await.unwrap()).unwrap()
                } else {
                    serde_json::to_value(store.operations.query(request::<QueryRequest>(query.clone())).await.unwrap()).unwrap()
                };
                if let Some(expected) = &expected { assert_eq!(&result, expected); }
                else { expected = Some(result.clone()); }
                if run == 1 {
                    let counts = census.snapshot();
                    assert_eq!(counts.begins, 1, "one customer attempt: {counts}");
                    assert_eq!(counts.rows_written, 0);
                    println!("N7_GSI_READ provider={} sequence={sequence} {counts} result={result}", TestStore::selected_backend());
                }
            }
        }
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_active_gsi_when_native_addresses_and_page_are_read_then_identity_is_preserved() {
    use futures::FutureExt as _;
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let Some(read) = store.native_gsi_read.as_ref() else {
            return;
        };
        let table = storage_types::TableName::new("n7_gsi_native");
        store
            .operations
            .create_table(create_request(table.as_ref(), &CONFIGS[1]))
            .await
            .unwrap();
        for key in ["a", "b", "c"] {
            put_ok(&store, table.as_ref(), key, "seed").await;
        }
        let keys = || {
            ["a", "b", "c"]
                .into_iter()
                .map(|key| storage_backfill::GsiProjectionKey {
                    index_name: storage_types::IndexName::new("by_g"),
                    index_key: serde_json::from_value(json!({"g": {"S": "g_seed"}})).unwrap(),
                    table_item_key: serde_json::from_value(json!({"pk": {"S": key}})).unwrap(),
                    ttl_index_key: None,
                })
                .collect()
        };
        for scan in [false, true] {
            let (expected, images) =
                read(table.clone(), if scan { Vec::new() } else { keys() }, scan)
                    .await
                    .unwrap();
            census.reset();
            let (actual, actual_images) =
                read(table.clone(), if scan { Vec::new() } else { keys() }, scan)
                    .await
                    .unwrap();
            assert!(actual == expected);
            assert_eq!(
                serde_json::to_value(actual_images).unwrap(),
                serde_json::to_value(images).unwrap()
            );
            assert_eq!(actual.len(), if scan { 0 } else { 3 });
            println!(
                "N7_GSI_NATIVE provider={} scan={scan} {} addresses={}",
                TestStore::selected_backend(),
                census.snapshot(),
                serde_json::to_value(actual).unwrap()
            );
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_cached_active_gsi_when_other_store_recreates_it_then_old_projection_is_not_scanned()
{
    use futures::FutureExt as _;
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let Some(reader) = store.independent_operations.as_ref() else { return; };
        let table = "n7_gsi_stale";
        store.operations.create_table(create_request(table, &CONFIGS[1])).await.unwrap();
        put_ok(&store, table, "a", "seed").await;
        let query = || request::<QueryRequest>(json!({"TableName": table, "IndexName": "by_g",
            "KeyConditionExpression": "g = :g", "ExpressionAttributeValues": {":g": {"S": "g_seed"}}}));
        assert_eq!(reader.query(query()).await.unwrap().count, 1);
        store.operations.update_table(request(json!({"TableName": table,
            "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "by_g"}}]}))).await.unwrap();
        census.reset();
        assert!(reader.query(query()).await.is_err());
        assert_eq!(census.snapshot().ranges, 0, "deleted index must reject before scanning");
        store.advance_time(Duration::from_secs(30));
        let create = create_request(table, &CONFIGS[1]).global_secondary_indexes.unwrap().remove(0);
        store.operations.update_table(request(json!({"TableName": table,
            "AttributeDefinitions": [{"AttributeName": "g", "AttributeType": "S"}],
            "GlobalSecondaryIndexUpdates": [{"Create": create}]}))).await.unwrap();
        census.reset();
        assert!(reader.query(query()).await.is_err(), "recreated Filling index must reject");
        assert_eq!(census.snapshot().ranges, 0, "Filling index must reject before scanning");
        super::gsi_lifecycle_tests::finish_backfill(&store, 8).await.unwrap();
        assert_eq!(reader.query(query()).await.unwrap().count, 1);
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_unfinished_gsi_when_filling_or_building_then_query_rejects_before_scan() {
    use futures::FutureExt as _;
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let table = storage_types::TableName::new("n7_gsi_building");
        let mut create = create_request(table.as_ref(), &CONFIGS[1]);
        let index = create.global_secondary_indexes.take().unwrap().remove(0);
        store.operations.create_table(create).await.unwrap();
        put_ok(&store, table.as_ref(), "a", "seed").await;
        store.operations.update_table(request(json!({"TableName": table,
            "AttributeDefinitions": [{"AttributeName": "g", "AttributeType": "S"}],
            "GlobalSecondaryIndexUpdates": [{"Create": index}]}))).await.unwrap();
        assert_eq!((store.catalog_gsi_phase)(table.clone()).await.unwrap(), storage_types::GsiPhase::Filling);
        let query = || request::<QueryRequest>(json!({"TableName": table, "IndexName": "by_g",
            "KeyConditionExpression": "g = :g", "ExpressionAttributeValues": {":g": {"S": "g_seed"}}}));
        for phase in [storage_types::GsiPhase::Filling, storage_types::GsiPhase::Building] {
            let mut reached = false;
            for _ in 0..16 {
                let current = (store.catalog_gsi_phase)(table.clone()).await.unwrap();
                if current == phase { reached = true; break; }
                assert_ne!(current, storage_types::GsiPhase::Active, "must observe Building before activation");
                store.maintenance.advance_gsi_build(super::gsi_workflow_tests::build_request(1)).await.unwrap();
            }
            assert!(reached, "expected {phase:?}");
            census.reset();
            assert!(store.operations.query(query()).await.is_err());
            assert_eq!(census.snapshot().ranges, 0);
        }
        super::gsi_lifecycle_tests::finish_backfill(&store, 8).await.unwrap();
        assert_eq!(store.operations.query(query()).await.unwrap().count, 1);
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// Current large-page characterization, independent of historical broad census
/// pins.
#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_large_query_pages_when_read_in_both_orders_then_contents_and_native_costs_hold() {
    characterize_large_range(false).await;
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_large_query_pages_when_bounded_reads_overlap_then_resources_and_contents_hold() {
    characterize_large_range(true).await;
}

async fn characterize_large_range(should_measure_resources: bool) {
    use futures::FutureExt as _;
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let meter = DurableMeter::open(&store, &TestStore::selected_backend()).await;
        let counts = run_page_case(&store, &census, &meter).await;
        println!("N7_LARGE_RANGE provider={} case=inline_first {counts}", TestStore::selected_backend());
        store.operations.create_table(request::<CreateTableRequest>(json!({
            "TableName": "n7_chunk_page", "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}]
        }))).await.unwrap();
        let puts = (0..12).map(|index| json!({"PutRequest": {"Item": {"pk": {"S": "page"}, "sk": {"S": format!("{index:03}")}, "ord": {"S": format!("{index:03}")}, "payload": {"S": "x".repeat(110_000)}}}})).collect::<Vec<_>>();
        let seeded = store.operations.batch_write_item(request::<BatchWriteItemRequest>(json!({"RequestItems": {"n7_chunk_page": puts}}))).await.unwrap();
        assert!(seeded.unprocessed_items.as_ref().is_none_or(|items| items.values().all(Vec::is_empty)));
        if should_measure_resources { measure_large_range_resources(&store, &census).await; }
        for (table, total, cutoff, excluded) in [("census_page", PAGE_ITEMS, 50, 100), ("n7_chunk_page", 12, 2, 4)] {
        for (reverse, projected, exclusive) in [(false, false, false), (true, false, false), (false, true, true), (true, true, true)] {
            let mut continuation = exclusive.then(|| json!({"pk": {"S": "page"}, "sk": {"S": format!("{cutoff:03}")}}));
            let mut keys = Vec::new();
            let mut page = 0;
            loop {
                let mut query = json!({"TableName": table, "KeyConditionExpression": "pk = :pk",
                    "ExpressionAttributeValues": {":pk": {"S": "page"}}, "Limit": PAGE_ITEMS,
                    "ScanIndexForward": !reverse, "ReturnConsumedCapacity": "TOTAL"});
                if let Some(key) = &continuation { query["ExclusiveStartKey"] = key.clone(); }
                if projected {
                    query["ProjectionExpression"] = json!("pk, sk");
                    query["FilterExpression"] = json!("ord <> :excluded");
                    query["ExpressionAttributeValues"][":excluded"] = json!({"S": format!("{excluded:03}")});
                }
                // The identical warm operation must preserve items, cursor and the zero capacity.
                let expected = serde_json::to_value(store.operations.query(request::<QueryRequest>(query.clone())).await.unwrap()).unwrap();
                census.reset();
                let response = serde_json::to_value(store.operations.query(request::<QueryRequest>(query)).await.unwrap()).unwrap();
                assert_eq!(response, expected);
                let counts = census.snapshot();
                assert_eq!(counts.begins, 1, "one native attempt: {counts}");
                assert_eq!(counts.rows_written, 0);
                assert_eq!(response["ConsumedCapacity"]["CapacityUnits"].as_f64(), Some(0.0));
                let items = response["Items"].as_array().unwrap();
                if projected {
                    assert!(items.iter().all(|item| item.get("payload").is_none()));
                } else {
                    let payload_bytes = if table == "census_page" { PAGE_PAYLOAD_BYTES } else { 110_000 };
                    assert!(items.iter().all(|item| {
                        let payload = item["payload"]["S"].as_str().unwrap();
                        payload.len() == payload_bytes && payload.bytes().all(|byte| byte == b'x')
                    }));
                }
                let chunk_keys = census.chunk_key_reads();
                let (returned_rows, returned_bytes) = census.returned_data();
                if table == "n7_chunk_page" && TestStore::selected_backend() == "foundationdb" {
                    assert!(chunk_keys > 0, "large values must hydrate native chunks");
                }
                keys.extend(items.iter().map(|item| item["sk"]["S"].as_str().unwrap().to_owned()));
                continuation = response.get("LastEvaluatedKey").filter(|key| !key.as_object().unwrap().is_empty()).cloned();
                println!("N7_LARGE_RANGE provider={} table={table} reverse={reverse} projected_filtered={projected} exclusive={exclusive} page={page} response_bytes={} count={} scanned={} capacity={} cursor={} chunk_keys={chunk_keys} returned_rows={returned_rows} returned_bytes={returned_bytes} {counts}", TestStore::selected_backend(), serde_json::to_vec(&response).unwrap().len(), response["Count"], response["ScannedCount"], response["ConsumedCapacity"]["CapacityUnits"], continuation.is_some());
                page += 1;
                assert!(page < 10);
                if continuation.is_none() { break; }
            }
            let mut expected = (0..total).filter(|index| !exclusive || if reverse { *index < cutoff } else { *index > cutoff }).filter(|index| !projected || *index != excluded).map(|index| format!("{index:03}")).collect::<Vec<_>>();
            if reverse { expected.reverse(); }
            assert_eq!(keys, expected, "complete ordered results without gaps or duplicates");
        }
        }
        for (table, limit) in [("census_page", None), ("n7_chunk_page", None), ("census_page", Some(PAGE_ITEMS)), ("n7_chunk_page", Some(PAGE_ITEMS))] {
            let mut page = 0;
            let mut continuation: Option<Value> = None;
            let mut keys = Vec::new();
            loop {
                let mut scan = json!({"TableName": table, "ReturnConsumedCapacity": "TOTAL"});
                if let Some(limit) = limit { scan["Limit"] = json!(limit); }
                if let Some(key) = &continuation { scan["ExclusiveStartKey"] = key.clone(); }
                let expected = serde_json::to_value(store.operations.scan(request::<storage_types::ScanRequest>(scan.clone())).await.unwrap()).unwrap();
                census.reset();
                let response = serde_json::to_value(store.operations.scan(request::<storage_types::ScanRequest>(scan)).await.unwrap()).unwrap();
                assert_eq!(response, expected);
                let counts = census.snapshot();
                assert_eq!(counts.begins, 1);
                assert_eq!(counts.rows_written, 0);
                keys.extend(response["Items"].as_array().unwrap().iter().map(|item| item["sk"]["S"].as_str().unwrap().to_owned()));
                continuation = response.get("LastEvaluatedKey").filter(|key| !key.as_object().unwrap().is_empty()).cloned();
                let (returned_rows, returned_bytes) = census.returned_data();
                let chunk_keys = census.chunk_key_reads();
                println!("N7_SCAN_RANGE provider={} table={table} limit={} page={page} chunk_keys={chunk_keys} returned_rows={returned_rows} returned_bytes={returned_bytes} response_bytes={} count={} scanned={} capacity={} cursor={} {counts}", TestStore::selected_backend(), limit.unwrap_or(100), serde_json::to_vec(&response).unwrap().len(), response["Count"], response["ScannedCount"], response["ConsumedCapacity"]["CapacityUnits"], continuation.is_some());
                page += 1;
                if continuation.is_none() { break; }
            }
            let total = if table == "census_page" { PAGE_ITEMS } else { 12 };
            assert_eq!(keys, (0..total).map(|index| format!("{index:03}")).collect::<Vec<_>>());
        }
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

// Keep one seed and traversal owner. Each future opens its own public
// operation.
async fn measure_large_range_resources(store: &TestStore, census: &NativeCensus) {
    for table in ["census_page", "n7_chunk_page"] {
        let query = request::<QueryRequest>(json!({"TableName": table,
            "KeyConditionExpression": "pk = :pk", "ExpressionAttributeValues": {":pk": {"S": "page"}},
            "Limit": PAGE_ITEMS, "ReturnConsumedCapacity": "TOTAL"}));
        let scan = request::<storage_types::ScanRequest>(json!({"TableName": table,
            "Limit": PAGE_ITEMS, "ReturnConsumedCapacity": "TOTAL"}));
        for concurrency in [1, 8] {
            measure_range_workload(
                census,
                table,
                "query",
                concurrency,
                || store.operations.query(query.clone()),
                |response| serde_json::to_value(response).unwrap(),
            )
            .await;
            measure_range_workload(
                census,
                table,
                "scan",
                concurrency,
                || store.operations.scan(scan.clone()),
                |response| serde_json::to_value(response).unwrap(),
            )
            .await;
        }
    }
}

async fn measure_range_workload<F, Fut, R, A>(
    census: &NativeCensus,
    table: &str,
    operation: &str,
    concurrency: usize,
    read: F,
    audit: A,
) where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<R, storage_types::StorageError>>,
    A: Fn(R) -> Value,
{
    let expected = audit(read().await.unwrap());
    let pid = std::process::id();
    println!(
        "N7_RESOURCE_MARK unix_us={} phase=warm pid={pid} table={table} operation={operation} \
         concurrency={concurrency}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    );
    census.reset();
    let mut latencies = Vec::with_capacity(32);
    println!(
        "N7_RESOURCE_MARK unix_us={} phase=run pid={pid} table={table} operation={operation} \
         concurrency={concurrency}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    );
    let started = std::time::Instant::now();
    for _ in 0..(32 / concurrency) {
        let reads = (0..concurrency).map(|_| {
            let future = read();
            async move {
                let started = std::time::Instant::now();
                let response = future.await.unwrap();
                (started.elapsed().as_micros(), response)
            }
        });
        for (elapsed, response) in futures::future::join_all(reads).await {
            latencies.push(elapsed);
            assert_eq!(audit(response), expected);
        }
    }
    let elapsed = started.elapsed();
    let counts = census.snapshot();
    let (returned_rows, returned_bytes) = census.returned_data();
    let chunk_keys = census.chunk_key_reads();
    println!(
        "N7_RESOURCE_MARK unix_us={} phase=drain pid={pid} table={table} operation={operation} \
         concurrency={concurrency}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    );
    assert_eq!(counts.begins, 32, "one attempt per independent operation");
    assert_eq!(counts.rows_written, 0);
    latencies.sort_unstable();
    println!(
        "N7_RESOURCE provider={} pid={pid} table={table} operation={operation} \
         concurrency={concurrency} completed=32 retained_outputs={concurrency} \
         elapsed_with_audit_us={} min_us={} p50_us={} p95_us={} max_us={} \
         returned_rows={returned_rows} returned_bytes={returned_bytes} chunk_keys={chunk_keys} \
         {counts}",
        TestStore::selected_backend(),
        elapsed.as_micros(),
        latencies[0],
        latencies[15],
        latencies[30],
        latencies[31]
    );
    println!(
        "N7_RESOURCE_MARK unix_us={} phase=audited pid={pid} table={table} operation={operation} \
         concurrency={concurrency}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    );
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_checked_table_cache_when_points_and_tiny_batches_run_then_native_costs_are_recorded()
{
    use futures::FutureExt as _;
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let table = "n7_checked_table_cost";
        store
            .operations
            .create_table(create_request(table, &CONFIGS[0]))
            .await
            .unwrap();
        put_ok(&store, table, "a", "seed").await;
        for operation in ["get", "put", "batch2"] {
            for run in 0..2 {
                census.reset();
                let trace_label = format!(
                    "fixture=checked_table provider={} config=plain cache_ms=0 \
                     operation={operation} run={run}",
                    TestStore::selected_backend()
                );
                println!("N9_ATTEMPT_WINDOW {trace_label}");
                census.start_native_trace();
                match operation {
                    "get" => {
                        let result = store
                            .operations
                            .get_item(request::<GetItemRequest>(json!({
                                "TableName": table, "Key": {"pk": {"S": "a"}}
                            })))
                            .await
                            .unwrap();
                        let response = serde_json::to_value(&result).unwrap();
                        assert_eq!(response, json!({"Item": item("a", "seed")}));
                        if run == 1 {
                            println!(
                                "N7_CHECKED_TABLE_RESPONSE provider={} operation=get \
                                 response={response}",
                                TestStore::selected_backend()
                            );
                        }
                    }
                    "put" => put_ok(&store, table, "a", "updated").await,
                    _ => {
                        let response = store
                            .operations
                            .batch_write_item(request::<BatchWriteItemRequest>(json!({
                                "RequestItems": {table: [
                                    {"PutRequest": {"Item": {"pk": {"S": "a"}}}},
                                    {"PutRequest": {"Item": {"pk": {"S": "b"}}}}
                                ]}
                            })))
                            .await
                            .unwrap();
                        assert!(
                            response
                                .unprocessed_items
                                .as_ref()
                                .is_none_or(|items| items.is_empty())
                        );
                    }
                }
                let counts = census.finish_native_trace(&trace_label);
                if run == 1 {
                    assert_eq!(counts.begins, 1);
                    let expected_commits = u64::from(
                        operation != "get" || TestStore::selected_backend() != "foundationdb",
                    );
                    assert_eq!(counts.commits, expected_commits);
                    println!(
                        "N7_CHECKED_TABLE provider={} operation={operation} {counts} returned={:?}",
                        TestStore::selected_backend(),
                        census.returned_data()
                    );
                }
            }
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// Every measured window contains the public operation, including metadata and
/// settlement. Item audit reads happen only after its counters are captured.
#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_stream_writer_routes_when_public_writes_run_then_complete_costs_and_images_hold() {
    use futures::FutureExt as _;
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let table = "n7_writer_route_cost";
        let mut create = create_request(table, &CONFIGS[0]);
        create.stream_specification = Some(serde_json::from_value(json!({"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"})).unwrap());
        store.operations.create_table(create).await.unwrap();
        for case in ["cold_put", "warm_put", "warm_update", "warm_delete", "absent_delete", "failed_condition", "batch2", "chunk_put", "chunk_update", "unrelated_reload", "unrelated_warm", "split_reload", "split_warm", "cold_absent_delete", "cold_failed_condition"] {
            if case == "unrelated_reload" {
                store.operations.create_table(create_request("n7_writer_route_other", &CONFIGS[0])).await.unwrap();
            }
            if case == "split_reload" {
                use stream_provider::{SystemStreamLineage, SystemStreamSplitChild, SystemStreamSplitOperationId, SystemStreamSplitRequest};
                let scope = store.stream_operations.system_stream_scope().await.unwrap();
                let parent = store.stream_operations.system_stream_lineage_catalog(scope.clone()).await.unwrap().into_iter().find(|lineage| !lineage.sealed()).unwrap();
                let children = ["cost-left", "cost-right"].map(|side| SystemStreamSplitChild::try_new(
                    SystemStreamLineage::try_new(side, format!("writer-{side}"), Some(parent.lineage_id().as_str().to_owned()), false).unwrap(), 1
                ).unwrap());
                store.stream_operations.split_system_stream(SystemStreamSplitRequest::try_new(scope, parent, children, SystemStreamSplitOperationId::try_new("writer-cost-split").unwrap()).unwrap()).await.unwrap();
            }
            if matches!(case, "cold_absent_delete" | "cold_failed_condition") {
                store.operations.create_table(create_request(case, &CONFIGS[0])).await.unwrap();
            }
            census.reset();
            let response = match case {
                "warm_update" | "chunk_update" => {
                    let value = if case == "chunk_update" { "y".repeat(110_000) } else { "updated".to_owned() };
                    serde_json::to_value(store.operations.update_item(request::<UpdateItemRequest>(json!({
                        "TableName": table, "Key": {"pk": {"S": "a"}}, "UpdateExpression": "SET payload = :v",
                        "ExpressionAttributeValues": {":v": {"S": value}}, "ReturnValues": "ALL_NEW"
                    }))).await.unwrap()).unwrap()
                }
                "warm_delete" | "absent_delete" | "cold_absent_delete" => serde_json::to_value(store.operations.delete_item(request::<DeleteItemRequest>(json!({
                    "TableName": table, "Key": {"pk": {"S": if case == "cold_absent_delete" { "absent" } else { "a" }}}, "ReturnValues": "ALL_OLD"
                }))).await.unwrap()).unwrap(),
                "failed_condition" | "cold_failed_condition" => {
                    let result = store.operations.put_item(request::<PutItemRequest>(json!({
                        "TableName": table, "Item": {"pk": {"S": "absent"}}, "ConditionExpression": "attribute_exists(pk)"
                    }))).await;
                    assert!(result.is_err());
                    json!({"rejected": format!("{}", result.unwrap_err())})
                }
                "batch2" => {
                    let result = store.operations.batch_write_item(request::<BatchWriteItemRequest>(json!({"RequestItems": {table: [
                        {"PutRequest": {"Item": {"pk": {"S": "a"}, "payload": {"S": "batch"}}}},
                        {"PutRequest": {"Item": {"pk": {"S": "b"}, "payload": {"S": "batch"}}}}
                    ]}}))).await.unwrap();
                    assert!(result.unprocessed_items.as_ref().is_none_or(|items| items.values().all(Vec::is_empty)));
                    serde_json::to_value(result).unwrap()
                }
                _ => {
                    let value = if case == "chunk_put" { "x".repeat(110_000) } else { case.to_owned() };
                    serde_json::to_value(store.operations.put_item(request::<PutItemRequest>(json!({
                        "TableName": table, "Item": {"pk": {"S": "a"}, "payload": {"S": value}}, "ReturnValues": "ALL_OLD"
                    }))).await.unwrap()).unwrap()
                }
            };
            let counts = census.snapshot();
            let returned = census.returned_data();
            let chunks = census.chunk_key_reads();
            assert_eq!(counts.begins, if case == "cold_put" { 2 } else { 1 }, "{case}: {counts}");
            if matches!(case, "absent_delete" | "failed_condition" | "cold_absent_delete" | "cold_failed_condition") { assert_eq!(counts.rows_written, 0); }
            if case == "warm_update" { assert_eq!(response["Attributes"]["payload"]["S"], "updated"); }
            if case == "warm_delete" { assert_eq!(response["Attributes"]["payload"]["S"], "updated"); }
            if case == "absent_delete" { assert!(response.get("Attributes").is_none()); }
            if case == "chunk_update" { assert_eq!(response["Attributes"]["payload"]["S"], "y".repeat(110_000)); }
            // Full public response bytes are retained, including large returned
            // images; no audit I/O belongs to this measured window.
            println!("N7_WRITER_ROUTE provider={} case={case} {counts} returned={returned:?} chunk_keys={chunks} response={response}", TestStore::selected_backend());
            let audit = store.operations.get_item(request::<GetItemRequest>(json!({"TableName": table, "Key": {"pk": {"S": "a"}}}))).await.unwrap();
            if matches!(case, "warm_delete" | "absent_delete" | "failed_condition") {
                assert!(audit.item.is_none());
            } else {
                assert!(audit.item.is_some());
            }
        }
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_warm_multi_reads_when_complete_requests_run_then_order_outputs_and_native_costs_hold()
 {
    use futures::FutureExt as _;
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        for table in ["multi_left", "multi_right"] {
            store.create_hash_table(storage_types::TableName::new(table)).await;
            put_ok(&store, table, "a", table).await;
        }
        for count in [1, 3, 100] {
            for operation in ["batch", "transact"] {
                let mut previous = None;
                for run in 0..2 {
                    census.reset();
                    let response = if operation == "batch" {
                        let mut tables = serde_json::Map::new();
                        for slot in 0..count {
                            let table = if slot % 2 == 0 { "multi_right" } else { "multi_left" };
                            let key = if slot < 2 { "a".to_owned() } else { format!("missing-{slot}") };
                            tables.entry(table).or_insert_with(|| json!({"Keys": [], "ConsistentRead": true}))["Keys"].as_array_mut().unwrap().push(json!({"pk": {"S": key}}));
                        }
                        serde_json::to_value(store.operations.batch_get_item(request::<storage_types::BatchGetItemRequest>(json!({"RequestItems": tables}))).await.unwrap()).unwrap()
                    } else {
                        let members = (0..count).map(|slot| {
                            let table = if slot % 2 == 0 { "multi_right" } else { "multi_left" };
                            let key = if slot < 2 { "a".to_owned() } else { format!("missing-{slot}") };
                            json!({"Get": {"TableName": table, "Key": {"pk": {"S": key}}}})
                        }).collect::<Vec<_>>();
                        let response = serde_json::to_value(store.operations.transact_get_items(request::<storage_types::TransactGetItemsRequest>(json!({"TransactItems": members, "ReturnConsumedCapacity": "TOTAL"}))).await.unwrap()).unwrap();
                        assert_eq!(response["Responses"].as_array().unwrap().len(), count);
                        assert_eq!(response["Responses"][0]["Item"], item("a", "multi_right"));
                        if count > 1 { assert_eq!(response["Responses"][1]["Item"], item("a", "multi_left")); }
                        if count > 2 { assert_eq!(response["Responses"][2], json!({})); }
                        response
                    };
                    if let Some(previous) = previous { assert_eq!(response, previous); }
                    previous = Some(response.clone());
                    if run == 1 {
                        let counts = census.snapshot();
                        if TestStore::selected_backend() == "postgres" {
                            assert_eq!(counts.waves, 1);
                            assert_eq!(counts.statements, if count == 1 { 1 } else { count as u64 + 1 });
                            assert_eq!(counts.rows_read, if count == 1 { 1 } else { 4 });
                            assert_eq!(counts.begins, 1);
                            assert_eq!(counts.commits, 1);
                            assert_eq!(counts.rollbacks, 0);
                        }
                        if TestStore::selected_backend() == "foundationdb" {
                            // PERF-FDB-1: read version, then one wave with every item and the catalog version.
                            assert_eq!(counts.waves, 2, "{operation} {count}: {counts}");
                            assert_eq!(counts.keys, count as u64 + 1, "{operation} {count}: {counts}");
                            assert_eq!(counts.begins, 1);
                        }
                        println!("N7_MULTI provider={} operation={operation} count={count} {counts} response={response}", TestStore::selected_backend());
                    }
                }
            }
        }
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_one_cold_table_when_multi_read_runs_then_full_resolution_precedes_one_ordinary_item_batch()
 {
    use futures::FutureExt as _;
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        store
            .create_hash_table(storage_types::TableName::new("mixed_warm"))
            .await;
        put_ok(&store, "mixed_warm", "a", "seed").await;
        store
            .operations
            .get_item(request::<GetItemRequest>(
                json!({"TableName": "mixed_warm", "Key": {"pk": {"S": "a"}}}),
            ))
            .await
            .unwrap();
        store
            .independent_operations
            .as_ref()
            .unwrap()
            .create_table(create_request("mixed_cold", &CONFIGS[0]))
            .await
            .unwrap();
        let mut previous = None;
        for run in 0..2 {
            census.reset();
            let response = serde_json::to_value(
                store
                    .operations
                    .batch_get_item(request::<storage_types::BatchGetItemRequest>(json!({
                        "RequestItems": {
                            "mixed_warm": {"Keys": [{"pk": {"S": "a"}}]},
                            "mixed_cold": {"Keys": [{"pk": {"S": "missing"}}]}
                        }
                    })))
                    .await
                    .unwrap(),
            )
            .unwrap();
            let counts = census.snapshot();
            if TestStore::selected_backend() == "postgres" {
                assert_eq!(counts.statements, if run == 0 { 4 } else { 3 });
                assert_eq!(counts.waves, if run == 0 { 4 } else { 1 });
                assert_eq!(counts.lone_begins, 0);
            }
            if let Some(previous) = previous {
                assert_eq!(response, previous);
            }
            previous = Some(response.clone());
            println!(
                "N7_MULTI_MIXED provider={} run={run} {counts} response={response}",
                TestStore::selected_backend()
            );
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_stream_reads_when_censused_then_postgres_begin_rides_with_first_statement() {
    use super::dynamodb_stream_history_tests::{
        create_stream_table, find_populated_shard, iterator, put_value,
    };
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let (table, arn) = create_stream_table(&store, "NEW_IMAGE").await;
    put_value(&store, &table, "a", "v").await;
    let shard = find_populated_shard(&store, &arn).await;
    let read = |token: String| storage_types::GetRecordsRequest {
        shard_iterator: token,
        limit: Some(10),
    };
    // Warm statement caches and the stream catalog.
    let token = iterator(&store, &arn, &shard, "TRIM_HORIZON", None).await;
    store.operations.get_records(read(token)).await.unwrap();
    census.reset();
    let token = iterator(&store, &arn, &shard, "TRIM_HORIZON", None).await;
    let shard_iterator = census.snapshot();
    census.reset();
    let records =
        serde_json::to_value(store.operations.get_records(read(token)).await.unwrap()).unwrap();
    let get_records = census.snapshot();
    println!(
        "PG_BEGIN_STREAM provider={} iterator {shard_iterator} records {get_records}",
        TestStore::selected_backend()
    );
    assert!(!records["Records"].as_array().unwrap().is_empty());
    if TestStore::selected_backend() == "postgres" {
        for counts in [&shard_iterator, &get_records] {
            assert!(counts.begins >= 1, "{counts}");
            assert_eq!(counts.rollbacks, 0, "{counts}");
            assert_eq!(counts.lone_begins, 0, "{counts}");
        }
    }
    store.finish().await;
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_warm_point_write_observations_when_public_mutations_run_then_costs_and_complete_images_hold()
 {
    use futures::FutureExt as _;
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        for config in &CONFIGS {
            let table = format!("n9_point_{}", config.name);
            store.operations.create_table(create_request(&table, config)).await.unwrap();
            put_ok(&store, &table, "a", "seed").await;
            for operation in ["put", "update", "delete"] {
                let mut measured = None;
                for run in 0..2 {
                    put_ok(&store, &table, "a", "before").await;
                    let before = serde_json::to_value(store.operations.get_item(request::<GetItemRequest>(json!({"TableName": table, "Key": {"pk": {"S": "a"}}}))).await.unwrap()).unwrap();
                    assert_eq!(before, json!({"Item": item("a", "before")}));
                    census.reset();
                    let trace_label = format!("fixture=point_write provider={} config={} cache_ms=0 operation={operation} run={run}", TestStore::selected_backend(), config.name);
                    println!("N9_ATTEMPT_WINDOW {trace_label}");
                    census.start_native_trace();
                    let response = match operation {
                        "put" => serde_json::to_value(store.operations.put_item(request::<PutItemRequest>(json!({"TableName": table, "Item": item("a", "after"), "ReturnValues": "ALL_OLD"}))).await.unwrap()).unwrap(),
                        "update" => serde_json::to_value(store.operations.update_item(request::<UpdateItemRequest>(json!({"TableName": table, "Key": {"pk": {"S": "a"}}, "UpdateExpression": "SET v = :v, g = :g", "ExpressionAttributeValues": {":v": {"S": "after"}, ":g": {"S": "g_after"}}, "ReturnValues": "ALL_NEW"}))).await.unwrap()).unwrap(),
                        _ => serde_json::to_value(store.operations.delete_item(request::<DeleteItemRequest>(json!({"TableName": table, "Key": {"pk": {"S": "a"}}, "ReturnValues": "ALL_OLD"}))).await.unwrap()).unwrap(),
                    };
                    let counts = census.finish_native_trace(&trace_label);
                    let after = serde_json::to_value(store.operations.get_item(request::<GetItemRequest>(json!({"TableName": table, "Key": {"pk": {"S": "a"}}}))).await.unwrap()).unwrap();
                    assert_eq!(response, json!({"Attributes": item("a", if operation == "update" { "after" } else { "before" })}));
                    assert_eq!(after, if operation == "delete" { json!({}) } else { json!({"Item": item("a", "after")}) });
                    if run == 1 {
                        assert_eq!(counts.commits, 1);
                        if TestStore::selected_backend() == "postgres" {
                            assert_eq!(counts.begins, 1);
                            assert_eq!(counts.waves, 2, "{operation}: {counts}");
                        }
                        measured = Some(format!("provider={} config={} operation={operation} {counts} cold_prepares={} before={before} response={response} after={after}", TestStore::selected_backend(), config.name, counts.cold_prepares));
                    }
                }
                println!("N9_POINT_WRITE {}", measured.unwrap());
            }
        }
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_warm_bulk_write_observations_when_public_mutations_run_then_two_waves_and_complete_data_hold()
 {
    use futures::FutureExt as _;
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        for config in &CONFIGS {
            let table = format!("n9_bulk_{}", config.name);
            store.operations.create_table(create_request(&table, config)).await.unwrap();
            for operation in ["batch", "transact"] {
                for run in 0..2 {
                    for key in ["a", "b", "c"] { put_ok(&store, &table, key, "before").await; }
                    let token = uuid::Uuid::new_v4().to_string();
                    let transaction = json!({"ClientRequestToken": token, "TransactItems": [
                        {"Put": {"TableName": table, "Item": item("a", "after")}},
                        {"Delete": {"TableName": table, "Key": {"pk": {"S": "b"}}}},
                        {"ConditionCheck": {"TableName": table, "Key": {"pk": {"S": "c"}}, "ConditionExpression": "attribute_exists(pk)"}}
                    ]});
                    census.reset();
                    let response = if operation == "batch" {
                        serde_json::to_value(store.operations.batch_write_item(request::<BatchWriteItemRequest>(json!({"RequestItems": {table.as_str(): [
                            {"PutRequest": {"Item": item("a", "after")}},
                            {"DeleteRequest": {"Key": {"pk": {"S": "b"}}}}
                        ]}}))).await.unwrap()).unwrap()
                    } else {
                        serde_json::to_value(store.operations.transact_write_items(request::<TransactWriteItemsRequest>(transaction.clone())).await.unwrap()).unwrap()
                    };
                    let counts = census.snapshot();
                    assert_eq!(response, if operation == "batch" { json!({"UnprocessedItems": {}}) } else { json!({}) });
                    for key in ["a", "b", "c"] {
                        let actual = serde_json::to_value(store.operations.get_item(request::<GetItemRequest>(json!({"TableName": table, "Key": {"pk": {"S": key}}}))).await.unwrap()).unwrap();
                        let expected = match key { "a" => json!({"Item": item("a", "after")}), "b" => json!({}), _ => json!({"Item": item("c", "before")}) };
                        assert_eq!(actual, expected);
                    }
                    if operation == "transact" {
                        let replay = serde_json::to_value(store.operations.transact_write_items(request::<TransactWriteItemsRequest>(transaction)).await.unwrap()).unwrap();
                        assert_eq!(replay, response);
                    }
                    if run == 1 {
                        assert_eq!(counts.commits, 1);
                        if TestStore::selected_backend() == "postgres" {
                            assert_eq!(counts.begins, 1);
                            assert_eq!(counts.cold_prepares, 0);
                            assert_eq!(counts.waves, 2, "{operation}: {counts}");
                        }
                        println!("N9_BULK_WRITE provider={} config={} operation={operation} {counts} cold_prepares={} response={response}", TestStore::selected_backend(), config.name, counts.cold_prepares);
                    }
                }
            }
        }
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_near_limit_escaped_and_binary_items_when_query_and_scan_page_then_complete_logical_responses_share_one_attempt()
 {
    use base64::Engine as _;
    use futures::FutureExt as _;
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        for binary in [false, true] {
            let table = storage_types::TableName::new(if binary { "range_binary" } else { "range_escaped" });
            store.create_hash_range_table(table.clone(), storage_types::KeyAttributeType::S).await;
            let payload = if binary { json!({"B": base64::engine::general_purpose::STANDARD.encode(vec![0_u8; 400_000])}) } else { json!({"S": "\"".repeat(400_000)}) };
            let images = (0..6).map(|index| json!({"pk": {"S": "page"}, "sk": {"S": format!("{index:03}")}, "payload": payload})).collect::<Vec<_>>();
            for item in &images {
                store.operations.put_item(request::<PutItemRequest>(json!({"TableName": table, "Item": item}))).await.unwrap();
            }
            for scan in [false, true] {
                let mut continuation: Option<Value> = None;
                for page in 0..3 {
                    let mut input = json!({"TableName": table, "ReturnConsumedCapacity": "TOTAL"});
                    if let Some(key) = continuation { input["ExclusiveStartKey"] = key; }
                    if !scan {
                        input["KeyConditionExpression"] = json!("pk = :pk");
                        input["ExpressionAttributeValues"] = json!({":pk": {"S": "page"}});
                    }
                    census.reset();
                    let response = if scan { serde_json::to_value(store.operations.scan(request::<storage_types::ScanRequest>(input)).await.unwrap()).unwrap() }
                        else { serde_json::to_value(store.operations.query(request::<QueryRequest>(input)).await.unwrap()).unwrap() };
                    let counts = census.snapshot();
                    assert_eq!(counts.begins, 1, "one snapshot despite physical pages: {counts}");
                    assert_eq!(counts.rows_written, 0);
                    let mut expected = json!({"Items": images[page * 2..page * 2 + 2], "Count": 2, "ScannedCount": 2,
                        "ConsumedCapacity": {"TableName": table, "CapacityUnits": 0.0}});
                    if page < 2 { expected["LastEvaluatedKey"] = json!({"pk": {"S": "page"}, "sk": {"S": format!("{:03}", page * 2 + 1)}}); }
                    assert!(response == expected, "binary={binary} scan={scan} page={page}: complete image/cursor/capacity mismatch");
                    continuation = response.get("LastEvaluatedKey").cloned();
                    println!("N7_NEAR_LIMIT provider={} binary={binary} scan={scan} page={page} response_bytes={} {counts}", TestStore::selected_backend(), serde_json::to_vec(&response).unwrap().len());
                }
                assert!(continuation.is_none());
            }
        }
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
