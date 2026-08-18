use std::time::Instant;

use alloc_counter::AllocationGuard;
use rusqlite::{Connection, params_from_iter, types::Value};

const ROWS: usize = 1_000;

#[test]
#[ignore = "performance evidence; run explicitly with --ignored --nocapture"]
fn given_realistic_rows_when_measuring_sql_indexer_columns_then_emit_comparable_evidence() {
    for indexer_count in [0_usize, 2, 4, 16, 32] {
        measure_shape(indexer_count);
    }
}

fn measure_shape(indexer_count: usize) {
    let temp_dir = crate::sql_test_support::temp_dir("indexer-sql-performance");
    let db_path = temp_dir
        .path()
        .join(format!("indexers-{indexer_count}.sqlite"));
    let mut connection = Connection::open(db_path).expect("open benchmark database");
    connection
        .execute_batch(&format!(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; CREATE TABLE items (pk TEXT NOT \
             NULL, sk TEXT NOT NULL, attributes_blob TEXT NOT NULL{} , PRIMARY KEY (pk, sk));",
            indexer_columns(indexer_count)
        ))
        .expect("create benchmark table");

    let insert_sql = insert_sql(indexer_count);
    let prepare_started = Instant::now();
    let transaction = connection.transaction().expect("begin insert transaction");
    let mut statement = transaction.prepare(&insert_sql).expect("prepare insert");
    let prepare_ns = prepare_started.elapsed().as_nanos();
    let insert_allocations = AllocationGuard::start(
        module_path!(),
        "sql_indexer_insert_performance",
        file!(),
        line!(),
        None,
    );
    let insert_started = Instant::now();
    for row in 0..ROWS {
        let values = row_values(row, indexer_count);
        statement
            .execute(params_from_iter(values.iter()))
            .expect("insert benchmark row");
    }
    let insert_elapsed = insert_started.elapsed();
    let insert_allocations = insert_allocations.finish();
    drop(statement);
    transaction.commit().expect("commit benchmark rows");

    let select_sql = select_sql(indexer_count);
    let mut select = connection.prepare(&select_sql).expect("prepare select");
    let select_allocations = AllocationGuard::start(
        module_path!(),
        "sql_indexer_select_performance",
        file!(),
        line!(),
        None,
    );
    let select_started = Instant::now();
    for row in 0..ROWS {
        select
            .query_row(
                [format!("entity#{row:084}"), format!("model#{row:010}")],
                |result| {
                    let residual: String = result.get(0)?;
                    std::hint::black_box(residual);
                    for column in 0..indexer_count {
                        let slot: Option<String> = result.get(column + 1)?;
                        std::hint::black_box(slot);
                    }
                    Ok(())
                },
            )
            .expect("select benchmark row");
    }
    let select_elapsed = select_started.elapsed();
    let select_allocations = select_allocations.finish();
    drop(select);

    let physical_bytes: i64 = connection
        .query_row(&physical_bytes_sql(indexer_count), [], |row| row.get(0))
        .expect("measure physical row bytes");
    let query_plan = connection
        .prepare("EXPLAIN QUERY PLAN SELECT attributes_blob FROM items WHERE pk = ?1 AND sk = ?2")
        .expect("prepare query plan")
        .query_map(["", ""], |row| row.get::<_, String>(3))
        .expect("query plan")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect query plan")
        .join(" | ");

    println!(
        "{}",
        serde_json::json!({
            "provider": "sqlite",
            "columns": indexer_count,
            "rows": ROWS,
            "insert_statement_bytes": insert_sql.len(),
            "select_statement_bytes": select_sql.len(),
            "prepare_ns": prepare_ns,
            "physical_bytes_per_row": physical_bytes / ROWS as i64,
            "insert_ns_per_row": insert_elapsed.as_nanos() / ROWS as u128,
            "select_ns_per_row": select_elapsed.as_nanos() / ROWS as u128,
            "insert_allocations_per_row": insert_allocations.allocation_count as f64 / ROWS as f64,
            "insert_allocated_bytes_per_row": insert_allocations.allocated_bytes as f64 / ROWS as f64,
            "select_allocations_per_row": select_allocations.allocation_count as f64 / ROWS as f64,
            "select_allocated_bytes_per_row": select_allocations.allocated_bytes as f64 / ROWS as f64,
            "query_plan": query_plan,
        })
    );
}

fn indexer_columns(count: usize) -> String {
    (0..count)
        .map(|ordinal| format!(", __aux_indexer_{ordinal} TEXT NULL"))
        .collect()
}

fn insert_sql(count: usize) -> String {
    let columns = (0..count)
        .map(|ordinal| format!(", __aux_indexer_{ordinal}"))
        .collect::<String>();
    let placeholders = (0..count)
        .map(|ordinal| format!(", ?{}", ordinal + 4))
        .collect::<String>();
    format!(
        "INSERT INTO items (pk, sk, attributes_blob{columns}) VALUES (?1, ?2, ?3{placeholders})"
    )
}

fn select_sql(count: usize) -> String {
    let columns = (0..count)
        .map(|ordinal| format!(", __aux_indexer_{ordinal}"))
        .collect::<String>();
    format!("SELECT attributes_blob{columns} FROM items WHERE pk = ?1 AND sk = ?2")
}

fn physical_bytes_sql(count: usize) -> String {
    let slots = (0..count)
        .map(|ordinal| format!(" + COALESCE(length(__aux_indexer_{ordinal}), 0)"))
        .collect::<String>();
    format!("SELECT SUM(length(pk) + length(sk) + length(attributes_blob){slots}) FROM items")
}

fn row_values(row: usize, count: usize) -> Vec<Value> {
    let residual = format!(
        "{{\"M\":{{\"payload\":{{\"S\":\"{}\"}},\"n\":{{\"N\":\"42\"}},\"ttl\":{{\"N\":\"\
         2000000000\"}}}}}}",
        "repeatable-payload".repeat(55)
    );
    let mut values = Vec::with_capacity(3 + count);
    values.push(Value::Text(format!("entity#{row:084}")));
    values.push(Value::Text(format!("model#{row:010}")));
    values.push(Value::Text(residual));
    for ordinal in 0..count {
        values.push(if ordinal % 2 == 0 {
            Value::Text(format!("value#{ordinal:02}"))
        } else {
            Value::Null
        });
    }
    values
}

#[cfg(feature = "turso-backend")]
#[test]
#[ignore = "performance evidence; run explicitly with --ignored --nocapture"]
fn given_realistic_rows_when_measuring_turso_indexer_columns_then_emit_comparable_evidence() {
    std::thread::Builder::new()
        .name("turso-indexer-performance".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build Turso benchmark runtime")
                .block_on(measure_turso_indexer_columns());
        })
        .expect("spawn Turso benchmark thread")
        .join()
        .expect("run Turso benchmark thread");
}

#[cfg(feature = "turso-backend")]
async fn measure_turso_indexer_columns() {
    let temp_dir = crate::sql_test_support::temp_dir("indexer-turso-performance");
    let database_path = temp_dir.path().join("indexers.turso");
    let database = turso::Builder::new_local(
        database_path
            .to_str()
            .expect("Turso benchmark path is UTF-8"),
    )
    .build()
    .await
    .expect("open Turso benchmark database");
    let connection = database
        .connect()
        .expect("connect Turso benchmark database");
    for indexer_count in [0_usize, 2, 4, 16, 32] {
        let table = format!("items_{indexer_count}");
        connection
            .execute_batch(format!(
                "CREATE TABLE {table} (pk TEXT NOT NULL, sk TEXT NOT NULL, attributes_blob TEXT \
                 NOT NULL{}, PRIMARY KEY (pk, sk));",
                indexer_columns(indexer_count)
            ))
            .await
            .expect("create Turso benchmark table");
        let insert_sql = insert_sql(indexer_count).replace("items", &table);
        let prepare_started = Instant::now();
        let mut insert = connection
            .prepare(&insert_sql)
            .await
            .expect("prepare Turso insert");
        let prepare_ns = prepare_started.elapsed().as_nanos();
        connection
            .execute("BEGIN", ())
            .await
            .expect("begin Turso insert");
        let insert_started = Instant::now();
        for row in 0..ROWS {
            insert
                .execute(turso_row_values(row, indexer_count))
                .await
                .expect("insert Turso benchmark row");
        }
        let insert_elapsed = insert_started.elapsed();
        connection
            .execute("COMMIT", ())
            .await
            .expect("commit Turso rows");

        let select_sql = select_sql(indexer_count).replace("items", &table);
        let mut select = connection
            .prepare(&select_sql)
            .await
            .expect("prepare Turso select");
        let select_started = Instant::now();
        for row in 0..ROWS {
            let mut rows = select
                .query([
                    turso::Value::Text(format!("entity#{row:084}")),
                    turso::Value::Text(format!("model#{row:010}")),
                ])
                .await
                .expect("query Turso benchmark row");
            let result = rows
                .next()
                .await
                .expect("read Turso benchmark row")
                .expect("Turso benchmark row exists");
            std::hint::black_box(result.get::<String>(0).expect("Turso residual"));
            for column in 0..indexer_count {
                std::hint::black_box(
                    result
                        .get::<Option<String>>(column + 1)
                        .expect("Turso slot"),
                );
            }
        }
        let select_elapsed = select_started.elapsed();
        let physical_sql = physical_bytes_sql(indexer_count).replace("items", &table);
        let mut physical_rows = connection
            .query(&physical_sql, ())
            .await
            .expect("query Turso physical bytes");
        let physical_bytes = physical_rows
            .next()
            .await
            .expect("read Turso physical bytes")
            .expect("Turso physical bytes row")
            .get::<i64>(0)
            .expect("Turso physical bytes value");
        let mut plan_rows = connection
            .query(
                &format!(
                    "EXPLAIN QUERY PLAN SELECT attributes_blob FROM {table} WHERE pk = ?1 AND sk \
                     = ?2"
                ),
                [
                    turso::Value::Text(String::new()),
                    turso::Value::Text(String::new()),
                ],
            )
            .await
            .expect("query Turso plan");
        let mut query_plan = Vec::new();
        while let Some(row) = plan_rows.next().await.expect("read Turso plan") {
            query_plan.push(row.get::<String>(3).expect("Turso plan detail"));
        }
        println!(
            "{}",
            serde_json::json!({
                "provider": "turso",
                "columns": indexer_count,
                "rows": ROWS,
                "insert_statement_bytes": insert_sql.len(),
                "select_statement_bytes": select_sql.len(),
                "prepare_ns": prepare_ns,
                "physical_bytes_per_row": physical_bytes / ROWS as i64,
                "insert_ns_per_row": insert_elapsed.as_nanos() / ROWS as u128,
                "select_ns_per_row": select_elapsed.as_nanos() / ROWS as u128,
                "query_plan": query_plan.join(" | "),
            })
        );
    }
}

#[cfg(feature = "turso-backend")]
fn turso_row_values(row: usize, count: usize) -> Vec<turso::Value> {
    row_values(row, count)
        .into_iter()
        .map(|value| match value {
            Value::Null => turso::Value::Null,
            Value::Text(value) => turso::Value::Text(value),
            _ => unreachable!("benchmark values are text or null"),
        })
        .collect()
}

#[cfg(feature = "postgres-backend")]
#[tokio::test]
#[ignore = "performance evidence; requires TEST_POSTGRES_DSN"]
async fn given_realistic_rows_when_measuring_postgres_indexer_columns_then_emit_comparable_evidence()
 {
    use tokio_postgres::types::ToSql;

    let dsn = std::env::var("TEST_POSTGRES_DSN").expect("TEST_POSTGRES_DSN");
    let (mut client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connect PostgreSQL benchmark database");
    tokio::spawn(async move {
        connection
            .await
            .expect("drive PostgreSQL benchmark connection");
    });
    let suffix = uuid::Uuid::now_v7().simple();
    for indexer_count in [0_usize, 2, 4, 16, 32] {
        let table = format!("indexer_perf_{suffix}_{indexer_count}");
        client
            .batch_execute(&format!(
                "CREATE TABLE {table} (pk TEXT NOT NULL, sk TEXT NOT NULL, attributes_blob TEXT \
                 NOT NULL{}, PRIMARY KEY (pk, sk));",
                indexer_columns(indexer_count)
            ))
            .await
            .expect("create PostgreSQL benchmark table");
        let insert_sql = postgres_insert_sql(&table, indexer_count);
        let prepare_started = Instant::now();
        let transaction = client.transaction().await.expect("begin PostgreSQL insert");
        let insert = transaction
            .prepare(&insert_sql)
            .await
            .expect("prepare PostgreSQL insert");
        let prepare_ns = prepare_started.elapsed().as_nanos();
        let insert_started = Instant::now();
        for row in 0..ROWS {
            let values = postgres_row_values(row, indexer_count);
            let params = values
                .iter()
                .map(|value| value.as_ref() as &(dyn ToSql + Sync))
                .collect::<Vec<_>>();
            transaction
                .execute(&insert, &params)
                .await
                .expect("insert PostgreSQL benchmark row");
        }
        let insert_elapsed = insert_started.elapsed();
        transaction.commit().await.expect("commit PostgreSQL rows");

        let select_sql = select_sql(indexer_count)
            .replace("items", &table)
            .replace("?1", "$1")
            .replace("?2", "$2");
        let select = client
            .prepare(&select_sql)
            .await
            .expect("prepare PostgreSQL select");
        let select_started = Instant::now();
        for row in 0..ROWS {
            let pk = format!("entity#{row:084}");
            let sk = format!("model#{row:010}");
            let result = client
                .query_one(&select, &[&pk, &sk])
                .await
                .expect("select PostgreSQL benchmark row");
            std::hint::black_box(result.get::<_, String>(0));
            for column in 0..indexer_count {
                std::hint::black_box(result.get::<_, Option<String>>(column + 1));
            }
        }
        let select_elapsed = select_started.elapsed();
        let physical_sql = physical_bytes_sql(indexer_count)
            .replace("length(", "octet_length(")
            .replace("items", &table);
        let physical_bytes = client
            .query_one(&physical_sql, &[])
            .await
            .expect("query PostgreSQL physical bytes")
            .get::<_, i64>(0);
        let query_plan = client
            .query(
                &format!(
                    "EXPLAIN (COSTS OFF) SELECT attributes_blob FROM {table} WHERE pk = $1 AND sk \
                     = $2"
                ),
                &[&String::new(), &String::new()],
            )
            .await
            .expect("query PostgreSQL plan")
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join(" | ");
        println!(
            "{}",
            serde_json::json!({
                "provider": "postgres",
                "columns": indexer_count,
                "rows": ROWS,
                "insert_statement_bytes": insert_sql.len(),
                "select_statement_bytes": select_sql.len(),
                "prepare_ns": prepare_ns,
                "physical_bytes_per_row": physical_bytes / ROWS as i64,
                "insert_ns_per_row": insert_elapsed.as_nanos() / ROWS as u128,
                "select_ns_per_row": select_elapsed.as_nanos() / ROWS as u128,
                "query_plan": query_plan,
            })
        );
        client
            .batch_execute(&format!("DROP TABLE {table}"))
            .await
            .expect("drop PostgreSQL benchmark table");
    }
}

#[cfg(feature = "postgres-backend")]
fn postgres_insert_sql(table: &str, count: usize) -> String {
    let columns = (0..count)
        .map(|ordinal| format!(", __aux_indexer_{ordinal}"))
        .collect::<String>();
    let placeholders = (0..count)
        .map(|ordinal| format!(", ${}", ordinal + 4))
        .collect::<String>();
    format!(
        "INSERT INTO {table} (pk, sk, attributes_blob{columns}) VALUES ($1, $2, $3{placeholders})"
    )
}

#[cfg(feature = "postgres-backend")]
fn postgres_row_values(
    row: usize,
    count: usize,
) -> Vec<Box<dyn tokio_postgres::types::ToSql + Sync>> {
    row_values(row, count)
        .into_iter()
        .map(|value| match value {
            Value::Null => Box::new(None::<String>) as Box<dyn tokio_postgres::types::ToSql + Sync>,
            Value::Text(value) => Box::new(Some(value)),
            _ => unreachable!("benchmark values are text or null"),
        })
        .collect()
}
