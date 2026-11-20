use std::{sync::Arc, time::Duration};

use storage_provider::DynamoTransactWriteAttempt;
use storage_types::{AttemptMode, AttemptPlan, DeadlineClass};

use crate::{SqlAttempt, SqlBackend, SqlEngine, SqlStatement, catalog::SqlCatalogAttempt};

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_held_token_when_receipt_changes_then_post_wait_read_and_prior_xid_are_preserved() {
    let fixture = crate::postgres::test_support::NativeFixture::open().await;
    fixture
        .admin
        .batch_execute(&format!(
            "SET search_path TO {}; CREATE TABLE sys_system_stream_scope (slot bigint NOT NULL); \
             CREATE TABLE sys_transact_receipts (token text PRIMARY KEY, receipt_blob bytea NOT \
             NULL)",
            fixture.schema,
        ))
        .await
        .unwrap();
    for prior_write in [false, true] {
        fixture.admin.batch_execute("BEGIN").await.unwrap();
        let (sql, binds) = crate::item_lock::token_lock_statement("saved-xid").into_parts();
        let [crate::SqlValue::Text(identities)] = binds.as_slice() else {
            panic!("token identity")
        };
        fixture
            .admin
            .query(sql.as_ref(), &[identities])
            .await
            .unwrap();
        let identity = &"token:saved-xid".to_owned();
        let backend = Arc::clone(&fixture.backend);
        let budgets = backend.attempt_deadlines();
        let writer = tokio::spawn(async move {
            backend
                .with_attempt(
                    AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                        .start(budgets),
                    move |attempt| {
                        Box::pin(async move {
                            if prior_write {
                                attempt
                                    .execute(SqlStatement::new(
                                        "INSERT INTO native_commit_probe VALUES (1)",
                                        vec![],
                                    ))
                                    .await?;
                            } else {
                                attempt
                                    .query(
                                        SqlStatement::new("SELECT 1::bigint", vec![]),
                                        crate::ReadProtection::Snapshot,
                                        storage_provider::CATALOG_READ_BYTES,
                                    )
                                    .await?;
                            }
                            let mut catalog = SqlCatalogAttempt::new(
                                attempt,
                                SqlEngine::Postgres,
                                AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                                    .start(budgets),
                            );
                            let receipt = catalog
                                .read_transact_receipt(storage_provider::ReceiptSlot::Request(
                                    "saved-xid",
                                ))
                                .await?;
                            assert_eq!(
                                catalog.catalog_lock.xid_before_ordering_lock(),
                                prior_write
                            );
                            assert_eq!(receipt, Some(vec![if prior_write { 2 } else { 1 }]));
                            Ok(())
                        })
                    },
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let waiting: i64 = fixture
                    .admin
                    .query_one(
                        "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT \
                         granted AND (classid::bigint::numeric * 4294967296 + objid::bigint) = \
                         ((hashtextextended($1, 0)::numeric + 18446744073709551616) % \
                         18446744073709551616)",
                        &[identity],
                    )
                    .await
                    .unwrap()
                    .get(0);
                if waiting > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("writer reaches held token lock");
        fixture
            .admin
            .execute(
                "INSERT INTO sys_transact_receipts VALUES ('saved-xid', $1) ON CONFLICT (token) \
                 DO UPDATE SET receipt_blob = EXCLUDED.receipt_blob",
                &[&vec![if prior_write { 2u8 } else { 1u8 }]],
            )
            .await
            .unwrap();
        fixture.admin.batch_execute("COMMIT").await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), writer)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_prior_read_or_write_when_item_or_catalog_locks_then_saved_xid_matches_prior_write() {
    let fixture = crate::postgres::test_support::NativeFixture::open().await;
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE TABLE {}.sys_system_stream_scope (slot bigint NOT NULL)",
            fixture.schema,
        ))
        .await
        .unwrap();
    for prior_write in [false, true] {
        for exclusive_catalog in [false, true] {
            let mode = if exclusive_catalog {
                AttemptMode::ReadWrite
            } else {
                AttemptMode::ReadWriteItems
            };
            let budgets = fixture.backend.attempt_deadlines();
            fixture
                .backend
                .with_attempt(
                    AttemptPlan::new(
                        mode,
                        if mode == AttemptMode::ReadOnly {
                            DeadlineClass::PointRead
                        } else {
                            DeadlineClass::Write
                        },
                    )
                    .start(budgets),
                    move |attempt| {
                        Box::pin(async move {
                            if prior_write {
                                attempt
                                    .execute(SqlStatement::new(
                                        "INSERT INTO native_commit_probe VALUES (1)",
                                        vec![],
                                    ))
                                    .await?;
                            } else {
                                attempt
                                    .query(
                                        SqlStatement::new("SELECT 1::bigint", vec![]),
                                        crate::ReadProtection::Snapshot,
                                        storage_provider::CATALOG_READ_BYTES,
                                    )
                                    .await?;
                            }
                            let table = crate::test_support::table();
                            let mut catalog = SqlCatalogAttempt::new(
                                attempt,
                                SqlEngine::Postgres,
                                AttemptPlan::new(
                                    mode,
                                    if mode == AttemptMode::ReadOnly {
                                        DeadlineClass::PointRead
                                    } else {
                                        DeadlineClass::Write
                                    },
                                )
                                .start(budgets),
                            );
                            if exclusive_catalog {
                                catalog
                                    .take_exclusive_catalog_lock(&table.info.table_name)
                                    .await?;
                            } else {
                                catalog
                                    .query_with_catalog_lock(
                                        &[&table.info.table_name],
                                        vec![],
                                        storage_provider::CATALOG_READ_BYTES,
                                    )
                                    .await?;
                                let mut key = storage_types::KeyAttributes::new();
                                key.insert("pk", storage_types::AttributeValue::S("value".into()));
                                let mut window = Vec::new();
                                let prefix = catalog.catalog_lock.item_lock_prefix(
                                    std::iter::once((&table, &key)),
                                    &mut window,
                                )?;
                                assert_eq!(
                                    window.len(),
                                    1,
                                    "the xid check shares the lock statement"
                                );
                                let mut pages = catalog
                                    .attempt
                                    .query_batch_protected(
                                        window,
                                        storage_provider::CATALOG_READ_BYTES,
                                    )
                                    .await?
                                    .into_iter();
                                catalog
                                    .catalog_lock
                                    .finish_lock_prefix(prefix, &mut pages)?;
                                assert!(pages.next().is_none());
                            }
                            assert_eq!(
                                catalog.catalog_lock.xid_before_ordering_lock(),
                                prior_write
                            );
                            Ok(())
                        })
                    },
                )
                .await
                .unwrap();
        }
    }
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_names_with_quotes_when_bound_as_one_array_then_postgres_reads_each_name() {
    let fixture = crate::postgres::test_support::NativeFixture::open().await;
    let names = [
        "plain",
        "quote\"d",
        "back\\slash",
        "comma,{brace}",
        "",
        "NULL",
    ];
    let crate::SqlValue::Text(literal) = crate::item_lock::postgres_text_array(&names) else {
        panic!("text literal")
    };
    let rows = fixture
        .admin
        .query(
            "SELECT name FROM unnest($1::text::text[]) WITH ORDINALITY AS names(name, ordinal) \
             ORDER BY ordinal",
            &[&literal],
        )
        .await
        .unwrap();
    let decoded = rows
        .iter()
        .map(|row| row.get::<_, Option<String>>(0))
        .collect::<Vec<_>>();
    assert_eq!(
        decoded,
        names
            .iter()
            .map(|name| Some((*name).to_owned()))
            .collect::<Vec<_>>()
    );
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_write_after_item_lock_when_window_adds_an_item_then_attempt_is_rejected() {
    let fixture = crate::postgres::test_support::NativeFixture::open().await;
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE TABLE {}.sys_system_stream_scope (slot bigint NOT NULL)",
            fixture.schema,
        ))
        .await
        .unwrap();
    for (write_between, second) in [(false, "second"), (true, "first"), (true, "second")] {
        let request = AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
            .start(fixture.backend.attempt_deadlines());
        let result = fixture
            .backend
            .with_attempt(request, move |attempt| {
                Box::pin(async move {
                    let table = crate::test_support::table();
                    let mut catalog = SqlCatalogAttempt::new(attempt, SqlEngine::Postgres, request);
                    catalog
                        .query_with_catalog_lock(
                            &[&table.info.table_name],
                            vec![],
                            storage_provider::CATALOG_READ_BYTES,
                        )
                        .await?;
                    for (ordinal, key) in ["first", second].into_iter().enumerate() {
                        if ordinal == 1 && write_between {
                            catalog
                                .attempt
                                .execute(SqlStatement::new(
                                    "INSERT INTO native_commit_probe VALUES (1)",
                                    vec![],
                                ))
                                .await?;
                        }
                        let mut target = storage_types::KeyAttributes::new();
                        target.insert("pk", storage_types::AttributeValue::S(key.into()));
                        let mut window = Vec::new();
                        let prefix = catalog
                            .catalog_lock
                            .item_lock_prefix(std::iter::once((&table, &target)), &mut window)?;
                        let mut pages = catalog
                            .attempt
                            .query_batch_protected(window, storage_provider::CATALOG_READ_BYTES)
                            .await?
                            .into_iter();
                        catalog
                            .catalog_lock
                            .finish_lock_prefix(prefix, &mut pages)?;
                    }
                    Ok(())
                })
            })
            .await;
        if write_between && second == "second" {
            let error = result.expect_err("a window adding an item after a write must be rejected");
            assert!(
                format!("{:?}", error.as_ref())
                    .contains("item lock window was requested after a write")
            );
        } else {
            result.unwrap();
        }
    }
    assert_eq!(
        fixture.count_records().await,
        1,
        "only the window that holds its item commits its write"
    );
    fixture.close().await;
}
