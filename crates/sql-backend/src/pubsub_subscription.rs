//! One subscription row layout for SQLite, Turso, and Postgres.

use std::num::NonZeroU32;

use pubsub_provider::{
    PendingSubscription, PendingSubscriptionCursor, PendingSubscriptionPage, SubscriptionArn,
    SubscriptionIdentity, SubscriptionPage, SubscriptionPageRequest, SubscriptionRecord,
    TopicIncarnation,
};
use storage_types::{MissExpectation, ReadProtection, StorageError, StorageResult, storage_serde};

use crate::{
    SqlAttempt, SqlLayout, SqlRow, SqlStatement, SqlValue, constants::PUBSUB_CATALOG_READ_BYTES,
};

pub(crate) async fn initialize_pubsub_subscriptions<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {relation_sys_pubsub_subscriptions} (subscription_arn {} \
         PRIMARY KEY, topic_arn {} NOT NULL, topic_incarnation {} NOT NULL, protocol {} NOT NULL, \
         endpoint_digest {} NOT NULL, endpoint {} NOT NULL, token {}, row_blob {} NOT NULL, \
         UNIQUE(topic_incarnation, protocol, endpoint_digest)){}",
        engine.engine().text_type(),
        engine.engine().text_type(),
        engine.engine().blob_type(),
        engine.engine().text_type(),
        engine.engine().blob_type(),
        engine.engine().text_type(),
        engine.engine().text_type(),
        engine.engine().blob_type(),
        engine.engine().table_suffix(),
        relation_sys_pubsub_subscriptions = engine.relation_name("sys_pubsub_subscriptions"),
    );
    native.execute(SqlStatement::new(sql, vec![])).await?;
    for sql in [
        crate::sql_template::sql_template!(
            engine,
            "CREATE INDEX IF NOT EXISTS "["sys_pubsub_subscription_topic"],
            " ON "["sys_pubsub_subscriptions"],
            "(topic_incarnation, subscription_arn)"
        ),
        // Tokens are unique UUIDv7 text and non-NULL only while pending, so
        // the pending subscriptions are one range of this index.
        crate::sql_template::sql_template!(
            engine,
            "CREATE UNIQUE INDEX IF NOT EXISTS "["sys_pubsub_subscription_pending_token"],
            " ON "["sys_pubsub_subscriptions"],
            "(token)"
        ),
    ] {
        native.execute(SqlStatement::new(sql, vec![])).await?;
    }
    Ok(())
}

pub(crate) async fn read_subscription<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    arn: &SubscriptionArn,
) -> StorageResult<Option<SubscriptionRecord>> {
    let sql = format!(
        "{} WHERE subscription_arn = {}",
        select_columns(engine),
        engine.engine().placeholder(1),
    );
    read_one(
        native,
        protection,
        SqlStatement::new(sql, vec![SqlValue::Text(arn.to_string())]),
    )
    .await
}

pub(crate) async fn find_subscription_by_identity<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    identity: SubscriptionIdentity<'_>,
) -> StorageResult<Option<SubscriptionRecord>> {
    let sql = format!(
        "{} WHERE topic_incarnation = {} AND protocol = {} AND endpoint_digest = {}",
        select_columns(engine),
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        engine.engine().placeholder(3),
    );
    let row = read_one(
        native,
        protection,
        SqlStatement::new(
            sql,
            vec![
                SqlValue::Blob(identity.topic_incarnation.as_bytes().to_vec()),
                SqlValue::Text(identity.protocol.as_str().to_owned()),
                SqlValue::Blob(identity.endpoint_digest().to_vec()),
            ],
        ),
    )
    .await?;
    if row
        .as_ref()
        .is_some_and(|record| record.subscription.endpoint != identity.endpoint)
    {
        return Err(StorageError::internal(
            "subscription endpoint digest collision",
        ));
    }
    Ok(row)
}

pub(crate) async fn find_subscription_by_token<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    incarnation: TopicIncarnation,
    token: &str,
) -> StorageResult<Option<SubscriptionRecord>> {
    let sql = format!(
        "{} WHERE topic_incarnation = {} AND token = {}",
        select_columns(engine),
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
    );
    read_one(
        native,
        protection,
        SqlStatement::new(
            sql,
            vec![
                SqlValue::Blob(incarnation.as_bytes().to_vec()),
                SqlValue::Text(token.to_owned()),
            ],
        ),
    )
    .await
}

pub(crate) async fn write_subscription<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    record: SubscriptionRecord,
    expected: Option<SubscriptionRecord>,
) -> StorageResult<()> {
    let row_blob = storage_serde::to_bytes(&record)?;
    let token = record
        .subscription
        .confirmation
        .token()
        .map(|token| SqlValue::Text(token.to_owned()))
        .unwrap_or(SqlValue::Null);
    let (sql, binds) = if let Some(previous) = expected {
        (
            format!(
                "UPDATE {relation_sys_pubsub_subscriptions} SET row_blob = {}, token = {} WHERE \
                 subscription_arn = {} AND row_blob = {}",
                engine.engine().placeholder(1),
                engine.engine().placeholder(2),
                engine.engine().placeholder(3),
                engine.engine().placeholder(4),
                relation_sys_pubsub_subscriptions =
                    engine.relation_name("sys_pubsub_subscriptions"),
            ),
            vec![
                SqlValue::Blob(row_blob),
                token,
                SqlValue::Text(record.subscription.subscription_arn.to_string()),
                SqlValue::Blob(storage_serde::to_bytes(&previous)?),
            ],
        )
    } else {
        (
            format!(
                "INSERT INTO {relation_sys_pubsub_subscriptions} (subscription_arn, topic_arn, \
                 topic_incarnation, protocol, endpoint_digest, endpoint, token, row_blob) VALUES \
                 ({}) ON CONFLICT DO NOTHING",
                engine.engine().placeholders(8),
                relation_sys_pubsub_subscriptions =
                    engine.relation_name("sys_pubsub_subscriptions"),
            ),
            vec![
                SqlValue::Text(record.subscription.subscription_arn.to_string()),
                SqlValue::Text(record.subscription.topic_arn.to_string()),
                SqlValue::Blob(record.topic_incarnation.as_bytes().to_vec()),
                SqlValue::Text(record.subscription.protocol.as_str().to_owned()),
                SqlValue::Blob(
                    pubsub_provider::digest_bytes(record.subscription.endpoint.as_bytes()).to_vec(),
                ),
                SqlValue::Text(record.subscription.endpoint),
                token,
                SqlValue::Blob(row_blob),
            ],
        )
    };
    expect_one(native.execute(SqlStatement::new(sql, binds)).await?)
}

pub(crate) async fn delete_subscription<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    record: SubscriptionRecord,
) -> StorageResult<()> {
    let sql = format!(
        "DELETE FROM {relation_sys_pubsub_subscriptions} WHERE subscription_arn = {} AND row_blob \
         = {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation_sys_pubsub_subscriptions = engine.relation_name("sys_pubsub_subscriptions"),
    );
    expect_one(
        native
            .execute(SqlStatement::new(
                sql,
                vec![
                    SqlValue::Text(record.subscription.subscription_arn.to_string()),
                    SqlValue::Blob(storage_serde::to_bytes(&record)?),
                ],
            ))
            .await?,
    )
}

pub(crate) async fn read_subscription_page<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    request: SubscriptionPageRequest<'_>,
) -> StorageResult<SubscriptionPage> {
    let mut binds = Vec::new();
    let mut conditions = Vec::new();
    if let Some(incarnation) = request.topic_incarnation {
        binds.push(SqlValue::Blob(incarnation.as_bytes().to_vec()));
        conditions.push(format!(
            "topic_incarnation = {}",
            engine.engine().placeholder(binds.len())
        ));
    }
    if let Some(after) = request.after {
        binds.push(SqlValue::Text(after.to_string()));
        conditions.push(format!(
            "subscription_arn > {}",
            engine.engine().placeholder(binds.len())
        ));
    }
    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };
    binds.push(SqlValue::Integer(i64::from(request.limit.get()) + 1));
    let sql = format!(
        "{}{} ORDER BY subscription_arn LIMIT {}",
        select_columns(engine),
        where_clause,
        engine.engine().placeholder(binds.len()),
    );
    let page = native
        .query(
            SqlStatement::new(sql, binds),
            ReadProtection::Snapshot,
            PUBSUB_CATALOG_READ_BYTES,
        )
        .await?;
    Ok(SubscriptionPage {
        rows: page
            .rows
            .into_iter()
            .map(decode_row)
            .collect::<StorageResult<Vec<_>>>()?,
        more: page.more,
    })
}

/// The pending-subscription page: binds (after token, limit + 1).
pub(crate) fn pending_subscription_sql(engine: impl SqlLayout) -> String {
    format!(
        "{} WHERE token > {} ORDER BY token LIMIT {}",
        select_columns(engine),
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
    )
}

/// Pending subscriptions after `after` in token order, read at `Snapshot`.
pub(crate) async fn read_pending_subscription_page<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    after: Option<&PendingSubscriptionCursor>,
    limit: NonZeroU32,
) -> StorageResult<PendingSubscriptionPage> {
    // Every token is longer than '', and a NULL token fails the comparison.
    let after = match after {
        Some(cursor) => String::from_utf8(cursor.0.clone())
            .map_err(|_| StorageError::internal("SQL pending subscription cursor is not UTF-8"))?,
        None => String::new(),
    };
    let sql = pending_subscription_sql(engine);
    let page = native
        .query(
            SqlStatement::new(
                sql,
                vec![
                    SqlValue::Text(after),
                    SqlValue::Integer(i64::from(limit.get()) + 1),
                ],
            ),
            ReadProtection::Snapshot,
            PUBSUB_CATALOG_READ_BYTES,
        )
        .await?;
    let more = page.more || page.rows.len() > limit.get() as usize;
    let rows = page
        .rows
        .into_iter()
        .take(limit.get() as usize)
        .map(|row| {
            let record = decode_row(row)?;
            let token = record.subscription.confirmation.token().ok_or_else(|| {
                StorageError::internal("SQL pending subscription row has no token")
            })?;
            Ok(PendingSubscription {
                cursor: PendingSubscriptionCursor(token.as_bytes().to_vec()),
                subscription_arn: record.subscription.subscription_arn.clone(),
                created_at: record.subscription.created_at,
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(PendingSubscriptionPage { rows, more })
}

fn select_columns(engine: impl SqlLayout) -> std::borrow::Cow<'static, str> {
    crate::sql_template::sql_template!(
        engine,
        "SELECT subscription_arn, topic_arn, topic_incarnation, protocol, endpoint, token, \
         row_blob FROM "["sys_pubsub_subscriptions"],
        ""
    )
}

async fn read_one<A: SqlAttempt>(
    native: &mut A,
    protection: ReadProtection,
    statement: SqlStatement,
) -> StorageResult<Option<SubscriptionRecord>> {
    let page = native
        .query(statement, protection, PUBSUB_CATALOG_READ_BYTES)
        .await?;
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal(
            "SQL subscription lookup returned multiple rows",
        ));
    }
    page.rows.into_iter().next().map(decode_row).transpose()
}

fn decode_row(row: SqlRow) -> StorageResult<SubscriptionRecord> {
    let [arn, topic, incarnation, protocol, endpoint, token, blob] =
        row.into_values()
            .try_into()
            .map_err(|_| StorageError::internal("SQL subscription row has invalid width"))?;
    let (
        SqlValue::Text(arn),
        SqlValue::Text(topic),
        SqlValue::Blob(incarnation),
        SqlValue::Text(protocol),
        SqlValue::Text(endpoint),
        SqlValue::Blob(blob),
    ) = (arn, topic, incarnation, protocol, endpoint, blob)
    else {
        return Err(StorageError::internal(
            "SQL subscription row has invalid types",
        ));
    };
    let record: SubscriptionRecord = storage_serde::from_bytes(&blob)?;
    let token = match token {
        SqlValue::Text(value) => Some(value),
        SqlValue::Null => None,
        _ => {
            return Err(StorageError::internal(
                "SQL subscription token has invalid type",
            ));
        }
    };
    if record.subscription.subscription_arn.as_str() != arn
        || record.subscription.topic_arn.as_str() != topic
        || record.topic_incarnation != TopicIncarnation::try_from(incarnation.as_slice())?
        || record.subscription.protocol.as_str() != protocol
        || record.subscription.endpoint != endpoint
        || record.subscription.confirmation.token() != token.as_deref()
    {
        return Err(StorageError::internal(
            "SQL subscription row identity differs",
        ));
    }
    Ok(record)
}

fn expect_one(changed: u64) -> StorageResult<()> {
    if changed == 1 {
        Ok(())
    } else {
        Err(StorageError::write_miss(
            "pubsub_subscription",
            MissExpectation::Rows {
                expected: 1,
                found: changed,
            },
        ))
    }
}
