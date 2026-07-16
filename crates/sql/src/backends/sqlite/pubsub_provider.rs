use async_trait::async_trait;
use pubsub_provider::{
    ClaimDeliveryRecordsRequest, ClaimDeliveryRecordsResponse, ConfirmSubscriptionRequest,
    ConfirmSubscriptionResponse, CreateTopicRequest, DeliveryRecord, DeliveryRecordId,
    DeliveryRecordKind, DeliveryStatus, DeliveryTarget, GetSubscriptionAttributesRequest,
    GetSubscriptionAttributesResponse, GetTopicAttributesRequest, GetTopicAttributesResponse,
    ListSubscriptionsRequest, ListSubscriptionsResponse, ListTopicsRequest, ListTopicsResponse,
    PubsubError, PubsubProvider, PubsubResult, PubsubValidationKind,
    SetSubscriptionAttributesRequest, SetTopicAttributesRequest, SubscribeRequest, Subscription,
    SubscriptionArn, SubscriptionConfirmation, SubscriptionProtocol, Topic, TopicArn,
};
use rusqlite::{OptionalExtension as _, params};
use storage_types::TimestampMillis;

use crate::backends::sqlite::SQLiteStorageProvider;

#[async_trait]
impl PubsubProvider for SQLiteStorageProvider {
    async fn initialize(&self) -> PubsubResult<()> {
        call_pubsub(&self.connection, |conn| {
            conn.execute_batch(
                r"
                CREATE TABLE IF NOT EXISTS sys_pubsub_topics (
                    topic_arn TEXT PRIMARY KEY,
                    topic_name TEXT NOT NULL UNIQUE,
                    display_name TEXT,
                    created_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sys_pubsub_subscriptions (
                    subscription_arn TEXT PRIMARY KEY,
                    topic_arn TEXT NOT NULL,
                    protocol TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    raw_message_delivery INTEGER NOT NULL,
                    pending_confirmation INTEGER NOT NULL,
                    confirmation_token TEXT,
                    extra_json TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    UNIQUE(topic_arn, protocol, endpoint),
                    FOREIGN KEY(topic_arn) REFERENCES sys_pubsub_topics(topic_arn) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS sys_pubsub_deliveries (
                    id TEXT PRIMARY KEY,
                    message_id TEXT NOT NULL,
                    delivery_kind TEXT NOT NULL DEFAULT 'notification',
                    confirmation_token TEXT,
                    subscription_arn TEXT NOT NULL,
                    message_body TEXT,
                    subject TEXT,
                    message_attributes_json TEXT NOT NULL DEFAULT '{}',
                    target TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    next_attempt_at INTEGER,
                    lease_owner TEXT,
                    lease_expires_at INTEGER,
                    last_error TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    subscription_json TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_pubsub_deliveries_claimable
                    ON sys_pubsub_deliveries(status, next_attempt_at, lease_expires_at);
                ",
            )
            .map_err(map_pubsub_sqlite_error)?;
            add_column_if_missing(
                conn,
                "sys_pubsub_subscriptions",
                "confirmation_token",
                "TEXT",
            )?;
            add_column_if_missing(
                conn,
                "sys_pubsub_deliveries",
                "delivery_kind",
                "TEXT NOT NULL DEFAULT 'notification'",
            )?;
            add_column_if_missing(conn, "sys_pubsub_deliveries", "confirmation_token", "TEXT")?;
            add_column_if_missing(conn, "sys_pubsub_deliveries", "subscription_json", "TEXT")?;
            Ok(())
        })
        .await
    }

    async fn create_topic(&self, request: CreateTopicRequest) -> PubsubResult<Topic> {
        call_pubsub(&self.connection, move |conn| {
            let existing = select_topic_by_name(conn, request.name.as_str())?;
            if let Some(topic) = existing {
                return Ok(topic);
            }
            let topic = Topic {
                topic_arn: TopicArn::compose("aws", "us-east-1", "000000000000", &request.name),
                name: request.name,
                display_name: request.attributes.get("DisplayName").cloned(),
                created_at: TimestampMillis::now(),
            };
            conn.execute(
                "INSERT INTO sys_pubsub_topics (topic_arn, topic_name, display_name, created_at) \
                 VALUES (?1, ?2, ?3, ?4)",
                (
                    topic.topic_arn.as_str(),
                    topic.name.as_str(),
                    topic.display_name.as_deref(),
                    topic.created_at.timestamp_millis(),
                ),
            )
            .map_err(map_pubsub_sqlite_error)?;
            Ok(topic)
        })
        .await
    }

    async fn delete_topic(&self, topic_arn: &TopicArn) -> PubsubResult<()> {
        let topic_arn = topic_arn.clone();
        call_pubsub(&self.connection, move |conn| {
            conn.execute(
                "DELETE FROM sys_pubsub_subscriptions WHERE topic_arn = ?1",
                [topic_arn.as_str()],
            )
            .map_err(map_pubsub_sqlite_error)?;
            conn.execute(
                "DELETE FROM sys_pubsub_topics WHERE topic_arn = ?1",
                [topic_arn.as_str()],
            )
            .map_err(map_pubsub_sqlite_error)?;
            Ok(())
        })
        .await
    }

    async fn get_topic(&self, topic_arn: &TopicArn) -> PubsubResult<Option<Topic>> {
        let topic_arn = topic_arn.clone();
        call_pubsub(&self.connection, move |conn| {
            select_topic_by_arn(conn, topic_arn.as_str())
        })
        .await
    }

    async fn get_topic_attributes(
        &self,
        request: GetTopicAttributesRequest,
    ) -> PubsubResult<GetTopicAttributesResponse> {
        call_pubsub(&self.connection, move |conn| {
            let Some(topic) = select_topic_by_arn(conn, request.topic_arn.as_str())? else {
                return Err(PubsubError::topic_not_found(request.topic_arn.to_string()));
            };
            let (confirmed, pending) = subscription_counts(conn, topic.topic_arn.as_str())?;
            Ok(GetTopicAttributesResponse {
                attributes: topic.attributes(confirmed, pending),
            })
        })
        .await
    }

    async fn set_topic_attributes(
        &self,
        request: SetTopicAttributesRequest,
    ) -> PubsubResult<Topic> {
        call_pubsub(&self.connection, move |conn| {
            let Some(mut topic) = select_topic_by_arn(conn, request.topic_arn.as_str())? else {
                return Err(PubsubError::topic_not_found(request.topic_arn.to_string()));
            };
            if let Some(display_name) = request.attributes.get("DisplayName") {
                topic.display_name = Some(display_name.clone());
                conn.execute(
                    "UPDATE sys_pubsub_topics SET display_name = ?1 WHERE topic_arn = ?2",
                    (display_name, topic.topic_arn.as_str()),
                )
                .map_err(map_pubsub_sqlite_error)?;
            }
            Ok(topic)
        })
        .await
    }

    async fn list_topics(&self, _request: ListTopicsRequest) -> PubsubResult<ListTopicsResponse> {
        call_pubsub(&self.connection, move |conn| {
            let mut statement = conn
                .prepare(
                    "SELECT topic_arn, topic_name, display_name, created_at FROM \
                     sys_pubsub_topics ORDER BY topic_arn",
                )
                .map_err(map_pubsub_sqlite_error)?;
            let topics = statement
                .query_map([], row_to_topic)
                .map_err(map_pubsub_sqlite_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(map_pubsub_sqlite_error)?;
            Ok(ListTopicsResponse {
                topics,
                next_token: None,
            })
        })
        .await
    }

    async fn create_subscription(&self, request: SubscribeRequest) -> PubsubResult<Subscription> {
        call_pubsub(&self.connection, move |conn| {
            if select_topic_by_arn(conn, request.topic_arn.as_str())?.is_none() {
                return Err(PubsubError::topic_not_found(request.topic_arn.to_string()));
            }
            if let Some(subscription) = select_subscription_by_identity(
                conn,
                request.topic_arn.as_str(),
                request.protocol.as_str(),
                &request.endpoint,
            )? {
                return Ok(subscription);
            }
            let subscription = Subscription {
                subscription_arn: SubscriptionArn::compose(&request.topic_arn),
                topic_arn: request.topic_arn,
                protocol: request.protocol,
                endpoint: request.endpoint,
                raw_message_delivery: request
                    .attributes
                    .get("RawMessageDelivery")
                    .is_some_and(|value| value.eq_ignore_ascii_case("true")),
                confirmation: request.protocol.subscription_confirmation(),
                extra_json: request.extra_json,
                created_at: TimestampMillis::now(),
            };
            insert_subscription(conn, &subscription)?;
            Ok(subscription)
        })
        .await
    }

    async fn confirm_subscription(
        &self,
        request: ConfirmSubscriptionRequest,
    ) -> PubsubResult<ConfirmSubscriptionResponse> {
        call_pubsub(&self.connection, move |conn| {
            let Some(mut subscription) = select_subscription_by_confirmation(
                conn,
                request.topic_arn.as_str(),
                &request.token,
            )?
            else {
                return Err(PubsubError::validation(PubsubValidationKind::InvalidToken));
            };
            subscription.confirmation = SubscriptionConfirmation::Confirmed;
            conn.execute(
                "UPDATE sys_pubsub_subscriptions SET pending_confirmation = 0, confirmation_token \
                 = NULL WHERE subscription_arn = ?1",
                [subscription.subscription_arn.as_str()],
            )
            .map_err(map_pubsub_sqlite_error)?;
            Ok(ConfirmSubscriptionResponse {
                subscription_arn: subscription.subscription_arn,
            })
        })
        .await
    }

    async fn delete_subscription(&self, subscription_arn: &SubscriptionArn) -> PubsubResult<()> {
        let subscription_arn = subscription_arn.clone();
        call_pubsub(&self.connection, move |conn| {
            conn.execute(
                "DELETE FROM sys_pubsub_subscriptions WHERE subscription_arn = ?1",
                [subscription_arn.as_str()],
            )
            .map_err(map_pubsub_sqlite_error)?;
            Ok(())
        })
        .await
    }

    async fn get_subscription(
        &self,
        subscription_arn: &SubscriptionArn,
    ) -> PubsubResult<Option<Subscription>> {
        let subscription_arn = subscription_arn.clone();
        call_pubsub(&self.connection, move |conn| {
            select_subscription_by_arn(conn, subscription_arn.as_str())
        })
        .await
    }

    async fn set_subscription_attributes(
        &self,
        request: SetSubscriptionAttributesRequest,
    ) -> PubsubResult<Subscription> {
        call_pubsub(&self.connection, move |conn| {
            let Some(mut subscription) =
                select_subscription_by_arn(conn, request.subscription_arn.as_str())?
            else {
                return Err(PubsubError::subscription_not_found(
                    request.subscription_arn.to_string(),
                ));
            };
            if let Some(raw) = request.attributes.get("RawMessageDelivery") {
                subscription.raw_message_delivery = raw.eq_ignore_ascii_case("true");
                conn.execute(
                    "UPDATE sys_pubsub_subscriptions SET raw_message_delivery = ?1 WHERE \
                     subscription_arn = ?2",
                    (
                        bool_to_i64(subscription.raw_message_delivery),
                        subscription.subscription_arn.as_str(),
                    ),
                )
                .map_err(map_pubsub_sqlite_error)?;
            }
            Ok(subscription)
        })
        .await
    }

    async fn get_subscription_attributes(
        &self,
        request: GetSubscriptionAttributesRequest,
    ) -> PubsubResult<GetSubscriptionAttributesResponse> {
        call_pubsub(&self.connection, move |conn| {
            let Some(subscription) =
                select_subscription_by_arn(conn, request.subscription_arn.as_str())?
            else {
                return Err(PubsubError::subscription_not_found(
                    request.subscription_arn.to_string(),
                ));
            };
            Ok(GetSubscriptionAttributesResponse {
                attributes: subscription.attributes(),
            })
        })
        .await
    }

    async fn list_subscriptions(
        &self,
        request: ListSubscriptionsRequest,
    ) -> PubsubResult<ListSubscriptionsResponse> {
        call_pubsub(&self.connection, move |conn| {
            let subscriptions = if let Some(topic_arn) = request.topic_arn {
                let mut statement = conn
                    .prepare(
                        "SELECT subscription_arn, topic_arn, protocol, endpoint, \
                         raw_message_delivery, pending_confirmation, confirmation_token, \
                         extra_json, created_at FROM sys_pubsub_subscriptions WHERE topic_arn = \
                         ?1 ORDER BY subscription_arn",
                    )
                    .map_err(map_pubsub_sqlite_error)?;
                statement
                    .query_map([topic_arn.as_str()], row_to_subscription)
                    .map_err(map_pubsub_sqlite_error)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(map_pubsub_sqlite_error)?
            } else {
                let mut statement = conn
                    .prepare(
                        "SELECT subscription_arn, topic_arn, protocol, endpoint, \
                         raw_message_delivery, pending_confirmation, confirmation_token, \
                         extra_json, created_at FROM sys_pubsub_subscriptions ORDER BY \
                         subscription_arn",
                    )
                    .map_err(map_pubsub_sqlite_error)?;
                statement
                    .query_map([], row_to_subscription)
                    .map_err(map_pubsub_sqlite_error)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(map_pubsub_sqlite_error)?
            };
            Ok(ListSubscriptionsResponse {
                subscriptions,
                next_token: None,
            })
        })
        .await
    }

    async fn put_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()> {
        call_pubsub(&self.connection, move |conn| {
            upsert_delivery_record(conn, &record)
        })
        .await
    }

    async fn claim_delivery_records(
        &self,
        request: ClaimDeliveryRecordsRequest,
    ) -> PubsubResult<ClaimDeliveryRecordsResponse> {
        call_pubsub(&self.connection, move |conn| {
            let tx = conn.transaction().map_err(map_pubsub_sqlite_error)?;
            let mut statement = tx
                .prepare(
                    "SELECT id, message_id, delivery_kind, confirmation_token, subscription_arn, \
                     message_body, subject, message_attributes_json, target, status, attempts, \
                     next_attempt_at, lease_owner, lease_expires_at, last_error, created_at, \
                     updated_at, subscription_json
                     FROM sys_pubsub_deliveries
                     WHERE status IN ('pending', 'retry_scheduled')
                       AND (next_attempt_at IS NULL OR next_attempt_at <= ?1)
                       AND (lease_expires_at IS NULL OR lease_expires_at <= ?1)
                     ORDER BY id
                     LIMIT ?2",
                )
                .map_err(map_pubsub_sqlite_error)?;
            let mut records = statement
                .query_map(
                    (request.now.timestamp_millis(), request.limit as i64),
                    row_to_delivery_record,
                )
                .map_err(map_pubsub_sqlite_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(map_pubsub_sqlite_error)?;
            drop(statement);

            for record in &mut records {
                record.lease_owner = Some(request.owner.clone());
                record.lease_expires_at = Some(request.lease_expires_at);
                record.updated_at = request.now;
                tx.execute(
                    "UPDATE sys_pubsub_deliveries SET lease_owner = ?1, lease_expires_at = ?2, \
                     updated_at = ?3 WHERE id = ?4",
                    (
                        record.lease_owner.as_deref(),
                        request.lease_expires_at.timestamp_millis(),
                        request.now.timestamp_millis(),
                        record.id.0.as_str(),
                    ),
                )
                .map_err(map_pubsub_sqlite_error)?;
            }
            tx.commit().map_err(map_pubsub_sqlite_error)?;
            Ok(ClaimDeliveryRecordsResponse { records })
        })
        .await
    }

    async fn update_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()> {
        call_pubsub(&self.connection, move |conn| {
            upsert_delivery_record(conn, &record)
        })
        .await
    }

    async fn get_delivery_record(
        &self,
        record_id: &DeliveryRecordId,
    ) -> PubsubResult<Option<DeliveryRecord>> {
        let id = record_id.0.clone();
        call_pubsub(&self.connection, move |conn| {
            conn.query_row(
                "SELECT id, message_id, delivery_kind, confirmation_token, subscription_arn, \
                 message_body, subject, message_attributes_json, target, status, attempts, \
                 next_attempt_at, lease_owner, lease_expires_at, last_error, created_at, \
                 updated_at, subscription_json FROM sys_pubsub_deliveries WHERE id = ?1",
                [id.as_str()],
                row_to_delivery_record,
            )
            .optional()
            .map_err(map_pubsub_sqlite_error)
        })
        .await
    }
}

async fn call_pubsub<F, R>(connection: &tokio_rusqlite::Connection, function: F) -> PubsubResult<R>
where
    F: FnOnce(&mut rusqlite::Connection) -> PubsubResult<R> + Send + 'static,
    R: Send + 'static,
{
    connection
        .call(move |conn| function(conn).map_err(|err| tokio_rusqlite::Error::Other(Box::new(err))))
        .await
        .map_err(map_tokio_rusqlite_pubsub_error)
}

fn map_tokio_rusqlite_pubsub_error(error: tokio_rusqlite::Error) -> PubsubError {
    match error {
        tokio_rusqlite::Error::Other(error) => match error.downcast::<PubsubError>() {
            Ok(error) => *error,
            Err(error) => PubsubError::storage(format!("sqlite pubsub call failed: {error}")),
        },
        tokio_rusqlite::Error::Rusqlite(error) => map_pubsub_sqlite_error(error),
        other => PubsubError::storage(format!("sqlite pubsub call failed: {other}")),
    }
}

fn map_pubsub_sqlite_error(error: rusqlite::Error) -> PubsubError {
    PubsubError::storage(format!("sqlite pubsub error: {error}"))
}

fn select_topic_by_name(
    conn: &rusqlite::Connection,
    topic_name: &str,
) -> PubsubResult<Option<Topic>> {
    conn.query_row(
        "SELECT topic_arn, topic_name, display_name, created_at FROM sys_pubsub_topics WHERE \
         topic_name = ?1",
        [topic_name],
        row_to_topic,
    )
    .optional()
    .map_err(map_pubsub_sqlite_error)
}

fn select_topic_by_arn(
    conn: &rusqlite::Connection,
    topic_arn: &str,
) -> PubsubResult<Option<Topic>> {
    conn.query_row(
        "SELECT topic_arn, topic_name, display_name, created_at FROM sys_pubsub_topics WHERE \
         topic_arn = ?1",
        [topic_arn],
        row_to_topic,
    )
    .optional()
    .map_err(map_pubsub_sqlite_error)
}

fn row_to_topic(row: &rusqlite::Row<'_>) -> rusqlite::Result<Topic> {
    let topic_arn: String = row.get(0)?;
    let topic_name: String = row.get(1)?;
    let display_name: Option<String> = row.get(2)?;
    let created_at: i64 = row.get(3)?;
    Ok(Topic {
        topic_arn: TopicArn::new(topic_arn).map_err(to_sql_conversion_error)?,
        name: pubsub_provider::TopicName::new(topic_name).map_err(to_sql_conversion_error)?,
        display_name,
        created_at: TimestampMillis::from_timestamp(created_at),
    })
}

fn subscription_counts(
    conn: &rusqlite::Connection,
    topic_arn: &str,
) -> PubsubResult<(usize, usize)> {
    let mut statement = conn
        .prepare(
            "SELECT pending_confirmation, COUNT(*) FROM sys_pubsub_subscriptions WHERE topic_arn \
             = ?1 GROUP BY pending_confirmation",
        )
        .map_err(map_pubsub_sqlite_error)?;
    let mut confirmed = 0usize;
    let mut pending = 0usize;
    let rows = statement
        .query_map([topic_arn], |row| {
            Ok((row.get::<_, i64>(0)? != 0, row.get::<_, i64>(1)? as usize))
        })
        .map_err(map_pubsub_sqlite_error)?;
    for row in rows {
        let (is_pending, count) = row.map_err(map_pubsub_sqlite_error)?;
        if is_pending {
            pending = count;
        } else {
            confirmed = count;
        }
    }
    Ok((confirmed, pending))
}

fn insert_subscription(
    conn: &rusqlite::Connection,
    subscription: &Subscription,
) -> PubsubResult<()> {
    let extra_json = serde_json::to_string(&subscription.extra_json)?;
    conn.execute(
        "INSERT INTO sys_pubsub_subscriptions (subscription_arn, topic_arn, protocol, endpoint, \
         raw_message_delivery, pending_confirmation, confirmation_token, extra_json, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        (
            subscription.subscription_arn.as_str(),
            subscription.topic_arn.as_str(),
            subscription.protocol.as_str(),
            subscription.endpoint.as_str(),
            bool_to_i64(subscription.raw_message_delivery),
            bool_to_i64(subscription.confirmation.pending_confirmation()),
            subscription.confirmation.token(),
            extra_json,
            subscription.created_at.timestamp_millis(),
        ),
    )
    .map_err(map_pubsub_sqlite_error)?;
    Ok(())
}

fn select_subscription_by_identity(
    conn: &rusqlite::Connection,
    topic_arn: &str,
    protocol: &str,
    endpoint: &str,
) -> PubsubResult<Option<Subscription>> {
    conn.query_row(
        "SELECT subscription_arn, topic_arn, protocol, endpoint, raw_message_delivery, \
         pending_confirmation, confirmation_token, extra_json, created_at FROM \
         sys_pubsub_subscriptions WHERE topic_arn = ?1 AND protocol = ?2 AND endpoint = ?3",
        (topic_arn, protocol, endpoint),
        row_to_subscription,
    )
    .optional()
    .map_err(map_pubsub_sqlite_error)
}

fn select_subscription_by_arn(
    conn: &rusqlite::Connection,
    subscription_arn: &str,
) -> PubsubResult<Option<Subscription>> {
    conn.query_row(
        "SELECT subscription_arn, topic_arn, protocol, endpoint, raw_message_delivery, \
         pending_confirmation, confirmation_token, extra_json, created_at FROM \
         sys_pubsub_subscriptions WHERE subscription_arn = ?1",
        [subscription_arn],
        row_to_subscription,
    )
    .optional()
    .map_err(map_pubsub_sqlite_error)
}

fn select_subscription_by_confirmation(
    conn: &rusqlite::Connection,
    topic_arn: &str,
    token: &str,
) -> PubsubResult<Option<Subscription>> {
    conn.query_row(
        "SELECT subscription_arn, topic_arn, protocol, endpoint, raw_message_delivery, \
         pending_confirmation, confirmation_token, extra_json, created_at FROM \
         sys_pubsub_subscriptions WHERE topic_arn = ?1 AND confirmation_token = ?2",
        (topic_arn, token),
        row_to_subscription,
    )
    .optional()
    .map_err(map_pubsub_sqlite_error)
}

fn row_to_subscription(row: &rusqlite::Row<'_>) -> rusqlite::Result<Subscription> {
    let subscription_arn: String = row.get(0)?;
    let topic_arn: String = row.get(1)?;
    let protocol: String = row.get(2)?;
    let endpoint: String = row.get(3)?;
    let raw_message_delivery: i64 = row.get(4)?;
    let pending_confirmation: i64 = row.get(5)?;
    let confirmation_token: Option<String> = row.get(6)?;
    let extra_json: String = row.get(7)?;
    let created_at: i64 = row.get(8)?;
    let Some(protocol) = SubscriptionProtocol::parse(&protocol) else {
        return Err(to_sql_conversion_message("invalid subscription protocol"));
    };
    Ok(Subscription {
        subscription_arn: SubscriptionArn::new(subscription_arn)
            .map_err(to_sql_conversion_error)?,
        topic_arn: TopicArn::new(topic_arn).map_err(to_sql_conversion_error)?,
        protocol,
        endpoint,
        raw_message_delivery: raw_message_delivery != 0,
        confirmation: if pending_confirmation != 0 {
            SubscriptionConfirmation::Pending {
                token: confirmation_token.unwrap_or_default(),
            }
        } else {
            SubscriptionConfirmation::Confirmed
        },
        extra_json: serde_json::from_str(&extra_json).map_err(to_sql_conversion_error)?,
        created_at: TimestampMillis::from_timestamp(created_at),
    })
}

fn upsert_delivery_record(
    conn: &rusqlite::Connection,
    record: &DeliveryRecord,
) -> PubsubResult<()> {
    conn.execute(
        "INSERT INTO sys_pubsub_deliveries (id, message_id, delivery_kind, confirmation_token, \
         subscription_arn, message_body, subject, message_attributes_json, target, status, \
         attempts, next_attempt_at, lease_owner, lease_expires_at, last_error, created_at, \
         updated_at, subscription_json)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
         ON CONFLICT(id) DO UPDATE SET
            message_id = excluded.message_id,
            delivery_kind = excluded.delivery_kind,
            confirmation_token = excluded.confirmation_token,
            subscription_arn = excluded.subscription_arn,
            message_body = excluded.message_body,
            subject = excluded.subject,
            message_attributes_json = excluded.message_attributes_json,
            target = excluded.target,
            status = excluded.status,
            attempts = excluded.attempts,
            next_attempt_at = excluded.next_attempt_at,
            lease_owner = excluded.lease_owner,
            lease_expires_at = excluded.lease_expires_at,
            last_error = excluded.last_error,
            subscription_json = excluded.subscription_json,
            updated_at = excluded.updated_at",
        params![
            record.id.0.as_str(),
            record.message_id.as_str(),
            delivery_kind_as_str(&record.kind),
            delivery_confirmation_token(&record.kind),
            record.subscription_arn.as_str(),
            record.message_body.as_deref(),
            record.subject.as_deref(),
            serde_json::to_string(&record.message_attributes)?,
            delivery_target_as_str(record.target),
            delivery_status_as_str(record.status),
            i64::from(record.attempts),
            record
                .next_attempt_at
                .map(TimestampMillis::timestamp_millis),
            record.lease_owner.as_deref(),
            record
                .lease_expires_at
                .map(TimestampMillis::timestamp_millis),
            record.last_error.as_deref(),
            record.created_at.timestamp_millis(),
            record.updated_at.timestamp_millis(),
            record
                .subscription
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
        ],
    )
    .map_err(map_pubsub_sqlite_error)?;
    Ok(())
}

fn row_to_delivery_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<DeliveryRecord> {
    let id: String = row.get(0)?;
    let message_id: String = row.get(1)?;
    let delivery_kind: String = row.get(2)?;
    let confirmation_token: Option<String> = row.get(3)?;
    let subscription_arn: String = row.get(4)?;
    let message_body = row.get(5)?;
    let subject = row.get(6)?;
    let message_attributes_json: String = row.get(7)?;
    let target: String = row.get(8)?;
    let status: String = row.get(9)?;
    let attempts: i64 = row.get(10)?;
    let next_attempt_at: Option<i64> = row.get(11)?;
    let lease_owner: Option<String> = row.get(12)?;
    let lease_expires_at: Option<i64> = row.get(13)?;
    let last_error: Option<String> = row.get(14)?;
    let created_at: i64 = row.get(15)?;
    let updated_at: i64 = row.get(16)?;
    let subscription_json: Option<String> = row.get(17)?;
    Ok(DeliveryRecord {
        id: DeliveryRecordId(id),
        kind: parse_delivery_kind(&delivery_kind, confirmation_token)?,
        message_id: pubsub_provider::PubsubMessageId::new_from_string(message_id)
            .map_err(to_sql_conversion_error)?,
        subscription_arn: SubscriptionArn::new(subscription_arn)
            .map_err(to_sql_conversion_error)?,
        subscription: subscription_json
            .map(|json| serde_json::from_str(&json))
            .transpose()
            .map_err(to_sql_conversion_error)?,
        message_body,
        subject,
        message_attributes: serde_json::from_str(&message_attributes_json)
            .map_err(to_sql_conversion_error)?,
        target: parse_delivery_target(&target)?,
        status: parse_delivery_status(&status)?,
        attempts: u32::try_from(attempts).map_err(to_sql_conversion_error)?,
        next_attempt_at: next_attempt_at.map(TimestampMillis::from_timestamp),
        lease_owner,
        lease_expires_at: lease_expires_at.map(TimestampMillis::from_timestamp),
        last_error,
        created_at: TimestampMillis::from_timestamp(created_at),
        updated_at: TimestampMillis::from_timestamp(updated_at),
    })
}

fn delivery_target_as_str(target: DeliveryTarget) -> &'static str {
    match target {
        DeliveryTarget::BuiltIn => "built_in",
        DeliveryTarget::CustomSender => "custom_sender",
    }
}

fn delivery_status_as_str(status: DeliveryStatus) -> &'static str {
    match status {
        DeliveryStatus::Pending => "pending",
        DeliveryStatus::Delivered => "delivered",
        DeliveryStatus::AcceptedByCustomSender => "accepted_by_custom_sender",
        DeliveryStatus::RetryScheduled => "retry_scheduled",
        DeliveryStatus::Failed => "failed",
    }
}

fn parse_delivery_target(value: &str) -> rusqlite::Result<DeliveryTarget> {
    match value {
        "built_in" => Ok(DeliveryTarget::BuiltIn),
        "custom_sender" => Ok(DeliveryTarget::CustomSender),
        _ => Err(to_sql_conversion_message("invalid delivery target")),
    }
}

fn parse_delivery_status(value: &str) -> rusqlite::Result<DeliveryStatus> {
    match value {
        "pending" => Ok(DeliveryStatus::Pending),
        "delivered" => Ok(DeliveryStatus::Delivered),
        "accepted_by_custom_sender" => Ok(DeliveryStatus::AcceptedByCustomSender),
        "retry_scheduled" => Ok(DeliveryStatus::RetryScheduled),
        "failed" => Ok(DeliveryStatus::Failed),
        _ => Err(to_sql_conversion_message("invalid delivery status")),
    }
}

fn delivery_kind_as_str(kind: &DeliveryRecordKind) -> &'static str {
    match kind {
        DeliveryRecordKind::Notification => "notification",
        DeliveryRecordKind::SubscriptionConfirmation { .. } => "subscription_confirmation",
    }
}

fn delivery_confirmation_token(kind: &DeliveryRecordKind) -> Option<&str> {
    match kind {
        DeliveryRecordKind::Notification => None,
        DeliveryRecordKind::SubscriptionConfirmation { token } => Some(token),
    }
}

fn parse_delivery_kind(
    value: &str,
    confirmation_token: Option<String>,
) -> rusqlite::Result<DeliveryRecordKind> {
    match value {
        "notification" => Ok(DeliveryRecordKind::Notification),
        "subscription_confirmation" => Ok(DeliveryRecordKind::SubscriptionConfirmation {
            token: confirmation_token.unwrap_or_default(),
        }),
        _ => Err(to_sql_conversion_message("invalid delivery kind")),
    }
}

fn add_column_if_missing(
    conn: &rusqlite::Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> PubsubResult<()> {
    let mut statement = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(map_pubsub_sqlite_error)?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(map_pubsub_sqlite_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(map_pubsub_sqlite_error)?;
    if columns.iter().any(|existing| existing == column) {
        return Ok(());
    }
    conn.execute(
        &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
        [],
    )
    .map_err(map_pubsub_sqlite_error)?;
    Ok(())
}

fn bool_to_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

fn to_sql_conversion_error(
    error: impl std::error::Error + Send + Sync + 'static,
) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

fn to_sql_conversion_message(message: &'static str) -> rusqlite::Error {
    to_sql_conversion_error(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    ))
}
