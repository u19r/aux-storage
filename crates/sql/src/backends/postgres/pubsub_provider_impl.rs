use async_trait::async_trait;
use pubsub_provider::{
    ClaimDeliveryRecordsRequest, ClaimDeliveryRecordsResponse, ConfirmSubscriptionRequest,
    ConfirmSubscriptionResponse, CreateTopicRequest, DeliveryRecord, DeliveryRecordId,
    GetSubscriptionAttributesRequest, GetSubscriptionAttributesResponse, GetTopicAttributesRequest,
    GetTopicAttributesResponse, ListSubscriptionsRequest, ListSubscriptionsResponse,
    ListTopicsRequest, ListTopicsResponse, PubsubError, PubsubProvider, PubsubResult,
    PubsubValidationKind, SetSubscriptionAttributesRequest, SetTopicAttributesRequest,
    SubscribeRequest, Subscription, SubscriptionArn, SubscriptionConfirmation, Topic, TopicArn,
};
use storage_types::TimestampMillis;

use crate::backends::postgres::PostgresStorageProvider;

const PUBSUB_TABLE: &str = "sys_pubsub_kv";
const TOPIC_PREFIX: &str = "topic:";
const TOPIC_NAME_PREFIX: &str = "topic_name:";
const SUBSCRIPTION_PREFIX: &str = "subscription:";
const SUBSCRIPTION_IDENTITY_PREFIX: &str = "subscription_identity:";
const SUBSCRIPTION_TOPIC_PREFIX: &str = "subscription_topic:";
const DELIVERY_PREFIX: &str = "delivery:";

#[async_trait]
impl PubsubProvider for PostgresStorageProvider {
    async fn initialize(&self) -> PubsubResult<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| PubsubError::storage(format!("acquire postgres client: {err}")))?;
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS sys_pubsub_kv (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );",
            )
            .await
            .map_err(|err| PubsubError::storage(format!("initialize postgres pubsub: {err}")))?;
        Ok(())
    }

    async fn create_topic(&self, request: CreateTopicRequest) -> PubsubResult<Topic> {
        if let Some(topic) = self.topic_by_name(request.name.as_str()).await? {
            return Ok(topic);
        }
        let topic = Topic {
            topic_arn: TopicArn::compose("aws", "us-east-1", "000000000000", &request.name),
            name: request.name,
            display_name: request.attributes.get("DisplayName").cloned(),
            created_at: TimestampMillis::now(),
        };
        self.put_json(&topic_key(&topic.topic_arn), &topic).await?;
        self.put_text(
            &topic_name_key(topic.name.as_str()),
            topic.topic_arn.as_str(),
        )
        .await?;
        Ok(topic)
    }

    async fn delete_topic(&self, topic_arn: &TopicArn) -> PubsubResult<()> {
        let subscriptions = self
            .list_subscriptions(ListSubscriptionsRequest {
                topic_arn: Some(topic_arn.clone()),
                next_token: None,
            })
            .await?
            .subscriptions;
        for subscription in subscriptions {
            self.delete_subscription(&subscription.subscription_arn)
                .await?;
        }
        if let Some(topic) = self.get_topic(topic_arn).await? {
            self.delete_key(&topic_name_key(topic.name.as_str()))
                .await?;
        }
        self.delete_key(&topic_key(topic_arn)).await?;
        Ok(())
    }

    async fn get_topic(&self, topic_arn: &TopicArn) -> PubsubResult<Option<Topic>> {
        self.get_json(&topic_key(topic_arn)).await
    }

    async fn get_topic_attributes(
        &self,
        request: GetTopicAttributesRequest,
    ) -> PubsubResult<GetTopicAttributesResponse> {
        let Some(topic) = self.get_topic(&request.topic_arn).await? else {
            return Err(PubsubError::topic_not_found(request.topic_arn.to_string()));
        };
        let subscriptions = self
            .list_subscriptions(ListSubscriptionsRequest {
                topic_arn: Some(topic.topic_arn.clone()),
                next_token: None,
            })
            .await?
            .subscriptions;
        let pending = subscriptions
            .iter()
            .filter(|subscription| subscription.confirmation.pending_confirmation())
            .count();
        let confirmed = subscriptions.len().saturating_sub(pending);
        Ok(GetTopicAttributesResponse {
            attributes: topic.attributes(confirmed, pending),
        })
    }

    async fn set_topic_attributes(
        &self,
        request: SetTopicAttributesRequest,
    ) -> PubsubResult<Topic> {
        let Some(mut topic) = self.get_topic(&request.topic_arn).await? else {
            return Err(PubsubError::topic_not_found(request.topic_arn.to_string()));
        };
        if let Some(display_name) = request.attributes.get("DisplayName") {
            topic.display_name = Some(display_name.clone());
            self.put_json(&topic_key(&topic.topic_arn), &topic).await?;
        }
        Ok(topic)
    }

    async fn list_topics(&self, _request: ListTopicsRequest) -> PubsubResult<ListTopicsResponse> {
        let mut topics = self.scan_json::<Topic>(TOPIC_PREFIX).await?;
        topics.sort_by(|left, right| left.topic_arn.as_str().cmp(right.topic_arn.as_str()));
        Ok(ListTopicsResponse {
            topics,
            next_token: None,
        })
    }

    async fn create_subscription(&self, request: SubscribeRequest) -> PubsubResult<Subscription> {
        if self.get_topic(&request.topic_arn).await?.is_none() {
            return Err(PubsubError::topic_not_found(request.topic_arn.to_string()));
        }
        let identity_key = subscription_identity_key(
            &request.topic_arn,
            request.protocol.as_str(),
            &request.endpoint,
        );
        if let Some(subscription_arn) = self.get_text(&identity_key).await?
            && let Some(subscription) = self
                .get_json::<Subscription>(&subscription_key_raw(&subscription_arn))
                .await?
        {
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
        self.put_json(
            &subscription_key(&subscription.subscription_arn),
            &subscription,
        )
        .await?;
        self.put_text(&identity_key, subscription.subscription_arn.as_str())
            .await?;
        self.put_text(
            &subscription_topic_key(&subscription.topic_arn, &subscription.subscription_arn),
            subscription.subscription_arn.as_str(),
        )
        .await?;
        Ok(subscription)
    }

    async fn confirm_subscription(
        &self,
        request: ConfirmSubscriptionRequest,
    ) -> PubsubResult<ConfirmSubscriptionResponse> {
        let mut subscriptions = self
            .list_subscriptions(ListSubscriptionsRequest {
                topic_arn: Some(request.topic_arn),
                next_token: None,
            })
            .await?
            .subscriptions;
        let Some(mut subscription) = subscriptions
            .drain(..)
            .find(|subscription| subscription.confirmation.token() == Some(request.token.as_str()))
        else {
            return Err(PubsubError::validation(PubsubValidationKind::InvalidToken));
        };
        subscription.confirmation = SubscriptionConfirmation::Confirmed;
        self.put_json(
            &subscription_key(&subscription.subscription_arn),
            &subscription,
        )
        .await?;
        Ok(ConfirmSubscriptionResponse {
            subscription_arn: subscription.subscription_arn,
        })
    }

    async fn delete_subscription(&self, subscription_arn: &SubscriptionArn) -> PubsubResult<()> {
        if let Some(subscription) = self.get_subscription(subscription_arn).await? {
            self.delete_key(&subscription_identity_key(
                &subscription.topic_arn,
                subscription.protocol.as_str(),
                &subscription.endpoint,
            ))
            .await?;
            self.delete_key(&subscription_topic_key(
                &subscription.topic_arn,
                &subscription.subscription_arn,
            ))
            .await?;
        }
        self.delete_key(&subscription_key(subscription_arn)).await?;
        Ok(())
    }

    async fn get_subscription(
        &self,
        subscription_arn: &SubscriptionArn,
    ) -> PubsubResult<Option<Subscription>> {
        self.get_json(&subscription_key(subscription_arn)).await
    }

    async fn set_subscription_attributes(
        &self,
        request: SetSubscriptionAttributesRequest,
    ) -> PubsubResult<Subscription> {
        let Some(mut subscription) = self.get_subscription(&request.subscription_arn).await? else {
            return Err(PubsubError::subscription_not_found(
                request.subscription_arn.to_string(),
            ));
        };
        if let Some(raw) = request.attributes.get("RawMessageDelivery") {
            subscription.raw_message_delivery = raw.eq_ignore_ascii_case("true");
            self.put_json(
                &subscription_key(&subscription.subscription_arn),
                &subscription,
            )
            .await?;
        }
        Ok(subscription)
    }

    async fn get_subscription_attributes(
        &self,
        request: GetSubscriptionAttributesRequest,
    ) -> PubsubResult<GetSubscriptionAttributesResponse> {
        let Some(subscription) = self.get_subscription(&request.subscription_arn).await? else {
            return Err(PubsubError::subscription_not_found(
                request.subscription_arn.to_string(),
            ));
        };
        Ok(GetSubscriptionAttributesResponse {
            attributes: subscription.attributes(),
        })
    }

    async fn list_subscriptions(
        &self,
        request: ListSubscriptionsRequest,
    ) -> PubsubResult<ListSubscriptionsResponse> {
        let mut subscriptions = if let Some(topic_arn) = request.topic_arn {
            let keys = self
                .scan_text(&format!(
                    "{SUBSCRIPTION_TOPIC_PREFIX}{}:",
                    topic_arn.as_str()
                ))
                .await?;
            let mut subscriptions = Vec::with_capacity(keys.len());
            for subscription_arn in keys {
                if let Some(subscription) = self
                    .get_json::<Subscription>(&subscription_key_raw(&subscription_arn))
                    .await?
                {
                    subscriptions.push(subscription);
                }
            }
            subscriptions
        } else {
            self.scan_json::<Subscription>(SUBSCRIPTION_PREFIX).await?
        };
        subscriptions.sort_by(|left, right| {
            left.subscription_arn
                .as_str()
                .cmp(right.subscription_arn.as_str())
        });
        Ok(ListSubscriptionsResponse {
            subscriptions,
            next_token: None,
        })
    }

    async fn put_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()> {
        self.put_json(&delivery_key(&record.id), &record).await
    }

    async fn claim_delivery_records(
        &self,
        request: ClaimDeliveryRecordsRequest,
    ) -> PubsubResult<ClaimDeliveryRecordsResponse> {
        let mut records = self.scan_json::<DeliveryRecord>(DELIVERY_PREFIX).await?;
        records.retain(|record| {
            matches!(
                record.status,
                pubsub_provider::DeliveryStatus::Pending
                    | pubsub_provider::DeliveryStatus::RetryScheduled
            ) && record
                .next_attempt_at
                .is_none_or(|next| next <= request.now)
                && record
                    .lease_expires_at
                    .is_none_or(|expires| expires <= request.now)
        });
        records.sort_by(|left, right| left.id.0.cmp(&right.id.0));
        records.truncate(request.limit);
        for record in &mut records {
            record.lease_owner = Some(request.owner.clone());
            record.lease_expires_at = Some(request.lease_expires_at);
            record.updated_at = request.now;
            self.put_json(&delivery_key(&record.id), record).await?;
        }
        Ok(ClaimDeliveryRecordsResponse { records })
    }

    async fn update_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()> {
        self.put_json(&delivery_key(&record.id), &record).await
    }

    async fn get_delivery_record(
        &self,
        record_id: &DeliveryRecordId,
    ) -> PubsubResult<Option<DeliveryRecord>> {
        self.get_json(&delivery_key(record_id)).await
    }
}

impl PostgresStorageProvider {
    async fn topic_by_name(&self, topic_name: &str) -> PubsubResult<Option<Topic>> {
        let Some(topic_arn) = self.get_text(&topic_name_key(topic_name)).await? else {
            return Ok(None);
        };
        self.get_json(&topic_key_raw(&topic_arn)).await
    }

    async fn put_json<T: serde::Serialize>(&self, key: &str, value: &T) -> PubsubResult<()> {
        self.put_text(key, &serde_json::to_string(value)?).await
    }

    async fn put_text(&self, key: &str, value: &str) -> PubsubResult<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| PubsubError::storage(format!("acquire postgres client: {err}")))?;
        client
            .execute(
                &format!(
                    "INSERT INTO {PUBSUB_TABLE} (key, value) VALUES ($1, $2)
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value"
                ),
                &[&key, &value],
            )
            .await
            .map_err(|err| PubsubError::storage(format!("write postgres pubsub key: {err}")))?;
        Ok(())
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, key: &str) -> PubsubResult<Option<T>> {
        self.get_text(key)
            .await?
            .map(|raw| serde_json::from_str(&raw).map_err(PubsubError::from))
            .transpose()
    }

    async fn get_text(&self, key: &str) -> PubsubResult<Option<String>> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| PubsubError::storage(format!("acquire postgres client: {err}")))?;
        let row = client
            .query_opt(
                &format!("SELECT value FROM {PUBSUB_TABLE} WHERE key = $1"),
                &[&key],
            )
            .await
            .map_err(|err| PubsubError::storage(format!("read postgres pubsub key: {err}")))?;
        row.map(|row| row.try_get("value"))
            .transpose()
            .map_err(|err| PubsubError::storage(format!("decode postgres pubsub value: {err}")))
    }

    async fn delete_key(&self, key: &str) -> PubsubResult<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| PubsubError::storage(format!("acquire postgres client: {err}")))?;
        client
            .execute(
                &format!("DELETE FROM {PUBSUB_TABLE} WHERE key = $1"),
                &[&key],
            )
            .await
            .map_err(|err| PubsubError::storage(format!("delete postgres pubsub key: {err}")))?;
        Ok(())
    }

    async fn scan_json<T: serde::de::DeserializeOwned>(
        &self,
        prefix: &str,
    ) -> PubsubResult<Vec<T>> {
        let values = self.scan_text(prefix).await?;
        values
            .into_iter()
            .map(|raw| serde_json::from_str(&raw).map_err(PubsubError::from))
            .collect()
    }

    async fn scan_text(&self, prefix: &str) -> PubsubResult<Vec<String>> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| PubsubError::storage(format!("acquire postgres client: {err}")))?;
        let like = format!("{prefix}%");
        let rows = client
            .query(
                &format!("SELECT value FROM {PUBSUB_TABLE} WHERE key LIKE $1 ORDER BY key"),
                &[&like],
            )
            .await
            .map_err(|err| PubsubError::storage(format!("scan postgres pubsub keys: {err}")))?;
        rows.into_iter()
            .map(|row| {
                row.try_get("value").map_err(|err| {
                    PubsubError::storage(format!("decode postgres pubsub value: {err}"))
                })
            })
            .collect()
    }
}

fn topic_key(topic_arn: &TopicArn) -> String {
    topic_key_raw(topic_arn.as_str())
}

fn topic_key_raw(topic_arn: &str) -> String {
    format!("{TOPIC_PREFIX}{topic_arn}")
}

fn topic_name_key(topic_name: &str) -> String {
    format!("{TOPIC_NAME_PREFIX}{topic_name}")
}

fn subscription_key(subscription_arn: &SubscriptionArn) -> String {
    subscription_key_raw(subscription_arn.as_str())
}

fn subscription_key_raw(subscription_arn: &str) -> String {
    format!("{SUBSCRIPTION_PREFIX}{subscription_arn}")
}

fn subscription_identity_key(topic_arn: &TopicArn, protocol: &str, endpoint: &str) -> String {
    format!(
        "{SUBSCRIPTION_IDENTITY_PREFIX}{}:{protocol}:{endpoint}",
        topic_arn.as_str()
    )
}

fn subscription_topic_key(topic_arn: &TopicArn, subscription_arn: &SubscriptionArn) -> String {
    format!(
        "{SUBSCRIPTION_TOPIC_PREFIX}{}:{}",
        topic_arn.as_str(),
        subscription_arn.as_str()
    )
}

fn delivery_key(id: &DeliveryRecordId) -> String {
    format!("{DELIVERY_PREFIX}{}", id.0)
}
