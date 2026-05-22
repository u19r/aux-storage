use async_trait::async_trait;
use pubsub_provider::{
    ClaimDeliveryRecordsRequest, ClaimDeliveryRecordsResponse, ConfirmSubscriptionRequest,
    ConfirmSubscriptionResponse, CreateTopicRequest, DeliveryRecord, DeliveryRecordId,
    DeliveryStatus, GetSubscriptionAttributesRequest, GetSubscriptionAttributesResponse,
    GetTopicAttributesRequest, GetTopicAttributesResponse, ListSubscriptionsRequest,
    ListSubscriptionsResponse, ListTopicsRequest, ListTopicsResponse, PubsubError, PubsubProvider,
    PubsubResult, PubsubValidationKind, SetSubscriptionAttributesRequest,
    SetTopicAttributesRequest, SubscribeRequest, Subscription, SubscriptionArn,
    SubscriptionConfirmation, SubscriptionProtocol, Topic, TopicArn, TopicName,
};
use storage_types::{StorageError, TimestampMillis};

use crate::{
    pubsub::constants::{
        CLAIM_SCAN_MAX_LIMIT, CLAIM_SCAN_MULTIPLIER, DELIVERY_CLAIM_PREFIX, DELIVERY_PREFIX,
        DELIVERY_SUBSCRIPTION_PREFIX, SUBSCRIPTION_DEDUPE_PREFIX, SUBSCRIPTION_PREFIX,
        SUBSCRIPTION_TOPIC_PREFIX, TOPIC_NAME_PREFIX, TOPIC_PREFIX,
    },
    sorted_kv::SortedKvDbStorageProvider,
    sorted_kv_store::{DirectWriteOperation, SortedKvStore},
};

#[async_trait]
impl<S> PubsubProvider for SortedKvDbStorageProvider<S>
where S: SortedKvStore + 'static
{
    async fn initialize(&self) -> PubsubResult<()> {
        Ok(())
    }

    async fn create_topic(&self, request: CreateTopicRequest) -> PubsubResult<Topic> {
        if let Some(topic) = self.get_topic_by_name(&request.name).await? {
            return Ok(topic);
        }

        let topic = Topic {
            topic_arn: TopicArn::compose("aws", "us-east-1", "000000000000", &request.name),
            name: request.name,
            display_name: request.attributes.get("DisplayName").cloned(),
            created_at: TimestampMillis::now(),
        };
        let topic_key = topic_key(&topic.topic_arn);
        let name_key = topic_name_key(&topic.name);
        let operations = vec![
            DirectWriteOperation::CheckValue {
                key: name_key.clone(),
                expected_value: None,
            },
            DirectWriteOperation::Put {
                key: topic_key,
                value: encode_pubsub(&topic)?,
            },
            DirectWriteOperation::Put {
                key: name_key,
                value: topic.topic_arn.as_str().as_bytes().to_vec(),
            },
        ];

        match self.kv_store.transact_write_unchecked(operations).await {
            Ok(()) => Ok(topic),
            Err(_) => self.get_topic_by_name(&topic.name).await?.ok_or_else(|| {
                PubsubError::storage(format!("topic creation conflicted for {}", topic.name))
            }),
        }
    }

    async fn delete_topic(&self, topic_arn: &TopicArn) -> PubsubResult<()> {
        let Some(topic) = self.get_topic(topic_arn).await? else {
            return Ok(());
        };
        let subscriptions = self
            .list_subscriptions(ListSubscriptionsRequest {
                topic_arn: Some(topic_arn.clone()),
                next_token: None,
            })
            .await?
            .subscriptions;
        let mut operations = vec![
            DirectWriteOperation::Delete {
                key: topic_key(topic_arn),
            },
            DirectWriteOperation::Delete {
                key: topic_name_key(&topic.name),
            },
        ];
        for subscription in subscriptions {
            self.push_delete_subscription_operations(&subscription, &mut operations)
                .await?;
        }
        self.kv_store
            .transact_write_unchecked(operations)
            .await
            .map_err(map_storage_error)?;
        Ok(())
    }

    async fn get_topic(&self, topic_arn: &TopicArn) -> PubsubResult<Option<Topic>> {
        self.get_record(&topic_key(topic_arn)).await
    }

    async fn get_topic_attributes(
        &self,
        request: GetTopicAttributesRequest,
    ) -> PubsubResult<GetTopicAttributesResponse> {
        let Some(topic) = self.get_topic(&request.topic_arn).await? else {
            return Err(PubsubError::topic_not_found(request.topic_arn.to_string()));
        };
        let (confirmed, pending) = self.subscription_counts(&request.topic_arn).await?;
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
        if let Some(value) = request.attributes.get("DisplayName") {
            topic.display_name = Some(value.clone());
        }
        self.kv_store
            .put(&topic_key(&topic.topic_arn), &encode_pubsub(&topic)?, None)
            .await
            .map_err(map_storage_error)?;
        Ok(topic)
    }

    async fn list_topics(&self, _request: ListTopicsRequest) -> PubsubResult<ListTopicsResponse> {
        let range = self
            .kv_store
            .get_prefix(TOPIC_PREFIX, true, None, true)
            .await
            .map_err(map_storage_error)?;
        let mut topics = Vec::with_capacity(range.items.len());
        for (_key, value) in range.items {
            topics.push(decode_pubsub::<Topic>(&value)?);
        }
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
        if let Some(subscription) = self.find_subscription_by_dedupe(&request).await? {
            return Ok(subscription);
        }

        let raw_message_delivery = request
            .attributes
            .get("RawMessageDelivery")
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        let subscription = Subscription {
            subscription_arn: SubscriptionArn::compose(&request.topic_arn),
            topic_arn: request.topic_arn,
            protocol: request.protocol,
            endpoint: request.endpoint,
            raw_message_delivery,
            confirmation: request.protocol.subscription_confirmation(),
            extra_json: request.extra_json,
            created_at: TimestampMillis::now(),
        };
        let dedupe_key = subscription_dedupe_key(
            &subscription.topic_arn,
            subscription.protocol,
            &subscription.endpoint,
        );
        let operations = vec![
            DirectWriteOperation::CheckValue {
                key: dedupe_key.clone(),
                expected_value: None,
            },
            DirectWriteOperation::Put {
                key: subscription_key(&subscription.subscription_arn),
                value: encode_pubsub(&subscription)?,
            },
            DirectWriteOperation::Put {
                key: subscription_topic_key(
                    &subscription.topic_arn,
                    &subscription.subscription_arn,
                ),
                value: encode_pubsub(&subscription)?,
            },
            DirectWriteOperation::Put {
                key: dedupe_key,
                value: subscription.subscription_arn.as_str().as_bytes().to_vec(),
            },
        ];

        match self.kv_store.transact_write_unchecked(operations).await {
            Ok(()) => Ok(subscription),
            Err(_) => self
                .find_subscription_by_identity(
                    &subscription.topic_arn,
                    subscription.protocol,
                    &subscription.endpoint,
                )
                .await?
                .ok_or_else(|| PubsubError::storage("subscription creation conflicted")),
        }
    }

    async fn confirm_subscription(
        &self,
        request: ConfirmSubscriptionRequest,
    ) -> PubsubResult<ConfirmSubscriptionResponse> {
        let subscriptions = self
            .list_subscriptions_for_topic(&request.topic_arn)
            .await?;
        let Some(mut subscription) = subscriptions
            .into_iter()
            .find(|subscription| subscription.confirmation.token() == Some(request.token.as_str()))
        else {
            return Err(PubsubError::validation(PubsubValidationKind::InvalidToken));
        };
        subscription.confirmation = SubscriptionConfirmation::Confirmed;
        self.write_subscription_record(&subscription).await?;
        Ok(ConfirmSubscriptionResponse {
            subscription_arn: subscription.subscription_arn,
        })
    }

    async fn delete_subscription(&self, subscription_arn: &SubscriptionArn) -> PubsubResult<()> {
        let Some(subscription) = self.get_subscription(subscription_arn).await? else {
            return Ok(());
        };
        let mut operations = Vec::new();
        self.push_delete_subscription_operations(&subscription, &mut operations)
            .await?;
        self.kv_store
            .transact_write_unchecked(operations)
            .await
            .map_err(map_storage_error)?;
        Ok(())
    }

    async fn get_subscription(
        &self,
        subscription_arn: &SubscriptionArn,
    ) -> PubsubResult<Option<Subscription>> {
        self.get_record(&subscription_key(subscription_arn)).await
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
        if let Some(value) = request.attributes.get("RawMessageDelivery") {
            subscription.raw_message_delivery = value.eq_ignore_ascii_case("true");
        }
        self.write_subscription_record(&subscription).await?;
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
            self.list_subscriptions_for_topic(&topic_arn).await?
        } else {
            let range = self
                .kv_store
                .get_prefix(SUBSCRIPTION_PREFIX, true, None, true)
                .await
                .map_err(map_storage_error)?;
            let mut found = Vec::with_capacity(range.items.len());
            for (_key, value) in range.items {
                found.push(decode_pubsub::<Subscription>(&value)?);
            }
            found
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
        self.upsert_delivery_record(record).await
    }

    async fn put_delivery_records(&self, records: Vec<DeliveryRecord>) -> PubsubResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        let keys = records
            .iter()
            .map(|record| delivery_key(&record.id))
            .collect::<Vec<_>>();
        let previous_values = self
            .kv_store
            .multi_get(keys, true)
            .await
            .map_err(map_storage_error)?;
        let mut operations = Vec::with_capacity(records.len() * 4);
        for (record, previous_value) in records.into_iter().zip(previous_values) {
            let previous = previous_value
                .as_deref()
                .map(decode_pubsub::<DeliveryRecord>)
                .transpose()?;
            push_delivery_record_operations(record, previous, &mut operations)?;
        }
        self.kv_store
            .transact_write_unchecked(operations)
            .await
            .map_err(map_storage_error)?;
        Ok(())
    }

    async fn claim_delivery_records(
        &self,
        request: ClaimDeliveryRecordsRequest,
    ) -> PubsubResult<ClaimDeliveryRecordsResponse> {
        let mut claimed = Vec::new();
        for status in [DeliveryStatus::Pending, DeliveryStatus::RetryScheduled] {
            if claimed.len() >= request.limit {
                break;
            }
            let candidates = self
                .claim_candidates(
                    status,
                    request.now,
                    request.limit.saturating_sub(claimed.len()),
                )
                .await?;
            for mut record in candidates {
                if claimed.len() >= request.limit {
                    break;
                }
                if !record.is_claimable(request.now) {
                    continue;
                }
                let old_record = encode_pubsub(&record)?;
                record.lease_owner = Some(request.owner.clone());
                record.lease_expires_at = Some(request.lease_expires_at);
                record.updated_at = request.now;
                let operations = vec![
                    DirectWriteOperation::CheckValue {
                        key: delivery_key(&record.id),
                        expected_value: Some(old_record),
                    },
                    DirectWriteOperation::Put {
                        key: delivery_key(&record.id),
                        value: encode_pubsub(&record)?,
                    },
                ];
                if self
                    .kv_store
                    .transact_write_unchecked(operations)
                    .await
                    .is_ok()
                {
                    claimed.push(record);
                }
            }
        }
        claimed.sort_by(|left, right| left.id.0.cmp(&right.id.0));
        Ok(ClaimDeliveryRecordsResponse { records: claimed })
    }

    async fn update_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()> {
        self.upsert_delivery_record(record).await
    }

    async fn get_delivery_record(
        &self,
        record_id: &DeliveryRecordId,
    ) -> PubsubResult<Option<DeliveryRecord>> {
        self.get_record(&delivery_key(record_id)).await
    }
}

impl<S> SortedKvDbStorageProvider<S>
where S: SortedKvStore + 'static
{
    async fn get_record<T>(&self, key: &[u8]) -> PubsubResult<Option<T>>
    where T: serde::de::DeserializeOwned {
        match self
            .kv_store
            .get(key, true)
            .await
            .map_err(map_storage_error)?
        {
            Some(value) => Ok(Some(decode_pubsub(&value)?)),
            None => Ok(None),
        }
    }

    async fn get_topic_by_name(&self, name: &TopicName) -> PubsubResult<Option<Topic>> {
        let Some(topic_arn_bytes) = self
            .kv_store
            .get(&topic_name_key(name), true)
            .await
            .map_err(map_storage_error)?
        else {
            return Ok(None);
        };
        let topic_arn_text = String::from_utf8(topic_arn_bytes)
            .map_err(|error| PubsubError::storage(format!("invalid topic name index: {error}")))?;
        let topic_arn = TopicArn::new(topic_arn_text)?;
        self.get_topic(&topic_arn).await
    }

    async fn find_subscription_by_dedupe(
        &self,
        request: &SubscribeRequest,
    ) -> PubsubResult<Option<Subscription>> {
        self.find_subscription_by_identity(&request.topic_arn, request.protocol, &request.endpoint)
            .await
    }

    async fn find_subscription_by_identity(
        &self,
        topic_arn: &TopicArn,
        protocol: SubscriptionProtocol,
        endpoint: &str,
    ) -> PubsubResult<Option<Subscription>> {
        let Some(subscription_arn_bytes) = self
            .kv_store
            .get(
                &subscription_dedupe_key(topic_arn, protocol, endpoint),
                true,
            )
            .await
            .map_err(map_storage_error)?
        else {
            return Ok(None);
        };
        let subscription_arn_text = String::from_utf8(subscription_arn_bytes).map_err(|error| {
            PubsubError::storage(format!("invalid subscription dedupe index: {error}"))
        })?;
        let subscription_arn = SubscriptionArn::new(subscription_arn_text)?;
        self.get_subscription(&subscription_arn).await
    }

    async fn list_subscriptions_for_topic(
        &self,
        topic_arn: &TopicArn,
    ) -> PubsubResult<Vec<Subscription>> {
        let range = self
            .kv_store
            .get_prefix(&subscription_topic_prefix(topic_arn), true, None, true)
            .await
            .map_err(map_storage_error)?;
        let mut subscriptions = Vec::with_capacity(range.items.len());
        for (key, value) in range.items {
            if let Ok(subscription) = decode_pubsub::<Subscription>(&value) {
                subscriptions.push(subscription);
                continue;
            }
            if let Some(subscription_arn) = subscription_arn_from_topic_index_key(&key)?
                && let Some(subscription) = self.get_subscription(&subscription_arn).await?
            {
                subscriptions.push(subscription);
            }
        }
        Ok(subscriptions)
    }

    async fn write_subscription_record(&self, subscription: &Subscription) -> PubsubResult<()> {
        let encoded = encode_pubsub(subscription)?;
        self.kv_store
            .transact_write_unchecked(vec![
                DirectWriteOperation::Put {
                    key: subscription_key(&subscription.subscription_arn),
                    value: encoded.clone(),
                },
                DirectWriteOperation::Put {
                    key: subscription_topic_key(
                        &subscription.topic_arn,
                        &subscription.subscription_arn,
                    ),
                    value: encoded,
                },
            ])
            .await
            .map_err(map_storage_error)?;
        Ok(())
    }

    async fn subscription_counts(&self, topic_arn: &TopicArn) -> PubsubResult<(usize, usize)> {
        let subscriptions = self.list_subscriptions_for_topic(topic_arn).await?;
        let mut confirmed = 0usize;
        let mut pending = 0usize;
        for subscription in subscriptions {
            if subscription.confirmation.pending_confirmation() {
                pending += 1;
            } else {
                confirmed += 1;
            }
        }
        Ok((confirmed, pending))
    }

    async fn push_delete_subscription_operations(
        &self,
        subscription: &Subscription,
        operations: &mut Vec<DirectWriteOperation>,
    ) -> PubsubResult<()> {
        operations.push(DirectWriteOperation::Delete {
            key: subscription_key(&subscription.subscription_arn),
        });
        operations.push(DirectWriteOperation::Delete {
            key: subscription_topic_key(&subscription.topic_arn, &subscription.subscription_arn),
        });
        operations.push(DirectWriteOperation::Delete {
            key: subscription_dedupe_key(
                &subscription.topic_arn,
                subscription.protocol,
                &subscription.endpoint,
            ),
        });

        let range = self
            .kv_store
            .get_prefix(
                &delivery_subscription_prefix(&subscription.subscription_arn),
                true,
                None,
                true,
            )
            .await
            .map_err(map_storage_error)?;
        for (key, value) in range.items {
            let record_id_text = String::from_utf8(value.into_vec()).map_err(|error| {
                PubsubError::storage(format!("invalid delivery subscription index: {error}"))
            })?;
            let record_id = DeliveryRecordId(record_id_text);
            if let Some(record) = self.get_delivery_record(&record_id).await? {
                if let Some(claim_key) = delivery_claim_key_for_record(&record) {
                    operations.push(DirectWriteOperation::Delete { key: claim_key });
                }
                operations.push(DirectWriteOperation::Delete {
                    key: delivery_key(&record.id),
                });
            }
            operations.push(DirectWriteOperation::Delete {
                key: key.into_vec(),
            });
        }
        Ok(())
    }

    async fn upsert_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()> {
        let previous = self.get_delivery_record(&record.id).await?;
        let mut operations = Vec::new();
        push_delivery_record_operations(record, previous, &mut operations)?;
        self.kv_store
            .transact_write_unchecked(operations)
            .await
            .map_err(map_storage_error)?;
        Ok(())
    }

    async fn claim_candidates(
        &self,
        status: DeliveryStatus,
        now: TimestampMillis,
        remaining: usize,
    ) -> PubsubResult<Vec<DeliveryRecord>> {
        let limit = remaining
            .saturating_mul(CLAIM_SCAN_MULTIPLIER)
            .max(remaining)
            .min(CLAIM_SCAN_MAX_LIMIT as usize) as u32;
        if limit == 0 {
            return Ok(Vec::new());
        }

        let range = self
            .kv_store
            .get_prefix(
                &delivery_claim_status_prefix(status),
                true,
                Some(limit),
                true,
            )
            .await
            .map_err(map_storage_error)?;
        let mut records = Vec::new();
        for (key, value) in range.items {
            let Some(due_at) = claim_due_at_from_key(&key)? else {
                continue;
            };
            if due_at > now {
                break;
            }
            let record_id_text = String::from_utf8(value.into_vec()).map_err(|error| {
                PubsubError::storage(format!("invalid delivery claim index: {error}"))
            })?;
            let record_id = DeliveryRecordId(record_id_text);
            if let Some(record) = self.get_delivery_record(&record_id).await? {
                records.push(record);
            }
        }
        Ok(records)
    }
}

fn push_delivery_record_operations(
    record: DeliveryRecord,
    previous: Option<DeliveryRecord>,
    operations: &mut Vec<DirectWriteOperation>,
) -> PubsubResult<()> {
    if let Some(previous) = previous {
        if let Some(previous_claim_key) = delivery_claim_key_for_record(&previous) {
            operations.push(DirectWriteOperation::Delete {
                key: previous_claim_key,
            });
        }
        operations.push(DirectWriteOperation::Delete {
            key: delivery_subscription_key(&previous.subscription_arn, &previous.id),
        });
    }
    if let Some(claim_key) = delivery_claim_key_for_record(&record) {
        operations.push(DirectWriteOperation::Put {
            key: claim_key,
            value: record.id.0.as_bytes().to_vec(),
        });
    }
    operations.push(DirectWriteOperation::Put {
        key: delivery_subscription_key(&record.subscription_arn, &record.id),
        value: record.id.0.as_bytes().to_vec(),
    });
    operations.push(DirectWriteOperation::Put {
        key: delivery_key(&record.id),
        value: encode_pubsub(&record)?,
    });
    Ok(())
}

fn topic_key(topic_arn: &TopicArn) -> Vec<u8> {
    prefixed_key(TOPIC_PREFIX, topic_arn.as_str())
}

fn topic_name_key(topic_name: &TopicName) -> Vec<u8> {
    prefixed_key(TOPIC_NAME_PREFIX, topic_name.as_str())
}

fn subscription_key(subscription_arn: &SubscriptionArn) -> Vec<u8> {
    prefixed_key(SUBSCRIPTION_PREFIX, subscription_arn.as_str())
}

fn subscription_topic_prefix(topic_arn: &TopicArn) -> Vec<u8> {
    nested_prefix(SUBSCRIPTION_TOPIC_PREFIX, topic_arn.as_str())
}

fn subscription_topic_key(topic_arn: &TopicArn, subscription_arn: &SubscriptionArn) -> Vec<u8> {
    nested_key(
        SUBSCRIPTION_TOPIC_PREFIX,
        topic_arn.as_str(),
        subscription_arn.as_str(),
    )
}

fn subscription_dedupe_key(
    topic_arn: &TopicArn,
    protocol: SubscriptionProtocol,
    endpoint: &str,
) -> Vec<u8> {
    nested_key(
        SUBSCRIPTION_DEDUPE_PREFIX,
        topic_arn.as_str(),
        &format!("{}:{endpoint}", protocol.as_str()),
    )
}

fn delivery_key(record_id: &DeliveryRecordId) -> Vec<u8> {
    prefixed_key(DELIVERY_PREFIX, &record_id.0)
}

fn delivery_subscription_prefix(subscription_arn: &SubscriptionArn) -> Vec<u8> {
    nested_prefix(DELIVERY_SUBSCRIPTION_PREFIX, subscription_arn.as_str())
}

fn delivery_subscription_key(
    subscription_arn: &SubscriptionArn,
    record_id: &DeliveryRecordId,
) -> Vec<u8> {
    nested_key(
        DELIVERY_SUBSCRIPTION_PREFIX,
        subscription_arn.as_str(),
        &record_id.0,
    )
}

fn delivery_claim_status_prefix(status: DeliveryStatus) -> Vec<u8> {
    nested_prefix(DELIVERY_CLAIM_PREFIX, delivery_status_key(status))
}

fn delivery_claim_key_for_record(record: &DeliveryRecord) -> Option<Vec<u8>> {
    if !matches!(
        record.status,
        DeliveryStatus::Pending | DeliveryStatus::RetryScheduled
    ) {
        return None;
    }
    let due_at = record
        .next_attempt_at
        .map_or(0, |timestamp| timestamp.timestamp_millis());
    Some(nested_key(
        DELIVERY_CLAIM_PREFIX,
        delivery_status_key(record.status),
        &format!("{due_at:020}:{}", record.id.0),
    ))
}

fn subscription_arn_from_topic_index_key(key: &[u8]) -> PubsubResult<Option<SubscriptionArn>> {
    let key_text = String::from_utf8(key.to_vec()).map_err(|error| {
        PubsubError::storage(format!("invalid subscription topic key: {error}"))
    })?;
    let Some((_, subscription_arn)) = key_text.rsplit_once('/') else {
        return Ok(None);
    };
    Ok(Some(SubscriptionArn::new(subscription_arn.to_string())?))
}

fn claim_due_at_from_key(key: &[u8]) -> PubsubResult<Option<TimestampMillis>> {
    let key_text = String::from_utf8(key.to_vec())
        .map_err(|error| PubsubError::storage(format!("invalid delivery claim key: {error}")))?;
    let Some((_, suffix)) = key_text.rsplit_once('/') else {
        return Ok(None);
    };
    let Some((due_text, _)) = suffix.split_once(':') else {
        return Ok(None);
    };
    let due_at = due_text.parse::<i64>().map_err(|error| {
        PubsubError::storage(format!("invalid delivery claim due time: {error}"))
    })?;
    Ok(Some(TimestampMillis::from(due_at)))
}

fn delivery_status_key(status: DeliveryStatus) -> &'static str {
    match status {
        DeliveryStatus::Pending => "pending",
        DeliveryStatus::Delivered => "delivered",
        DeliveryStatus::AcceptedByCustomSender => "accepted_by_custom_sender",
        DeliveryStatus::RetryScheduled => "retry_scheduled",
        DeliveryStatus::Failed => "failed",
    }
}

fn prefixed_key(prefix: &[u8], value: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + value.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(value.as_bytes());
    key
}

fn nested_key(prefix: &[u8], left: &str, right: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + left.len() + 1 + right.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(left.as_bytes());
    key.push(b'/');
    key.extend_from_slice(right.as_bytes());
    key
}

fn nested_prefix(prefix: &[u8], value: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + value.len() + 1);
    key.extend_from_slice(prefix);
    key.extend_from_slice(value.as_bytes());
    key.push(b'/');
    key
}

fn encode_pubsub<T>(value: &T) -> PubsubResult<Vec<u8>>
where T: serde::Serialize {
    storage_types::storage_serde::to_bytes(value).map_err(map_storage_error)
}

fn decode_pubsub<T>(bytes: &[u8]) -> PubsubResult<T>
where T: serde::de::DeserializeOwned {
    storage_types::storage_serde::from_bytes(bytes).map_err(map_storage_error)
}

fn map_storage_error(error: StorageError) -> PubsubError {
    PubsubError::storage(error)
}
