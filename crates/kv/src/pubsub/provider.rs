use async_trait::async_trait;
use futures_util::future::join_all;
use pubsub_provider::{
    ClaimDeliveryRecordsRequest, ClaimDeliveryRecordsResponse, ConfirmSubscriptionRequest,
    ConfirmSubscriptionResponse, CreateTopicRequest, DeliveryRecord, DeliveryRecordId,
    DeliveryStatus, GetSubscriptionAttributesRequest, GetSubscriptionAttributesResponse,
    GetTopicAttributesRequest, GetTopicAttributesResponse, ListSubscriptionsRequest,
    ListSubscriptionsResponse, ListTopicsRequest, ListTopicsResponse, PublishRequest, PubsubError,
    PubsubMessageId, PubsubProvider, PubsubResult, PubsubValidationKind,
    SetSubscriptionAttributesRequest, SetTopicAttributesRequest, SubscribeRequest, Subscription,
    SubscriptionArn, SubscriptionConfirmation, SubscriptionProtocol, Topic, TopicArn, TopicName,
};
use serde::{Deserialize, Serialize};
use storage_types::{StorageEnum, StorageError, TimestampMillis};
use uuid::Uuid;

use crate::{
    keyspace::compact::{self, PubsubRecordKind, U48},
    pubsub::constants::{CLAIM_SCAN_MAX_LIMIT, CLAIM_SCAN_MULTIPLIER},
    sorted_kv::SortedKvDbStorageProvider,
    sorted_kv_store::{DirectWriteOperation, SortedKvStore},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct PubsubStorageId(U48);

const SUBSCRIPTION_SNAPSHOT_CHUNK_SIZE: usize = 10;
const SUBSCRIPTION_SNAPSHOT_PUBLISH_ATTEMPTS: usize = 16;
const PUBLISH_INTENT_LEASE_MILLIS: i64 = 30_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SubscriptionSnapshotRoot {
    generation: String,
    chunk_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSubscriptionSnapshot {
    subscription_id: PubsubStorageId,
    subscription: Subscription,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PublishIntent {
    topic_id: PubsubStorageId,
    message_id: PubsubMessageId,
    request: PublishRequest,
    snapshot: SubscriptionSnapshotRoot,
    custom_sender_enabled: bool,
    next_chunk: u32,
    lease_expires_at: Option<TimestampMillis>,
}

impl PubsubStorageId {
    fn new(value: u64) -> PubsubResult<Self> {
        U48::new(value)
            .map(Self)
            .map_err(|error| PubsubError::storage(format!("invalid pubsub storage id: {error}")))
    }

    const fn get(self) -> U48 {
        self.0
    }
}

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
        let topic_id = self
            .allocate_pubsub_id(compact::topic_id_allocator_key())
            .await?;
        let topic_key = topic_key_for_id(topic_id);
        let arn_key = topic_arn_lookup_key(&topic.topic_arn);
        let name_key = topic_name_key(&topic.name);
        let topic_id_bytes = encode_pubsub_storage_id(topic_id);
        let snapshot = SubscriptionSnapshotRoot {
            generation: Uuid::now_v7().simple().to_string(),
            chunk_count: 0,
        };
        let operations = vec![
            DirectWriteOperation::CheckValue {
                key: name_key.clone(),
                expected_value: None,
            },
            DirectWriteOperation::CheckValue {
                key: arn_key.clone(),
                expected_value: None,
            },
            DirectWriteOperation::Put {
                key: topic_key,
                value: encode_pubsub(&topic)?,
            },
            DirectWriteOperation::Put {
                key: arn_key,
                value: topic_id_bytes.clone(),
            },
            DirectWriteOperation::Put {
                key: name_key,
                value: topic_id_bytes,
            },
            DirectWriteOperation::Put {
                key: subscription_snapshot_root_key(topic_id),
                value: encode_pubsub(&snapshot)?,
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
        let topic_id = self
            .id_from_lookup(topic_arn_lookup_key(topic_arn))
            .await?
            .ok_or_else(|| PubsubError::topic_not_found(topic_arn.to_string()))?;
        let snapshot = self
            .kv_store
            .get(&subscription_snapshot_root_key(topic_id), true)
            .await
            .map_err(map_storage_error)?
            .map(|bytes| decode_pubsub::<SubscriptionSnapshotRoot>(&bytes))
            .transpose()?;
        let subscriptions = self
            .list_subscriptions(ListSubscriptionsRequest {
                topic_arn: Some(topic_arn.clone()),
                next_token: None,
            })
            .await?
            .subscriptions;
        let mut operations = vec![
            DirectWriteOperation::Delete {
                key: topic_arn_lookup_key(topic_arn),
            },
            DirectWriteOperation::Delete {
                key: topic_name_key(&topic.name),
            },
            DirectWriteOperation::Delete {
                key: topic_key_for_id(topic_id),
            },
            DirectWriteOperation::Delete {
                key: subscription_snapshot_root_key(topic_id),
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
        if let Some(snapshot) = snapshot {
            self.cleanup_subscription_snapshot_if_unreferenced(topic_id, &snapshot)
                .await?;
        }
        Ok(())
    }

    async fn get_topic(&self, topic_arn: &TopicArn) -> PubsubResult<Option<Topic>> {
        let Some(topic_id) = self.id_from_lookup(topic_arn_lookup_key(topic_arn)).await? else {
            return Ok(None);
        };
        self.get_record(&topic_key_for_id(topic_id)).await
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
        let topic_id = self
            .id_from_lookup(topic_arn_lookup_key(&topic.topic_arn))
            .await?
            .ok_or_else(|| PubsubError::topic_not_found(topic.topic_arn.to_string()))?;
        self.kv_store
            .put(&topic_key_for_id(topic_id), &encode_pubsub(&topic)?, None)
            .await
            .map_err(map_storage_error)?;
        Ok(topic)
    }

    async fn list_topics(&self, _request: ListTopicsRequest) -> PubsubResult<ListTopicsResponse> {
        let range = self
            .kv_store
            .get_prefix(
                &compact::pubsub_kind_prefix(PubsubRecordKind::Topic).start,
                true,
                None,
                true,
            )
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
        let topic_id = self
            .id_from_lookup(topic_arn_lookup_key(&subscription.topic_arn))
            .await?
            .ok_or_else(|| PubsubError::topic_not_found(subscription.topic_arn.to_string()))?;
        let subscription_id = self
            .allocate_pubsub_id(compact::subscription_id_allocator_key())
            .await?;
        let dedupe_key =
            subscription_dedupe_key_for_id(topic_id, subscription.protocol, &subscription.endpoint);
        let arn_key = subscription_arn_lookup_key(&subscription.subscription_arn);
        let subscription_id_bytes = encode_pubsub_storage_id(subscription_id);
        let operations = vec![
            DirectWriteOperation::CheckValue {
                key: dedupe_key.clone(),
                expected_value: None,
            },
            DirectWriteOperation::CheckValue {
                key: arn_key.clone(),
                expected_value: None,
            },
            DirectWriteOperation::Put {
                key: subscription_key_for_id(subscription_id),
                value: encode_pubsub(&subscription)?,
            },
            DirectWriteOperation::Put {
                key: subscription_topic_key_for_id(topic_id, subscription_id),
                value: subscription_id_bytes.clone(),
            },
            DirectWriteOperation::Put {
                key: arn_key,
                value: subscription_id_bytes.clone(),
            },
            DirectWriteOperation::Put {
                key: dedupe_key,
                value: subscription_id_bytes,
            },
        ];

        let stored = match self.kv_store.transact_write_unchecked(operations).await {
            Ok(()) => subscription,
            Err(_) => self
                .find_subscription_by_identity(
                    &subscription.topic_arn,
                    subscription.protocol,
                    &subscription.endpoint,
                )
                .await?
                .ok_or_else(|| PubsubError::storage("subscription creation conflicted"))?,
        };
        self.publish_subscription_snapshot(&stored.topic_arn)
            .await?;
        Ok(stored)
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
        self.publish_subscription_snapshot(&subscription.topic_arn)
            .await?;
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
        self.publish_subscription_snapshot(&subscription.topic_arn)
            .await?;
        Ok(())
    }

    async fn get_subscription(
        &self,
        subscription_arn: &SubscriptionArn,
    ) -> PubsubResult<Option<Subscription>> {
        let Some(subscription_id) = self
            .id_from_lookup(subscription_arn_lookup_key(subscription_arn))
            .await?
        else {
            return Ok(None);
        };
        self.get_record(&subscription_key_for_id(subscription_id))
            .await
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
        self.publish_subscription_snapshot(&subscription.topic_arn)
            .await?;
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
                .get_prefix(
                    &compact::pubsub_kind_prefix(PubsubRecordKind::Subscription).start,
                    true,
                    None,
                    true,
                )
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

    async fn accept_publish(
        &self,
        request: PublishRequest,
        message_id: PubsubMessageId,
        custom_sender_enabled: bool,
    ) -> PubsubResult<()> {
        let topic_id = self
            .id_from_lookup(topic_arn_lookup_key(&request.topic_arn))
            .await?
            .ok_or_else(|| PubsubError::topic_not_found(request.topic_arn.to_string()))?;
        let root_key = subscription_snapshot_root_key(topic_id);
        let intent_key = publish_intent_key(&message_id);
        for _ in 0..SUBSCRIPTION_SNAPSHOT_PUBLISH_ATTEMPTS {
            let snapshot_bytes = self
                .kv_store
                .get(&root_key, true)
                .await
                .map_err(map_storage_error)?
                .ok_or_else(|| PubsubError::storage("topic subscription snapshot is missing"))?;
            let intent = PublishIntent {
                topic_id,
                message_id: message_id.clone(),
                request: request.clone(),
                snapshot: decode_pubsub(&snapshot_bytes)?,
                custom_sender_enabled,
                next_chunk: 0,
                lease_expires_at: None,
            };
            match self
                .kv_store
                .transact_write_unchecked(vec![
                    DirectWriteOperation::CheckValue {
                        key: root_key.clone(),
                        expected_value: Some(snapshot_bytes),
                    },
                    DirectWriteOperation::CheckValue {
                        key: intent_key.clone(),
                        expected_value: None,
                    },
                    DirectWriteOperation::Put {
                        key: intent_key.clone(),
                        value: encode_pubsub(&intent)?,
                    },
                ])
                .await
            {
                Ok(()) => return Ok(()),
                Err(error)
                    if matches!(
                        error.as_ref(),
                        StorageEnum::ConditionalCheckFailed
                            | StorageEnum::TransactionCanceled { .. }
                    ) => {}
                Err(error) => return Err(map_storage_error(error)),
            }
        }
        Err(PubsubError::storage(format!(
            "publish acceptance remained contended for {}",
            request.topic_arn
        )))
    }

    async fn materialize_publish_intents(&self, limit: usize) -> PubsubResult<usize> {
        let range = self
            .kv_store
            .get_prefix(
                &compact::pubsub_kind_prefix(PubsubRecordKind::PublishIntent).start,
                true,
                Some(u32::try_from(limit.max(1)).unwrap_or(u32::MAX)),
                true,
            )
            .await
            .map_err(map_storage_error)?;
        let now = TimestampMillis::now();
        let mut materialized = 0usize;
        for (key, bytes) in range.items {
            let intent_key = key.to_vec();
            let mut intent = decode_pubsub::<PublishIntent>(&bytes)?;
            if intent
                .lease_expires_at
                .is_some_and(|lease_expires_at| lease_expires_at > now)
            {
                continue;
            }
            intent.lease_expires_at = Some(TimestampMillis::from_timestamp(
                now.timestamp_millis()
                    .saturating_add(PUBLISH_INTENT_LEASE_MILLIS),
            ));
            let claimed_bytes = encode_pubsub(&intent)?;
            if self
                .kv_store
                .transact_write_unchecked(vec![
                    DirectWriteOperation::CheckValue {
                        key: intent_key.clone(),
                        expected_value: Some(bytes.to_vec()),
                    },
                    DirectWriteOperation::Put {
                        key: intent_key.clone(),
                        value: claimed_bytes.clone(),
                    },
                ])
                .await
                .is_err()
            {
                continue;
            }
            let mut operations = Vec::new();
            let mut chunk_delivery_count = 0usize;
            if intent.next_chunk < intent.snapshot.chunk_count {
                let chunk_key = subscription_snapshot_chunk_key(
                    intent.topic_id,
                    &intent.snapshot.generation,
                    intent.next_chunk,
                );
                let snapshots = self
                    .kv_store
                    .get(&chunk_key, true)
                    .await
                    .map_err(map_storage_error)?
                    .map(|bytes| decode_pubsub::<Vec<StoredSubscriptionSnapshot>>(&bytes))
                    .transpose()?
                    .ok_or_else(|| PubsubError::storage("publish snapshot chunk is missing"))?;
                let records = snapshots
                    .into_iter()
                    .filter(|snapshot| !snapshot.subscription.confirmation.pending_confirmation())
                    .map(|snapshot| {
                        let record = DeliveryRecord::pending_notification(
                            &intent.request,
                            &intent.message_id,
                            &snapshot.subscription,
                            intent.custom_sender_enabled,
                        );
                        (record, snapshot.subscription_id)
                    })
                    .collect::<Vec<_>>();
                chunk_delivery_count = records.len();
                operations = self.snapshot_delivery_record_operations(records).await?;
                intent.next_chunk = intent.next_chunk.saturating_add(1);
            }
            intent.lease_expires_at = None;
            operations.push(DirectWriteOperation::CheckValue {
                key: intent_key.clone(),
                expected_value: Some(claimed_bytes),
            });
            let completed = intent.next_chunk >= intent.snapshot.chunk_count;
            if completed {
                operations.push(DirectWriteOperation::Delete { key: intent_key });
            } else {
                operations.push(DirectWriteOperation::Put {
                    key: intent_key,
                    value: encode_pubsub(&intent)?,
                });
            }
            self.kv_store
                .transact_write_unchecked(operations)
                .await
                .map_err(map_storage_error)?;
            materialized = materialized.saturating_add(chunk_delivery_count);
            if completed {
                self.cleanup_subscription_snapshot_if_unreferenced(
                    intent.topic_id,
                    &intent.snapshot,
                )
                .await?;
            }
        }
        Ok(materialized)
    }

    async fn put_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()> {
        self.upsert_delivery_record(record).await
    }

    async fn put_delivery_records(&self, records: Vec<DeliveryRecord>) -> PubsubResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut operations = Vec::with_capacity(records.len() * 4);
        for record in records {
            let previous = self.get_delivery_record(&record.id).await?;
            let delivery_id = match self
                .id_from_lookup(delivery_record_lookup_key(&record.id))
                .await?
            {
                Some(id) => id,
                None => {
                    self.allocate_pubsub_id(compact::delivery_id_allocator_key())
                        .await?
                }
            };
            let subscription_id = self
                .ensure_subscription_id_for_arn(&record.subscription_arn)
                .await?;
            push_delivery_record_operations(
                record,
                previous,
                delivery_id,
                subscription_id,
                &mut operations,
            )?;
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
            let mut claims = Vec::with_capacity(candidates.len().min(request.limit));
            for (delivery_id, mut record) in candidates {
                if !record.is_claimable(request.now)
                    || claims.len() >= request.limit.saturating_sub(claimed.len())
                {
                    continue;
                }
                let old_record = encode_pubsub(&record)?;
                record.lease_owner = Some(request.owner.clone());
                record.lease_expires_at = Some(request.lease_expires_at);
                record.updated_at = request.now;
                let operations = vec![
                    DirectWriteOperation::CheckValue {
                        key: delivery_key_for_id(delivery_id),
                        expected_value: Some(old_record),
                    },
                    DirectWriteOperation::Put {
                        key: delivery_key_for_id(delivery_id),
                        value: encode_pubsub(&record)?,
                    },
                ];
                claims.push(async move {
                    self.kv_store
                        .transact_write_unchecked(operations)
                        .await
                        .is_ok()
                        .then_some(record)
                });
            }
            for record in join_all(claims).await.into_iter().flatten() {
                if claimed.len() < request.limit {
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
        let Some(delivery_id) = self
            .id_from_lookup(delivery_record_lookup_key(record_id))
            .await?
        else {
            return Ok(None);
        };
        self.get_record(&delivery_key_for_id(delivery_id)).await
    }
}

impl<S> SortedKvDbStorageProvider<S>
where S: SortedKvStore + 'static
{
    async fn allocate_pubsub_id(&self, allocator_key: Vec<u8>) -> PubsubResult<PubsubStorageId> {
        let allocator_value = self
            .kv_store
            .get(&allocator_key, true)
            .await
            .map_err(map_storage_error)?;
        let id = match allocator_value.as_deref() {
            Some(bytes) => decode_pubsub_storage_id(bytes)?,
            None => PubsubStorageId::new(1)?,
        };
        let next_id = PubsubStorageId::new(id.get().get().saturating_add(1))?;
        self.kv_store
            .transact_write_unchecked(vec![
                DirectWriteOperation::CheckValue {
                    key: allocator_key.clone(),
                    expected_value: allocator_value,
                },
                DirectWriteOperation::Put {
                    key: allocator_key,
                    value: encode_pubsub_storage_id(next_id),
                },
            ])
            .await
            .map_err(map_storage_error)?;
        Ok(id)
    }

    async fn id_from_lookup(&self, key: Vec<u8>) -> PubsubResult<Option<PubsubStorageId>> {
        self.kv_store
            .get(&key, true)
            .await
            .map_err(map_storage_error)?
            .as_deref()
            .map(decode_pubsub_storage_id)
            .transpose()
    }

    async fn ensure_subscription_id_for_arn(
        &self,
        subscription_arn: &SubscriptionArn,
    ) -> PubsubResult<PubsubStorageId> {
        if let Some(subscription_id) = self
            .id_from_lookup(subscription_arn_lookup_key(subscription_arn))
            .await?
        {
            return Ok(subscription_id);
        }
        let subscription_id = self
            .allocate_pubsub_id(compact::subscription_id_allocator_key())
            .await?;
        self.kv_store
            .transact_write_unchecked(vec![
                DirectWriteOperation::CheckValue {
                    key: subscription_arn_lookup_key(subscription_arn),
                    expected_value: None,
                },
                DirectWriteOperation::Put {
                    key: subscription_arn_lookup_key(subscription_arn),
                    value: encode_pubsub_storage_id(subscription_id),
                },
            ])
            .await
            .map_err(map_storage_error)?;
        Ok(subscription_id)
    }

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
        let Some(topic_id) = self.id_from_lookup(topic_name_key(name)).await? else {
            return Ok(None);
        };
        self.get_record(&topic_key_for_id(topic_id)).await
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
        let Some(topic_id) = self.id_from_lookup(topic_arn_lookup_key(topic_arn)).await? else {
            return Ok(None);
        };
        let Some(subscription_id) = self
            .id_from_lookup(subscription_dedupe_key_for_id(topic_id, protocol, endpoint))
            .await?
        else {
            return Ok(None);
        };
        self.get_record(&subscription_key_for_id(subscription_id))
            .await
    }

    async fn list_subscriptions_for_topic(
        &self,
        topic_arn: &TopicArn,
    ) -> PubsubResult<Vec<Subscription>> {
        let Some(topic_id) = self.id_from_lookup(topic_arn_lookup_key(topic_arn)).await? else {
            return Ok(Vec::new());
        };
        let range = self
            .kv_store
            .get_prefix(
                &subscription_topic_prefix_for_id(topic_id),
                true,
                None,
                true,
            )
            .await
            .map_err(map_storage_error)?;
        let mut subscriptions = Vec::with_capacity(range.items.len());
        for (_key, value) in range.items {
            let subscription_id = decode_pubsub_storage_id(&value)?;
            if let Some(subscription) = self
                .get_record(&subscription_key_for_id(subscription_id))
                .await?
            {
                subscriptions.push(subscription);
            }
        }
        Ok(subscriptions)
    }

    async fn list_subscription_snapshots_for_topic(
        &self,
        topic_id: PubsubStorageId,
    ) -> PubsubResult<Vec<StoredSubscriptionSnapshot>> {
        let range = self
            .kv_store
            .get_prefix(
                &subscription_topic_prefix_for_id(topic_id),
                true,
                None,
                true,
            )
            .await
            .map_err(map_storage_error)?;
        let mut snapshots = Vec::with_capacity(range.items.len());
        for (_key, value) in range.items {
            let subscription_id = decode_pubsub_storage_id(&value)?;
            if let Some(subscription) = self
                .get_record(&subscription_key_for_id(subscription_id))
                .await?
            {
                snapshots.push(StoredSubscriptionSnapshot {
                    subscription_id,
                    subscription,
                });
            }
        }
        snapshots.sort_by(|left, right| {
            left.subscription
                .subscription_arn
                .as_str()
                .cmp(right.subscription.subscription_arn.as_str())
        });
        Ok(snapshots)
    }

    async fn publish_subscription_snapshot(&self, topic_arn: &TopicArn) -> PubsubResult<()> {
        let topic_id = self
            .id_from_lookup(topic_arn_lookup_key(topic_arn))
            .await?
            .ok_or_else(|| PubsubError::topic_not_found(topic_arn.to_string()))?;
        let root_key = subscription_snapshot_root_key(topic_id);
        for _ in 0..SUBSCRIPTION_SNAPSHOT_PUBLISH_ATTEMPTS {
            let previous_root = self
                .kv_store
                .get(&root_key, true)
                .await
                .map_err(map_storage_error)?;
            let snapshots = self.list_subscription_snapshots_for_topic(topic_id).await?;
            let root = SubscriptionSnapshotRoot {
                generation: Uuid::now_v7().simple().to_string(),
                chunk_count: u32::try_from(
                    snapshots.len().div_ceil(SUBSCRIPTION_SNAPSHOT_CHUNK_SIZE),
                )
                .map_err(|error| PubsubError::storage(error.to_string()))?,
            };
            for (chunk_index, chunk) in snapshots
                .chunks(SUBSCRIPTION_SNAPSHOT_CHUNK_SIZE)
                .enumerate()
            {
                self.kv_store
                    .put(
                        &subscription_snapshot_chunk_key(
                            topic_id,
                            &root.generation,
                            u32::try_from(chunk_index)
                                .map_err(|error| PubsubError::storage(error.to_string()))?,
                        ),
                        &encode_pubsub(&chunk.to_vec())?,
                        None,
                    )
                    .await
                    .map_err(map_storage_error)?;
            }
            let root_bytes = encode_pubsub(&root)?;
            match self
                .kv_store
                .transact_write_unchecked(vec![
                    DirectWriteOperation::CheckValue {
                        key: root_key.clone(),
                        expected_value: previous_root.clone(),
                    },
                    DirectWriteOperation::Put {
                        key: root_key.clone(),
                        value: root_bytes,
                    },
                ])
                .await
            {
                Ok(()) => {
                    if let Some(previous_root) = previous_root
                        .map(|bytes| decode_pubsub::<SubscriptionSnapshotRoot>(&bytes))
                        .transpose()?
                    {
                        self.cleanup_subscription_snapshot_if_unreferenced(
                            topic_id,
                            &previous_root,
                        )
                        .await?;
                    }
                    return Ok(());
                }
                Err(error)
                    if matches!(
                        error.as_ref(),
                        StorageEnum::ConditionalCheckFailed
                            | StorageEnum::TransactionCanceled { .. }
                    ) =>
                {
                    for chunk_index in 0..root.chunk_count {
                        self.kv_store
                            .delete(&subscription_snapshot_chunk_key(
                                topic_id,
                                &root.generation,
                                chunk_index,
                            ))
                            .await
                            .map_err(map_storage_error)?;
                    }
                }
                Err(error) => return Err(map_storage_error(error)),
            }
        }
        Err(PubsubError::storage(format!(
            "subscription snapshot publication remained contended for {topic_arn}"
        )))
    }

    async fn cleanup_subscription_snapshot_if_unreferenced(
        &self,
        topic_id: PubsubStorageId,
        snapshot: &SubscriptionSnapshotRoot,
    ) -> PubsubResult<()> {
        let current = self
            .kv_store
            .get(&subscription_snapshot_root_key(topic_id), true)
            .await
            .map_err(map_storage_error)?
            .map(|bytes| decode_pubsub::<SubscriptionSnapshotRoot>(&bytes))
            .transpose()?;
        if current
            .as_ref()
            .is_some_and(|current| current.generation == snapshot.generation)
        {
            return Ok(());
        }
        let intents = self
            .kv_store
            .get_prefix(
                &compact::pubsub_kind_prefix(PubsubRecordKind::PublishIntent).start,
                true,
                None,
                true,
            )
            .await
            .map_err(map_storage_error)?;
        for (_key, bytes) in intents.items {
            let intent = decode_pubsub::<PublishIntent>(&bytes)?;
            if intent.topic_id == topic_id && intent.snapshot.generation == snapshot.generation {
                return Ok(());
            }
        }
        for chunk_index in 0..snapshot.chunk_count {
            self.kv_store
                .delete(&subscription_snapshot_chunk_key(
                    topic_id,
                    &snapshot.generation,
                    chunk_index,
                ))
                .await
                .map_err(map_storage_error)?;
        }
        Ok(())
    }

    async fn write_subscription_record(&self, subscription: &Subscription) -> PubsubResult<()> {
        let encoded = encode_pubsub(subscription)?;
        let subscription_id = self
            .id_from_lookup(subscription_arn_lookup_key(&subscription.subscription_arn))
            .await?
            .ok_or_else(|| {
                PubsubError::subscription_not_found(subscription.subscription_arn.to_string())
            })?;
        let topic_id = self
            .id_from_lookup(topic_arn_lookup_key(&subscription.topic_arn))
            .await?
            .ok_or_else(|| PubsubError::topic_not_found(subscription.topic_arn.to_string()))?;
        self.kv_store
            .transact_write_unchecked(vec![
                DirectWriteOperation::Put {
                    key: subscription_key_for_id(subscription_id),
                    value: encoded,
                },
                DirectWriteOperation::Put {
                    key: subscription_topic_key_for_id(topic_id, subscription_id),
                    value: encode_pubsub_storage_id(subscription_id),
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
        let Some(subscription_id) = self
            .id_from_lookup(subscription_arn_lookup_key(&subscription.subscription_arn))
            .await?
        else {
            return Ok(());
        };
        let Some(topic_id) = self
            .id_from_lookup(topic_arn_lookup_key(&subscription.topic_arn))
            .await?
        else {
            return Ok(());
        };
        operations.push(DirectWriteOperation::Delete {
            key: subscription_key_for_id(subscription_id),
        });
        operations.push(DirectWriteOperation::Delete {
            key: subscription_topic_key_for_id(topic_id, subscription_id),
        });
        operations.push(DirectWriteOperation::Delete {
            key: subscription_dedupe_key_for_id(
                topic_id,
                subscription.protocol,
                &subscription.endpoint,
            ),
        });
        Ok(())
    }

    async fn upsert_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()> {
        let previous = self.get_delivery_record(&record.id).await?;
        let delivery_id = match self
            .id_from_lookup(delivery_record_lookup_key(&record.id))
            .await?
        {
            Some(id) => id,
            None => {
                self.allocate_pubsub_id(compact::delivery_id_allocator_key())
                    .await?
            }
        };
        let subscription_id = self
            .ensure_subscription_id_for_arn(&record.subscription_arn)
            .await?;
        let mut operations = Vec::new();
        push_delivery_record_operations(
            record,
            previous,
            delivery_id,
            subscription_id,
            &mut operations,
        )?;
        self.kv_store
            .transact_write_unchecked(operations)
            .await
            .map_err(map_storage_error)?;
        Ok(())
    }

    async fn snapshot_delivery_record_operations(
        &self,
        records: Vec<(DeliveryRecord, PubsubStorageId)>,
    ) -> PubsubResult<Vec<DirectWriteOperation>> {
        let mut operations = Vec::with_capacity(records.len().saturating_mul(4));
        for (record, subscription_id) in records {
            if self.get_delivery_record(&record.id).await?.is_some() {
                continue;
            }
            let delivery_id = self
                .allocate_pubsub_id(compact::delivery_id_allocator_key())
                .await?;
            push_delivery_record_operations(
                record,
                None,
                delivery_id,
                subscription_id,
                &mut operations,
            )?;
        }
        Ok(operations)
    }

    async fn claim_candidates(
        &self,
        status: DeliveryStatus,
        now: TimestampMillis,
        remaining: usize,
    ) -> PubsubResult<Vec<(PubsubStorageId, DeliveryRecord)>> {
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
        let mut delivery_ids = Vec::new();
        for (key, value) in range.items {
            let Some(due_at) = claim_due_at_from_key(&key)? else {
                continue;
            };
            if due_at > now {
                break;
            }
            let delivery_id = decode_pubsub_storage_id(&value)?;
            delivery_ids.push(delivery_id);
        }
        if delivery_ids.is_empty() {
            return Ok(Vec::new());
        }
        let record_keys = delivery_ids
            .iter()
            .copied()
            .map(delivery_key_for_id)
            .collect();
        let record_values = self
            .kv_store
            .multi_get(record_keys, true)
            .await
            .map_err(map_storage_error)?;
        if record_values.len() != delivery_ids.len() {
            return Err(PubsubError::storage(format!(
                "pubsub claim multi-get returned {} values for {} delivery ids",
                record_values.len(),
                delivery_ids.len()
            )));
        }
        let mut records = Vec::with_capacity(delivery_ids.len());
        for (delivery_id, value) in delivery_ids.into_iter().zip(record_values) {
            if let Some(value) = value {
                records.push((delivery_id, decode_pubsub(&value)?));
            }
        }
        Ok(records)
    }
}

fn push_delivery_record_operations(
    record: DeliveryRecord,
    previous: Option<DeliveryRecord>,
    delivery_id: PubsubStorageId,
    subscription_id: PubsubStorageId,
    operations: &mut Vec<DirectWriteOperation>,
) -> PubsubResult<()> {
    if let Some(previous) = previous {
        if let Some(previous_claim_key) = delivery_claim_key_for_record(&previous, delivery_id) {
            operations.push(DirectWriteOperation::Delete {
                key: previous_claim_key,
            });
        }
        operations.push(DirectWriteOperation::Delete {
            key: delivery_subscription_key_for_id(subscription_id, delivery_id),
        });
    }
    if let Some(claim_key) = delivery_claim_key_for_record(&record, delivery_id) {
        operations.push(DirectWriteOperation::Put {
            key: claim_key,
            value: encode_pubsub_storage_id(delivery_id),
        });
    }
    operations.push(DirectWriteOperation::Put {
        key: delivery_subscription_key_for_id(subscription_id, delivery_id),
        value: encode_pubsub_storage_id(delivery_id),
    });
    operations.push(DirectWriteOperation::Put {
        key: delivery_record_lookup_key(&record.id),
        value: encode_pubsub_storage_id(delivery_id),
    });
    operations.push(DirectWriteOperation::Put {
        key: delivery_key_for_id(delivery_id),
        value: encode_pubsub(&record)?,
    });
    Ok(())
}

fn encode_pubsub_storage_id(id: PubsubStorageId) -> Vec<u8> {
    let bytes = id.get().get().to_be_bytes();
    bytes[2..].to_vec()
}

fn decode_pubsub_storage_id(bytes: &[u8]) -> PubsubResult<PubsubStorageId> {
    if bytes.len() != 6 {
        return Err(PubsubError::storage(format!(
            "invalid pubsub storage id width: expected 6 bytes, got {}",
            bytes.len()
        )));
    }
    let mut padded = [0u8; 8];
    padded[2..].copy_from_slice(bytes);
    PubsubStorageId::new(u64::from_be_bytes(padded))
}

fn topic_key_for_id(topic_id: PubsubStorageId) -> Vec<u8> {
    compact::pubsub_record_key(PubsubRecordKind::Topic, topic_id.get(), None, b"")
}

fn topic_arn_lookup_key(topic_arn: &TopicArn) -> Vec<u8> {
    compact::pubsub_global_record_key(
        PubsubRecordKind::Topic,
        &stable_lookup_hash(topic_arn.as_str()),
    )
}

fn topic_name_key(topic_name: &TopicName) -> Vec<u8> {
    compact::pubsub_global_record_key(
        PubsubRecordKind::TopicName,
        &stable_lookup_hash(topic_name.as_str()),
    )
}

fn subscription_key_for_id(subscription_id: PubsubStorageId) -> Vec<u8> {
    compact::pubsub_record_key(
        PubsubRecordKind::Subscription,
        subscription_id.get(),
        None,
        b"",
    )
}

fn subscription_arn_lookup_key(subscription_arn: &SubscriptionArn) -> Vec<u8> {
    compact::pubsub_global_record_key(
        PubsubRecordKind::Subscription,
        &stable_lookup_hash(subscription_arn.as_str()),
    )
}

fn subscription_topic_prefix_for_id(topic_id: PubsubStorageId) -> Vec<u8> {
    compact::pubsub_record_prefix(PubsubRecordKind::SubscriptionTopic, topic_id.get()).start
}

fn subscription_topic_key_for_id(
    topic_id: PubsubStorageId,
    subscription_id: PubsubStorageId,
) -> Vec<u8> {
    compact::pubsub_record_key(
        PubsubRecordKind::SubscriptionTopic,
        topic_id.get(),
        Some(subscription_id.get()),
        b"",
    )
}

fn subscription_dedupe_key_for_id(
    topic_id: PubsubStorageId,
    protocol: SubscriptionProtocol,
    endpoint: &str,
) -> Vec<u8> {
    compact::pubsub_record_key(
        PubsubRecordKind::SubscriptionDedupe,
        topic_id.get(),
        None,
        &stable_lookup_hash(&format!("{}:{endpoint}", protocol.as_str())),
    )
}

fn delivery_key_for_id(delivery_id: PubsubStorageId) -> Vec<u8> {
    compact::pubsub_record_key(PubsubRecordKind::Delivery, delivery_id.get(), None, b"")
}

fn subscription_snapshot_root_key(topic_id: PubsubStorageId) -> Vec<u8> {
    compact::pubsub_record_key(
        PubsubRecordKind::SubscriptionSnapshotRoot,
        topic_id.get(),
        None,
        b"",
    )
}

fn subscription_snapshot_chunk_key(
    topic_id: PubsubStorageId,
    generation: &str,
    chunk_index: u32,
) -> Vec<u8> {
    let mut suffix = Vec::with_capacity(generation.len() + 4);
    suffix.extend_from_slice(generation.as_bytes());
    suffix.extend_from_slice(&chunk_index.to_be_bytes());
    compact::pubsub_record_key(
        PubsubRecordKind::SubscriptionSnapshotChunk,
        topic_id.get(),
        None,
        &suffix,
    )
}

fn publish_intent_key(message_id: &PubsubMessageId) -> Vec<u8> {
    compact::pubsub_global_record_key(
        PubsubRecordKind::PublishIntent,
        message_id.as_str().as_bytes(),
    )
}

fn delivery_record_lookup_key(record_id: &DeliveryRecordId) -> Vec<u8> {
    compact::pubsub_global_record_key(
        PubsubRecordKind::Delivery,
        &stable_lookup_hash(&record_id.0),
    )
}

fn delivery_subscription_key_for_id(
    subscription_id: PubsubStorageId,
    delivery_id: PubsubStorageId,
) -> Vec<u8> {
    compact::pubsub_record_key(
        PubsubRecordKind::DeliverySubscription,
        subscription_id.get(),
        Some(delivery_id.get()),
        b"",
    )
}

fn delivery_claim_status_prefix(status: DeliveryStatus) -> Vec<u8> {
    compact::pubsub_record_prefix(
        PubsubRecordKind::DeliveryClaim,
        delivery_status_id(status).get(),
    )
    .start
}

fn delivery_claim_key_for_id(
    status: DeliveryStatus,
    due_at: TimestampMillis,
    delivery_id: PubsubStorageId,
) -> Vec<u8> {
    let mut suffix = due_at.timestamp_millis().to_be_bytes().to_vec();
    suffix.extend_from_slice(&encode_pubsub_storage_id(delivery_id));
    compact::pubsub_record_key(
        PubsubRecordKind::DeliveryClaim,
        delivery_status_id(status).get(),
        Some(delivery_id.get()),
        &suffix,
    )
}

fn delivery_status_id(status: DeliveryStatus) -> PubsubStorageId {
    let value = match status {
        DeliveryStatus::Pending => 1,
        DeliveryStatus::Delivered => 2,
        DeliveryStatus::AcceptedByCustomSender => 3,
        DeliveryStatus::RetryScheduled => 4,
        DeliveryStatus::Failed => 5,
    };
    PubsubStorageId(compact::U48::masked(value))
}

fn stable_lookup_hash(value: &str) -> [u8; 8] {
    let digest = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, value.as_bytes()).into_bytes();
    [
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ]
}

fn delivery_claim_key_for_record(
    record: &DeliveryRecord,
    delivery_id: PubsubStorageId,
) -> Option<Vec<u8>> {
    if !matches!(
        record.status,
        DeliveryStatus::Pending | DeliveryStatus::RetryScheduled
    ) {
        return None;
    }
    let due_at = record
        .next_attempt_at
        .unwrap_or_else(|| TimestampMillis::from(0));
    Some(delivery_claim_key_for_id(
        record.status,
        due_at,
        delivery_id,
    ))
}

fn claim_due_at_from_key(key: &[u8]) -> PubsubResult<Option<TimestampMillis>> {
    let Ok(compact::ParsedCompactKey::PubsubRecord {
        kind: PubsubRecordKind::DeliveryClaim,
        suffix,
        ..
    }) = compact::parse_compact_key(key)
    else {
        return Ok(None);
    };
    let Some(bytes) = suffix.get(..8) else {
        return Ok(None);
    };
    let mut due = [0u8; 8];
    due.copy_from_slice(bytes);
    Ok(Some(TimestampMillis::from(i64::from_be_bytes(due))))
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
