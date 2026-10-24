//! Shared subscription lifecycle and visibility decisions.

use async_trait::async_trait;
use storage_common::retry::retry_native_conflicts;
use storage_types::{AttemptMode, StorageError, StorageResult};

use crate::{
    ConfirmSubscriptionRequest, ConfirmSubscriptionResponse, ListSubscriptionsRequest,
    ListSubscriptionsResponse, PreparedSubscription, PubsubAttempt, PubsubError, PubsubPort,
    PubsubResult, PubsubSubscriptionRepository, PubsubValidationKind,
    SetSubscriptionAttributesRequest, Subscription, SubscriptionAcceptanceInput, SubscriptionArn,
    SubscriptionConfirmation, SubscriptionPage, SubscriptionPageRequest, SubscriptionRecord,
    constants::{PUBSUB_PAGE_LIMIT, PUBSUB_WRITE_RETRY},
    pubsub_port::read_topic,
};

#[async_trait]
impl<P: PubsubPort> PubsubSubscriptionRepository for P {
    async fn create_subscription(
        &self,
        input: SubscriptionAcceptanceInput,
    ) -> PubsubResult<Subscription> {
        retry_native_conflicts(self, PUBSUB_WRITE_RETRY, || {
            let input = input.clone();
            self.with_pubsub_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move { create_subscription_in_attempt(attempt, input).await })
            })
        })
        .await
        .map_err(PubsubError::from)?
    }

    async fn confirm_subscription(
        &self,
        request: ConfirmSubscriptionRequest,
    ) -> PubsubResult<ConfirmSubscriptionResponse> {
        retry_native_conflicts(self, PUBSUB_WRITE_RETRY, || {
            let request = request.clone();
            self.with_pubsub_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move { confirm_subscription_in_attempt(attempt, request).await })
            })
        })
        .await
        .map_err(PubsubError::from)?
    }

    async fn delete_subscription(&self, arn: &SubscriptionArn) -> PubsubResult<()> {
        let arn = arn.clone();
        retry_native_conflicts(self, PUBSUB_WRITE_RETRY, || {
            let arn = arn.clone();
            self.with_pubsub_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move { delete_subscription_in_attempt(attempt, &arn).await })
            })
        })
        .await
        .map_err(PubsubError::from)
    }

    async fn get_subscription(&self, arn: &SubscriptionArn) -> PubsubResult<Option<Subscription>> {
        let arn = arn.clone();
        self.with_pubsub_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let Some(record) = attempt.read_subscription(&arn).await? else {
                    return Ok(None);
                };
                Ok(is_active(attempt, &record)
                    .await?
                    .then_some(record.subscription))
            })
        })
        .await
        .map_err(PubsubError::from)
    }

    async fn set_subscription_attributes(
        &self,
        request: SetSubscriptionAttributesRequest,
    ) -> PubsubResult<Subscription> {
        retry_native_conflicts(self, PUBSUB_WRITE_RETRY, || {
            let request = request.clone();
            self.with_pubsub_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(
                    async move { set_subscription_attributes_in_attempt(attempt, request).await },
                )
            })
        })
        .await
        .map_err(PubsubError::from)?
    }

    async fn list_subscriptions(
        &self,
        request: ListSubscriptionsRequest,
    ) -> PubsubResult<ListSubscriptionsResponse> {
        let after = request.next_token.map(SubscriptionArn::new).transpose()?;
        self.with_pubsub_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let topic_incarnation = if let Some(arn) = &request.topic_arn {
                    let Some(topic) = read_topic(attempt, arn).await? else {
                        return Ok(ListSubscriptionsResponse {
                            subscriptions: Vec::new(),
                            next_token: None,
                        });
                    };
                    Some(topic.incarnation)
                } else {
                    None
                };
                let page = attempt
                    .read_subscription_page(SubscriptionPageRequest {
                        topic_incarnation,
                        after: after.as_ref(),
                        limit: PUBSUB_PAGE_LIMIT,
                    })
                    .await?;
                validate_page(&page, after.as_ref())?;
                let more = page.more || page.rows.len() > PUBSUB_PAGE_LIMIT.get() as usize;
                let selected = page
                    .rows
                    .into_iter()
                    .take(PUBSUB_PAGE_LIMIT.get() as usize)
                    .collect::<Vec<_>>();
                let next_token = more
                    .then(|| {
                        selected
                            .last()
                            .map(|row| row.subscription.subscription_arn.to_string())
                    })
                    .flatten();
                let subscriptions = if topic_incarnation.is_some() {
                    selected.into_iter().map(|row| row.subscription).collect()
                } else {
                    let arns = selected
                        .iter()
                        .map(|row| row.subscription.topic_arn.clone())
                        .collect();
                    let topics = attempt.read_topics(arns).await?;
                    if topics.len() != selected.len() {
                        return Err(StorageError::internal(
                            "subscription topic batch count differs",
                        ));
                    }
                    selected
                        .into_iter()
                        .zip(topics)
                        .filter_map(|(row, topic)| {
                            topic
                                .filter(|topic| topic.incarnation == row.topic_incarnation)
                                .map(|_| row.subscription)
                        })
                        .collect()
                };
                Ok(ListSubscriptionsResponse {
                    subscriptions,
                    next_token,
                })
            })
        })
        .await
        .map_err(PubsubError::from)
    }
}

async fn is_active<A: PubsubAttempt>(
    attempt: &mut A,
    record: &SubscriptionRecord,
) -> StorageResult<bool> {
    Ok(read_topic(attempt, &record.subscription.topic_arn)
        .await?
        .is_some_and(|topic| topic.incarnation == record.topic_incarnation))
}

pub(crate) fn validate_page(
    page: &SubscriptionPage,
    after: Option<&SubscriptionArn>,
) -> StorageResult<()> {
    if page.rows.len() > PUBSUB_PAGE_LIMIT.get() as usize + 1 || page.more && page.rows.is_empty() {
        return Err(StorageError::internal(
            "subscription page has invalid bounds",
        ));
    }
    let mut previous = after.map(SubscriptionArn::as_str);
    for row in &page.rows {
        let arn = row.subscription.subscription_arn.as_str();
        if previous.is_some_and(|previous| arn <= previous) {
            return Err(StorageError::internal("subscription page is unordered"));
        }
        previous = Some(arn);
    }
    Ok(())
}

pub async fn create_subscription_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    input: SubscriptionAcceptanceInput,
) -> StorageResult<PubsubResult<Subscription>> {
    let arn = input.subscription().topic_arn.clone();
    let Some(prepared) = PreparedSubscription::prepare(attempt, input).await? else {
        return Ok(Err(PubsubError::topic_not_found(arn.to_string())));
    };
    Ok(Ok(prepared.apply().await?))
}

pub async fn confirm_subscription_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    request: ConfirmSubscriptionRequest,
) -> StorageResult<PubsubResult<ConfirmSubscriptionResponse>> {
    let Some(topic) = read_topic(attempt, &request.topic_arn).await? else {
        return Ok(Err(PubsubError::validation(
            PubsubValidationKind::InvalidToken,
        )));
    };
    let Some(previous) = attempt
        .find_subscription_by_token(topic.incarnation, &request.token)
        .await?
    else {
        return Ok(Err(PubsubError::validation(
            PubsubValidationKind::InvalidToken,
        )));
    };
    let mut current = previous.clone();
    current.subscription.confirmation = SubscriptionConfirmation::Confirmed;
    let arn = current.subscription.subscription_arn.clone();
    current.validate_revision(&previous)?;
    attempt.write_subscription(current, Some(previous)).await?;
    Ok(Ok(ConfirmSubscriptionResponse {
        subscription_arn: arn,
    }))
}

pub async fn delete_subscription_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    arn: &SubscriptionArn,
) -> StorageResult<()> {
    if let Some(record) = attempt.read_subscription(arn).await? {
        attempt.delete_subscription(record).await?;
    }
    Ok(())
}

pub async fn set_subscription_attributes_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    request: SetSubscriptionAttributesRequest,
) -> StorageResult<PubsubResult<Subscription>> {
    let arn = request.subscription_arn.clone();
    let Some(previous) = attempt.read_subscription(&arn).await? else {
        return Ok(Err(PubsubError::subscription_not_found(arn.to_string())));
    };
    if !is_active(attempt, &previous).await? {
        return Ok(Err(PubsubError::subscription_not_found(arn.to_string())));
    }
    let mut current = previous.clone();
    if let Some(value) = request.attributes.get("RawMessageDelivery") {
        current.subscription.raw_message_delivery = value.eq_ignore_ascii_case("true");
    }
    let subscription = current.subscription.clone();
    current.validate_revision(&previous)?;
    attempt.write_subscription(current, Some(previous)).await?;
    Ok(Ok(subscription))
}
