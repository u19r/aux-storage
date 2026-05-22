use std::{
    collections::{BTreeMap, HashMap},
    io,
    str::Utf8Error,
};

use http_request::reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, USER_AGENT};
use pubsub_provider::{
    PublishRequest, PubsubError, PubsubMessageId, PubsubResult, Subscription, TopicArn,
};
use serde::Serialize;
use storage_types::TimestampMillis;
use url::Url;

use crate::manager::PubsubDeliveryConfig;

pub trait PubsubNotificationSigner: Send + Sync {
    fn sign(&self, request: PubsubNotificationSignRequest<'_>) -> PubsubResult<String>;
}

#[derive(Debug, Clone, Copy)]
pub struct PubsubNotificationSignRequest<'a> {
    pub signature_version: &'static str,
    pub string_to_sign: &'a str,
}

pub(crate) struct NotificationRenderContext<'a> {
    pub request: &'a PublishRequest,
    pub message_id: &'a PubsubMessageId,
    pub subscription: &'a Subscription,
    pub delivery_config: &'a PubsubDeliveryConfig,
    pub signer: Option<&'a dyn PubsubNotificationSigner>,
}

pub(crate) struct ConfirmationRenderContext<'a> {
    pub topic_arn: &'a TopicArn,
    pub message_id: &'a PubsubMessageId,
    pub token: &'a str,
    pub delivery_config: &'a PubsubDeliveryConfig,
    pub signer: Option<&'a dyn PubsubNotificationSigner>,
}

pub(crate) fn notification_body(context: NotificationRenderContext<'_>) -> PubsubResult<String> {
    let timestamp = TimestampMillis::now().to_rfc3339();
    let signing_cert_url = context
        .delivery_config
        .signing_cert_url
        .as_deref()
        .unwrap_or("");
    let unsubscribe_url = unsubscribe_url(context.delivery_config, context.subscription)?;
    let string_to_sign =
        notification_string_to_sign(context.request, context.message_id, &timestamp);
    let signature = sign(context.signer, &string_to_sign)?;

    let mut body = String::with_capacity(notification_body_capacity(
        &context,
        &timestamp,
        signing_cert_url,
        &unsubscribe_url,
        &signature,
    ));
    body.push_str("{\n");
    append_json_string_field(&mut body, "Type", "Notification", false)?;
    append_json_string_field(&mut body, "MessageId", context.message_id.as_str(), false)?;
    append_json_string_field(
        &mut body,
        "TopicArn",
        context.request.topic_arn.as_str(),
        false,
    )?;
    if let Some(subject) = context.request.subject.as_deref() {
        append_json_string_field(&mut body, "Subject", subject, false)?;
    }
    append_json_string_field(&mut body, "Message", &context.request.message, false)?;
    append_json_string_field(&mut body, "Timestamp", &timestamp, false)?;
    append_json_string_field(&mut body, "SignatureVersion", "1", false)?;
    append_json_string_field(&mut body, "Signature", &signature, false)?;
    append_json_string_field(&mut body, "SigningCertURL", signing_cert_url, false)?;
    append_json_string_field(
        &mut body,
        "UnsubscribeURL",
        &unsubscribe_url,
        context.request.message_attributes.is_empty(),
    )?;
    if !context.request.message_attributes.is_empty() {
        body.push_str("  \"MessageAttributes\" : {\n");
        append_message_attributes(&mut body, &context.request.message_attributes)?;
        body.push_str("  }\n");
    }
    body.push('}');
    Ok(body)
}

pub(crate) fn confirmation_body(context: ConfirmationRenderContext<'_>) -> PubsubResult<String> {
    let timestamp = TimestampMillis::now().to_rfc3339();
    let signing_cert_url = context
        .delivery_config
        .signing_cert_url
        .as_deref()
        .unwrap_or("");
    let subscribe_url = subscribe_url(context.delivery_config, context.topic_arn, context.token)?;
    let message = format!(
        "You have chosen to subscribe to the topic {}.\nTo confirm the subscription, visit the \
         SubscribeURL included in this message.",
        context.topic_arn
    );
    let string_to_sign = confirmation_string_to_sign(
        &message,
        context.message_id,
        &subscribe_url,
        &timestamp,
        context.token,
        context.topic_arn,
    );
    let signature = sign(context.signer, &string_to_sign)?;

    let mut body = String::with_capacity(confirmation_body_capacity(
        &context,
        &message,
        &subscribe_url,
        &timestamp,
        signing_cert_url,
        &signature,
    ));
    body.push_str("{\n");
    append_json_string_field(&mut body, "Type", "SubscriptionConfirmation", false)?;
    append_json_string_field(&mut body, "MessageId", context.message_id.as_str(), false)?;
    append_json_string_field(&mut body, "Token", context.token, false)?;
    append_json_string_field(&mut body, "TopicArn", context.topic_arn.as_str(), false)?;
    append_json_string_field(&mut body, "Message", &message, false)?;
    append_json_string_field(&mut body, "SubscribeURL", &subscribe_url, false)?;
    append_json_string_field(&mut body, "Timestamp", &timestamp, false)?;
    append_json_string_field(&mut body, "SignatureVersion", "1", false)?;
    append_json_string_field(&mut body, "Signature", &signature, false)?;
    append_json_string_field(&mut body, "SigningCertURL", signing_cert_url, true)?;
    body.push('}');
    Ok(body)
}

pub(crate) fn notification_headers(
    message_id: &PubsubMessageId,
    subscription: &Subscription,
    request: &PublishRequest,
) -> PubsubResult<HeaderMap> {
    let mut headers = base_headers();
    insert_header(&mut headers, "x-amz-sns-message-type", "Notification")?;
    insert_header(
        &mut headers,
        "x-amz-sns-message-id",
        &message_id.to_string(),
    )?;
    insert_header(
        &mut headers,
        "x-amz-sns-topic-arn",
        &request.topic_arn.to_string(),
    )?;
    insert_header(
        &mut headers,
        "x-amz-sns-subscription-arn",
        &subscription.subscription_arn.to_string(),
    )?;
    Ok(headers)
}

pub(crate) fn confirmation_headers(
    message_id: &PubsubMessageId,
    topic_arn: &TopicArn,
) -> PubsubResult<HeaderMap> {
    let mut headers = base_headers();
    insert_header(
        &mut headers,
        "x-amz-sns-message-type",
        "SubscriptionConfirmation",
    )?;
    insert_header(
        &mut headers,
        "x-amz-sns-message-id",
        &message_id.to_string(),
    )?;
    insert_header(&mut headers, "x-amz-sns-topic-arn", &topic_arn.to_string())?;
    Ok(headers)
}

pub(crate) fn notification_string_to_sign(
    request: &PublishRequest,
    message_id: &PubsubMessageId,
    timestamp: &str,
) -> String {
    let mut rendered = String::with_capacity(notification_string_to_sign_capacity(
        request, message_id, timestamp,
    ));
    append_string_to_sign_part(&mut rendered, "Message", request.message.as_str());
    append_string_to_sign_part(&mut rendered, "MessageId", message_id.as_str());
    if let Some(subject) = request.subject.as_deref() {
        append_string_to_sign_part(&mut rendered, "Subject", subject);
    }
    append_string_to_sign_part(&mut rendered, "Timestamp", timestamp);
    append_string_to_sign_part(&mut rendered, "TopicArn", request.topic_arn.as_str());
    append_string_to_sign_part(&mut rendered, "Type", "Notification");
    rendered
}

pub(crate) fn confirmation_string_to_sign(
    message: &str,
    message_id: &PubsubMessageId,
    subscribe_url: &str,
    timestamp: &str,
    token: &str,
    topic_arn: &TopicArn,
) -> String {
    let mut rendered = String::with_capacity(confirmation_string_to_sign_capacity(
        message,
        message_id,
        subscribe_url,
        timestamp,
        token,
        topic_arn,
    ));
    append_string_to_sign_part(&mut rendered, "Message", message);
    append_string_to_sign_part(&mut rendered, "MessageId", message_id.as_str());
    append_string_to_sign_part(&mut rendered, "SubscribeURL", subscribe_url);
    append_string_to_sign_part(&mut rendered, "Timestamp", timestamp);
    append_string_to_sign_part(&mut rendered, "Token", token);
    append_string_to_sign_part(&mut rendered, "TopicArn", topic_arn.as_str());
    append_string_to_sign_part(&mut rendered, "Type", "SubscriptionConfirmation");
    rendered
}

fn sign(
    signer: Option<&dyn PubsubNotificationSigner>,
    string_to_sign: &str,
) -> PubsubResult<String> {
    match signer {
        Some(signer) => signer.sign(PubsubNotificationSignRequest {
            signature_version: "1",
            string_to_sign,
        }),
        None => Ok(String::new()),
    }
}

fn append_message_attributes(
    body: &mut String,
    message_attributes: &HashMap<String, String>,
) -> PubsubResult<()> {
    let attributes: BTreeMap<_, _> = message_attributes.iter().collect();
    for (index, (name, value)) in attributes.iter().enumerate() {
        let trailing_comma = if index + 1 == attributes.len() {
            ""
        } else {
            ","
        };
        body.push_str("    ");
        append_json_string(body, name)?;
        body.push_str(" : {\"Type\":\"String\",\"Value\":");
        append_json_string(body, value)?;
        body.push('}');
        body.push_str(trailing_comma);
        body.push('\n');
    }
    Ok(())
}

fn append_json_string_field(
    body: &mut String,
    name: &str,
    value: &str,
    final_field: bool,
) -> PubsubResult<()> {
    body.push_str("  ");
    append_json_string(body, name)?;
    body.push_str(" : ");
    append_json_string(body, value)?;
    if !final_field {
        body.push(',');
    }
    body.push('\n');
    Ok(())
}

fn base_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=UTF-8"),
    );
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static("Amazon Simple Notification Service Agent"),
    );
    headers
}

fn insert_header(headers: &mut HeaderMap, name: &'static str, value: &str) -> PubsubResult<()> {
    headers.insert(
        HeaderName::from_static(name),
        HeaderValue::from_str(value).map_err(PubsubError::storage)?,
    );
    Ok(())
}

fn unsubscribe_url(
    delivery_config: &PubsubDeliveryConfig,
    subscription: &Subscription,
) -> PubsubResult<String> {
    let Some(base) = delivery_config.unsubscribe_url_base.as_deref() else {
        return Ok(String::new());
    };
    append_raw_query_pairs(
        base,
        [
            ("Action", "Unsubscribe"),
            ("SubscriptionArn", subscription.subscription_arn.as_str()),
        ],
    )
}

fn subscribe_url(
    delivery_config: &PubsubDeliveryConfig,
    topic_arn: &TopicArn,
    token: &str,
) -> PubsubResult<String> {
    let Some(base) = delivery_config.subscribe_url_base.as_deref() else {
        return Ok(String::new());
    };
    append_raw_query_pairs(
        base,
        [
            ("Action", "ConfirmSubscription"),
            ("TopicArn", topic_arn.as_str()),
            ("Token", token),
        ],
    )
}

fn append_raw_query_pairs<const N: usize>(
    base: &str,
    pairs: [(&str, &str); N],
) -> PubsubResult<String> {
    Url::parse(base).map_err(PubsubError::storage)?;
    let mut rendered = String::with_capacity(raw_query_pairs_capacity(base, &pairs));
    rendered.push_str(base);
    for (index, (name, value)) in pairs.iter().copied().enumerate() {
        let separator = if index == 0 {
            if rendered.contains('?') {
                if rendered.ends_with('?') || rendered.ends_with('&') {
                    ""
                } else {
                    "&"
                }
            } else {
                "?"
            }
        } else {
            "&"
        };
        rendered.push_str(separator);
        rendered.push_str(name);
        rendered.push('=');
        rendered.push_str(value);
    }
    Ok(rendered)
}

fn append_json_string(body: &mut String, value: &str) -> PubsubResult<()> {
    value
        .serialize(&mut serde_json::Serializer::new(StringWriter { body }))
        .map_err(PubsubError::storage)
}

fn append_string_to_sign_part(rendered: &mut String, name: &str, value: &str) {
    if !rendered.is_empty() {
        rendered.push('\n');
    }
    rendered.push_str(name);
    rendered.push('\n');
    rendered.push_str(value);
}

fn notification_body_capacity(
    context: &NotificationRenderContext<'_>,
    timestamp: &str,
    signing_cert_url: &str,
    unsubscribe_url: &str,
    signature: &str,
) -> usize {
    let request = context.request;
    let mut capacity = 320
        + context.message_id.as_str().len()
        + request.topic_arn.as_str().len()
        + request.message.len()
        + timestamp.len()
        + signature.len()
        + signing_cert_url.len()
        + unsubscribe_url.len()
        + message_attributes_capacity(&request.message_attributes);
    if let Some(subject) = request.subject.as_deref() {
        capacity += subject.len() + 18;
    }
    capacity
}

fn confirmation_body_capacity(
    context: &ConfirmationRenderContext<'_>,
    message: &str,
    subscribe_url: &str,
    timestamp: &str,
    signing_cert_url: &str,
    signature: &str,
) -> usize {
    360 + context.message_id.as_str().len()
        + context.token.len()
        + context.topic_arn.as_str().len()
        + message.len()
        + subscribe_url.len()
        + timestamp.len()
        + signature.len()
        + signing_cert_url.len()
}

fn message_attributes_capacity(message_attributes: &HashMap<String, String>) -> usize {
    if message_attributes.is_empty() {
        return 0;
    }
    32 + message_attributes
        .iter()
        .map(|(name, value)| 42 + name.len() + value.len())
        .sum::<usize>()
}

fn notification_string_to_sign_capacity(
    request: &PublishRequest,
    message_id: &PubsubMessageId,
    timestamp: &str,
) -> usize {
    string_to_sign_part_capacity("Message", request.message.as_str())
        + string_to_sign_part_capacity("MessageId", message_id.as_str())
        + request
            .subject
            .as_deref()
            .map(|subject| string_to_sign_part_capacity("Subject", subject))
            .unwrap_or(0)
        + string_to_sign_part_capacity("Timestamp", timestamp)
        + string_to_sign_part_capacity("TopicArn", request.topic_arn.as_str())
        + string_to_sign_part_capacity("Type", "Notification")
}

fn confirmation_string_to_sign_capacity(
    message: &str,
    message_id: &PubsubMessageId,
    subscribe_url: &str,
    timestamp: &str,
    token: &str,
    topic_arn: &TopicArn,
) -> usize {
    string_to_sign_part_capacity("Message", message)
        + string_to_sign_part_capacity("MessageId", message_id.as_str())
        + string_to_sign_part_capacity("SubscribeURL", subscribe_url)
        + string_to_sign_part_capacity("Timestamp", timestamp)
        + string_to_sign_part_capacity("Token", token)
        + string_to_sign_part_capacity("TopicArn", topic_arn.as_str())
        + string_to_sign_part_capacity("Type", "SubscriptionConfirmation")
}

fn string_to_sign_part_capacity(name: &str, value: &str) -> usize {
    name.len() + value.len() + 2
}

fn raw_query_pairs_capacity(base: &str, pairs: &[(&str, &str)]) -> usize {
    base.len()
        + pairs
            .iter()
            .map(|(name, value)| 2 + name.len() + value.len())
            .sum::<usize>()
}

struct StringWriter<'a> {
    body: &'a mut String,
}

impl io::Write for StringWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let value = std::str::from_utf8(buffer).map_err(invalid_utf8)?;
        self.body.push_str(value);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn invalid_utf8(error: Utf8Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}
