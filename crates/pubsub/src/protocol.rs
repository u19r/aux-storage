use std::collections::{BTreeMap, HashMap};

use http_error::HttpApiError;
use pubsub_provider::{
    ConfirmSubscriptionRequest, GetSubscriptionAttributesRequest, GetTopicAttributesRequest,
    ListSubscriptionsRequest, ListTopicsRequest, PublishRequest, PubsubError, PubsubMessageId,
    PubsubResult, PubsubValidationKind, SetSubscriptionAttributesRequest,
    SetTopicAttributesRequest, SubscribeRequest, Subscription, SubscriptionArn,
    SubscriptionProtocol, TopicArn, TopicName,
};

const PUBSUB_QUERY_XMLNS: &str = "https://sns.amazonaws.com/doc/2010-03-31/";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubsubAction {
    CreateTopic {
        name: TopicName,
        attributes: HashMap<String, String>,
    },
    DeleteTopic {
        topic_arn: TopicArn,
    },
    GetTopicAttributes(GetTopicAttributesRequest),
    SetTopicAttributes(SetTopicAttributesRequest),
    ListTopics(ListTopicsRequest),
    Subscribe(SubscribeRequest),
    ConfirmSubscription(ConfirmSubscriptionRequest),
    Unsubscribe {
        subscription_arn: SubscriptionArn,
    },
    GetSubscriptionAttributes(GetSubscriptionAttributesRequest),
    SetSubscriptionAttributes(SetSubscriptionAttributesRequest),
    ListSubscriptions(ListSubscriptionsRequest),
    ListSubscriptionsByTopic(ListSubscriptionsRequest),
    Publish(PublishRequest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubsubSuccess {
    CreateTopic {
        topic_arn: TopicArn,
    },
    DeleteTopic,
    GetTopicAttributes {
        attributes: HashMap<String, String>,
    },
    SetTopicAttributes,
    ListTopics {
        topic_arns: Vec<TopicArn>,
    },
    Subscribe {
        subscription_arn: String,
    },
    ConfirmSubscription {
        subscription_arn: SubscriptionArn,
    },
    Unsubscribe,
    GetSubscriptionAttributes {
        attributes: HashMap<String, String>,
    },
    SetSubscriptionAttributes,
    ListSubscriptions {
        subscriptions: Vec<SubscriptionView>,
    },
    Publish {
        message_id: PubsubMessageId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionView {
    pub topic_arn: TopicArn,
    pub subscription_arn: SubscriptionArn,
    pub protocol: SubscriptionProtocol,
    pub endpoint: String,
}

impl From<Subscription> for SubscriptionView {
    fn from(subscription: Subscription) -> Self {
        Self {
            topic_arn: subscription.topic_arn,
            subscription_arn: subscription.subscription_arn,
            protocol: subscription.protocol,
            endpoint: subscription.endpoint,
        }
    }
}

pub fn decode_query_request(body: &[u8]) -> PubsubResult<PubsubAction> {
    let fields = url::form_urlencoded::parse(body)
        .into_owned()
        .collect::<BTreeMap<String, String>>();
    let action = required(&fields, "Action")?;
    match action.as_str() {
        "CreateTopic" => Ok(PubsubAction::CreateTopic {
            name: TopicName::new(required(&fields, "Name")?)?,
            attributes: decode_entry_map(&fields, "Attributes"),
        }),
        "DeleteTopic" => Ok(PubsubAction::DeleteTopic {
            topic_arn: TopicArn::new(required(&fields, "TopicArn")?)?,
        }),
        "GetTopicAttributes" => Ok(PubsubAction::GetTopicAttributes(
            GetTopicAttributesRequest {
                topic_arn: TopicArn::new(required(&fields, "TopicArn")?)?,
            },
        )),
        "SetTopicAttributes" => Ok(PubsubAction::SetTopicAttributes(
            SetTopicAttributesRequest {
                topic_arn: TopicArn::new(required(&fields, "TopicArn")?)?,
                attributes: decode_named_attribute(&fields)?,
            },
        )),
        "ListTopics" => Ok(PubsubAction::ListTopics(ListTopicsRequest {
            next_token: fields.get("NextToken").cloned(),
        })),
        "Subscribe" => {
            let protocol_text = required(&fields, "Protocol")?;
            let Some(protocol) = SubscriptionProtocol::parse(&protocol_text) else {
                return Err(PubsubError::validation_with_detail(
                    PubsubValidationKind::UnsupportedProtocol,
                    protocol_text,
                ));
            };
            Ok(PubsubAction::Subscribe(SubscribeRequest {
                topic_arn: TopicArn::new(required(&fields, "TopicArn")?)?,
                protocol,
                endpoint: required(&fields, "Endpoint")?,
                attributes: decode_entry_map(&fields, "Attributes"),
                extra_json: fields
                    .get("ExtraJson")
                    .map(|raw| serde_json::from_str(raw))
                    .transpose()?
                    .unwrap_or(serde_json::Value::Null),
            }))
        }
        "Unsubscribe" => Ok(PubsubAction::Unsubscribe {
            subscription_arn: SubscriptionArn::new(required(&fields, "SubscriptionArn")?)?,
        }),
        "ConfirmSubscription" => Ok(PubsubAction::ConfirmSubscription(
            ConfirmSubscriptionRequest {
                topic_arn: TopicArn::new(required(&fields, "TopicArn")?)?,
                token: required(&fields, "Token")?,
            },
        )),
        "GetSubscriptionAttributes" => Ok(PubsubAction::GetSubscriptionAttributes(
            GetSubscriptionAttributesRequest {
                subscription_arn: SubscriptionArn::new(required(&fields, "SubscriptionArn")?)?,
            },
        )),
        "SetSubscriptionAttributes" => Ok(PubsubAction::SetSubscriptionAttributes(
            SetSubscriptionAttributesRequest {
                subscription_arn: SubscriptionArn::new(required(&fields, "SubscriptionArn")?)?,
                attributes: decode_named_attribute(&fields)?,
            },
        )),
        "ListSubscriptions" => Ok(PubsubAction::ListSubscriptions(ListSubscriptionsRequest {
            topic_arn: None,
            next_token: fields.get("NextToken").cloned(),
        })),
        "ListSubscriptionsByTopic" => Ok(PubsubAction::ListSubscriptionsByTopic(
            ListSubscriptionsRequest {
                topic_arn: Some(TopicArn::new(required(&fields, "TopicArn")?)?),
                next_token: fields.get("NextToken").cloned(),
            },
        )),
        "Publish" => Ok(PubsubAction::Publish(PublishRequest {
            topic_arn: TopicArn::new(required(&fields, "TopicArn")?)?,
            message: required(&fields, "Message")?,
            subject: fields.get("Subject").cloned(),
            message_attributes: decode_message_attributes(&fields),
        })),
        _ => Err(PubsubError::validation_with_detail(
            PubsubValidationKind::UnsupportedAttribute,
            action,
        )),
    }
}

pub fn render_query_success(success: &PubsubSuccess, request_id: &str) -> String {
    match success {
        PubsubSuccess::CreateTopic { topic_arn } => render_response(
            "CreateTopic",
            &format!("<TopicArn>{}</TopicArn>", escape(topic_arn.as_str())),
            request_id,
        ),
        PubsubSuccess::DeleteTopic => render_response("DeleteTopic", "", request_id),
        PubsubSuccess::GetTopicAttributes { attributes } => render_response(
            "GetTopicAttributes",
            &render_attributes("Attributes", attributes),
            request_id,
        ),
        PubsubSuccess::SetTopicAttributes => render_response("SetTopicAttributes", "", request_id),
        PubsubSuccess::ListTopics { topic_arns } => {
            let members = topic_arns
                .iter()
                .map(|arn| {
                    format!(
                        "<member><TopicArn>{}</TopicArn></member>",
                        escape(arn.as_str())
                    )
                })
                .collect::<String>();
            render_response(
                "ListTopics",
                &format!("<Topics>{members}</Topics>"),
                request_id,
            )
        }
        PubsubSuccess::Subscribe { subscription_arn } => render_response(
            "Subscribe",
            &format!(
                "<SubscriptionArn>{}</SubscriptionArn>",
                escape(subscription_arn)
            ),
            request_id,
        ),
        PubsubSuccess::ConfirmSubscription { subscription_arn } => render_response(
            "ConfirmSubscription",
            &format!(
                "<SubscriptionArn>{}</SubscriptionArn>",
                escape(subscription_arn.as_str())
            ),
            request_id,
        ),
        PubsubSuccess::Unsubscribe => render_response("Unsubscribe", "", request_id),
        PubsubSuccess::GetSubscriptionAttributes { attributes } => render_response(
            "GetSubscriptionAttributes",
            &render_attributes("Attributes", attributes),
            request_id,
        ),
        PubsubSuccess::SetSubscriptionAttributes => {
            render_response("SetSubscriptionAttributes", "", request_id)
        }
        PubsubSuccess::ListSubscriptions { subscriptions } => {
            let members = subscriptions
                .iter()
                .map(render_subscription_member)
                .collect::<String>();
            render_response(
                "ListSubscriptions",
                &format!("<Subscriptions>{members}</Subscriptions>"),
                request_id,
            )
        }
        PubsubSuccess::Publish { message_id } => render_response(
            "Publish",
            &format!("<MessageId>{}</MessageId>", escape(message_id.as_str())),
            request_id,
        ),
    }
}

pub fn render_query_error(error: &PubsubError, request_id: &str) -> String {
    render_query_api_error(&HttpApiError::from(error), request_id)
}

pub fn render_query_api_error(error: &HttpApiError, request_id: &str) -> String {
    format!(
        "<ErrorResponse \
         xmlns=\"{PUBSUB_QUERY_XMLNS}\"><Error><Type>Sender</Type><Code>{}</Code><Message>{}</\
         Message></Error><RequestId>{}</RequestId></ErrorResponse>",
        escape(&error.error_type),
        escape(&error.message),
        escape(request_id)
    )
}

fn render_response(action: &str, result_body: &str, request_id: &str) -> String {
    format!(
        "<{action}Response \
         xmlns=\"{PUBSUB_QUERY_XMLNS}\"><{action}Result>{result_body}</\
         {action}Result><ResponseMetadata><RequestId>{}</RequestId></ResponseMetadata></\
         {action}Response>",
        escape(request_id)
    )
}

fn render_subscription_member(subscription: &SubscriptionView) -> String {
    format!(
        "<member><TopicArn>{}</TopicArn><Protocol>{}</Protocol><Endpoint>{}</\
         Endpoint><SubscriptionArn>{}</SubscriptionArn><Owner></Owner></member>",
        escape(subscription.topic_arn.as_str()),
        escape(subscription.protocol.as_str()),
        escape(&subscription.endpoint),
        escape(subscription.subscription_arn.as_str())
    )
}

fn render_attributes(root: &str, attributes: &HashMap<String, String>) -> String {
    let mut attributes = attributes.iter().collect::<Vec<_>>();
    attributes.sort_by(|left, right| left.0.cmp(right.0));
    let members = attributes
        .into_iter()
        .map(|(key, value)| {
            format!(
                "<entry><key>{}</key><value>{}</value></entry>",
                escape(key),
                escape(value)
            )
        })
        .collect::<String>();
    format!("<{root}>{members}</{root}>")
}

fn required(fields: &BTreeMap<String, String>, name: &str) -> PubsubResult<String> {
    fields.get(name).cloned().ok_or_else(|| {
        PubsubError::validation_with_detail(PubsubValidationKind::UnsupportedAttribute, name)
    })
}

fn decode_named_attribute(
    fields: &BTreeMap<String, String>,
) -> PubsubResult<HashMap<String, String>> {
    Ok(HashMap::from([(
        required(fields, "AttributeName")?,
        required(fields, "AttributeValue")?,
    )]))
}

fn decode_entry_map(fields: &BTreeMap<String, String>, prefix: &str) -> HashMap<String, String> {
    let mut pairs: BTreeMap<usize, (Option<String>, Option<String>)> = BTreeMap::new();
    let key_prefix = format!("{prefix}.entry.");
    for (field, value) in fields {
        let Some(rest) = field.strip_prefix(&key_prefix) else {
            continue;
        };
        let Some((index, part)) = rest.split_once('.') else {
            continue;
        };
        let Ok(index) = index.parse::<usize>() else {
            continue;
        };
        let pair = pairs.entry(index).or_default();
        match part {
            "key" => pair.0 = Some(value.clone()),
            "value" => pair.1 = Some(value.clone()),
            _ => {}
        }
    }
    pairs
        .into_values()
        .filter_map(|(key, value)| Some((key?, value?)))
        .collect()
}

fn decode_message_attributes(fields: &BTreeMap<String, String>) -> HashMap<String, String> {
    let mut names: BTreeMap<usize, String> = BTreeMap::new();
    let mut values: BTreeMap<usize, String> = BTreeMap::new();
    for (field, value) in fields {
        let Some(rest) = field.strip_prefix("MessageAttributes.entry.") else {
            continue;
        };
        let Some((index, part)) = rest.split_once('.') else {
            continue;
        };
        let Ok(index) = index.parse::<usize>() else {
            continue;
        };
        match part {
            "Name" => {
                names.insert(index, value.clone());
            }
            "Value.StringValue" => {
                values.insert(index, value.clone());
            }
            _ => {}
        }
    }
    names
        .into_iter()
        .filter_map(|(index, name)| Some((name, values.remove(&index)?)))
        .collect()
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
