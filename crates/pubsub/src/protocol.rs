use std::collections::{BTreeMap, HashMap};

use http_error::HttpApiError;
use pubsub_provider::{
    ConfirmSubscriptionRequest, CreateTopicRequest, GetSubscriptionAttributesRequest,
    GetTopicAttributesRequest, ListSubscriptionsRequest, ListTopicsRequest, PublishBatchEntry,
    PublishRequest, PubsubError, PubsubMessageId, PubsubResult, PubsubValidationKind,
    SetSubscriptionAttributesRequest, SetTopicAttributesRequest, SubscribeRequest, Subscription,
    SubscriptionArn, SubscriptionProtocol, TopicArn, TopicName,
};

const PUBSUB_QUERY_XMLNS: &str = "https://sns.amazonaws.com/doc/2010-03-31/";

#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub enum PubsubAction {
    CreateTopic(CreateTopicRequest),
    TagResource {
        resource_arn: TopicArn,
        tags: BTreeMap<String, String>,
    },
    UntagResource {
        resource_arn: TopicArn,
        tag_keys: Vec<String>,
    },
    ListTagsForResource {
        resource_arn: TopicArn,
    },
    DeleteTopic {
        topic_arn: TopicArn,
    },
    GetTopicAttributes(GetTopicAttributesRequest),
    SetTopicAttributes(SetTopicAttributesRequest),
    ListTopics(ListTopicsRequest),
    Subscribe {
        request: SubscribeRequest,
        /// `ReturnSubscriptionArn=true`: answer a pending subscription with
        /// its real ARN instead of `pending confirmation`.
        return_subscription_arn: bool,
    },
    ConfirmSubscription(ConfirmSubscriptionRequest),
    Unsubscribe {
        subscription_arn: SubscriptionArn,
    },
    GetSubscriptionAttributes(GetSubscriptionAttributesRequest),
    SetSubscriptionAttributes(SetSubscriptionAttributesRequest),
    ListSubscriptions(ListSubscriptionsRequest),
    ListSubscriptionsByTopic(ListSubscriptionsRequest),
    Publish(PublishRequest),
    PublishBatch {
        topic_arn: TopicArn,
        entries: Vec<PublishBatchEntry>,
    },
}

/// A failed member of a `PublishBatch` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishBatchFailure {
    pub id: String,
    pub code: &'static str,
    pub message: String,
    pub sender_fault: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubsubSuccess {
    CreateTopic {
        topic_arn: TopicArn,
    },
    TagResource,
    UntagResource,
    ListTagsForResource {
        tags: BTreeMap<String, String>,
    },
    DeleteTopic,
    GetTopicAttributes {
        attributes: HashMap<String, String>,
    },
    SetTopicAttributes,
    ListTopics {
        topic_arns: Vec<TopicArn>,
        next_token: Option<String>,
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
        /// `ListSubscriptions` or `ListSubscriptionsByTopic`.
        by_topic: bool,
        subscriptions: Vec<SubscriptionView>,
        next_token: Option<String>,
    },
    Publish {
        message_id: PubsubMessageId,
    },
    PublishBatch {
        successful: Vec<(String, PubsubMessageId)>,
        failed: Vec<PublishBatchFailure>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionView {
    pub topic_arn: TopicArn,
    pub subscription_arn: SubscriptionArn,
    pub protocol: SubscriptionProtocol,
    pub endpoint: String,
    /// The account of the topic.
    pub owner: String,
    /// Listed as `PendingConfirmation` instead of its ARN.
    pub pending: bool,
}

impl From<Subscription> for SubscriptionView {
    fn from(subscription: Subscription) -> Self {
        Self {
            owner: subscription.owner().to_owned(),
            pending: subscription.confirmation.pending_confirmation(),
            topic_arn: subscription.topic_arn,
            subscription_arn: subscription.subscription_arn,
            protocol: subscription.protocol,
            endpoint: subscription.endpoint,
        }
    }
}

pub fn decode_query_request(body: &[u8]) -> PubsubResult<PubsubAction> {
    let mut fields = url::form_urlencoded::parse(body)
        .into_owned()
        .collect::<BTreeMap<String, String>>();
    let action = required(&mut fields, "Action")?;
    match action.as_str() {
        "CreateTopic" => Ok(PubsubAction::CreateTopic(CreateTopicRequest {
            name: TopicName::new(required(&mut fields, "Name")?)?,
            tags: decode_tags(&fields)?,
            attributes: decode_entry_map(fields, "Attributes"),
        })),
        "TagResource" => Ok(PubsubAction::TagResource {
            resource_arn: TopicArn::new(required(&mut fields, "ResourceArn")?)?,
            tags: decode_tags(&fields)?,
        }),
        "UntagResource" => Ok(PubsubAction::UntagResource {
            resource_arn: TopicArn::new(required(&mut fields, "ResourceArn")?)?,
            tag_keys: decode_members(&fields, "TagKeys.member."),
        }),
        "ListTagsForResource" => Ok(PubsubAction::ListTagsForResource {
            resource_arn: TopicArn::new(required(&mut fields, "ResourceArn")?)?,
        }),
        "DeleteTopic" => Ok(PubsubAction::DeleteTopic {
            topic_arn: TopicArn::new(required(&mut fields, "TopicArn")?)?,
        }),
        "GetTopicAttributes" => Ok(PubsubAction::GetTopicAttributes(
            GetTopicAttributesRequest {
                topic_arn: TopicArn::new(required(&mut fields, "TopicArn")?)?,
            },
        )),
        "SetTopicAttributes" => Ok(PubsubAction::SetTopicAttributes(
            SetTopicAttributesRequest {
                topic_arn: TopicArn::new(required(&mut fields, "TopicArn")?)?,
                attributes: decode_named_attribute(&mut fields)?,
            },
        )),
        "ListTopics" => Ok(PubsubAction::ListTopics(ListTopicsRequest {
            next_token: fields.remove("NextToken"),
        })),
        "Subscribe" => {
            let protocol_text = required(&mut fields, "Protocol")?;
            let protocol = SubscriptionProtocol::parse(&protocol_text)
                .map_err(|rejection| rejection.error(&protocol_text))?;
            let topic_arn = TopicArn::new(required(&mut fields, "TopicArn")?)?;
            let endpoint = required(&mut fields, "Endpoint")?;
            let extra_json = fields
                .remove("ExtraJson")
                .map(|raw| serde_json::from_str(&raw))
                .transpose()?
                .unwrap_or(serde_json::Value::Null);
            let return_subscription_arn = fields
                .remove("ReturnSubscriptionArn")
                .is_some_and(|value| value.eq_ignore_ascii_case("true"));
            Ok(PubsubAction::Subscribe {
                request: SubscribeRequest {
                    topic_arn,
                    protocol,
                    endpoint,
                    attributes: decode_entry_map(fields, "Attributes"),
                    extra_json,
                },
                return_subscription_arn,
            })
        }
        "Unsubscribe" => Ok(PubsubAction::Unsubscribe {
            subscription_arn: SubscriptionArn::new(required(&mut fields, "SubscriptionArn")?)?,
        }),
        "ConfirmSubscription" => Ok(PubsubAction::ConfirmSubscription(
            ConfirmSubscriptionRequest {
                topic_arn: TopicArn::new(required(&mut fields, "TopicArn")?)?,
                token: required(&mut fields, "Token")?,
            },
        )),
        "GetSubscriptionAttributes" => Ok(PubsubAction::GetSubscriptionAttributes(
            GetSubscriptionAttributesRequest {
                subscription_arn: SubscriptionArn::new(required(&mut fields, "SubscriptionArn")?)?,
            },
        )),
        "SetSubscriptionAttributes" => Ok(PubsubAction::SetSubscriptionAttributes(
            SetSubscriptionAttributesRequest {
                subscription_arn: SubscriptionArn::new(required(&mut fields, "SubscriptionArn")?)?,
                attributes: decode_named_attribute(&mut fields)?,
            },
        )),
        "ListSubscriptions" => Ok(PubsubAction::ListSubscriptions(ListSubscriptionsRequest {
            topic_arn: None,
            next_token: fields.remove("NextToken"),
        })),
        "ListSubscriptionsByTopic" => Ok(PubsubAction::ListSubscriptionsByTopic(
            ListSubscriptionsRequest {
                topic_arn: Some(TopicArn::new(required(&mut fields, "TopicArn")?)?),
                next_token: fields.remove("NextToken"),
            },
        )),
        "Publish" => {
            let message = required(&mut fields, "Message")?;
            if fields.remove("MessageStructure").is_some() {
                return Err(PubsubError::validation_with_detail(
                    PubsubValidationKind::UnsupportedAttribute,
                    "MessageStructure",
                ));
            }
            let topic_arn = fields.remove("TopicArn").ok_or_else(|| {
                PubsubError::validation_with_detail(
                    PubsubValidationKind::InvalidAttributeValue,
                    "TopicArn or TargetArn Reason: no value for required parameter",
                )
            })?;
            let topic_arn = TopicArn::new(topic_arn)?;
            let subject = fields.remove("Subject");
            let message_attributes = decode_message_attributes(fields)?;
            Ok(PubsubAction::Publish(PublishRequest {
                topic_arn,
                message,
                subject,
                message_attributes,
            }))
        }
        "PublishBatch" => {
            let topic_arn = TopicArn::new(required(&mut fields, "TopicArn")?)?;
            Ok(PubsubAction::PublishBatch {
                entries: decode_publish_batch(&topic_arn, fields)?,
                topic_arn,
            })
        }
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
        PubsubSuccess::TagResource => render_response("TagResource", "", request_id),
        PubsubSuccess::UntagResource => render_response("UntagResource", "", request_id),
        PubsubSuccess::ListTagsForResource { tags } => {
            let members = tags
                .iter()
                .map(|(key, value)| {
                    format!(
                        "<member><Key>{}</Key><Value>{}</Value></member>",
                        escape(key),
                        escape(value)
                    )
                })
                .collect::<String>();
            render_response(
                "ListTagsForResource",
                &format!("<Tags>{members}</Tags>"),
                request_id,
            )
        }
        PubsubSuccess::GetTopicAttributes { attributes } => render_response(
            "GetTopicAttributes",
            &render_attributes("Attributes", attributes),
            request_id,
        ),
        PubsubSuccess::SetTopicAttributes => render_response("SetTopicAttributes", "", request_id),
        PubsubSuccess::ListTopics {
            topic_arns,
            next_token,
        } => {
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
                &format!(
                    "<Topics>{members}</Topics>{}",
                    render_next_token(next_token)
                ),
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
        PubsubSuccess::ListSubscriptions {
            by_topic,
            subscriptions,
            next_token,
        } => {
            let members = subscriptions
                .iter()
                .map(render_subscription_member)
                .collect::<String>();
            render_response(
                if *by_topic {
                    "ListSubscriptionsByTopic"
                } else {
                    "ListSubscriptions"
                },
                &format!(
                    "<Subscriptions>{members}</Subscriptions>{}",
                    render_next_token(next_token)
                ),
                request_id,
            )
        }
        PubsubSuccess::Publish { message_id } => render_response(
            "Publish",
            &format!("<MessageId>{}</MessageId>", escape(message_id.as_str())),
            request_id,
        ),
        PubsubSuccess::PublishBatch { successful, failed } => render_response(
            "PublishBatch",
            &render_batch(successful, failed),
            request_id,
        ),
    }
}

fn render_next_token(next_token: &Option<String>) -> String {
    next_token
        .as_ref()
        .map(|token| format!("<NextToken>{}</NextToken>", escape(token)))
        .unwrap_or_default()
}

/// Member children in the AWS order (probe A7): `MessageId`, `Id`; and
/// `Code`, `SenderFault`, `Id`, `Message`.
fn render_batch(
    successful: &[(String, PubsubMessageId)],
    failed: &[PublishBatchFailure],
) -> String {
    let successful = successful
        .iter()
        .map(|(id, message_id)| {
            format!(
                "<member><MessageId>{}</MessageId><Id>{}</Id></member>",
                escape(message_id.as_str()),
                escape(id)
            )
        })
        .collect::<String>();
    let failed = failed
        .iter()
        .map(|failure| {
            format!(
                "<member><Code>{}</Code><SenderFault>{}</SenderFault><Id>{}</Id><Message>{}</\
                 Message></member>",
                escape(failure.code),
                failure.sender_fault,
                escape(&failure.id),
                escape(&failure.message)
            )
        })
        .collect::<String>();
    let element = |name: &str, members: String| {
        if members.is_empty() {
            format!("<{name}/>")
        } else {
            format!("<{name}>{members}</{name}>")
        }
    };
    format!(
        "{}{}",
        element("Successful", successful),
        element("Failed", failed)
    )
}

const BATCH_ENTRY_LIST: &str = "PublishBatchRequestEntries";
const BATCH_ENTRY_PREFIX: &str = "PublishBatchRequestEntries.member.";
const MAX_BATCH_ENTRIES: usize = 10;
const MAX_BATCH_ENTRY_ID_LENGTH: usize = 80;

/// Request errors fail the whole `PublishBatch` (probe A7); entry errors
/// (empty message, `MessageStructure`) become `Failed` members later.
fn decode_publish_batch(
    topic_arn: &TopicArn,
    fields: BTreeMap<String, String>,
) -> PubsubResult<Vec<PublishBatchEntry>> {
    let mut members = BTreeMap::<usize, BTreeMap<String, String>>::new();
    let fields_have_empty_list = fields.contains_key(BATCH_ENTRY_LIST);
    for (field, value) in fields {
        let Some(rest) = field.strip_prefix(BATCH_ENTRY_PREFIX) else {
            continue;
        };
        // A member field name without `<index>.<name>` is a request error
        // (review D10; AWS text not probed).
        let invalid = || {
            PubsubError::validation_with_detail(PubsubValidationKind::InvalidBatchMember, &field)
        };
        let (index, part) = rest.split_once('.').ok_or_else(invalid)?;
        let index = index.parse::<usize>().map_err(|_| invalid())?;
        members
            .entry(index)
            .or_default()
            .insert(part.to_owned(), value);
    }
    if members.is_empty() {
        // An SDK sends an empty list as the bare parameter; AWS answers it
        // with `EmptyBatchRequest` and an absent one with `ValidationError`
        // (probe A7).
        if fields_have_empty_list {
            return Err(PubsubError::validation(
                PubsubValidationKind::EmptyBatchRequest,
            ));
        }
        return Err(PubsubError::validation_with_detail(
            PubsubValidationKind::MissingBatchMember,
            "publishBatchRequestEntries",
        ));
    }
    if members.len() > MAX_BATCH_ENTRIES {
        return Err(PubsubError::validation(
            PubsubValidationKind::TooManyEntriesInBatchRequest,
        ));
    }
    let mut ids = std::collections::HashSet::new();
    let mut total = 0_usize;
    let mut entries = Vec::with_capacity(members.len());
    for (position, mut member) in members.into_values().enumerate() {
        let id = member.remove("Id").unwrap_or_default();
        if id.len() > MAX_BATCH_ENTRY_ID_LENGTH {
            return Err(PubsubError::validation_with_detail(
                PubsubValidationKind::BatchEntryIdTooLong,
                &id,
            ));
        }
        if id.is_empty()
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err(PubsubError::validation_with_detail(
                PubsubValidationKind::BatchEntryIdCharacter,
                &id,
            ));
        }
        if !ids.insert(id.clone()) {
            return Err(PubsubError::validation(
                PubsubValidationKind::BatchEntryIdsNotDistinct,
            ));
        }
        let Some(message) = member.remove("Message") else {
            return Err(PubsubError::validation_with_detail(
                PubsubValidationKind::MissingBatchMember,
                format!("publishBatchRequestEntries.{}.member.message", position + 1),
            ));
        };
        check_attribute_types(&member)?;
        let subject = member.remove("Subject");
        let message_structure = member.remove("MessageStructure");
        // Accepted and ignored on a standard topic (probe A7).
        member.remove("MessageGroupId");
        member.remove("MessageDeduplicationId");
        let message_attributes = decode_message_attributes(member)?;
        // Body plus name, data type and value of each attribute; a binary
        // value counts its decoded bytes (review D10; AWS counting not
        // probed).
        total += message.len()
            + message_attributes
                .iter()
                .map(|(name, value)| name.len() + value.data_type().len() + value.byte_len())
                .sum::<usize>();
        entries.push(PublishBatchEntry {
            id,
            request: PublishRequest {
                topic_arn: topic_arn.clone(),
                message,
                subject,
                message_attributes,
            },
            message_structure,
        });
    }
    if total > pubsub_provider::SNS_DEFAULT_MAX_MESSAGE_SIZE_BYTES {
        return Err(PubsubError::validation(
            PubsubValidationKind::BatchRequestTooLong,
        ));
    }
    Ok(entries)
}

/// A data type without the `Binary`, `Number` or `String` prefix fails the
/// whole request with `ParameterValueInvalid` (probe A7).
fn check_attribute_types(member: &BTreeMap<String, String>) -> PubsubResult<()> {
    for (field, data_type) in member {
        let Some(index) = field
            .strip_prefix("MessageAttributes.entry.")
            .and_then(|rest| rest.strip_suffix(".Value.DataType"))
        else {
            continue;
        };
        if ["Binary", "Number", "String"]
            .iter()
            .any(|prefix| data_type.starts_with(prefix))
        {
            continue;
        }
        let name = member
            .get(&format!("MessageAttributes.entry.{index}.Name"))
            .cloned()
            .unwrap_or_default();
        return Err(PubsubError::validation_with_detail(
            PubsubValidationKind::InvalidMessageAttributeType,
            name,
        ));
    }
    Ok(())
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
    // AWS lists an unconfirmed subscription under this literal.
    let listed_arn = if subscription.pending {
        "PendingConfirmation"
    } else {
        subscription.subscription_arn.as_str()
    };
    format!(
        "<member><TopicArn>{}</TopicArn><Protocol>{}</Protocol><Endpoint>{}</\
         Endpoint><SubscriptionArn>{}</SubscriptionArn><Owner>{}</Owner></member>",
        escape(subscription.topic_arn.as_str()),
        escape(subscription.protocol.as_str()),
        escape(&subscription.endpoint),
        escape(listed_arn),
        escape(&subscription.owner)
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

fn required(fields: &mut BTreeMap<String, String>, name: &str) -> PubsubResult<String> {
    fields.remove(name).ok_or_else(|| {
        PubsubError::validation_with_detail(PubsubValidationKind::UnsupportedAttribute, name)
    })
}

fn decode_named_attribute(
    fields: &mut BTreeMap<String, String>,
) -> PubsubResult<HashMap<String, String>> {
    Ok(HashMap::from([(
        required(fields, "AttributeName")?,
        required(fields, "AttributeValue")?,
    )]))
}

fn decode_entry_map(fields: BTreeMap<String, String>, prefix: &str) -> HashMap<String, String> {
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
            "key" => pair.0 = Some(value),
            "value" => pair.1 = Some(value),
            _ => {}
        }
    }
    pairs
        .into_values()
        .filter_map(|(key, value)| Some((key?, value?)))
        .collect()
}

/// `Tags.member.N.Key` and `Tags.member.N.Value`. A tag without a key is
/// invalid.
fn decode_tags(fields: &BTreeMap<String, String>) -> PubsubResult<BTreeMap<String, String>> {
    let mut entries = BTreeMap::<usize, (Option<String>, Option<String>)>::new();
    for (field, value) in fields {
        let Some((index, part)) = field
            .strip_prefix("Tags.member.")
            .and_then(|rest| rest.split_once('.'))
        else {
            continue;
        };
        let Ok(index) = index.parse::<usize>() else {
            continue;
        };
        let entry = entries.entry(index).or_default();
        match part {
            "Key" => entry.0 = Some(value.clone()),
            "Value" => entry.1 = Some(value.clone()),
            _ => {}
        }
    }
    entries
        .into_values()
        .map(|(key, value)| {
            let key = key.ok_or_else(|| {
                PubsubError::validation_with_detail(
                    PubsubValidationKind::InvalidAttributeValue,
                    "Tags Reason: Tag key is invalid",
                )
            })?;
            Ok((key, value.unwrap_or_default()))
        })
        .collect()
}

/// `{prefix}N` members in index order.
fn decode_members(fields: &BTreeMap<String, String>, prefix: &str) -> Vec<String> {
    let mut members = fields
        .iter()
        .filter_map(|(field, value)| {
            let index = field.strip_prefix(prefix)?.parse::<usize>().ok()?;
            Some((index, value.clone()))
        })
        .collect::<Vec<_>>();
    members.sort_by_key(|(index, _)| *index);
    members.into_iter().map(|(_, value)| value).collect()
}

#[derive(Default)]
struct MessageAttributeFields {
    name: Option<String>,
    data_type: Option<String>,
    string_value: Option<String>,
    binary_value: Option<String>,
}

impl MessageAttributeFields {
    fn decode(self) -> PubsubResult<(String, pubsub_provider::MessageAttribute)> {
        let invalid = || PubsubError::validation(PubsubValidationKind::InvalidMessageAttribute);
        let name = self.name.ok_or_else(invalid)?;
        let data_type = self.data_type.ok_or_else(invalid)?;
        let rule = |text: String| {
            PubsubError::validation_with_detail(PubsubValidationKind::MessageAttributeRule, text)
        };
        let (field, wrong, own) = match data_type.as_str() {
            "Binary" => ("Binary", self.string_value, self.binary_value),
            "String" | "String.Array" | "Number" => {
                ("String", self.binary_value, self.string_value)
            }
            _ => return Err(invalid()),
        };
        let value = match (own, wrong) {
            (Some(value), None) if !value.is_empty() => value,
            (Some(_) | None, None) => {
                return Err(rule(format!(
                    "The message attribute '{name}' must contain non-empty message attribute \
                     value for message attribute type '{data_type}'."
                )));
            }
            (_, Some(_)) => {
                return Err(rule(format!(
                    "The message attribute '{name}' with type '{data_type}' must use field \
                     '{field}'."
                )));
            }
        };
        let attribute = pubsub_provider::MessageAttribute::try_from_wire(&data_type, value)
            .map_err(|error| {
                if data_type == "Number" {
                    rule(format!(
                        "Could not cast message attribute '{name}' value to number."
                    ))
                } else {
                    error
                }
            })?;
        Ok((name, attribute))
    }
}

fn decode_message_attributes(
    fields: BTreeMap<String, String>,
) -> PubsubResult<HashMap<String, pubsub_provider::MessageAttribute>> {
    let invalid = || PubsubError::validation(PubsubValidationKind::InvalidMessageAttribute);
    let mut entries = BTreeMap::<usize, MessageAttributeFields>::new();
    for (field, value) in fields {
        let Some(rest) = field.strip_prefix("MessageAttributes.entry.") else {
            continue;
        };
        let (index, part) = rest.split_once('.').ok_or_else(invalid)?;
        let index = index.parse::<usize>().map_err(|_| invalid())?;
        let entry = entries.entry(index).or_default();
        match part {
            "Name" => entry.name = Some(value),
            "Value.DataType" => entry.data_type = Some(value),
            "Value.StringValue" => entry.string_value = Some(value),
            "Value.BinaryValue" => entry.binary_value = Some(value),
            _ => return Err(invalid()),
        }
    }
    let mut attributes = HashMap::new();
    for entry in entries.into_values() {
        let (name, value) = entry.decode()?;
        if attributes.insert(name, value).is_some() {
            return Err(invalid());
        }
    }
    Ok(attributes)
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
