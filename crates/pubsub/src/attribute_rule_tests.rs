//! W3 tests 30, 31 and 36 at the query protocol boundary: the messaging
//! identity, one attribute rule for create and set paths with stored tags,
//! and the response for a protocol that this service does not implement.

use std::sync::Arc;

use pubsub_provider::PubsubError;
use queue::QueueManager;
use sql_backend::{SqlStore, sqlite::SqliteBackend};
use storage_types::MessagingIdentity;

use crate::{
    manager::PubsubManager,
    protocol::{PubsubSuccess, decode_query_request, render_query_success},
};

async fn managers(identity: MessagingIdentity) -> (PubsubManager, Arc<QueueManager>) {
    let driver = SqliteBackend::initialize(":memory:", storage_types::AttemptDeadlines::default())
        .await
        .unwrap();
    let store = Arc::new(
        SqlStore::initialize_schema(driver, storage_types::InitialStreamLineages::ONE, true)
            .await
            .unwrap(),
    );
    let queues = Arc::new(
        QueueManager::new(store.clone())
            .with_messaging_identity(identity.clone())
            .with_endpoint(queue::QueueEndpoint {
                public_base_url: "http://localhost".to_owned(),
            }),
    );
    let manager = PubsubManager::builder()
        .provider(store)
        .queue_wake(queues.wake_handle())
        .messaging_identity(identity)
        .build()
        .unwrap();
    (manager, queues)
}

async fn manager(identity: MessagingIdentity) -> PubsubManager {
    managers(identity).await.0
}

async fn call(manager: &PubsubManager, query: &str) -> Result<PubsubSuccess, PubsubError> {
    manager
        .execute_query_action(decode_query_request(query.as_bytes())?)
        .await
}

fn reason(result: Result<PubsubSuccess, PubsubError>) -> String {
    let error = result.unwrap_err();
    assert_eq!(
        error.aws_query_error_type(),
        "InvalidParameter",
        "{error:?}"
    );
    error.aws_query_message()
}

#[test]
fn given_aws_protocols_this_service_lacks_when_subscribing_then_the_reason_names_them() {
    let subscribe = |protocol: &str| {
        decode_query_request(
            format!(
                "Action=Subscribe&TopicArn=arn:aws:sns:us-east-1:000000000000:t&\
                 Protocol={protocol}&Endpoint=x"
            )
            .as_bytes(),
        )
        .unwrap_err()
        .aws_query_message()
    };
    assert_eq!(
        subscribe("email"),
        "Invalid parameter: Protocol Reason: email is not supported by this service"
    );
    assert_eq!(
        subscribe("lambda"),
        "Invalid parameter: Protocol Reason: lambda is not supported by this service"
    );
    assert_eq!(
        subscribe("ftp"),
        "Invalid parameter: Amazon SNS does not support this protocol string: ftp",
        "the AWS fixture text for an unknown protocol"
    );
}

#[tokio::test]
async fn given_configured_identity_when_topics_and_endpoints_are_used_then_they_carry_it() {
    let manager = manager(MessagingIdentity {
        partition: "aws".to_owned(),
        region: "eu-west-1".to_owned(),
        account_id: "123456789012".to_owned(),
    })
    .await;
    let topic = manager.create_topic("identity").await.unwrap().topic_arn;
    assert_eq!(
        topic.as_str(),
        "arn:aws:sns:eu-west-1:123456789012:identity"
    );
    let attributes = manager
        .get_topic_attributes(pubsub_provider::GetTopicAttributesRequest {
            topic_arn: topic.clone(),
        })
        .await
        .unwrap()
        .attributes;
    assert_eq!(attributes["Owner"], "123456789012");
    let subscribe = |account: &str| {
        format!(
            "Action=Subscribe&TopicArn={topic}&Protocol=sqs&Endpoint=arn:aws:sqs:eu-west-1:\
             {account}:identity-queue"
        )
    };
    assert_eq!(
        reason(call(&manager, &subscribe("000000000000")).await),
        "Invalid parameter: SQS endpoint ARN"
    );
    assert!(call(&manager, &subscribe("123456789012")).await.is_ok());
}

#[tokio::test]
async fn given_create_and_tag_requests_when_executed_then_one_attribute_rule_applies() {
    let manager = manager(MessagingIdentity::default()).await;
    for attribute in ["Policy", "DeliveryPolicy", "KmsMasterKeyId", "Bogus"] {
        let query = format!(
            "Action=CreateTopic&Name=rule&Attributes.entry.1.key={attribute}&Attributes.entry.1.\
             value=x"
        );
        assert_eq!(
            reason(call(&manager, &query).await),
            "Invalid parameter: AttributeName",
            "{attribute}"
        );
    }
    let fifo = "Action=CreateTopic&Name=rule&Attributes.entry.1.key=FifoTopic&Attributes.entry.1.\
                value=true";
    assert_eq!(
        reason(call(&manager, fifo).await),
        "Invalid parameter: AttributeName"
    );
    let created = call(
        &manager,
        "Action=CreateTopic&Name=rule&Attributes.entry.1.key=FifoTopic&Attributes.entry.1.\
         value=false&Tags.member.1.Key=team&Tags.member.1.Value=core&Tags.member.2.Key=env&Tags.\
         member.2.Value=dev",
    )
    .await
    .unwrap();
    let PubsubSuccess::CreateTopic { topic_arn } = created else {
        panic!("{created:?}");
    };
    let display = "Action=CreateTopic&Name=rule&Attributes.entry.1.key=DisplayName&Attributes.\
                   entry.1.value=Other";
    assert_eq!(
        reason(call(&manager, display).await),
        "Invalid parameter: Attributes Reason: Topic already exists with different attributes"
    );
    call(
        &manager,
        &format!(
            "Action=TagResource&ResourceArn={topic_arn}&Tags.member.1.Key=owner&Tags.member.1.\
             Value=ops"
        ),
    )
    .await
    .unwrap();
    call(
        &manager,
        &format!("Action=UntagResource&ResourceArn={topic_arn}&TagKeys.member.1=env"),
    )
    .await
    .unwrap();
    let listed = call(
        &manager,
        &format!("Action=ListTagsForResource&ResourceArn={topic_arn}"),
    )
    .await
    .unwrap();
    let PubsubSuccess::ListTagsForResource { tags } = &listed else {
        panic!("{listed:?}");
    };
    assert_eq!(
        tags.iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>(),
        ["owner=ops", "team=core"]
    );
    let xml = render_query_success(&listed, "request");
    assert!(xml.contains(
        "<ListTagsForResourceResult><Tags><member><Key>owner</Key><Value>ops</Value></member>"
    ));
    let subscribe = |attributes: &str| {
        format!(
            "Action=Subscribe&TopicArn={topic_arn}&Protocol=sqs&Endpoint=arn:aws:sqs:us-east-1:\
             000000000000:rule-queue{attributes}"
        )
    };
    assert_eq!(
        reason(
            call(
                &manager,
                &subscribe(
                    "&Attributes.entry.1.key=SubscriptionRoleArn&Attributes.entry.1.value=x"
                )
            )
            .await
        ),
        "Invalid parameter: Attributes Reason: Unknown attribute SubscriptionRoleArn"
    );
    assert!(
        call(
            &manager,
            &subscribe("&Attributes.entry.1.key=DeliveryPolicy&Attributes.entry.1.value=%7B%7D")
        )
        .await
        .is_ok(),
        "DeliveryPolicy is accepted on a subscription (AWS, section 14)"
    );
    assert_eq!(
        reason(
            call(
                &manager,
                &subscribe(
                    "&Attributes.entry.1.key=RawMessageDelivery&Attributes.entry.1.value=true"
                )
            )
            .await
        ),
        "Invalid parameter: Attributes Reason: Subscription already exists with different \
         attributes"
    );
}

/// Review C2: under a non-default partition, region and account, the queue
/// ARN that `GetQueueAttributes` returns is accepted by `Subscribe` and by an
/// SQS `RedrivePolicy`, and a dead-letter move reports the source in the same
/// form. An ARN in the default partition and region is rejected by SQS.
#[tokio::test]
async fn given_non_default_identity_when_queue_arn_is_reused_then_sqs_and_sns_agree() {
    let identity = MessagingIdentity {
        partition: "aws-cn".to_owned(),
        region: "eu-west-1".to_owned(),
        account_id: "123456789012".to_owned(),
    };
    let (manager, queues) = managers(identity).await;
    let create = |name: &'static str| {
        let queues = queues.clone();
        async move {
            queues
                .create_queue(queue_provider::CreateQueueRequest {
                    queue_name: name.to_owned(),
                    attributes: None,
                    ..Default::default()
                })
                .await
                .unwrap()
                .queue_url
        }
    };
    let arn_of = |url: String| {
        let queues = queues.clone();
        async move {
            queues
                .get_queue_attributes(queue_provider::GetQueueAttributesRequest {
                    queue_url: url,
                    attribute_names: Some(vec!["QueueArn".to_owned()]),
                })
                .await
                .unwrap()
                .attributes["QueueArn"]
                .clone()
        }
    };
    let dlq = create("identity-dlq").await;
    let source = create("identity-source").await;
    let dlq_arn = arn_of(dlq.clone()).await;
    assert_eq!(
        dlq_arn,
        "arn:aws-cn:sqs:eu-west-1:123456789012:identity-dlq"
    );
    let topic = manager.create_topic("identity").await.unwrap().topic_arn;
    assert_eq!(
        topic.as_str(),
        "arn:aws-cn:sns:eu-west-1:123456789012:identity"
    );
    call(
        &manager,
        &format!("Action=Subscribe&TopicArn={topic}&Protocol=sqs&Endpoint={dlq_arn}"),
    )
    .await
    .expect("Subscribe accepts the GetQueueAttributes ARN");
    let set_policy = |target: String| {
        let queues = queues.clone();
        let source = source.clone();
        async move {
            queues
                .set_queue_attributes(queue_provider::SetQueueAttributesRequest {
                    queue_url: source,
                    attributes: std::collections::HashMap::from([(
                        "RedrivePolicy".to_owned(),
                        format!("{{\"deadLetterTargetArn\":\"{target}\",\"maxReceiveCount\":1}}"),
                    )]),
                })
                .await
        }
    };
    assert!(
        set_policy(dlq_arn.replace("aws-cn:sqs:eu-west-1", "aws:sqs:us-east-1"))
            .await
            .is_err(),
        "the default partition and region are another deployment"
    );
    set_policy(dlq_arn.clone())
        .await
        .expect("RedrivePolicy accepts the GetQueueAttributes ARN");
    queues
        .send_message(queue_provider::SendMessageRequest {
            queue_url: source.clone(),
            message_body: "moved".to_owned(),
            delay_seconds: None,
            message_attributes: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let receive = |url: String| queue_provider::ReceiveMessageRequest {
        queue_url: url,
        max_number_of_messages: Some(1),
        visibility_timeout: Some(0),
        wait_time_seconds: Some(0),
        attribute_names: Some(vec!["All".to_owned()]),
        message_attribute_names: None,
    };
    for _ in 0..2 {
        queues
            .receive_message(receive(source.clone()))
            .await
            .unwrap();
    }
    let moved = queues.receive_message(receive(dlq)).await.unwrap().messages;
    let moved = moved
        .iter()
        .find(|message| message.body == "moved")
        .expect("the message moved");
    assert_eq!(
        moved.attributes.as_ref().unwrap()["DeadLetterQueueSourceArn"],
        arn_of(source).await
    );
}

/// Review C6: a repeated `Subscribe` whose policies differ only in JSON
/// whitespace or key order is the same subscription.
#[tokio::test]
async fn given_repeated_subscribe_when_policies_differ_in_whitespace_then_it_succeeds() {
    let manager = manager(MessagingIdentity::default()).await;
    let topic = manager.create_topic("whitespace").await.unwrap().topic_arn;
    let subscribe = |filter: &str, delivery: &str| {
        format!(
            "Action=Subscribe&TopicArn={topic}&Protocol=sqs&Endpoint=arn:aws:sqs:us-east-1:\
             000000000000:whitespace&Attributes.entry.1.key=FilterPolicy&Attributes.entry.1.\
             value={filter}&Attributes.entry.2.key=DeliveryPolicy&Attributes.entry.2.\
             value={delivery}"
        )
    };
    let first = call(
        &manager,
        &subscribe(
            "%7B%22a%22%3A%5B%22x%22%5D%2C%22b%22%3A%5B%22y%22%5D%7D",
            "%7B%7D",
        ),
    )
    .await
    .unwrap();
    let second = call(
        &manager,
        &subscribe(
            "%7B%20%22b%22%20%3A%20%5B%22y%22%5D%2C%20%22a%22%3A%5B%22x%22%5D%20%7D",
            "%7B%20%7D",
        ),
    )
    .await
    .expect("the same policies with other whitespace and key order");
    assert_eq!(first, second, "the same subscription ARN");
}
