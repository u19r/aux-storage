use pubsub_provider::{
    PubsubError, PubsubMessageId, PubsubValidationKind, SubscriptionProtocol, TopicArn,
};
use serde::Deserialize;

use crate::{
    PubsubAction,
    protocol::{
        PubsubSuccess, SubscriptionView, decode_query_request, render_query_error,
        render_query_success,
    },
};

#[test]
fn decode_query_create_topic_attributes() {
    let action = decode_query_request(
        b"Action=CreateTopic&Version=2010-03-31&Name=orders&Attributes.entry.1.key=DisplayName&Attributes.entry.1.value=Orders",
    )
    .unwrap();

    match action {
        PubsubAction::CreateTopic { name, attributes } => {
            assert_eq!(name.as_str(), "orders");
            assert_eq!(attributes.get("DisplayName"), Some(&"Orders".to_string()));
        }
        other => panic!("unexpected action: {other:?}"),
    }
}

#[test]
fn decode_query_subscribe_with_extra_json() {
    let action = decode_query_request(
        b"Action=Subscribe&TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A000000000000%3Aorders&Protocol=https&Endpoint=https%3A%2F%2Fexample.com%2Fhook&ExtraJson=%7B%22token_ref%22%3A%22secret%22%7D",
    )
    .unwrap();

    match action {
        PubsubAction::Subscribe(request) => {
            assert_eq!(request.protocol, SubscriptionProtocol::Https);
            assert_eq!(request.extra_json["token_ref"], "secret");
        }
        other => panic!("unexpected action: {other:?}"),
    }
}

#[test]
fn decode_query_publish_message_attributes() {
    let action = decode_query_request(
        b"Action=Publish&TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A000000000000%3Aorders&Message=hello&MessageAttributes.entry.1.Name=event&MessageAttributes.entry.1.Value.DataType=String&MessageAttributes.entry.1.Value.StringValue=created",
    )
    .unwrap();

    match action {
        PubsubAction::Publish(request) => {
            assert_eq!(request.message, "hello");
            assert_eq!(
                request.message_attributes.get("event"),
                Some(&"created".to_string())
            );
        }
        other => panic!("unexpected action: {other:?}"),
    }
}

#[test]
fn decode_query_set_subscription_attributes_named_attribute() {
    let action = decode_query_request(
        b"Action=SetSubscriptionAttributes&SubscriptionArn=arn%3Aaws%3Asns%3Aus-east-1%3A000000000000%3Aorders%3Asub&AttributeName=RawMessageDelivery&AttributeValue=true",
    )
    .unwrap();

    match action {
        PubsubAction::SetSubscriptionAttributes(request) => {
            assert_eq!(
                request.attributes.get("RawMessageDelivery"),
                Some(&"true".to_string())
            );
        }
        other => panic!("unexpected action: {other:?}"),
    }
}

#[test]
fn decode_query_confirm_subscription() {
    let action = decode_query_request(
        b"Action=ConfirmSubscription&TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A000000000000%3Aorders&Token=token-1",
    )
    .unwrap();

    match action {
        PubsubAction::ConfirmSubscription(request) => {
            assert_eq!(
                request.topic_arn.as_str(),
                "arn:aws:sns:us-east-1:000000000000:orders"
            );
            assert_eq!(request.token, "token-1");
        }
        other => panic!("unexpected action: {other:?}"),
    }
}

#[test]
fn decode_query_errors_use_aws_query_error_kinds() {
    assert_decode_error(
        b"Version=2010-03-31",
        "InvalidParameter",
        "Invalid parameter: AttributeName",
    );
    assert_decode_error(
        b"Action=Unsupported&Version=2010-03-31",
        "InvalidParameter",
        "Invalid parameter: AttributeName",
    );
    assert_decode_error(
        b"Action=CreateTopic&Version=2010-03-31",
        "InvalidParameter",
        "Invalid parameter: AttributeName",
    );
    assert_decode_error(
        b"Action=Subscribe&TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A000000000000%3Aorders&Protocol=ftp&Endpoint=https%3A%2F%2Fexample.com%2Fhook",
        "InvalidParameter",
        "Invalid parameter: Amazon SNS does not support this protocol string: ftp",
    );
    assert_decode_error(
        b"Action=Publish&TopicArn=invalid&Message=hello",
        "InvalidParameter",
        "Invalid parameter: TopicArn Reason: An ARN must have at least 6 elements, not 1",
    );
}

#[test]
fn render_query_create_topic_response_shape() {
    let topic_arn = TopicArn::new("arn:aws:sns:us-east-1:000000000000:orders").unwrap();
    let xml = render_query_success(
        &PubsubSuccess::CreateTopic { topic_arn },
        "00000000-0000-0000-0000-000000000000",
    );

    assert!(
        xml.starts_with(
            "<CreateTopicResponse xmlns=\"https://sns.amazonaws.com/doc/2010-03-31/\">"
        )
    );
    assert!(xml.contains(
        "<CreateTopicResult><TopicArn>arn:aws:sns:us-east-1:000000000000:orders</TopicArn></\
         CreateTopicResult>"
    ));
    assert!(xml.contains(
        "<ResponseMetadata><RequestId>00000000-0000-0000-0000-000000000000</RequestId></\
         ResponseMetadata>"
    ));
}

#[test]
fn render_query_list_subscriptions_member_shape() {
    let topic_arn = TopicArn::new("arn:aws:sns:us-east-1:000000000000:orders").unwrap();
    let subscription_arn =
        pubsub_provider::SubscriptionArn::new("arn:aws:sns:us-east-1:000000000000:orders:sub")
            .unwrap();
    let xml = render_query_success(
        &PubsubSuccess::ListSubscriptions {
            subscriptions: vec![SubscriptionView {
                topic_arn,
                subscription_arn,
                protocol: SubscriptionProtocol::Https,
                endpoint: "https://example.com/hook".to_string(),
            }],
        },
        "request-id",
    );

    assert!(xml.contains("<Subscriptions><member>"));
    assert!(xml.contains("<Protocol>https</Protocol>"));
    assert!(xml.contains("<Endpoint>https://example.com/hook</Endpoint>"));
}

#[test]
fn render_query_publish_response_shape() {
    let message_id = PubsubMessageId::new();
    let xml = render_query_success(&PubsubSuccess::Publish { message_id }, "request-id");

    assert!(xml.contains("<PublishResponse xmlns=\"https://sns.amazonaws.com/doc/2010-03-31/\">"));
    assert!(xml.contains("<PublishResult><MessageId>"));
}

#[test]
fn render_query_confirm_subscription_response_shape() {
    let subscription_arn =
        pubsub_provider::SubscriptionArn::new("arn:aws:sns:us-east-1:000000000000:orders:sub")
            .unwrap();
    let xml = render_query_success(
        &PubsubSuccess::ConfirmSubscription { subscription_arn },
        "request-id",
    );

    assert!(xml.contains("<ConfirmSubscriptionResponse"));
    assert!(xml.contains(
        "<ConfirmSubscriptionResult><SubscriptionArn>arn:aws:sns:us-east-1:000000000000:orders:\
         sub</SubscriptionArn></ConfirmSubscriptionResult>"
    ));
}

#[test]
fn render_query_get_subscription_attributes_response_shape() {
    let xml = render_query_success(
        &PubsubSuccess::GetSubscriptionAttributes {
            attributes: std::collections::HashMap::from([(
                "RawMessageDelivery".to_string(),
                "true".to_string(),
            )]),
        },
        "request-id",
    );

    assert!(xml.contains("<GetSubscriptionAttributesResponse"));
    assert!(xml.contains("<entry><key>RawMessageDelivery</key><value>true</value></entry>"));
}

#[test]
fn render_query_errors_match_normalized_aws_fixtures() {
    let fixtures = query_error_fixtures();
    assert_query_error(
        &fixtures,
        "invalid_topic_name",
        PubsubError::validation(PubsubValidationKind::InvalidTopicName),
    );
    assert_query_error(
        &fixtures,
        "invalid_topic_arn",
        PubsubError::validation(PubsubValidationKind::InvalidTopicArn),
    );
    assert_query_error(
        &fixtures,
        "missing_topic",
        PubsubError::topic_not_found("arn:aws:sns:us-east-1:000000000000:missing"),
    );
    assert_query_error(
        &fixtures,
        "unsupported_protocol",
        PubsubError::validation_with_detail(PubsubValidationKind::UnsupportedProtocol, "ftp"),
    );
    assert_query_error(
        &fixtures,
        "invalid_attribute_name",
        PubsubError::validation_with_detail(PubsubValidationKind::UnsupportedAttribute, "Invalid"),
    );
    assert_query_error(
        &fixtures,
        "empty_message",
        PubsubError::validation(PubsubValidationKind::EmptyMessage),
    );
    assert_query_error(
        &fixtures,
        "invalid_token",
        PubsubError::validation(PubsubValidationKind::InvalidToken),
    );
}

#[test]
fn render_query_error_includes_complete_aws_error_response_shape() {
    let xml = render_query_error(
        &PubsubError::validation(PubsubValidationKind::InvalidTopicName),
        "request-id",
    );

    assert!(xml.starts_with("<ErrorResponse xmlns=\"https://sns.amazonaws.com/doc/2010-03-31/\">"));
    assert!(xml.contains("<Error><Type>Sender</Type><Code>InvalidParameter</Code>"));
    assert!(xml.contains("<Message>Invalid parameter: Topic Name</Message>"));
    assert!(xml.contains("</Error><RequestId>request-id</RequestId></ErrorResponse>"));
}

fn assert_decode_error(body: &[u8], expected_code: &str, expected_message: &str) {
    let error = decode_query_request(body).expect_err("request should fail validation");

    assert_eq!(error.aws_query_error_type(), expected_code);
    assert_eq!(error.aws_query_message(), expected_message);
    assert_eq!(error.aws_query_status_code(), 400);
}

fn assert_query_error(fixtures: &QueryErrorFixtures, name: &str, error: PubsubError) {
    let expected = fixtures.cases.get(name).unwrap_or_else(|| {
        panic!("missing query error fixture case {name}");
    });
    let xml = render_query_error(&error, "request-id");
    assert_eq!(extract_xml_value(&xml, "Code"), expected.code);
    assert_eq!(extract_xml_value(&xml, "Message"), expected.message);
}

fn query_error_fixtures() -> QueryErrorFixtures {
    serde_json::from_str(include_str!("../tests/fixtures/aws/query-errors.json"))
        .expect("query error fixture JSON should be valid")
}

fn extract_xml_value(xml: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let (_, rest) = xml
        .split_once(&open)
        .unwrap_or_else(|| panic!("missing opening tag {open} in {xml}"));
    let (value, _) = rest
        .split_once(&close)
        .unwrap_or_else(|| panic!("missing closing tag {close} in {xml}"));
    value.to_string()
}

#[derive(Debug, Deserialize)]
struct QueryErrorFixtures {
    cases: std::collections::HashMap<String, QueryErrorCase>,
}

#[derive(Debug, Deserialize)]
struct QueryErrorCase {
    code: String,
    message: String,
}
