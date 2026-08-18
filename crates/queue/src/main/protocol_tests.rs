use axum::{
    body::{self, Bytes},
    http::{HeaderMap, HeaderValue, StatusCode, header},
};
use http_error::HttpApiError;
use serde_json::json;

use crate::protocol::{
    QueueAction, QueueProtocol, QueueRequest, add_common_headers, api_error_response,
    decode_request, error_response, ok_response,
};

fn query_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-www-form-urlencoded"),
    );
    headers
}

#[test]
fn json_request_decodes_action_and_payload() {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-amz-json-1.0"),
    );
    headers.insert(
        "x-amz-target",
        HeaderValue::from_static("AmazonSQS.CreateQueue"),
    );

    let request = decode_request(&headers, Bytes::from_static(br#"{"QueueName":"jobs"}"#))
        .expect("json request decodes");

    assert_eq!(request.protocol, QueueProtocol::Json);
    let QueueRequest::CreateQueue(request) = request.request else {
        panic!("expected create queue request");
    };
    assert_eq!(request.queue_name, "jobs");
}

#[test]
fn query_request_decodes_action_and_flat_payload() {
    let headers = query_headers();

    let request = decode_request(
        &headers,
        Bytes::from_static(b"Action=GetQueueUrl&Version=2012-11-05&QueueName=jobs"),
    )
    .expect("query request decodes");

    assert_eq!(request.protocol, QueueProtocol::Query);
    let QueueRequest::GetQueueUrl(request) = request.request else {
        panic!("expected get queue URL request");
    };
    assert_eq!(request.queue_name, "jobs");
}

#[test]
fn query_request_decodes_numeric_payload_fields() {
    let headers = query_headers();

    let request = decode_request(
        &headers,
        Bytes::from_static(
            b"Action=ReceiveMessage&QueueUrl=http%3A%2F%2Flocalhost%2Fqueue&MaxNumberOfMessages=2&VisibilityTimeout=30",
        ),
    )
    .expect("query request decodes");

    let QueueRequest::ReceiveMessage(request) = request.request else {
        panic!("expected receive request");
    };
    assert_eq!(request.max_number_of_messages, Some(2));
    assert_eq!(request.visibility_timeout, Some(30));
}

#[test]
fn unsupported_json_target_is_invalid_action() {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-amz-json-1.0"),
    );
    headers.insert(
        "x-amz-target",
        HeaderValue::from_static("AmazonSQS.Unsupported"),
    );

    let error =
        decode_request(&headers, Bytes::from_static(b"{}")).expect_err("unsupported action fails");

    assert_eq!(error.code, "InvalidAction");
    assert_eq!(error.message, "unsupported_action");
}

#[test]
fn given_a_client_sends_an_unknown_wire_format_when_decoding_then_the_request_is_rejected_before_routing()
 {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );

    let error = decode_request(&headers, Bytes::from_static(b"{}"))
        .expect_err("unsupported content type fails");

    assert_eq!(error.code, "InvalidParameterValue");
    assert_eq!(error.message, "unsupported_content_type");
}

#[test]
fn given_json_protocol_without_an_amazon_sqs_target_when_decoding_then_no_action_is_inferred() {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-amz-json-1.0"),
    );

    let error =
        decode_request(&headers, Bytes::from_static(b"{}")).expect_err("missing json target fails");

    assert_eq!(error.code, "InvalidAction");
    assert_eq!(error.message, "missing_x_amz_target");
}

#[test]
fn given_json_protocol_with_another_service_target_when_decoding_then_the_target_is_rejected() {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-amz-json-1.0"),
    );
    headers.insert(
        "x-amz-target",
        HeaderValue::from_static("AmazonSNS.CreateQueue"),
    );

    let error = decode_request(&headers, Bytes::from_static(b"{}"))
        .expect_err("wrong json target prefix fails");

    assert_eq!(error.code, "InvalidAction");
    assert_eq!(error.message, "invalid_x_amz_target");
}

#[test]
fn given_json_protocol_with_malformed_payload_when_decoding_then_payload_validation_fails() {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-amz-json-1.0"),
    );
    headers.insert(
        "x-amz-target",
        HeaderValue::from_static("AmazonSQS.CreateQueue"),
    );

    let error =
        decode_request(&headers, Bytes::from_static(b"{")).expect_err("malformed json body fails");

    assert_eq!(error.code, "InvalidParameterValue");
    assert!(error.message.starts_with("invalid_json:"));
}

#[test]
fn given_query_protocol_without_an_action_when_decoding_then_no_operation_is_inferred() {
    let headers = query_headers();

    let error = decode_request(&headers, Bytes::from_static(b"QueueName=jobs"))
        .expect_err("missing query action fails");

    assert_eq!(error.code, "InvalidAction");
    assert_eq!(error.message, "missing_action");
}

#[test]
fn given_query_protocol_with_an_unsupported_action_when_decoding_then_the_action_is_rejected() {
    let headers = query_headers();

    let error = decode_request(&headers, Bytes::from_static(b"Action=Unknown"))
        .expect_err("unsupported query action fails");

    assert_eq!(error.code, "InvalidAction");
    assert_eq!(error.message, "unsupported_action");
}

#[test]
fn given_query_protocol_queue_attributes_when_decoding_then_numbered_name_value_pairs_become_an_attribute_map()
 {
    let headers = query_headers();

    let request = decode_request(
        &headers,
        Bytes::from_static(
            b"Action=SetQueueAttributes&QueueUrl=http%3A%2F%2Flocalhost%2Fqueue&Attribute.2.Value=20&Attribute.1.Name=VisibilityTimeout&Attribute.2.Name=DelaySeconds&Attribute.1.Value=30",
        ),
    )
    .expect("attributes decode");

    let QueueRequest::SetQueueAttributes(request) = request.request else {
        panic!("expected set attributes request");
    };
    assert_eq!(request.attributes["VisibilityTimeout"], "30");
    assert_eq!(request.attributes["DelaySeconds"], "20");
}

#[test]
fn given_query_protocol_attribute_name_lists_when_decoding_then_numbered_members_remain_a_json_array()
 {
    let headers = query_headers();

    let request = decode_request(
        &headers,
        Bytes::from_static(
            b"Action=ReceiveMessage&QueueUrl=http%3A%2F%2Flocalhost%2Fqueue&AttributeName.1=All&MessageAttributeName.1=TraceId",
        ),
    )
    .expect("attribute names decode");

    let QueueRequest::ReceiveMessage(request) = request.request else {
        panic!("expected receive request");
    };
    assert_eq!(request.attribute_names, Some(vec!["All".to_string()]));
    assert_eq!(
        request.message_attribute_names,
        Some(vec!["TraceId".to_string()])
    );
}

#[test]
fn given_query_protocol_message_attributes_when_decoding_then_nested_value_fields_are_grouped_by_name()
 {
    let headers = query_headers();

    let request = decode_request(
        &headers,
        Bytes::from_static(
            b"Action=SendMessage&QueueUrl=http%3A%2F%2Flocalhost%2Fqueue&MessageBody=body&MessageAttribute.1.Name=Trace&MessageAttribute.1.Value.DataType=String&MessageAttribute.1.Value.StringValue=abc",
        ),
    )
    .expect("message attributes decode");

    let QueueRequest::SendMessage(request) = request.request else {
        panic!("expected send request");
    };
    let attributes = request
        .message_attributes
        .as_ref()
        .expect("message attributes");
    let trace = attributes.get("Trace").expect("trace attribute");
    assert_eq!(trace.data_type, "String");
    assert_eq!(trace.string_value.as_deref(), Some("abc"));
}

#[test]
fn given_query_protocol_batch_entries_when_decoding_then_numbered_entries_preserve_batch_order() {
    let headers = query_headers();

    let request = decode_request(
        &headers,
        Bytes::from_static(
            b"Action=SendMessageBatch&QueueUrl=http%3A%2F%2Flocalhost%2Fqueue&SendMessageBatchRequestEntry.2.Id=second&SendMessageBatchRequestEntry.2.MessageBody=two&SendMessageBatchRequestEntry.1.Id=first&SendMessageBatchRequestEntry.1.MessageBody=one&SendMessageBatchRequestEntry.1.DelaySeconds=5",
        ),
    )
    .expect("send batch entries decode");

    let QueueRequest::SendMessageBatch(request) = request.request else {
        panic!("expected send batch request");
    };
    assert_eq!(request.entries[0].id, "first");
    assert_eq!(request.entries[0].delay_seconds, Some(5));
    assert_eq!(request.entries[1].id, "second");
    assert_eq!(request.entries[1].message_body, "two");
}

#[test]
fn given_query_protocol_delete_batch_entries_when_decoding_then_entries_use_the_delete_batch_shape()
{
    let headers = query_headers();

    let request = decode_request(
        &headers,
        Bytes::from_static(
            b"Action=DeleteMessageBatch&QueueUrl=http%3A%2F%2Flocalhost%2Fqueue&DeleteMessageBatchRequestEntry.1.Id=one&DeleteMessageBatchRequestEntry.1.ReceiptHandle=abc",
        ),
    )
    .expect("delete batch entries decode");

    let QueueRequest::DeleteMessageBatch(request) = request.request else {
        panic!("expected delete batch request");
    };
    assert_eq!(request.entries[0].id, "one");
    assert_eq!(request.entries[0].receipt_handle.to_string(), "abc");
}

#[test]
fn given_query_protocol_visibility_batch_entries_when_decoding_then_visibility_timeout_is_numeric()
{
    let headers = query_headers();

    let request = decode_request(
        &headers,
        Bytes::from_static(
            b"Action=ChangeMessageVisibilityBatch&QueueUrl=http%3A%2F%2Flocalhost%2Fqueue&ChangeMessageVisibilityBatchRequestEntry.1.Id=one&ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle=abc&ChangeMessageVisibilityBatchRequestEntry.1.VisibilityTimeout=45",
        ),
    )
    .expect("visibility batch entries decode");

    let QueueRequest::ChangeMessageVisibilityBatch(request) = request.request else {
        panic!("expected visibility batch request");
    };
    assert_eq!(request.entries[0].id, "one");
    assert_eq!(request.entries[0].visibility_timeout, 45);
}

#[tokio::test]
async fn given_json_protocol_success_when_rendering_then_amazon_json_headers_are_returned() {
    let response = ok_response(
        "req-json",
        QueueProtocol::Json,
        QueueAction::CreateQueue,
        &json!({"QueueUrl": "https://queue.example/jobs"}),
    );

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/x-amz-json-1.0"
    );
    assert_eq!(
        response.headers().get("x-amzn-requestid").unwrap(),
        "req-json"
    );
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body reads");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json body decodes");
    assert_eq!(payload["QueueUrl"], "https://queue.example/jobs");
}

#[tokio::test]
async fn given_query_protocol_success_when_rendering_then_aws_query_xml_uses_action_specific_members()
 {
    let response = ok_response(
        "req<&\"'",
        QueueProtocol::Query,
        QueueAction::ListQueues,
        &json!({
            "QueueUrls": ["https://queue.example/one"],
            "Attributes": {
                "VisibilityTimeout": "30",
                "DelaySeconds": "0"
            },
            "Empty": null
        }),
    );

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/xml; charset=utf-8"
    );
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body reads");
    let body = String::from_utf8(body.to_vec()).expect("xml is utf8");

    assert!(body.contains("<ListQueuesResponse"));
    assert!(body.contains("<QueueUrl>https://queue.example/one</QueueUrl>"));
    assert!(body.contains("<Attribute><Name>DelaySeconds</Name><Value>0</Value></Attribute>"));
    assert!(
        body.contains("<Attribute><Name>VisibilityTimeout</Name><Value>30</Value></Attribute>")
    );
    assert!(body.contains("<RequestId>req&lt;&amp;&quot;&apos;</RequestId>"));
    assert!(!body.contains("<Empty>"));
}

#[tokio::test]
async fn given_query_protocol_error_when_rendering_then_dotted_error_types_are_returned_as_query_codes()
 {
    let response = error_response(
        "req&1",
        QueueProtocol::Query,
        StatusCode::BAD_REQUEST,
        "AWS.SimpleQueueService.QueueDoesNotExist",
        "queue <missing>",
    );

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response.headers().get("x-amzn-query-error").unwrap(),
        "AWS.SimpleQueueService.QueueDoesNotExist;Sender"
    );
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body reads");
    let body = String::from_utf8(body.to_vec()).expect("xml is utf8");

    assert!(body.contains("<Code>QueueDoesNotExist</Code>"));
    assert!(body.contains("<Message>queue &lt;missing&gt;</Message>"));
    assert!(body.contains("<RequestId>req&amp;1</RequestId>"));
}

#[tokio::test]
async fn given_json_protocol_error_when_rendering_then_error_type_and_message_are_preserved() {
    let response = error_response(
        "bad\nrequest-id",
        QueueProtocol::Json,
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        "bad input",
    );

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response.headers().get("x-amzn-requestid").unwrap(),
        "invalid"
    );
    assert_eq!(
        response.headers().get("x-amzn-query-error").unwrap(),
        "InvalidParameterValue;Sender"
    );
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body reads");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json body decodes");

    assert_eq!(
        payload["__type"],
        "com.amazon.coral.service#InvalidParameterValueException"
    );
    assert_eq!(payload["message"], "bad input");
}

#[tokio::test]
async fn given_api_error_with_invalid_status_code_when_rendering_then_the_response_falls_back_to_internal_error()
 {
    let response = api_error_response(
        "req-api",
        QueueProtocol::Json,
        HttpApiError {
            error_type: "CustomError".to_string(),
            message: "bad status".to_string(),
            status_code: 99,
            cancellation_reasons: None,
            item: None,
            response_headers: Vec::new(),
        },
    );

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body reads");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json body decodes");
    assert_eq!(payload["__type"], "com.amazonaws.sqs#InternalError");
}

#[tokio::test]
async fn given_service_unavailable_when_rendering_then_retry_after_is_preserved_for_both_protocols()
{
    for protocol in [QueueProtocol::Json, QueueProtocol::Query] {
        let response =
            api_error_response("req-api", protocol, HttpApiError::service_unavailable(7));

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers().get(header::RETRY_AFTER).unwrap(), "7");
        let _ = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body reads");
    }
}

#[test]
fn given_invalid_header_values_when_adding_common_headers_then_header_values_fall_back_to_invalid()
{
    let mut headers = HeaderMap::new();

    add_common_headers(&mut headers, "bad\nrequest-id", Some("bad\nerror"));

    assert_eq!(
        headers.get(header::CONTENT_TYPE).unwrap(),
        "application/x-amz-json-1.0"
    );
    assert_eq!(headers.get("x-amzn-requestid").unwrap(), "invalid");
    assert_eq!(headers.get("x-amzn-query-error").unwrap(), "invalid");
}
