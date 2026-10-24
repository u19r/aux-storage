use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
};

use alloc_counter::AllocationGuard;
use aws_sigv4_signing::{AwsRequestSigner, AwsStaticCredentials, CredentialSource};
use storage_provider::{
    PhysicalWrite, RemoteCredentialStrategy, RemoteStorageSettings, StorageProvider,
    TtlWriteContext,
};
use storage_types::{
    AllOld, AttributeValue, GetItemRequest, KeyAttributes, PutItemRequest, TableName,
};

use crate::RemoteStorageProvider;

const OPERATIONS: usize = 24;

struct HttpFixture {
    process: Child,
    url: String,
}

impl HttpFixture {
    fn start(config: &serde_json::Value) -> Self {
        let mut process = Command::new("python3")
            .args(["-u", "-c", include_str!("remote_body_fixture.py")])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("start isolated HTTP fixture");
        let mut input = process.stdin.take().expect("fixture input");
        serde_json::to_writer(&mut input, config).expect("write fixture config");
        input.write_all(b"\n").expect("finish fixture config");
        drop(input);
        let mut port = String::new();
        BufReader::new(process.stdout.take().expect("fixture output"))
            .read_line(&mut port)
            .expect("read fixture port");
        let port: u16 = port.trim().parse().expect("valid fixture port");
        Self {
            process,
            url: format!("http://127.0.0.1:{port}/"),
        }
    }

    async fn verify_attempts(&self, expected: usize) {
        let response = http_request::reqwest::Client::new()
            .get(&self.url)
            .send()
            .await
            .expect("fixture observations");
        assert!(response.status().is_success());
        let body = response.bytes().await.expect("fixture report body");
        let report: serde_json::Value = serde_json::from_slice(&body).expect("fixture report");
        assert_eq!(report["attempts"], expected);
        assert_eq!(report["errors"], serde_json::json!([]));
    }
}

impl Drop for HttpFixture {
    fn drop(&mut self) {
        self.process.kill().expect("stop own fixture");
        self.process.wait().expect("reap own fixture");
    }
}

async fn create_provider(url: &str, signed: bool) -> RemoteStorageProvider {
    let mut provider = RemoteStorageProvider::new(RemoteStorageSettings {
        endpoint_urls: vec![url.to_owned()],
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: None,
    })
    .await
    .expect("remote provider");
    if signed {
        // This seam exercises signing bytes over local HTTP; it does not prove
        // TLS.
        provider.endpoints[0].requires_signature = true;
        provider.endpoints[0].uri = "https://dynamodb.eu-west-2.amazonaws.com/"
            .parse()
            .expect("signing URI");
        provider.signer = Some(
            AwsRequestSigner::new(
                "eu-west-2",
                CredentialSource::Static(AwsStaticCredentials {
                    access_key_id: "AKIDEXAMPLE".to_owned(),
                    secret_access_key: "fixture-only-secret".to_owned(),
                    session_token: None,
                }),
                "dynamodb",
            )
            .expect("fixture signer"),
        );
    }
    provider
}

async fn measure_case(size: usize, retry: bool, is_put: bool, signed: bool) {
    let table = TableName::new("remote-body-proof");
    let key: KeyAttributes = HashMap::from([
        ("pk".to_owned(), AttributeValue::S("p".repeat(50))),
        ("sk".to_owned(), AttributeValue::S("s".repeat(50))),
    ])
    .into();
    let mut item = HashMap::from([
        ("pk".to_owned(), AttributeValue::S("p".repeat(50))),
        ("sk".to_owned(), AttributeValue::S("s".repeat(50))),
        ("payload".to_owned(), AttributeValue::S("x".repeat(size))),
    ]);
    for index in 0..7 {
        item.insert(
            format!("field{index}"),
            AttributeValue::S("é\"\\\n".to_owned()),
        );
    }
    let put = PutItemRequest::builder()
        .table_name(table.clone())
        .item(storage_types::WireItem::try_from(&item).expect("valid item"))
        .return_values(Some(AllOld::AllOld))
        .build();
    let mut get = GetItemRequest::new(table.clone(), key.clone());
    get.consistent_read = Some(true);
    let request = if is_put {
        serde_json::to_string(&put).expect("expected put bytes")
    } else {
        serde_json::to_string(&get).expect("expected get bytes")
    };
    let response = if is_put {
        serde_json::json!({"Attributes": item})
    } else {
        serde_json::json!({"Item": item})
    };
    let operation = if is_put { "PutItem" } else { "GetItem" };
    let response_bytes = serde_json::to_string(&response).expect("expected response bytes");
    let fixture = HttpFixture::start(&serde_json::json!({
        "request": request,
        "response": response_bytes,
        "target": format!("DynamoDB_20120810.{operation}"),
        "retry": retry,
        "signed": signed,
    }));
    let provider = create_provider(&fixture.url, signed).await;
    for _ in 0..2 {
        complete_operation(&provider, &put, &key, is_put).await;
    }
    let guard = AllocationGuard::start(module_path!(), "remote_body", file!(), line!(), None);
    for _ in 0..OPERATIONS {
        complete_operation(&provider, &put, &key, is_put).await;
    }
    let report = guard.finish();
    fixture
        .verify_attempts((OPERATIONS + 2) * if retry { 2 } else { 1 })
        .await;
    println!(
        "REMOTE_BODY {}",
        serde_json::json!({
            "operation": operation, "payload_bytes": size, "retry": retry,
            "signed": signed,
            "operations": OPERATIONS, "request_bytes": request.len(),
            "response_bytes": response_bytes.len(),
            "allocations": report.allocation_count, "allocated_bytes": report.allocated_bytes,
        })
    );
}

async fn complete_operation(
    provider: &RemoteStorageProvider,
    request: &PutItemRequest,
    key: &KeyAttributes,
    is_put: bool,
) {
    if is_put {
        let response = provider
            .put_item_request(
                PhysicalWrite::try_new(
                    0,
                    request.table_name.clone(),
                    request.clone(),
                    TtlWriteContext::direct(request.table_name.clone()).expect("ttl context"),
                )
                .expect("physical write"),
            )
            .await
            .expect("put success");
        let attributes = response.attributes.expect("old attributes");
        assert_eq!(
            attributes,
            request.item.to_attribute_map().expect("valid request")
        );
    } else {
        let response = provider
            .get_item(request.table_name.clone(), key.clone(), true)
            .await
            .expect("get success")
            .expect("present item")
            .into_attribute_map()
            .expect("owned wire item remains valid");
        assert_eq!(
            response,
            request.item.to_attribute_map().expect("valid request")
        );
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "isolated full-provider allocation comparison; requires python3"]
async fn given_remote_bodies_when_provider_operations_complete_then_allocations_are_reported() {
    for size in [0, 1024, 128 * 1024, 1024 * 1024] {
        for retry in [false, true] {
            for is_put in [false, true] {
                measure_case(size, retry, is_put, false).await;
            }
        }
    }
    for retry in [false, true] {
        measure_case(1024, retry, true, true).await;
    }
}

#[tokio::test]
async fn given_wire_response_when_provider_is_dropped_then_all_item_bytes_remain_owned() {
    let server = httpmock::MockServer::start_async().await;
    let item = serde_json::json!({"pk": {"S": "p"}, "data": {"S": "é\"\\\n".repeat(256)}});
    let expected = item.clone();
    let response = server
        .mock_async(|when, then| {
            when.method(httpmock::Method::POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .json_body(serde_json::json!({
                    "TableName": "remote-body-proof", "Key": {"pk": {"S": "p"}},
                    "ConsistentRead": true,
                }));
            then.status(200)
                .json_body(serde_json::json!({"Item": item}));
        })
        .await;
    let provider = create_provider(&server.url("/"), false).await;
    let result = provider
        .get_item(
            TableName::new("remote-body-proof"),
            HashMap::from([("pk".to_owned(), AttributeValue::S("p".to_owned()))]).into(),
            true,
        )
        .await
        .expect("get success")
        .expect("present item");
    drop(provider);
    let attributes = result.into_attribute_map().expect("owned item decode");
    assert_eq!(
        serde_json::to_value(attributes).expect("item JSON"),
        expected
    );
    response.assert_calls_async(1).await;
}

#[tokio::test]
async fn given_invalid_success_body_when_provider_decodes_then_it_returns_serialization_error() {
    use storage_types::{StorageEnum, context::WrappedError as _};

    let server = httpmock::MockServer::start_async().await;
    let response = server
        .mock_async(|when, then| {
            when.method(httpmock::Method::POST);
            then.status(200).body("{invalid-json");
        })
        .await;
    let provider = create_provider(&server.url("/"), false).await;
    let error = provider
        .get_item(
            TableName::new("remote-body-proof"),
            HashMap::from([("pk".to_owned(), AttributeValue::S("p".to_owned()))]).into(),
            true,
        )
        .await
        .expect_err("malformed success body is not an item");
    assert!(matches!(error.to_enum(), StorageEnum::Serialization(_)));
    response.assert_calls_async(1).await;
}
