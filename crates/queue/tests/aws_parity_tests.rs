use std::{
    env, fs,
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    time::Duration,
};

use aws_config::{BehaviorVersion, Region};
const RUN_GUARD: &str = "AUX_STORAGE_RUN_AWS_PARITY";
const PREFIX_ENV: &str = "AUX_STORAGE_AWS_PARITY_QUEUE_PREFIX";
const REGION_ENV: &str = "AWS_REGION";
const PROFILE_ENV: &str = "AWS_PROFILE";

#[derive(Debug)]
struct AwsParityConfig {
    profile: String,
    region: String,
    queue_prefix: String,
    fixture_dir: PathBuf,
}

impl AwsParityConfig {
    fn from_env() -> Self {
        let run_guard = env::var(RUN_GUARD).unwrap_or_default();
        assert_eq!(
            run_guard, "1",
            "set {RUN_GUARD}=1 to run ignored AWS SQS parity tests"
        );

        let profile = required_env(PROFILE_ENV);
        let region = required_env(REGION_ENV);
        let queue_prefix = required_env(PREFIX_ENV);
        assert!(
            queue_prefix.starts_with("aux-storage-parity-") && queue_prefix.len() >= 24,
            "{PREFIX_ENV} must start with aux-storage-parity- and be long enough to avoid shared \
             resources"
        );

        Self {
            profile,
            region,
            queue_prefix,
            fixture_dir: PathBuf::from("target/aws-sqs-fixtures"),
        }
    }

    fn queue_name(&self, suffix: &str) -> String {
        format!("{}{}", self.queue_prefix, suffix)
    }
}

#[test]
#[ignore = "requires explicit AWS credentials and disposable queue prefix"]
fn given_guarded_aws_and_local_when_queue_lifecycle_runs_then_shapes_match() {
    let config = AwsParityConfig::from_env();
    let queue_name = config.queue_name("create-get-list-delete");
    let aws_client = AwsCli::aws(&config);
    let local_server = LocalQueueServer::start();
    let local_client = AwsCli::local(&config.region, &local_server.base_url);
    let mut aws_cleanup = AwsQueueCleanup::new(&aws_client);
    let mut local_cleanup = AwsQueueCleanup::new(&local_client);

    let aws_create = aws_client
        .sqs(["create-queue", "--queue-name", queue_name.as_str()])
        .capture(&config.fixture_dir, "lifecycle-aws-create")
        .success();
    assert!(
        aws_create.stdout.contains("QueueUrl"),
        "aws create-queue output should include QueueUrl: {aws_create:?}"
    );
    let aws_queue_url = extract_json_string_field(&aws_create.stdout, "QueueUrl");
    aws_cleanup.track_queue_url(aws_queue_url);

    let local_create = local_client
        .sqs(["create-queue", "--queue-name", queue_name.as_str()])
        .capture(&config.fixture_dir, "lifecycle-local-create")
        .success();
    assert!(
        local_create.stdout.contains("QueueUrl"),
        "local create-queue output should include QueueUrl: {local_create:?}"
    );
    let local_queue_url = extract_json_string_field(&local_create.stdout, "QueueUrl");
    local_cleanup.track_queue_url(local_queue_url);

    let aws_get = aws_client
        .sqs(["get-queue-url", "--queue-name", queue_name.as_str()])
        .capture(&config.fixture_dir, "lifecycle-aws-get")
        .success();
    assert!(
        aws_get.stdout.contains("QueueUrl"),
        "aws get-queue-url output should include QueueUrl: {aws_get:?}"
    );

    let local_get = local_client
        .sqs(["get-queue-url", "--queue-name", queue_name.as_str()])
        .capture(&config.fixture_dir, "lifecycle-local-get")
        .success();
    assert!(
        local_get.stdout.contains("QueueUrl"),
        "local get-queue-url output should include QueueUrl: {local_get:?}"
    );

    let aws_list = aws_client
        .sqs([
            "list-queues",
            "--queue-name-prefix",
            config.queue_prefix.as_str(),
        ])
        .capture(&config.fixture_dir, "lifecycle-aws-list")
        .success();
    assert!(
        aws_list.stdout.contains(&queue_name),
        "aws list-queues output should include created queue: {aws_list:?}"
    );

    let local_list = local_client
        .sqs([
            "list-queues",
            "--queue-name-prefix",
            config.queue_prefix.as_str(),
        ])
        .capture(&config.fixture_dir, "lifecycle-local-list")
        .success();
    assert!(
        local_list.stdout.contains(&queue_name),
        "local list-queues output should include created queue: {local_list:?}"
    );
}

#[tokio::test]
#[ignore = "requires explicit AWS credentials and disposable queue prefix"]
async fn given_guarded_aws_sdk_when_queue_connectivity_check_runs_then_disposable_resource_is_cleaned_up()
 {
    let config = AwsParityConfig::from_env();
    let queue_name = config.queue_name("sdk-connectivity");
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .profile_name(&config.profile)
        .region(Region::new(config.region.clone()))
        .load()
        .await;
    let client = aws_sdk_sqs::Client::new(&sdk_config);
    let mut queue_url = None;

    let result: Result<(), String> = async {
        let create = client
            .create_queue()
            .queue_name(&queue_name)
            .send()
            .await
            .map_err(|err| format!("sdk create_queue failed: {err}"))?;
        let created_url = create
            .queue_url()
            .ok_or_else(|| "sdk create_queue missing queue_url".to_string())?
            .to_string();
        queue_url = Some(created_url.clone());

        let send = client
            .send_message()
            .queue_url(&created_url)
            .message_body("sdk-connectivity-message")
            .send()
            .await
            .map_err(|err| format!("sdk send_message failed: {err}"))?;
        assert!(
            send.message_id().is_some(),
            "sdk send_message should return message_id"
        );

        let receive = client
            .receive_message()
            .queue_url(&created_url)
            .max_number_of_messages(1)
            .send()
            .await
            .map_err(|err| format!("sdk receive_message failed: {err}"))?;
        assert!(
            !receive.messages().is_empty(),
            "sdk receive_message should return one message"
        );
        Ok(())
    }
    .await;

    if let Some(url) = queue_url {
        let _ = client.delete_queue().queue_url(url).send().await;
    }
    result.unwrap_or_else(|err| panic!("{err}"));
}

#[test]
#[ignore = "requires explicit AWS credentials and disposable queue prefix"]
fn given_guarded_aws_and_local_when_message_attributes_are_sent_then_md5_matches() {
    let config = AwsParityConfig::from_env();
    let queue_name = config.queue_name("attribute-md5");
    let aws_client = AwsCli::aws(&config);
    let local_server = LocalQueueServer::start();
    let local_client = AwsCli::local(&config.region, &local_server.base_url);
    let mut aws_cleanup = AwsQueueCleanup::new(&aws_client);
    let mut local_cleanup = AwsQueueCleanup::new(&local_client);

    let aws_create = aws_client
        .sqs(["create-queue", "--queue-name", queue_name.as_str()])
        .capture(&config.fixture_dir, "md5-aws-create")
        .success();
    let aws_queue_url = extract_json_string_field(&aws_create.stdout, "QueueUrl");
    aws_cleanup.track_queue_url(aws_queue_url.clone());

    let local_create = local_client
        .sqs(["create-queue", "--queue-name", queue_name.as_str()])
        .capture(&config.fixture_dir, "md5-local-create")
        .success();
    let local_queue_url = extract_json_string_field(&local_create.stdout, "QueueUrl");
    local_cleanup.track_queue_url(local_queue_url.clone());

    let message_attributes = r#"{"kind":{"DataType":"String","StringValue":"blue"},"count":{"DataType":"Number.int","StringValue":"42"}}"#;
    let aws_send = aws_client
        .sqs([
            "send-message",
            "--queue-url",
            aws_queue_url.as_str(),
            "--message-body",
            "message-with-attributes",
            "--message-attributes",
            message_attributes,
        ])
        .capture(&config.fixture_dir, "md5-aws-send")
        .success();
    let local_send = local_client
        .sqs([
            "send-message",
            "--queue-url",
            local_queue_url.as_str(),
            "--message-body",
            "message-with-attributes",
            "--message-attributes",
            message_attributes,
        ])
        .capture(&config.fixture_dir, "md5-local-send")
        .success();

    assert_eq!(
        extract_json_string_field(&local_send.stdout, "MD5OfMessageAttributes"),
        extract_json_string_field(&aws_send.stdout, "MD5OfMessageAttributes")
    );
}

#[test]
#[ignore = "requires explicit AWS credentials and disposable queue prefix"]
fn given_guarded_aws_and_local_when_attributes_are_set_then_supported_values_match() {
    let config = AwsParityConfig::from_env();
    let queue_name = config.queue_name("attributes");
    let aws_client = AwsCli::aws(&config);
    let local_server = LocalQueueServer::start();
    let local_client = AwsCli::local(&config.region, &local_server.base_url);
    let mut aws_cleanup = AwsQueueCleanup::new(&aws_client);
    let mut local_cleanup = AwsQueueCleanup::new(&local_client);

    let aws_queue_url = create_tracked_queue(
        &aws_client,
        &mut aws_cleanup,
        &config.fixture_dir,
        "attributes-aws-create",
        &queue_name,
    );
    let local_queue_url = create_tracked_queue(
        &local_client,
        &mut local_cleanup,
        &config.fixture_dir,
        "attributes-local-create",
        &queue_name,
    );

    let attributes = "VisibilityTimeout=45,ReceiveMessageWaitTimeSeconds=0";
    aws_client
        .sqs([
            "set-queue-attributes",
            "--queue-url",
            aws_queue_url.as_str(),
            "--attributes",
            attributes,
        ])
        .capture(&config.fixture_dir, "attributes-aws-set")
        .success();
    local_client
        .sqs([
            "set-queue-attributes",
            "--queue-url",
            local_queue_url.as_str(),
            "--attributes",
            attributes,
        ])
        .capture(&config.fixture_dir, "attributes-local-set")
        .success();

    let aws_get = aws_client
        .sqs([
            "get-queue-attributes",
            "--queue-url",
            aws_queue_url.as_str(),
            "--attribute-names",
            "VisibilityTimeout",
            "ReceiveMessageWaitTimeSeconds",
        ])
        .capture(&config.fixture_dir, "attributes-aws-get")
        .success();
    let local_get = local_client
        .sqs([
            "get-queue-attributes",
            "--queue-url",
            local_queue_url.as_str(),
            "--attribute-names",
            "VisibilityTimeout",
            "ReceiveMessageWaitTimeSeconds",
        ])
        .capture(&config.fixture_dir, "attributes-local-get")
        .success();

    assert_eq!(
        extract_json_pointer_string(&local_get.stdout, "/Attributes/VisibilityTimeout"),
        extract_json_pointer_string(&aws_get.stdout, "/Attributes/VisibilityTimeout")
    );
    assert_eq!(
        extract_json_pointer_string(
            &local_get.stdout,
            "/Attributes/ReceiveMessageWaitTimeSeconds"
        ),
        extract_json_pointer_string(&aws_get.stdout, "/Attributes/ReceiveMessageWaitTimeSeconds")
    );
}

#[test]
#[ignore = "requires explicit AWS credentials and disposable queue prefix"]
fn given_guarded_aws_and_local_when_send_receive_delete_runs_then_message_shapes_match() {
    let config = AwsParityConfig::from_env();
    let queue_name = config.queue_name("send-receive-delete");
    let aws_client = AwsCli::aws(&config);
    let local_server = LocalQueueServer::start();
    let local_client = AwsCli::local(&config.region, &local_server.base_url);
    let mut aws_cleanup = AwsQueueCleanup::new(&aws_client);
    let mut local_cleanup = AwsQueueCleanup::new(&local_client);

    let aws_queue_url = create_tracked_queue(
        &aws_client,
        &mut aws_cleanup,
        &config.fixture_dir,
        "send-receive-delete-aws-create",
        &queue_name,
    );
    let local_queue_url = create_tracked_queue(
        &local_client,
        &mut local_cleanup,
        &config.fixture_dir,
        "send-receive-delete-local-create",
        &queue_name,
    );

    let body = "send-receive-delete-body";
    let aws_send = aws_client
        .sqs([
            "send-message",
            "--queue-url",
            aws_queue_url.as_str(),
            "--message-body",
            body,
        ])
        .capture(&config.fixture_dir, "send-receive-delete-aws-send")
        .success();
    let local_send = local_client
        .sqs([
            "send-message",
            "--queue-url",
            local_queue_url.as_str(),
            "--message-body",
            body,
        ])
        .capture(&config.fixture_dir, "send-receive-delete-local-send")
        .success();
    assert_eq!(
        extract_json_string_field(&local_send.stdout, "MD5OfMessageBody"),
        extract_json_string_field(&aws_send.stdout, "MD5OfMessageBody")
    );

    let aws_receive = aws_client
        .sqs([
            "receive-message",
            "--queue-url",
            aws_queue_url.as_str(),
            "--max-number-of-messages",
            "1",
            "--visibility-timeout",
            "30",
        ])
        .capture(&config.fixture_dir, "send-receive-delete-aws-receive")
        .success();
    let local_receive = local_client
        .sqs([
            "receive-message",
            "--queue-url",
            local_queue_url.as_str(),
            "--max-number-of-messages",
            "1",
            "--visibility-timeout",
            "30",
        ])
        .capture(&config.fixture_dir, "send-receive-delete-local-receive")
        .success();
    assert_eq!(
        extract_json_pointer_string(&local_receive.stdout, "/Messages/0/Body"),
        extract_json_pointer_string(&aws_receive.stdout, "/Messages/0/Body")
    );

    aws_client
        .sqs([
            "delete-message",
            "--queue-url",
            aws_queue_url.as_str(),
            "--receipt-handle",
            extract_json_pointer_string(&aws_receive.stdout, "/Messages/0/ReceiptHandle").as_str(),
        ])
        .capture(&config.fixture_dir, "send-receive-delete-aws-delete")
        .success();
    local_client
        .sqs([
            "delete-message",
            "--queue-url",
            local_queue_url.as_str(),
            "--receipt-handle",
            extract_json_pointer_string(&local_receive.stdout, "/Messages/0/ReceiptHandle")
                .as_str(),
        ])
        .capture(&config.fixture_dir, "send-receive-delete-local-delete")
        .success();
}

#[test]
#[ignore = "requires explicit AWS credentials and disposable queue prefix"]
fn given_guarded_aws_and_local_when_purge_runs_then_queues_are_empty() {
    let config = AwsParityConfig::from_env();
    let queue_name = config.queue_name("purge");
    let aws_client = AwsCli::aws(&config);
    let local_server = LocalQueueServer::start();
    let local_client = AwsCli::local(&config.region, &local_server.base_url);
    let mut aws_cleanup = AwsQueueCleanup::new(&aws_client);
    let mut local_cleanup = AwsQueueCleanup::new(&local_client);

    let aws_queue_url = create_tracked_queue(
        &aws_client,
        &mut aws_cleanup,
        &config.fixture_dir,
        "purge-aws-create",
        &queue_name,
    );
    let local_queue_url = create_tracked_queue(
        &local_client,
        &mut local_cleanup,
        &config.fixture_dir,
        "purge-local-create",
        &queue_name,
    );

    for (label, client, queue_url) in [
        ("purge-aws-send", &aws_client, aws_queue_url.as_str()),
        ("purge-local-send", &local_client, local_queue_url.as_str()),
    ] {
        client
            .sqs([
                "send-message",
                "--queue-url",
                queue_url,
                "--message-body",
                "purge-body",
            ])
            .capture(&config.fixture_dir, label)
            .success();
    }

    aws_client
        .sqs(["purge-queue", "--queue-url", aws_queue_url.as_str()])
        .capture(&config.fixture_dir, "purge-aws-purge")
        .success();
    local_client
        .sqs(["purge-queue", "--queue-url", local_queue_url.as_str()])
        .capture(&config.fixture_dir, "purge-local-purge")
        .success();

    let aws_receive = aws_client
        .sqs([
            "receive-message",
            "--queue-url",
            aws_queue_url.as_str(),
            "--max-number-of-messages",
            "1",
        ])
        .capture(&config.fixture_dir, "purge-aws-receive-empty")
        .success();
    let local_receive = local_client
        .sqs([
            "receive-message",
            "--queue-url",
            local_queue_url.as_str(),
            "--max-number-of-messages",
            "1",
        ])
        .capture(&config.fixture_dir, "purge-local-receive-empty")
        .success();
    assert_eq!(
        json_has_messages(&local_receive.stdout),
        json_has_messages(&aws_receive.stdout)
    );
}

#[test]
#[ignore = "requires explicit AWS credentials and disposable queue prefix"]
fn given_guarded_aws_and_local_when_batch_send_delete_runs_then_shapes_match() {
    let config = AwsParityConfig::from_env();
    let queue_name = config.queue_name("batch");
    let aws_client = AwsCli::aws(&config);
    let local_server = LocalQueueServer::start();
    let local_client = AwsCli::local(&config.region, &local_server.base_url);
    let mut aws_cleanup = AwsQueueCleanup::new(&aws_client);
    let mut local_cleanup = AwsQueueCleanup::new(&local_client);

    let aws_queue_url = create_tracked_queue(
        &aws_client,
        &mut aws_cleanup,
        &config.fixture_dir,
        "batch-aws-create",
        &queue_name,
    );
    let local_queue_url = create_tracked_queue(
        &local_client,
        &mut local_cleanup,
        &config.fixture_dir,
        "batch-local-create",
        &queue_name,
    );

    let entries =
        r#"[{"Id":"first","MessageBody":"batch-one"},{"Id":"second","MessageBody":"batch-two"}]"#;
    let aws_send = aws_client
        .sqs([
            "send-message-batch",
            "--queue-url",
            aws_queue_url.as_str(),
            "--entries",
            entries,
        ])
        .capture(&config.fixture_dir, "batch-aws-send")
        .success();
    let local_send = local_client
        .sqs([
            "send-message-batch",
            "--queue-url",
            local_queue_url.as_str(),
            "--entries",
            entries,
        ])
        .capture(&config.fixture_dir, "batch-local-send")
        .success();
    assert_eq!(
        json_array_len(&local_send.stdout, "Successful"),
        json_array_len(&aws_send.stdout, "Successful")
    );
    assert_eq!(
        json_array_len(&local_send.stdout, "Failed"),
        json_array_len(&aws_send.stdout, "Failed")
    );

    let aws_receive = aws_client
        .sqs([
            "receive-message",
            "--queue-url",
            aws_queue_url.as_str(),
            "--max-number-of-messages",
            "2",
            "--visibility-timeout",
            "30",
        ])
        .capture(&config.fixture_dir, "batch-aws-receive")
        .success();
    let local_receive = local_client
        .sqs([
            "receive-message",
            "--queue-url",
            local_queue_url.as_str(),
            "--max-number-of-messages",
            "2",
            "--visibility-timeout",
            "30",
        ])
        .capture(&config.fixture_dir, "batch-local-receive")
        .success();
    assert_eq!(
        json_array_len(&local_receive.stdout, "Messages"),
        json_array_len(&aws_receive.stdout, "Messages")
    );

    let aws_delete_entries = delete_batch_entries_json(&aws_receive.stdout);
    let local_delete_entries = delete_batch_entries_json(&local_receive.stdout);
    let aws_delete = aws_client
        .sqs([
            "delete-message-batch",
            "--queue-url",
            aws_queue_url.as_str(),
            "--entries",
            aws_delete_entries.as_str(),
        ])
        .capture(&config.fixture_dir, "batch-aws-delete")
        .success();
    let local_delete = local_client
        .sqs([
            "delete-message-batch",
            "--queue-url",
            local_queue_url.as_str(),
            "--entries",
            local_delete_entries.as_str(),
        ])
        .capture(&config.fixture_dir, "batch-local-delete")
        .success();
    assert_eq!(
        json_array_len(&local_delete.stdout, "Successful"),
        json_array_len(&aws_delete.stdout, "Successful")
    );
    assert_eq!(
        json_array_len(&local_delete.stdout, "Failed"),
        json_array_len(&aws_delete.stdout, "Failed")
    );
}

#[test]
#[ignore = "requires explicit AWS credentials and disposable queue prefix"]
fn given_guarded_aws_account_when_missing_queue_is_requested_then_error_shape_is_captured() {
    let config = AwsParityConfig::from_env();
    let queue_name = config.queue_name("missing-queue");
    let client = AwsCli::aws(&config);

    let output = client
        .sqs(["get-queue-url", "--queue-name", queue_name.as_str()])
        .capture(&config.fixture_dir, "missing-queue-aws-get")
        .failure();
    assert!(
        output
            .stderr
            .contains("AWS.SimpleQueueService.NonExistentQueue")
            || output.stderr.contains("NonExistentQueue"),
        "missing queue output should include SQS not-found code: {output:?}"
    );
}

#[test]
#[ignore = "requires explicit AWS credentials and disposable queue prefix"]
fn given_guarded_aws_and_local_when_validation_errors_are_triggered_then_error_text_matches() {
    let config = AwsParityConfig::from_env();
    let queue_name = config.queue_name("validation-errors");
    let aws_client = AwsCli::aws(&config);
    let local_server = LocalQueueServer::start();
    let local_client = AwsCli::local(&config.region, &local_server.base_url);
    let mut aws_cleanup = AwsQueueCleanup::new(&aws_client);
    let mut local_cleanup = AwsQueueCleanup::new(&local_client);

    let aws_invalid_name = aws_client
        .sqs(["create-queue", "--queue-name", "invalid.name"])
        .capture(&config.fixture_dir, "validation-errors-aws-invalid-name")
        .failure();
    let local_invalid_name = local_client
        .sqs(["create-queue", "--queue-name", "invalid.name"])
        .capture(&config.fixture_dir, "validation-errors-local-invalid-name")
        .failure();
    for output in [&aws_invalid_name, &local_invalid_name] {
        output.assert_stderr_contains("InvalidParameterValue");
        output.assert_stderr_contains(
            "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length",
        );
    }

    let aws_queue_url = create_tracked_queue(
        &aws_client,
        &mut aws_cleanup,
        &config.fixture_dir,
        "validation-errors-aws-create",
        &queue_name,
    );
    let local_queue_url = create_tracked_queue(
        &local_client,
        &mut local_cleanup,
        &config.fixture_dir,
        "validation-errors-local-create",
        &queue_name,
    );

    let duplicate_entries =
        r#"[{"Id":"dup","MessageBody":"one"},{"Id":"dup","MessageBody":"two"}]"#;
    let aws_duplicate = aws_client
        .sqs([
            "send-message-batch",
            "--queue-url",
            aws_queue_url.as_str(),
            "--entries",
            duplicate_entries,
        ])
        .capture(
            &config.fixture_dir,
            "validation-errors-aws-duplicate-batch-id",
        )
        .failure();
    let local_duplicate = local_client
        .sqs([
            "send-message-batch",
            "--queue-url",
            local_queue_url.as_str(),
            "--entries",
            duplicate_entries,
        ])
        .capture(
            &config.fixture_dir,
            "validation-errors-local-duplicate-batch-id",
        )
        .failure();
    for output in [&aws_duplicate, &local_duplicate] {
        output.assert_stderr_contains("BatchEntryIdsNotDistinct");
        output.assert_stderr_contains("Id dup repeated.");
    }

    let aws_invalid_receipt = aws_client
        .sqs([
            "delete-message",
            "--queue-url",
            aws_queue_url.as_str(),
            "--receipt-handle",
            "invalid",
        ])
        .capture(&config.fixture_dir, "validation-errors-aws-invalid-receipt")
        .failure();
    let local_invalid_receipt = local_client
        .sqs([
            "delete-message",
            "--queue-url",
            local_queue_url.as_str(),
            "--receipt-handle",
            "invalid",
        ])
        .capture(
            &config.fixture_dir,
            "validation-errors-local-invalid-receipt",
        )
        .failure();
    for output in [&aws_invalid_receipt, &local_invalid_receipt] {
        output.assert_stderr_contains("ReceiptHandleIsInvalid");
        output.assert_stderr_contains(
            "The input receipt handle \"invalid\" is not a valid receipt handle.",
        );
    }
}

#[derive(Debug)]
struct AwsCli {
    profile: Option<String>,
    region: String,
    endpoint_url: Option<String>,
    local_credentials: bool,
}

impl AwsCli {
    fn aws(config: &AwsParityConfig) -> Self {
        Self {
            profile: Some(config.profile.clone()),
            region: config.region.clone(),
            endpoint_url: None,
            local_credentials: false,
        }
    }

    fn local(region: &str, endpoint_url: &str) -> Self {
        Self {
            profile: None,
            region: region.to_string(),
            endpoint_url: Some(endpoint_url.to_string()),
            local_credentials: true,
        }
    }

    fn sqs<const N: usize>(&self, args: [&str; N]) -> AwsCliOutput {
        let mut command = Command::new("aws");
        if let Some(profile) = &self.profile {
            command.arg("--profile").arg(profile);
        }
        if let Some(endpoint_url) = &self.endpoint_url {
            command.arg("--endpoint-url").arg(endpoint_url);
        }
        if self.local_credentials {
            command
                .env("AWS_ACCESS_KEY_ID", "local")
                .env("AWS_SECRET_ACCESS_KEY", "local")
                .env("AWS_SESSION_TOKEN", "local");
        }
        let output = command
            .arg("--region")
            .arg(&self.region)
            .arg("sqs")
            .args(args)
            .output()
            .unwrap_or_else(|err| panic!("failed to run aws cli: {err}"));
        AwsCliOutput::from_output(output)
    }
}

struct AwsQueueCleanup<'a> {
    client: &'a AwsCli,
    queue_urls: Vec<String>,
}

impl<'a> AwsQueueCleanup<'a> {
    fn new(client: &'a AwsCli) -> Self {
        Self {
            client,
            queue_urls: Vec::new(),
        }
    }

    fn track_queue_url(&mut self, queue_url: String) {
        self.queue_urls.push(queue_url);
    }
}

impl Drop for AwsQueueCleanup<'_> {
    fn drop(&mut self) {
        for queue_url in &self.queue_urls {
            let _ = self
                .client
                .sqs(["delete-queue", "--queue-url", queue_url.as_str()]);
        }
    }
}

struct LocalQueueServer {
    child: Child,
    base_url: String,
}

impl LocalQueueServer {
    fn start() -> Self {
        let port = reserve_port();
        let base_url = format!("http://127.0.0.1:{port}");
        let child = Command::new(env!("CARGO_BIN_EXE_queue"))
            .arg("--port")
            .arg(port.to_string())
            .arg("--db-path")
            .arg(":memory:")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn queue server");
        let server = Self { child, base_url };
        server.wait_until_ready(port);
        server
    }

    fn wait_until_ready(&self, port: u16) {
        for _ in 0..80 {
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("queue server did not become ready");
    }
}

impl Drop for LocalQueueServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn reserve_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

#[derive(Debug)]
struct AwsCliOutput {
    stdout: String,
    stderr: String,
    status_success: bool,
}

impl AwsCliOutput {
    fn from_output(output: Output) -> Self {
        Self {
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            status_success: output.status.success(),
        }
    }

    fn success(self) -> Self {
        assert!(
            self.status_success,
            "aws cli command should succeed: stdout={} stderr={}",
            self.stdout, self.stderr
        );
        self
    }

    fn failure(self) -> Self {
        assert!(
            !self.status_success,
            "aws cli command should fail: stdout={} stderr={}",
            self.stdout, self.stderr
        );
        self
    }

    fn capture(self, fixture_dir: &Path, label: &str) -> Self {
        fs::create_dir_all(fixture_dir).expect("create aws fixture directory");
        fs::write(
            fixture_dir.join(format!("{label}.stdout.json")),
            self.stdout.as_bytes(),
        )
        .expect("write aws fixture stdout");
        fs::write(
            fixture_dir.join(format!("{label}.stderr.txt")),
            self.stderr.as_bytes(),
        )
        .expect("write aws fixture stderr");
        fs::write(
            fixture_dir.join(format!("{label}.status.txt")),
            if self.status_success {
                b"success".as_slice()
            } else {
                b"failure".as_slice()
            },
        )
        .expect("write aws fixture status");
        self
    }

    fn assert_stderr_contains(&self, expected: &str) {
        assert!(
            self.stderr.contains(expected),
            "stderr should contain {expected:?}: {self:?}"
        );
    }
}

fn required_env(name: &str) -> String {
    let value = env::var(name).unwrap_or_else(|_| panic!("{name} is required"));
    assert!(!value.trim().is_empty(), "{name} must not be empty");
    value
}

fn create_tracked_queue(
    client: &AwsCli,
    cleanup: &mut AwsQueueCleanup<'_>,
    fixture_dir: &Path,
    label: &str,
    queue_name: &str,
) -> String {
    let create = client
        .sqs(["create-queue", "--queue-name", queue_name])
        .capture(fixture_dir, label)
        .success();
    let queue_url = extract_json_string_field(&create.stdout, "QueueUrl");
    cleanup.track_queue_url(queue_url.clone());
    queue_url
}

fn extract_json_string_field(json: &str, field: &str) -> String {
    let value: serde_json::Value =
        serde_json::from_str(json).unwrap_or_else(|err| panic!("invalid aws cli json: {err}"));
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("missing string field {field} in {json}"))
        .to_string()
}

fn extract_json_pointer_string(json: &str, pointer: &str) -> String {
    let value: serde_json::Value =
        serde_json::from_str(json).unwrap_or_else(|err| panic!("invalid aws cli json: {err}"));
    value
        .pointer(pointer)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("missing string pointer {pointer} in {json}"))
        .to_string()
}

fn json_has_messages(json: &str) -> bool {
    if json.trim().is_empty() {
        return false;
    }
    let value: serde_json::Value =
        serde_json::from_str(json).unwrap_or_else(|err| panic!("invalid aws cli json: {err}"));
    value
        .get("Messages")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|messages| !messages.is_empty())
}

fn json_array_len(json: &str, field: &str) -> usize {
    let value: serde_json::Value =
        serde_json::from_str(json).unwrap_or_else(|err| panic!("invalid aws cli json: {err}"));
    value
        .get(field)
        .and_then(serde_json::Value::as_array)
        .map_or(0, Vec::len)
}

fn delete_batch_entries_json(receive_json: &str) -> String {
    let value: serde_json::Value = serde_json::from_str(receive_json)
        .unwrap_or_else(|err| panic!("invalid receive json: {err}"));
    let messages = value
        .get("Messages")
        .and_then(serde_json::Value::as_array)
        .unwrap_or_else(|| panic!("receive json missing Messages: {receive_json}"));
    let entries: Vec<_> = messages
        .iter()
        .enumerate()
        .map(|(index, message)| {
            serde_json::json!({
                "Id": format!("delete-{index}"),
                "ReceiptHandle": message
                    .get("ReceiptHandle")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_else(|| panic!("receive message missing ReceiptHandle: {message}")),
            })
        })
        .collect();
    serde_json::to_string(&entries).expect("delete batch entries json")
}
