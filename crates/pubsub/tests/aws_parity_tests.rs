use std::{
    env, fs,
    path::PathBuf,
    process::{Command, Output},
};

use aws_config::{BehaviorVersion, Region};

const RUN_GUARD: &str = "AUX_STORAGE_RUN_AWS_SNS_PARITY";
const PREFIX_ENV: &str = "AUX_STORAGE_AWS_PARITY_TOPIC_PREFIX";
const REGION_ENV: &str = "AWS_REGION";
const PROFILE_ENV: &str = "AWS_PROFILE";

#[derive(Debug)]
struct AwsSnsParityConfig {
    profile: String,
    region: String,
    topic_prefix: String,
    fixture_dir: PathBuf,
}

impl AwsSnsParityConfig {
    fn from_env() -> Self {
        let run_guard = env::var(RUN_GUARD).unwrap_or_default();
        assert_eq!(
            run_guard, "1",
            "set {RUN_GUARD}=1 to run ignored AWS SNS parity tests"
        );

        let profile = required_env(PROFILE_ENV);
        let region = required_env(REGION_ENV);
        let topic_prefix = required_env(PREFIX_ENV);
        assert!(
            topic_prefix.starts_with("aux-storage-parity-") && topic_prefix.len() >= 24,
            "{PREFIX_ENV} must start with aux-storage-parity- and be long enough to avoid shared \
             resources"
        );

        Self {
            profile,
            region,
            topic_prefix,
            fixture_dir: PathBuf::from("target/aws-sns-fixtures"),
        }
    }

    fn topic_name(&self, suffix: &str) -> String {
        format!("{}{}", self.topic_prefix, suffix)
    }
}

#[test]
#[ignore = "requires explicit AWS credentials and disposable topic prefix"]
fn given_guarded_aws_when_topic_lifecycle_runs_then_raw_fixtures_are_captured() {
    let config = AwsSnsParityConfig::from_env();
    let client = AwsCli::new(&config);
    let topic_name = config.topic_name("topic-lifecycle");
    let mut cleanup = AwsSnsCleanup::new(&client, &config.topic_prefix);

    let create = client
        .sns(["create-topic", "--name", topic_name.as_str()])
        .capture(&config.fixture_dir, "topic-lifecycle-aws-create")
        .success();
    let topic_arn = extract_json_string_field(&create.stdout, "TopicArn");
    cleanup.track_topic_arn(topic_arn.clone());

    client
        .sns(["get-topic-attributes", "--topic-arn", topic_arn.as_str()])
        .capture(&config.fixture_dir, "topic-lifecycle-aws-get-attributes")
        .success();

    client
        .sns([
            "set-topic-attributes",
            "--topic-arn",
            topic_arn.as_str(),
            "--attribute-name",
            "DisplayName",
            "--attribute-value",
            "Orders",
        ])
        .capture(&config.fixture_dir, "topic-lifecycle-aws-set-attributes")
        .success();

    client
        .sns(["list-topics"])
        .capture(&config.fixture_dir, "topic-lifecycle-aws-list")
        .success();

    client
        .sns(["delete-topic", "--topic-arn", topic_arn.as_str()])
        .capture(&config.fixture_dir, "topic-lifecycle-aws-delete")
        .success();
    cleanup.mark_deleted(&topic_arn);
}

#[tokio::test]
#[ignore = "requires explicit AWS credentials and disposable topic prefix"]
async fn given_guarded_aws_sdk_when_topic_publish_connectivity_check_runs_then_disposable_resource_is_cleaned_up()
 {
    let config = AwsSnsParityConfig::from_env();
    let topic_name = config.topic_name("sdk-connectivity");
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .profile_name(&config.profile)
        .region(Region::new(config.region.clone()))
        .load()
        .await;
    let client = aws_sdk_sns::Client::new(&sdk_config);
    let mut topic_arn = None;

    let result: Result<(), String> = async {
        let create = client
            .create_topic()
            .name(&topic_name)
            .send()
            .await
            .map_err(|err| format!("sdk create_topic failed: {err}"))?;
        let created_arn = create
            .topic_arn()
            .ok_or_else(|| "sdk create_topic missing topic_arn".to_string())?
            .to_string();
        topic_arn = Some(created_arn.clone());

        let attributes = client
            .get_topic_attributes()
            .topic_arn(&created_arn)
            .send()
            .await
            .map_err(|err| format!("sdk get_topic_attributes failed: {err}"))?;
        assert!(
            attributes
                .attributes()
                .and_then(|attrs| attrs.get("TopicArn"))
                .is_some(),
            "sdk get_topic_attributes should include TopicArn"
        );

        let publish = client
            .publish()
            .topic_arn(&created_arn)
            .message("sdk-connectivity-message")
            .send()
            .await
            .map_err(|err| format!("sdk publish failed: {err}"))?;
        assert!(
            publish.message_id().is_some(),
            "sdk publish should return message_id"
        );
        Ok(())
    }
    .await;

    if let Some(arn) = topic_arn {
        let _ = client.delete_topic().topic_arn(arn).send().await;
    }
    result.unwrap_or_else(|err| panic!("{err}"));
}

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} must be set"))
}

struct AwsCli {
    profile: String,
    region: String,
}

impl AwsCli {
    fn new(config: &AwsSnsParityConfig) -> Self {
        Self {
            profile: config.profile.clone(),
            region: config.region.clone(),
        }
    }

    fn sns<const N: usize>(&self, args: [&str; N]) -> AwsCliOutput {
        let output = Command::new("aws")
            .arg("--profile")
            .arg(&self.profile)
            .arg("--region")
            .arg(&self.region)
            .arg("sns")
            .args(args)
            .output()
            .unwrap_or_else(|err| panic!("failed to run aws cli: {err}"));
        AwsCliOutput::from_output(output)
    }
}

struct AwsSnsCleanup<'a> {
    client: &'a AwsCli,
    topic_prefix: &'a str,
    topic_arns: Vec<String>,
}

impl<'a> AwsSnsCleanup<'a> {
    fn new(client: &'a AwsCli, topic_prefix: &'a str) -> Self {
        Self {
            client,
            topic_prefix,
            topic_arns: Vec::new(),
        }
    }

    fn track_topic_arn(&mut self, topic_arn: String) {
        assert!(
            topic_arn.ends_with(self.topic_prefix)
                || topic_arn
                    .rsplit_once(':')
                    .is_some_and(|(_, name)| name.starts_with(self.topic_prefix)),
            "refusing to track SNS topic outside disposable prefix: {topic_arn}"
        );
        self.topic_arns.push(topic_arn);
    }

    fn mark_deleted(&mut self, topic_arn: &str) {
        self.topic_arns.retain(|tracked| tracked != topic_arn);
    }
}

impl Drop for AwsSnsCleanup<'_> {
    fn drop(&mut self) {
        for topic_arn in &self.topic_arns {
            let _ = self
                .client
                .sns(["delete-topic", "--topic-arn", topic_arn.as_str()]);
        }
    }
}

#[derive(Debug)]
struct AwsCliOutput {
    status: i32,
    stdout: String,
    stderr: String,
}

impl AwsCliOutput {
    fn from_output(output: Output) -> Self {
        Self {
            status: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }

    fn capture(self, fixture_dir: &PathBuf, name: &str) -> Self {
        fs::create_dir_all(fixture_dir)
            .unwrap_or_else(|err| panic!("failed to create fixture dir: {err}"));
        let body = format!(
            "status={}\n--- stdout ---\n{}\n--- stderr ---\n{}",
            self.status, self.stdout, self.stderr
        );
        fs::write(fixture_dir.join(format!("{name}.txt")), body)
            .unwrap_or_else(|err| panic!("failed to write fixture {name}: {err}"));
        self
    }

    fn success(self) -> Self {
        assert_eq!(self.status, 0, "aws cli command failed: {self:?}");
        self
    }
}

fn extract_json_string_field(body: &str, field: &str) -> String {
    let value: serde_json::Value =
        serde_json::from_str(body).unwrap_or_else(|err| panic!("invalid JSON output: {err}"));
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("missing JSON string field {field}: {body}"))
        .to_string()
}
