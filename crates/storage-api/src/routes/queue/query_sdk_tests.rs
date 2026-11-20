//! A real Query-protocol SDK parser against the assembled `/queue` route.
//! Ignored: it needs Python with botocore older than 1.31.81 (see
//! `scripts/sqs-query-sdk-check.py`); `SQS_QUERY_SDK_PYTHON` names it.

use super::test_support::QueueRoute;

#[tokio::test]
#[ignore = "needs SQS_QUERY_SDK_PYTHON with botocore < 1.31.81"]
async fn given_query_sdk_client_when_core_flow_runs_then_botocore_parses_every_response() {
    let python = std::env::var("SQS_QUERY_SDK_PYTHON").expect("SQS_QUERY_SDK_PYTHON");
    let endpoint = QueueRoute::start().await.serve().await;
    let script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../scripts/sqs-query-sdk-check.py"
    );
    let output = tokio::process::Command::new(python)
        .args(["-I", script, &endpoint])
        .output()
        .await
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("{stdout}{stderr}");
    assert!(output.status.success(), "{stdout}{stderr}");
}
