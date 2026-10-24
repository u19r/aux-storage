//! Rejected logical CDC checkpoints through the replacement stream operation.

use std::{
    panic::{AssertUnwindSafe, resume_unwind},
    sync::Arc,
};

use futures::{FutureExt as _, future::join_all};
use storage_observability_test_support::observation::observe;
use storage_types::{SystemStreamCheckpointToken, SystemStreamScope};
use stream_provider::SystemStreamPageRequest;
use tokio::sync::Barrier;
use tracing::Level;

use super::test_support::{TestStore, assert_dynamo_error_response_exact};

// Validation, negative, and adversarial rules: invalid logical checkpoints
// receive exact client errors and safe, actionable diagnostics.
#[tokio::test]
async fn given_wrong_scope_generation_or_forged_checkpoint_when_system_cdc_is_read_then_each_rejection_is_logged_safely()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let scope = store.stream_operations.system_stream_scope().await.unwrap();
        let forged_text = format!("v1.{}", "ab".repeat(16));
        let forged = SystemStreamCheckpointToken::try_from(forged_text.as_str()).unwrap();
        let cases = [
            (
                SystemStreamPageRequest::try_new(
                    SystemStreamScope::try_new("other", 1).unwrap(),
                    None,
                    1,
                )
                .unwrap(),
                "SystemStream checkpoint scope does not match the request",
            ),
            (
                SystemStreamPageRequest::try_new(
                    SystemStreamScope::try_new(scope.scope_id().as_str(), scope.generation() + 1)
                        .unwrap(),
                    None,
                    1,
                )
                .unwrap(),
                "SystemStream checkpoint generation does not match the request",
            ),
            (
                SystemStreamPageRequest::try_new(scope, Some(forged), 1).unwrap(),
                "SystemStream checkpoint token is not issued for this source",
            ),
        ];

        for (request, message) in cases {
            let failure = observe(store.stream_operations.get_system_stream_page(request)).await;
            let warning = failure.only_warning_or_error_in_span(
                "system_stream_operation",
                &[("operation", "GetSystemStreamPage"), ("stream_name", "system")],
            );
            let span = failure.one_span_with(
                "system_stream_operation",
                &[("operation", "GetSystemStreamPage"), ("stream_name", "system")],
            );
            assert_eq!(span.fields.len(), 2, "only fixed stream context is traced");
            assert_eq!(warning.level, Level::WARN);
            assert_eq!(
                warning.fields.get("error_code").map(String::as_str),
                Some("ValidationException"),
            );
            assert_eq!(warning.fields.get("diagnostic").map(String::as_str), Some(message));
            assert_eq!(
                warning.fields.get("failure_site").map(String::as_str),
                Some("system_stream.read.contract"),
            );
            assert_eq!(
                warning.fields.get("message").map(String::as_str),
                Some("system stream request rejected"),
            );
            assert_eq!(warning.fields.len(), 4, "no token, key, or payload fields");
            assert!(!format!("{:?}{:?}", failure.events, failure.spans).contains(&forged_text));
            let expected = format!(
                r#"{{"__type":"com.amazon.coral.validate#ValidationException","message":"{message}"}}"#
            );
            assert_dynamo_error_response_exact(
                failure.result.unwrap_err().into_storage_error(),
                axum::http::StatusCode::BAD_REQUEST,
                &expected,
            );
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Concurrency and performance rules: simultaneous readers of one frontier
// publish one durable checkpoint token rather than one token per request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_concurrent_empty_system_cdc_reads_when_checkpoint_is_issued_then_every_reader_receives_one_token()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let scope = store.stream_operations.system_stream_scope().await.unwrap();
        let request = SystemStreamPageRequest::try_new(scope, None, 1).unwrap();
        let barrier = Arc::new(Barrier::new(8));
        let readers = (0..8).map(|_| {
            let operations = store.stream_operations.clone();
            let request = request.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                observe(operations.get_system_stream_page(request)).await
            })
        });

        let mut issued_token = None;
        for result in join_all(readers).await {
            let observed = result.unwrap();
            observed.assert_no_warning_or_error();
            observed.one_span_with(
                "system_stream_operation",
                &[
                    ("operation", "GetSystemStreamPage"),
                    ("stream_name", "system"),
                ],
            );
            let page = observed.result.unwrap();
            assert!(page.records().is_empty());
            assert!(page.terminal());
            match &issued_token {
                Some(token) => assert_eq!(page.issued_checkpoint().token(), token),
                None => issued_token = Some(page.issued_checkpoint().token().clone()),
            }
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Boundary and negative rules from the legacy per-database scope test: each
// independent store owns its scope, and a foreign scope cannot read its CDC.
#[tokio::test]
async fn given_two_independent_stores_when_system_cdc_scopes_are_read_then_foreign_scope_is_rejected()
 {
    let first = TestStore::new().await;
    let second = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let first_scope = observe(first.stream_operations.system_stream_scope()).await;
        first_scope.assert_no_warning_or_error();
        let first_scope = first_scope.result.unwrap();
        let second_scope = observe(second.stream_operations.system_stream_scope()).await;
        second_scope.assert_no_warning_or_error();
        let second_scope = second_scope.result.unwrap();
        assert_ne!(first_scope.scope_id(), second_scope.scope_id());
        assert_eq!(first_scope.generation(), 1);
        assert_eq!(second_scope.generation(), 1);
        assert_eq!(
            first.stream_operations.system_stream_scope().await.unwrap(),
            first_scope,
        );

        let own_page = observe(first.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(first_scope.clone(), None, 1).unwrap(),
        ))
        .await;
        own_page.assert_no_warning_or_error();
        assert!(own_page.result.unwrap().records().is_empty());

        let foreign_page = observe(first.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(second_scope.clone(), None, 1).unwrap(),
        ))
        .await;
        let warning = foreign_page.only_warning_or_error_in_span(
            "system_stream_operation",
            &[("operation", "GetSystemStreamPage"), ("stream_name", "system")],
        );
        let span = foreign_page.one_span_with(
            "system_stream_operation",
            &[("operation", "GetSystemStreamPage"), ("stream_name", "system")],
        );
        assert_eq!(span.fields.len(), 2);
        assert_eq!(warning.level, Level::WARN);
        assert_eq!(
            warning.fields.get("error_code").map(String::as_str),
            Some("ValidationException"),
        );
        assert_eq!(
            warning.fields.get("diagnostic").map(String::as_str),
            Some("SystemStream checkpoint scope does not match the request"),
        );
        assert_eq!(
            warning.fields.get("failure_site").map(String::as_str),
            Some("system_stream.read.contract"),
        );
        assert_eq!(
            warning.fields.get("message").map(String::as_str),
            Some("system stream request rejected"),
        );
        assert_eq!(warning.fields.len(), 4);
        let observed = format!("{:?}{:?}", foreign_page.events, foreign_page.spans);
        assert!(!observed.contains(first_scope.scope_id().as_str()));
        assert!(!observed.contains(second_scope.scope_id().as_str()));
        assert_dynamo_error_response_exact(
            foreign_page.result.unwrap_err().into_storage_error(),
            axum::http::StatusCode::BAD_REQUEST,
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"SystemStream checkpoint scope does not match the request"}"#,
        );
    })
    .catch_unwind()
    .await;
    first.finish().await;
    second.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
