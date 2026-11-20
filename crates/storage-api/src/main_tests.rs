use std::{sync::Mutex, time::Duration};

use axum::Router;
use storage_types::{StorageEnum, StorageError, context::WrappedError as _};
use tokio::net::TcpListener;

use crate::service::{
    poll_server_with_readiness, run_after_listener_ready, runtime_worker_threads,
    spawn_http_server, stop_http_server,
};

#[tokio::test]
async fn deferred_startup_runs_after_server_is_actively_polled() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let (server_task, server_ready, server_shutdown) =
        spawn_http_server(listener, Router::new(), Duration::from_secs(1));
    let events = Mutex::new(Vec::from(["constructed"]));

    run_after_listener_ready(server_ready, async {
        assert!(
            !server_task.is_finished(),
            "listener readiness must be observed while server task is serving"
        );
        events.lock().expect("event lock").push("ready_check");
        Ok(())
    })
    .await
    .expect("deferred startup");

    assert_eq!(
        *events.lock().expect("event lock"),
        ["constructed", "ready_check"]
    );
    stop_http_server(server_task, server_shutdown).await;
}

#[tokio::test]
async fn failed_server_does_not_signal_listener_readiness() {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let mut server =
            Box::pin(async { Err::<(), std::io::Error>(std::io::Error::other("server failed")) });
        let mut ready_tx = Some(ready_tx);
        std::future::poll_fn(move |cx| {
            poll_server_with_readiness(server.as_mut(), &mut ready_tx, cx)
        })
        .await
    });

    let error = run_after_listener_ready(ready_rx, async {
        panic!("startup action must not run after an immediately failed server");
    })
    .await
    .expect_err("failed server must be reported before startup action");
    let StorageEnum::InternalServerError { message } = error.to_enum() else {
        panic!("unexpected readiness error: {error:?}");
    };
    assert!(
        message.contains("stopped before listener readiness"),
        "unexpected readiness error: {message}"
    );
    assert!(server_task.await.expect("server task join").is_err());
}

#[tokio::test]
async fn failed_deferred_startup_stops_server_before_returning_error() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let (server_task, server_ready, server_shutdown) =
        spawn_http_server(listener, Router::new(), Duration::from_secs(1));

    let error = run_after_listener_ready(server_ready, async {
        Err(StorageError::internal("readiness check failed"))
    })
    .await
    .expect_err("deferred startup failure");
    assert!(matches!(
        error.to_enum(),
        StorageEnum::InternalServerError { .. }
    ));
    stop_http_server(server_task, server_shutdown).await;
}

#[test]
fn given_configured_worker_threads_when_runtime_is_sized_then_the_setting_wins() {
    assert_eq!(runtime_worker_threads(std::num::NonZeroUsize::new(3)), 3);
    assert_eq!(
        runtime_worker_threads(None),
        std::thread::available_parallelism().map_or(1, usize::from)
    );
}

#[cfg(not(feature = "tokio-console"))]
#[test]
fn given_tokio_console_without_its_feature_when_starting_then_rejected() {
    let error = crate::service::require_tokio_console_build(&config::RuntimeFeatures {
        tokio_console: true,
        ..Default::default()
    })
    .expect_err("console needs its build feature");

    assert!(
        error
            .to_string()
            .contains("features.runtime.tokio_console requires the tokio-console build feature"),
        "{error}"
    );
    assert!(
        crate::service::require_tokio_console_build(&config::RuntimeFeatures::default()).is_ok()
    );
}
