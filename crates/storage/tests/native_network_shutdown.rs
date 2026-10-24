#![cfg(all(feature = "foundationdb", feature = "test-support"))]

use std::{
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

#[test]
#[ignore = "requires an owned FDB_CLUSTER_FILE and native client tracing"]
fn given_native_client_tracing_when_runtime_drops_then_explicit_shutdown_exits_cleanly() {
    if std::env::var_os("AUX_STORAGE_NETWORK_SHUTDOWN_CHILD").is_some() {
        prove_native_shutdown();
        return;
    }
    let traces = tempfile::tempdir().expect("private native traces");
    let mut child = Command::new(std::env::current_exe().expect("current test binary"))
        .args([
            "--exact",
            "given_native_client_tracing_when_runtime_drops_then_explicit_shutdown_exits_cleanly",
            "--ignored",
            "--nocapture",
        ])
        .env("AUX_STORAGE_NETWORK_SHUTDOWN_CHILD", "1")
        .env("FDB_NETWORK_OPTION_TRACE_ENABLE", traces.path())
        .env("FDB_NETWORK_OPTION_TRACE_FORMAT", "json")
        .env("FDB_NETWORK_OPTION_TRACE_CLOCK_SOURCE", "realtime")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("isolated native process");
    let deadline = Instant::now() + Duration::from_secs(30);
    while child.try_wait().expect("child status").is_none() {
        if Instant::now() >= deadline {
            child.kill().expect("bounded native child");
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().expect("reaped native child");
    assert!(
        output.status.success(),
        "native child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(std::fs::read_dir(traces.path()).unwrap().next().is_some());
}

fn prove_native_shutdown() {
    storage::shutdown_foundationdb_network().expect("uninitialized shutdown is a no-op");
    {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let scope = storage::FoundationDbTestScope::try_new(
                std::env::var("FDB_CLUSTER_FILE").expect("owned cluster"),
            )
            .await
            .expect("native public facade initialization");
            // Exercise periodic native monitor state before its exit-time
            // cleanup.
            tokio::time::sleep(Duration::from_secs(6)).await;
            scope.cleanup().await.expect("owned namespace cleanup");
        });
    }
    storage::shutdown_foundationdb_network().expect("stop and join before process exit");
    storage::shutdown_foundationdb_network().expect("shutdown is idempotent");
}
