use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use super::pool::ConnectionTask;

struct HeldConnection {
    dropped: Arc<AtomicBool>,
    entered_drop: Option<std::sync::mpsc::Sender<()>>,
    release_drop: Option<std::sync::mpsc::Receiver<()>>,
    is_ready: bool,
}

impl Future for HeldConnection {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        if self.is_ready {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for HeldConnection {
    fn drop(&mut self) {
        if let Some(entered) = self.entered_drop.take() {
            entered.send(()).unwrap();
            self.release_drop.take().unwrap().recv().unwrap();
        }
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn given_unpolled_connection_when_task_aborted_then_cleanup_is_observed() {
    let dropped = Arc::new(AtomicBool::new(false));
    let (connection, mut stopped) = ConnectionTask::new(HeldConnection {
        dropped: dropped.clone(),
        entered_drop: None,
        release_drop: None,
        is_ready: false,
    });
    let task = tokio::spawn(connection);
    task.abort();
    stopped.wait_for(|stopped| *stopped).await.unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    assert!(task.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn given_ready_connection_when_task_finishes_then_future_is_destroyed_before_notification() {
    let dropped = Arc::new(AtomicBool::new(false));
    let (connection, mut stopped) = ConnectionTask::new(HeldConnection {
        dropped: dropped.clone(),
        entered_drop: None,
        release_drop: None,
        is_ready: true,
    });
    tokio::spawn(connection);
    stopped.wait_for(|stopped| *stopped).await.unwrap();
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_held_destructor_when_task_aborted_then_notification_waits_for_destruction() {
    let dropped = Arc::new(AtomicBool::new(false));
    let (entered, entered_receiver) = std::sync::mpsc::channel();
    let (release, release_receiver) = std::sync::mpsc::channel();
    let (connection, mut stopped) = ConnectionTask::new(HeldConnection {
        dropped: dropped.clone(),
        entered_drop: Some(entered),
        release_drop: Some(release_receiver),
        is_ready: false,
    });
    let task = tokio::spawn(connection);
    task.abort();
    tokio::task::spawn_blocking(move || entered_receiver.recv().unwrap())
        .await
        .unwrap();
    assert!(!*stopped.borrow());
    assert!(!dropped.load(Ordering::SeqCst));
    release.send(()).unwrap();
    stopped.wait_for(|stopped| *stopped).await.unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    assert!(task.await.unwrap_err().is_cancelled());
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_native_pool_when_reused_then_cache_survives_and_discard_observes_cleanup() {
    let dsn = std::env::var("TEST_POSTGRES_DSN").unwrap();
    let pool = super::pool::Pool::builder(
        super::pool::Manager::try_new(
            dsn.parse().unwrap(),
            tokio_postgres::NoTls,
            storage_types::AttemptDeadlines::default(),
        )
        .unwrap(),
    )
    .max_size(1)
    .build()
    .unwrap();
    let client = pool.get().await.unwrap();
    let cache = client.statement_cache.clone();
    let first = client.prepare_cached("SELECT 1").await.unwrap();
    client.query_one(&first, &[]).await.unwrap();
    drop(client);
    let client = pool.get().await.unwrap();
    assert!(Arc::ptr_eq(&cache, &client.statement_cache));
    let second = client.prepare_cached("SELECT 1").await.unwrap();
    assert_eq!(cache.size(), 1);
    client.query_one(&second, &[]).await.unwrap();
    let client = super::pool::Client::take(client);
    let super::pool::PostgresClient {
        client,
        stopped,
        warning,
    } = client;
    assert!(!*stopped.borrow());
    super::pool::PostgresClient {
        client,
        stopped,
        warning,
    }
    .discard()
    .await;
    assert_eq!(pool.status().size, 0);
    let replacement = pool.get().await.unwrap();
    assert!(!Arc::ptr_eq(&cache, &replacement.statement_cache));
    replacement.simple_query("SELECT 1").await.unwrap();
    super::pool::Client::take(replacement).discard().await;
}

#[test]
fn given_fractional_budgets_when_settings_derived_then_strict_milliseconds_are_checked() {
    use std::time::Duration;

    use storage_types::{AttemptDeadlines, DeadlineClass};
    let budgets = [Duration::from_micros(1501); DeadlineClass::COUNT];
    let settings = super::pool::SessionSettings::try_new(
        AttemptDeadlines::try_new(budgets, Duration::from_micros(501)).unwrap(),
    )
    .unwrap();
    assert_eq!(settings.statement_ms, 3);
    // The 100 ms floor is capped at `statement_ms`.
    assert_eq!(settings.connection_check_ms, 3);
    let huge = AttemptDeadlines::try_new(
        [Duration::from_millis(i32::MAX as u64); DeadlineClass::COUNT],
        Duration::from_millis(1),
    )
    .unwrap();
    assert!(super::pool::SessionSettings::try_new(huge).is_err());
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_existing_options_when_physical_clients_created_then_owned_settings_override_and_others_survive()
 {
    use std::time::Duration;

    use storage_types::{AttemptDeadlines, DeadlineClass};
    let dsn = std::env::var("TEST_POSTGRES_DSN").unwrap();
    let mut config: tokio_postgres::Config = dsn.parse().unwrap();
    config.options(
        "-c search_path=\"escaped\\ space\",pg_catalog -c statement_timeout=1 -c \
         transaction_timeout=2 -c client_min_messages=error",
    );
    let deadlines = AttemptDeadlines::try_new(
        [Duration::from_micros(2_000_501); DeadlineClass::COUNT],
        Duration::from_micros(501),
    )
    .unwrap();
    let pool = super::pool::Pool::builder(
        super::pool::Manager::try_new(config, tokio_postgres::NoTls, deadlines).unwrap(),
    )
    .max_size(1)
    .build()
    .unwrap();
    let mut first_pid = None;
    for _ in 0..2 {
        let client = pool.get().await.unwrap();
        let row = client
            .query_one(
                "SELECT pg_backend_pid(), current_setting('search_path'), \
                 current_setting('statement_timeout'), \
                 current_setting('idle_in_transaction_session_timeout'), \
                 current_setting('transaction_timeout'), current_setting('client_min_messages')",
                &[],
            )
            .await
            .unwrap();
        let pid = row.get::<_, i32>(0);
        if let Some(previous) = first_pid {
            assert_ne!(previous, pid);
        } else {
            first_pid = Some(pid);
        }
        assert_eq!(row.get::<_, &str>(1), "\"escaped space\",pg_catalog");
        assert_eq!(row.get::<_, &str>(2), "2002ms");
        assert_eq!(row.get::<_, &str>(3), "2002ms");
        assert_eq!(row.get::<_, &str>(4), "0");
        assert_eq!(row.get::<_, &str>(5), "notice");
        super::pool::Client::take(client).discard().await;
    }
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_notice_then_warning_when_connection_polled_then_only_warning_sticks_and_prevents_reuse()
 {
    let dsn = std::env::var("TEST_POSTGRES_DSN").unwrap();
    let pool = super::pool::Pool::builder(
        super::pool::Manager::try_new(
            dsn.parse().unwrap(),
            tokio_postgres::NoTls,
            storage_types::AttemptDeadlines::default(),
        )
        .unwrap(),
    )
    .max_size(1)
    .build()
    .unwrap();
    let client = pool.get().await.unwrap();
    let cache = client.statement_cache.clone();
    client
        .batch_execute("DO $$ BEGIN RAISE NOTICE 'fixture'; END $$")
        .await
        .unwrap();
    assert!(!client.has_warning());
    client
        .batch_execute("DO $$ BEGIN RAISE WARNING 'fixture'; END $$; SELECT 1")
        .await
        .unwrap();
    assert!(client.has_warning());
    client.batch_execute("SELECT 2").await.unwrap();
    assert!(client.has_warning(), "warning must never reset");
    drop(client);
    let replacement = pool.get().await.unwrap();
    assert!(!Arc::ptr_eq(&cache, &replacement.statement_cache));
    assert!(!replacement.has_warning());
    super::pool::Client::take(replacement).discard().await;
}

#[tokio::test]
async fn given_buffered_warning_and_commit_when_polled_then_warning_is_published_before_completion()
{
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let (socket, mut peer) = tokio::io::duplex(4096);
    let peer_task = tokio::spawn(async move {
        let startup_length = peer.read_u32().await.unwrap();
        let mut startup = vec![0; startup_length as usize - 4];
        peer.read_exact(&mut startup).await.unwrap();
        peer.write_all(b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x05I")
            .await
            .unwrap();
        assert_eq!(peer.read_u8().await.unwrap(), b'Q');
        let length = peer.read_u32().await.unwrap();
        let mut query = vec![0; length as usize - 4];
        peer.read_exact(&mut query).await.unwrap();
        assert_eq!(query, b"COMMIT\0");
        let notice = b"SWARNING\0VWARNING\0C01000\0Mfixture\0\0";
        let mut response = vec![b'N'];
        response.extend_from_slice(&((notice.len() + 4) as u32).to_be_bytes());
        response.extend_from_slice(notice);
        response.extend_from_slice(b"C\0\0\0\x0bCOMMIT\0Z\0\0\0\x05I");
        // One peer write deliberately coalesces NoticeResponse and completion.
        peer.write_all(&response).await.unwrap();
        let mut remaining = Vec::new();
        peer.read_to_end(&mut remaining).await.unwrap();
    });
    let mut config = tokio_postgres::Config::new();
    config.user("fixture");
    let (client, connection) = config
        .connect_raw(socket, tokio_postgres::NoTls)
        .await
        .unwrap();
    let warning = Arc::new(AtomicBool::new(false));
    let (task, mut stopped) = super::pool::ConnectionTask::new(super::pool::observe_connection(
        connection,
        warning.clone(),
    ));
    let task = tokio::spawn(task);
    client.batch_execute("COMMIT").await.unwrap();
    assert!(warning.load(Ordering::Acquire));
    drop(client);
    task.abort();
    stopped.wait_for(|stopped| *stopped).await.unwrap();
    peer_task.await.unwrap();
}

#[tokio::test]
async fn given_held_creation_validation_when_acquisition_expires_then_unpublished_socket_is_closed()
{
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (held, hold_received) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let length = socket.read_u32().await.unwrap();
        let mut startup = vec![0; length as usize - 4];
        socket.read_exact(&mut startup).await.unwrap();
        socket
            .write_all(b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x05I")
            .await
            .unwrap();
        assert_eq!(
            socket.read_u8().await.unwrap(),
            b'P',
            "validation uses extended query"
        );
        held.send(()).unwrap();
        // Consume any already-submitted validation messages, withholding all
        // responses. Only destruction of the unpublished socket ends this read.
        let mut pending = Vec::new();
        socket.read_to_end(&mut pending).await.unwrap();
    });
    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user("fixture")
        .ssl_mode(tokio_postgres::config::SslMode::Disable);
    let pool = super::pool::Pool::builder(
        super::pool::Manager::try_new(
            config,
            tokio_postgres::NoTls,
            storage_types::AttemptDeadlines::default(),
        )
        .unwrap(),
    )
    .max_size(1)
    .build()
    .unwrap();
    let mut acquisition = Box::pin(pool.get());
    tokio::select! {
        _ = hold_received => {},
        _ = &mut acquisition => panic!("validation cannot complete without peer replies"),
    }
    // Start the cancellation bound only after the peer confirms validation is
    // held, so correctness does not depend on a small connection-time budget.
    tokio::time::pause();
    let expires = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    let result = tokio::time::timeout_at(expires, acquisition).await;
    assert!(result.is_err());
    peer.await.unwrap();
    assert_eq!(pool.status().size, 0);
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_mismatched_effective_session_setting_when_checked_then_session_is_rejected() {
    let dsn = std::env::var("TEST_POSTGRES_DSN").unwrap();
    let deadlines = storage_types::AttemptDeadlines::default();
    let pool = super::pool::Pool::builder(
        super::pool::Manager::try_new(dsn.parse().unwrap(), tokio_postgres::NoTls, deadlines)
            .unwrap(),
    )
    .max_size(1)
    .build()
    .unwrap();
    let settings = super::pool::SessionSettings::try_new(deadlines).unwrap();
    let client = pool.get().await.unwrap();
    for (setting, wrong, expected) in [
        // Wrong timeouts stay long enough that they never cancel this test's
        // own statements on a loaded machine.
        (
            "statement_timeout",
            (settings.statement_ms + 1).to_string(),
            settings.statement_ms.to_string(),
        ),
        (
            "idle_in_transaction_session_timeout",
            (settings.statement_ms + 1).to_string(),
            settings.statement_ms.to_string(),
        ),
        ("transaction_timeout", "600000".to_owned(), "0".to_owned()),
        (
            "client_min_messages",
            "error".to_owned(),
            "notice".to_owned(),
        ),
    ] {
        // Changes only this test-owned physical session; never database/role
        // settings.
        client
            .query_one("SELECT set_config($1, $2, false)", &[&setting, &wrong])
            .await
            .unwrap();
        assert!(
            settings
                .check_session(&client, &client.warning)
                .await
                .is_err(),
            "{setting}"
        );
        client
            .query_one("SELECT set_config($1, $2, false)", &[&setting, &expected])
            .await
            .unwrap();
        settings
            .check_session(&client, &client.warning)
            .await
            .unwrap();
    }
    super::pool::Client::take(client).discard().await;
}

fn settings_for(
    budget: std::time::Duration,
    grace: std::time::Duration,
) -> super::pool::SessionSettings {
    use storage_types::{AttemptDeadlines, DeadlineClass};
    super::pool::SessionSettings::try_new(
        AttemptDeadlines::try_new([budget; DeadlineClass::COUNT], grace).unwrap(),
    )
    .unwrap()
}

#[test]
fn given_two_second_commit_grace_when_settings_derived_then_connection_check_is_the_grace() {
    use std::time::Duration;

    let settings = settings_for(Duration::from_secs(1), Duration::from_secs(2));
    assert_eq!(settings.connection_check_ms, 2000);
    assert_eq!(settings.statement_ms, 3001);
}

#[test]
fn given_short_commit_grace_when_settings_derived_then_connection_check_is_the_floor() {
    use std::time::Duration;

    let settings = settings_for(Duration::from_secs(1), Duration::from_millis(50));
    assert_eq!(settings.connection_check_ms, 100);
    assert_eq!(settings.statement_ms, 1051);
}

#[test]
fn given_statement_budget_below_the_floor_when_settings_derived_then_connection_check_is_statement_ms()
 {
    use std::time::Duration;

    let settings = settings_for(Duration::from_millis(10), Duration::from_millis(20));
    assert_eq!(settings.statement_ms, 31);
    assert_eq!(settings.connection_check_ms, settings.statement_ms);
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_dsn_option_for_connection_check_when_client_created_then_configured_interval_wins() {
    use std::time::Duration;

    use storage_types::{AttemptDeadlines, DeadlineClass};
    let dsn = std::env::var("TEST_POSTGRES_DSN").unwrap();
    let mut config: tokio_postgres::Config = dsn.parse().unwrap();
    config.options("-c client_connection_check_interval=1");
    let deadlines = AttemptDeadlines::try_new(
        [Duration::from_secs(1); DeadlineClass::COUNT],
        Duration::from_secs(2),
    )
    .unwrap();
    let pool = super::pool::Pool::builder(
        super::pool::Manager::try_new(config, tokio_postgres::NoTls, deadlines).unwrap(),
    )
    .max_size(1)
    .build()
    .unwrap();
    let client = pool.get().await.unwrap();
    let row = client
        .query_one(
            "SELECT current_setting('client_connection_check_interval'), (extract(epoch FROM \
             current_setting('client_connection_check_interval')::interval) * 1000)::bigint",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "2s");
    assert_eq!(row.get::<_, i64>(1), 2000);
    super::pool::Client::take(client).discard().await;
}
