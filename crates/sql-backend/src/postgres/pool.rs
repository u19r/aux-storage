//! Owns physical connections and observes destruction of their connection
//! tasks.

use std::{
    future::Future,
    ops::Deref,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use deadpool::managed;
use deadpool_postgres::ClientWrapper;
use futures::future::BoxFuture;
use storage_types::{AttemptDeadlines, DeadlineClass, StorageError, StorageResult};
use tokio::sync::watch;
use tokio_postgres::{
    AsyncMessage, Config, Connection, Socket,
    tls::{MakeTlsConnect, TlsConnect},
};

pub(super) type Pool = managed::Pool<Manager>;
pub(super) type Client = managed::Object<Manager>;

type Connect = dyn Fn() -> BoxFuture<'static, StorageResult<PostgresClient>> + Send + Sync;

pub(super) struct Manager {
    connect: Box<Connect>,
    #[cfg(feature = "fault-injection")]
    pub(super) cleanup_probe: CleanupProbe,
}

type CleanupProbe = Arc<std::sync::OnceLock<Arc<dyn crate::SqlFaultProbe>>>;

impl Manager {
    pub(super) fn try_new<T>(
        mut config: Config,
        tls: T,
        deadlines: AttemptDeadlines,
    ) -> StorageResult<Self>
    where
        T: MakeTlsConnect<Socket> + Clone + Sync + Send + 'static,
        T::Stream: Sync + Send,
        T::TlsConnect: Sync + Send,
        <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        let settings = SessionSettings::try_new(deadlines)?;
        let options = config.get_options().unwrap_or_default();
        // Session values are the backstop; each BEGIN sets the attempt's own
        // `statement_timeout` and `transaction_timeout`; the session
        // `transaction_timeout` is 0 so the BEGIN's `SET LOCAL` arms the timer.
        // A statement outside the attempt's timers (the first wave's Parse
        // waiting on a lock) ends within one check interval after the client
        // discards the connection, instead of at `statement_timeout`.
        config.options(format!(
            "{options} -c statement_timeout={} -c idle_in_transaction_session_timeout={} -c \
             transaction_timeout=0 -c client_connection_check_interval={} -c \
             client_min_messages=notice",
            settings.statement_ms, settings.statement_ms, settings.connection_check_ms
        ));
        // A dead peer or path closes the socket instead of holding it: probes
        // after one commit grace of silence, and a bound on unacknowledged
        // sent data (Linux only; elsewhere the driver ignores it). The socket
        // takes whole keepalive seconds and Linux rejects zero, so the grace
        // rounds up to at least one second.
        let probe_after = Duration::from_secs(
            deadlines
                .commit_grace()
                .as_secs()
                .saturating_add(u64::from(deadlines.commit_grace().subsec_nanos() > 0))
                .max(1),
        );
        config
            .keepalives(true)
            .keepalives_idle(probe_after)
            .keepalives_interval(probe_after)
            .keepalives_retries(3)
            .tcp_user_timeout(settings.creation_budget);
        let cleanup_probe: CleanupProbe = Default::default();
        Ok(Self {
            #[cfg(feature = "fault-injection")]
            cleanup_probe: cleanup_probe.clone(),
            connect: Box::new(move || {
                let config = config.clone();
                let tls = tls.clone();
                let cleanup_probe = cleanup_probe.clone();
                Box::pin(async move {
                    let creation_by = tokio::time::Instant::now() + settings.creation_budget;
                    let (client, connection) =
                        tokio::time::timeout_at(creation_by, config.connect(tls))
                            .await
                            .map_err(|_| {
                                StorageError::internal("Postgres connection creation timed out")
                            })?
                            .map_err(super::error::map_connection_error)?;
                    let warning = Arc::new(AtomicBool::new(false));
                    let mut connection = Box::pin(observe_connection(connection, warning.clone()));
                    // Until validation succeeds, this create future owns the
                    // actual socket. Cancellation drops it synchronously rather
                    // than merely requesting a spawned task's abort.
                    tokio::time::timeout_at(creation_by, async {
                        tokio::select! {
                            result = settings.check_session(&client, &warning) => result,
                            () = &mut connection => Err(StorageError::internal("Postgres connection stopped during validation")),
                        }
                    }).await.map_err(|_| StorageError::internal("Postgres session validation timed out"))??;
                    let (mut task, stopped) = ConnectionTask::new(connection);
                    task.cleanup_probe = Some(cleanup_probe);
                    // Construct the guard before spawn: abort before the first
                    // poll must still destroy the socket before notifying.
                    let task = tokio::spawn(task);
                    Ok(PostgresClient {
                        client: ClientWrapper::new(client, task),
                        stopped,
                        warning,
                    })
                })
            }),
        })
    }
}

impl managed::Manager for Manager {
    type Type = PostgresClient;
    type Error = StorageError;

    async fn create(&self) -> StorageResult<PostgresClient> {
        (self.connect)().await
    }

    async fn recycle(
        &self,
        client: &mut PostgresClient,
        _: &managed::Metrics,
    ) -> managed::RecycleResult<StorageError> {
        // Preserve Fast recycling: a healthy checkout adds no health query.
        if client.is_closed() || client.has_warning() {
            Err(managed::RecycleError::message("Connection closed"))
        } else {
            Ok(())
        }
    }
}

pub(super) struct PostgresClient {
    pub(super) client: ClientWrapper,
    pub(super) stopped: watch::Receiver<bool>,
    pub(super) warning: Arc<AtomicBool>,
}

impl PostgresClient {
    pub(super) fn has_warning(&self) -> bool {
        self.warning.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn observe_cleanup(&self) -> watch::Receiver<bool> {
        self.stopped.clone()
    }

    pub(super) async fn discard(self) {
        let Self {
            client,
            mut stopped,
            warning: _,
        } = self;
        drop(client);
        // The sender is released only after the connection's socket is dropped.
        // A closed channel therefore also establishes cleanup completion.
        let _ = stopped.wait_for(|stopped| *stopped).await;
    }
}

impl Deref for PostgresClient {
    type Target = ClientWrapper;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

pub(super) struct ConnectionTask<F> {
    connection: Option<Pin<Box<F>>>,
    stopped: Option<watch::Sender<bool>>,
    cleanup_probe: Option<CleanupProbe>,
}

impl<F> ConnectionTask<F> {
    pub(super) fn new(connection: F) -> (Self, watch::Receiver<bool>) {
        let (stopped, receiver) = watch::channel(false);
        (
            Self {
                connection: Some(Box::pin(connection)),
                stopped: Some(stopped),
                cleanup_probe: None,
            },
            receiver,
        )
    }
}

impl<F: Future> Future for ConnectionTask<F> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        match this.connection.as_mut() {
            Some(connection) => connection.as_mut().poll(cx).map(|_| ()),
            None => Poll::Ready(()),
        }
    }
}

impl<F> Drop for ConnectionTask<F> {
    fn drop(&mut self) {
        if let Some(slot) = &self.cleanup_probe
            && let Some(probe) = slot.get()
        {
            probe.before_postgres_connection_cleanup();
        }
        // Dropping the actual connection destroys its socket. An abort request
        // alone does not establish this boundary or prove server termination.
        drop(self.connection.take());
        if let Some(stopped) = self.stopped.take() {
            let _ = stopped.send(true);
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct SessionSettings {
    pub(super) statement_ms: i32,
    pub(super) connection_check_ms: i32,
    creation_budget: Duration,
}

impl SessionSettings {
    pub(super) fn try_new(deadlines: AttemptDeadlines) -> StorageResult<Self> {
        let longest = DeadlineClass::ALL
            .into_iter()
            .map(|class| deadlines.budget(class))
            .fold(Duration::ZERO, Duration::max);
        let total = longest
            .checked_add(deadlines.commit_grace())
            .ok_or_else(|| StorageError::validation("Postgres deadline settings overflow"))?;
        let milliseconds = total
            .as_millis()
            .checked_add(1)
            .and_then(|value| i32::try_from(value).ok())
            .ok_or_else(|| {
                StorageError::validation("Postgres deadline settings exceed supported milliseconds")
            })?;
        // Never above `statement_ms`: the commit grace is part of its total.
        let connection_check_ms = deadlines
            .commit_grace()
            .max(crate::constants::POSTGRES_CONNECTION_CHECK_MIN_INTERVAL)
            .as_millis()
            .min(milliseconds as u128) as i32;
        Ok(Self {
            statement_ms: milliseconds,
            connection_check_ms,
            creation_budget: Duration::from_millis(milliseconds as u64),
        })
    }

    pub(super) async fn check_session(
        &self,
        client: &tokio_postgres::Client,
        warning: &AtomicBool,
    ) -> StorageResult<()> {
        let row = client
            .query_one(
                "SELECT current_setting('server_version_num')::int, (extract(epoch FROM \
                 current_setting('statement_timeout')::interval) * 1000)::bigint, (extract(epoch \
                 FROM current_setting('idle_in_transaction_session_timeout')::interval) * \
                 1000)::bigint, (extract(epoch FROM \
                 current_setting('transaction_timeout')::interval) * 1000)::bigint, \
                 current_setting('client_min_messages'), (extract(epoch FROM \
                 current_setting('client_connection_check_interval')::interval) * 1000)::bigint",
                &[],
            )
            .await
            .map_err(|error| super::error::map_driver_error(&error))?;
        if row.get::<_, i32>(0) < 170000
            || row.get::<_, i64>(1) != i64::from(self.statement_ms)
            || row.get::<_, i64>(2) != i64::from(self.statement_ms)
            || row.get::<_, i64>(3) != 0
            || row.get::<_, &str>(4) != "notice"
            || row.get::<_, i64>(5) != i64::from(self.connection_check_ms)
            || warning.load(Ordering::Acquire)
        {
            return Err(StorageError::internal(
                "Postgres session deadline settings are not qualified",
            ));
        }
        Ok(())
    }
}

pub(super) async fn observe_connection<S, T>(
    mut connection: Connection<S, T>,
    warning: Arc<AtomicBool>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    futures::future::poll_fn(move |cx| {
        loop {
            match connection.poll_message(cx) {
                Poll::Ready(Some(Ok(AsyncMessage::Notice(notice)))) => {
                    if notice.code().code() == "01000"
                        && matches!(
                            notice.parsed_severity(),
                            None | Some(tokio_postgres::error::Severity::Warning)
                        )
                    {
                        // Publish before polling the same socket again: the
                        // next buffered response can acknowledge COMMIT.
                        warning.store(true, Ordering::Release);
                    }
                }
                Poll::Ready(Some(Ok(_))) => {}
                Poll::Ready(Some(Err(_))) | Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => return Poll::Pending,
            }
        }
    })
    .await;
}
