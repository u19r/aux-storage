//! Cross-node wake hints for waiting receives over LISTEN/NOTIFY (N1).

use std::{sync::Arc, time::Duration};

use futures::{StreamExt as _, future::BoxFuture};
use queue_provider::{QueueWakeBoard, QueueWakeSlot, QueueWakeTasks};
use storage_types::{AttemptDeadlines, StorageResult};
use tokio_postgres::{
    Config, Socket,
    tls::{MakeTlsConnect, TlsConnect},
};

const QUEUE_WAKE_CHANNEL: &str = "aux_queue_wake";
/// Coalesces the committed slots of this node into one NOTIFY.
const QUEUE_WAKE_NOTIFY_SPACING: Duration = Duration::from_millis(10);
/// Bound on a NOTIFY lost between a sender's commit and its notifier.
const QUEUE_WAKE_FALLBACK: Duration = Duration::from_secs(10);
const QUEUE_WAKE_MIN_BACKOFF: Duration = Duration::from_millis(100);
const QUEUE_WAKE_MAX_BACKOFF: Duration = Duration::from_secs(5);

/// One session outside the pool: a client and the notifications of its
/// connection. The receiver ends when the connection ends.
pub(super) struct WakeSession {
    client: tokio_postgres::Client,
    notifications: tokio::sync::mpsc::UnboundedReceiver<tokio_postgres::Notification>,
}

pub(super) type WakeConnect =
    Arc<dyn Fn() -> BoxFuture<'static, StorageResult<WakeSession>> + Send + Sync>;

/// Built in `PostgresBackend::initialize` from the same `Config` and TLS
/// connector as the pool, with the same keepalive settings as `pool.rs`
/// `Manager::try_new`, and without its statement settings.
pub(super) fn wake_connect<T>(config: Config, tls: T, deadlines: AttemptDeadlines) -> WakeConnect
where
    T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    T::Stream: Send + Sync + 'static,
    T::TlsConnect: Send + Sync,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let mut config = config;
    config
        .keepalives(true)
        .keepalives_idle(deadlines.commit_grace())
        .keepalives_interval(deadlines.commit_grace())
        .keepalives_retries(3);
    Arc::new(move || {
        let config = config.clone();
        let tls = tls.clone();
        Box::pin(async move {
            let (client, mut connection) = config
                .connect(tls)
                .await
                .map_err(super::error::map_connection_error)?;
            let (sender, notifications) = tokio::sync::mpsc::unbounded_channel();
            tokio::spawn(async move {
                // Ends with the connection; dropping `sender` ends the session.
                let mut messages = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
                while let Some(Ok(message)) = messages.next().await {
                    if let tokio_postgres::AsyncMessage::Notification(notification) = message {
                        let _ = sender.send(notification);
                    }
                }
            });
            Ok(WakeSession {
                client,
                notifications,
            })
        })
    })
}

pub(super) fn start(connect: WakeConnect) -> Option<QueueWakeTasks> {
    let handle = tokio::runtime::Handle::try_current().ok()?;
    let board = QueueWakeBoard::feed(true);
    let listener = handle.spawn(listen(Arc::clone(&board), Arc::clone(&connect)));
    let notifier = handle.spawn(notify(Arc::clone(&board), connect));
    Some(QueueWakeTasks::new(
        board,
        vec![listener.abort_handle(), notifier.abort_handle()],
    ))
}

async fn listen(board: Arc<QueueWakeBoard>, connect: WakeConnect) {
    let mut backoff = QUEUE_WAKE_MIN_BACKOFF;
    while !board.is_closed() {
        // (A match guard cannot await: connect, then run LISTEN, as two steps.)
        let listening = match connect().await {
            Ok(session) => session
                .client
                .batch_execute("LISTEN aux_queue_wake")
                .await
                .is_ok()
                .then_some(session),
            Err(_) => None,
        };
        let session = match listening {
            Some(session) => session,
            None => {
                tracing::warn!("queue.wake.listen.failed");
                tokio::select! { () = tokio::time::sleep(backoff) => {}, () = board.closed() => return }
                backoff = (backoff * 2).min(QUEUE_WAKE_MAX_BACKOFF);
                continue;
            }
        };
        let listened_at = tokio::time::Instant::now();
        // From here every NOTIFY that commits reaches this session. Covering
        // advances every slot: changes during the gap are probed once.
        board.cover_all();
        let WakeSession {
            client: _client,
            mut notifications,
        } = session;
        let mut fallback = tokio::time::interval_at(
            tokio::time::Instant::now() + QUEUE_WAKE_FALLBACK,
            QUEUE_WAKE_FALLBACK,
        );
        loop {
            tokio::select! {
                notification = notifications.recv() => match notification {
                    Some(notification) => for slot in parse_slots(notification.payload()) { board.advance(slot) },
                    None => break,
                },
                _ = fallback.tick() => board.advance_all(),
                () = board.closed() => return,
            }
        }
        board.uncover_all();
        if listened_at.elapsed() >= QUEUE_WAKE_FALLBACK {
            backoff = QUEUE_WAKE_MIN_BACKOFF;
        } else {
            tracing::warn!("queue.wake.listen.ended");
            tokio::select! { () = tokio::time::sleep(backoff) => {}, () = board.closed() => return }
            backoff = (backoff * 2).min(QUEUE_WAKE_MAX_BACKOFF);
        }
    }
}

/// Slot numbers separated by commas; anything else is ignored (internal API).
pub(super) fn parse_slots(payload: &str) -> impl Iterator<Item = QueueWakeSlot> + '_ {
    payload
        .split(',')
        .filter_map(|part| part.parse::<u16>().ok())
        .filter_map(QueueWakeSlot::from_index)
}

pub(super) fn payload(slots: &[QueueWakeSlot]) -> String {
    slots
        .iter()
        .map(|slot| slot.index().to_string())
        .collect::<Vec<_>>()
        .join(",")
}

async fn notify(board: Arc<QueueWakeBoard>, connect: WakeConnect) {
    let mut session: Option<WakeSession> = None;
    let mut backoff = QUEUE_WAKE_MIN_BACKOFF;
    loop {
        tokio::select! { () = board.commits_pending() => {}, () = board.closed() => return }
        tokio::time::sleep(QUEUE_WAKE_NOTIFY_SPACING).await; // coalesce
        let slots = board.take_committed();
        if slots.is_empty() {
            continue;
        }
        if session.is_none() {
            // A hint: losing it on a crash is recovered by the listeners'
            // fallback, so the commit does not wait for the WAL flush and the
            // database-wide notify lock is held only briefly.
            let opened = match connect().await {
                Ok(new) => new
                    .client
                    .batch_execute("SET synchronous_commit = off")
                    .await
                    .is_ok()
                    .then_some(new),
                Err(_) => None,
            };
            match opened {
                Some(new) => {
                    session = Some(new);
                    backoff = QUEUE_WAKE_MIN_BACKOFF;
                }
                None => {
                    tracing::warn!("queue.wake.notify.failed"); // these slots are dropped
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(QUEUE_WAKE_MAX_BACKOFF);
                    continue;
                }
            }
        }
        let Some(open) = session.as_ref() else {
            continue;
        };
        if open
            .client
            .execute(
                "SELECT pg_notify($1, $2)",
                &[&QUEUE_WAKE_CHANNEL, &payload(&slots)],
            )
            .await
            .is_err()
        {
            tracing::warn!("queue.wake.notify.failed");
            session = None;
        }
    }
}
