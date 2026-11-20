//! No-TLS TCP fixture that forwards an actual settlement command and withholds
//! its replies.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// One owned connection. Arm only after healthy schema and seed commits finish.
pub struct ReplyLossForwarder {
    pub port: u16,
    pub commands: Arc<AtomicUsize>,
    peer: tokio::task::JoinHandle<()>,
    stop: tokio::sync::oneshot::Sender<()>,
    armed: Arc<AtomicBool>,
}

impl ReplyLossForwarder {
    pub async fn bind(dsn: &str, command: &'static [u8]) -> Self {
        let upstream: tokio_postgres::Config = dsn.parse().expect("Postgres fixture config");
        let hostname = match upstream.get_hosts().first() {
            Some(tokio_postgres::config::Host::Tcp(host)) => host.clone(),
            _ => panic!("reply-loss fixture requires TCP"),
        };
        let upstream_port = upstream.get_ports().first().copied().unwrap_or(5432);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let armed = Arc::new(AtomicBool::new(false));
        let commands = Arc::new(AtomicUsize::new(0));
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let is_armed = armed.clone();
        let observed = commands.clone();
        let peer = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => {
                        let (client, _) = accepted.unwrap();
                        let host = hostname.clone();
                        let armed = is_armed.clone();
                        let count = observed.clone();
                        connections.spawn(forward_connection(client, host, upstream_port, command, armed, count));
                    }
                }
            }
            while let Some(result) = connections.join_next().await {
                result.unwrap();
            }
        });
        Self {
            port,
            commands,
            peer,
            armed,
            stop,
        }
    }

    pub fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }
    pub fn disarm(&self) {
        self.armed.store(false, Ordering::Release);
    }
    pub async fn close(self) {
        let _ = self.stop.send(());
        self.peer.await.unwrap();
    }
}

async fn forward_connection(
    mut client: tokio::net::TcpStream,
    hostname: String,
    upstream_port: u16,
    command: &'static [u8],
    is_armed: Arc<AtomicBool>,
    observed: Arc<AtomicUsize>,
) {
    let mut server = tokio::net::TcpStream::connect((hostname.as_str(), upstream_port))
        .await
        .unwrap();
    let size = client.read_u32().await.unwrap();
    server.write_u32(size).await.unwrap();
    let mut startup = vec![0; size as usize - 4];
    client.read_exact(&mut startup).await.unwrap();
    server.write_all(&startup).await.unwrap();
    let (mut client_read, mut client_write) = client.split();
    let (mut server_read, mut server_write) = server.split();
    let suppress = AtomicBool::new(false);
    let forward_commands = async {
        while let Ok(tag) = client_read.read_u8().await {
            let Ok(size) = client_read.read_u32().await else {
                break;
            };
            let mut payload = vec![0; size as usize - 4];
            if client_read.read_exact(&mut payload).await.is_err() {
                break;
            }
            if is_armed.load(Ordering::Acquire) && tag == b'Q' && payload == command {
                suppress.store(true, Ordering::Release);
                observed.fetch_add(1, Ordering::Release);
            }
            if server_write.write_u8(tag).await.is_err() {
                break;
            }
            if server_write.write_u32(size).await.is_err() {
                break;
            }
            if server_write.write_all(&payload).await.is_err() {
                break;
            }
        }
    };
    let forward_replies = async {
        while let Ok(tag) = server_read.read_u8().await {
            let Ok(size) = server_read.read_u32().await else {
                break;
            };
            let mut payload = vec![0; size as usize - 4];
            if server_read.read_exact(&mut payload).await.is_err() {
                break;
            }
            if suppress.load(Ordering::Acquire) {
                continue;
            }
            if client_write.write_u8(tag).await.is_err() {
                break;
            }
            if client_write.write_u32(size).await.is_err() {
                break;
            }
            if client_write.write_all(&payload).await.is_err() {
                break;
            }
        }
    };
    tokio::select! { () = forward_commands => {}, () = forward_replies => {} }
}
