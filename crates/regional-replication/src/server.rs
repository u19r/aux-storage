//! Accept loop: one request per connection, bounded concurrency, envelope
//! check, handler.

use std::sync::Arc;

use async_trait::async_trait;
use storage_types::RegionId;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::JoinHandle,
    time::Instant,
};

use crate::{
    config::RegionalConfig,
    constants::{FRAME_REQUEST, FRAME_RESPONSE},
    message::{RegionalEnvelope, RegionalRejection, RegionalRequest, RegionalResponse},
    transport::{connection::TransportConnection, error::TransportResult, framing},
};

#[async_trait]
pub trait RegionalRequestHandler: Send + Sync + 'static {
    /// Never panics on bad input; maps every native failure to
    /// `Rejected(Unavailable)`.
    async fn handle(&self, from: RegionId, request: RegionalRequest) -> RegionalResponse;
}

/// The caller aborts the handle at shutdown.
pub fn spawn_server(
    listener: TcpListener,
    config: Arc<RegionalConfig>,
    handler: Arc<dyn RegionalRequestHandler>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let permits = Arc::new(Semaphore::new(config.max_concurrent_requests));
        loop {
            let socket = match listener.accept().await {
                Ok((socket, _)) => socket,
                Err(error) => {
                    tracing::warn!(%error, "regional listener accept failed");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
            };
            let Ok(permit) = permits.clone().acquire_owned().await else {
                return;
            };
            let config = config.clone();
            let handler = handler.clone();
            tokio::spawn(async move {
                if let Err(error) = serve_one(socket, &config, handler.as_ref()).await {
                    tracing::debug!(%error, "regional request connection ended");
                }
                drop(permit);
            });
        }
    })
}

async fn serve_one(
    socket: TcpStream,
    config: &RegionalConfig,
    handler: &dyn RegionalRequestHandler,
) -> TransportResult<()> {
    socket.set_nodelay(true)?;
    let mut connection = TransportConnection::new(socket, Instant::now() + config.request_timeout);
    let envelope: RegionalEnvelope =
        framing::read(&mut connection, FRAME_REQUEST, config.frame_limit_bytes).await?;
    let response = if envelope.to != config.local.id || envelope.from != config.peer.id {
        RegionalResponse::Rejected(RegionalRejection::WrongRegion)
    } else {
        handler.handle(envelope.from, envelope.request).await
    };
    connection.set_deadline(Instant::now() + config.request_timeout);
    framing::write(
        &mut connection,
        FRAME_RESPONSE,
        response,
        config.frame_limit_bytes,
    )
    .await
}
