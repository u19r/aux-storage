//! One request, one response, one TCP connection. A transport error is an
//! unknown outcome: the caller repeats the same request later; every handler is
//! idempotent (REG-6, 10, 11).

use std::sync::Arc;

use tokio::{
    net::TcpStream,
    time::{Instant, timeout_at},
};

use crate::{
    config::RegionalConfig,
    constants::{FRAME_REQUEST, FRAME_RESPONSE},
    message::{RegionalEnvelope, RegionalRequest, RegionalResponse},
    transport::{TransportError, connection::TransportConnection, error::TransportResult, framing},
};

#[derive(Clone)]
pub struct RegionalPeerClient {
    config: Arc<RegionalConfig>,
}

impl RegionalPeerClient {
    #[must_use]
    pub fn new(config: Arc<RegionalConfig>) -> Self {
        Self { config }
    }

    #[must_use]
    pub fn config(&self) -> &RegionalConfig {
        &self.config
    }

    pub async fn call(&self, request: RegionalRequest) -> TransportResult<RegionalResponse> {
        let deadline = Instant::now() + self.config.request_timeout;
        let socket = timeout_at(
            deadline,
            TcpStream::connect(self.config.peer.address.as_str()),
        )
        .await
        .map_err(|_| TransportError::Timeout)??;
        socket.set_nodelay(true)?;
        let mut connection = TransportConnection::new(socket, deadline);
        let envelope = RegionalEnvelope {
            from: self.config.local.id,
            to: self.config.peer.id,
            request,
        };
        framing::write(
            &mut connection,
            FRAME_REQUEST,
            envelope,
            self.config.frame_limit_bytes,
        )
        .await?;
        framing::read(
            &mut connection,
            FRAME_RESPONSE,
            self.config.frame_limit_bytes,
        )
        .await
    }
}
