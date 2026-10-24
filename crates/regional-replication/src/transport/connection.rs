//! A socket's I/O deadline survives every protocol stage and native settlement.
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, timeout_at},
};

use super::error::{TransportError, TransportResult};

pub(super) struct TransportConnection {
    socket: TcpStream,
    deadline: Instant,
}

impl TransportConnection {
    pub(super) fn new(socket: TcpStream, deadline: Instant) -> Self {
        Self { socket, deadline }
    }

    pub(super) fn require_live(&self) -> TransportResult<()> {
        if Instant::now() >= self.deadline {
            return Err(TransportError::Timeout);
        }
        Ok(())
    }

    pub(super) async fn read_exact(&mut self, bytes: &mut [u8]) -> TransportResult<()> {
        self.require_live()?;
        timeout_at(self.deadline, self.socket.read_exact(bytes))
            .await
            .map_err(|_| TransportError::Timeout)??;
        Ok(())
    }

    pub(super) async fn write_all(&mut self, bytes: &[u8]) -> TransportResult<()> {
        self.require_live()?;
        timeout_at(self.deadline, self.socket.write_all(bytes))
            .await
            .map_err(|_| TransportError::Timeout)??;
        Ok(())
    }
}
