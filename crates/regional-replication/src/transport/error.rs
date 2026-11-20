#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("regional socket I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid regional transport configuration")]
    Configuration,
    #[error("invalid or oversized regional frame")]
    Frame,
    #[error("regional frame exceeds its configured capacity")]
    FrameTooLarge,
    #[error("unexpected configured regional peer")]
    Peer,
    #[error("regional request deadline expired")]
    Timeout,
    #[error("regional frame worker failed")]
    Worker,
}

pub type TransportResult<T> = Result<T, TransportError>;
