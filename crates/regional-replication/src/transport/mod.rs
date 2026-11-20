//! Generic TCP framing kept as the base of the design B transport.
pub mod codec;
pub mod connection;
pub mod error;
pub mod frame_read;
pub mod frame_write;
pub mod framing;

pub use error::TransportError;
