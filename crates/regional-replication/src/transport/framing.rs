use serde::{Serialize, de::DeserializeOwned};

use super::{
    codec::{self, EncodedFrame},
    connection::TransportConnection,
    error::{TransportError, TransportResult},
};
use crate::constants::{
    TRANSPORT_HEADER_BYTES, TRANSPORT_MAGIC, TRANSPORT_VERSION, TRANSPORT_ZLIB,
};

pub async fn read<T: DeserializeOwned + Send + 'static>(
    socket: &mut TransportConnection,
    kind: u8,
    limit: usize,
) -> TransportResult<T> {
    let (_, packed, raw_bytes) = read_bytes(socket, Some(kind), limit).await?;
    socket.require_live()?;
    tokio::task::spawn_blocking(move || codec::decode(packed, raw_bytes))
        .await
        .map_err(|_| TransportError::Worker)?
}

pub async fn read_bytes(
    socket: &mut TransportConnection,
    kind: Option<u8>,
    limit: usize,
) -> TransportResult<(u8, Vec<u8>, u32)> {
    let mut header = [0; TRANSPORT_HEADER_BYTES];
    socket.read_exact(&mut header).await?;
    let packed = u32::from_be_bytes(
        header[8..12]
            .try_into()
            .map_err(|_| TransportError::Frame)?,
    );
    let raw_bytes = u32::from_be_bytes(
        header[12..16]
            .try_into()
            .map_err(|_| TransportError::Frame)?,
    );
    if header[..4] != TRANSPORT_MAGIC
        || u16::from_be_bytes([header[4], header[5]]) != TRANSPORT_VERSION
        || kind.is_some_and(|kind| header[6] != kind)
        || header[6] == 0
        || header[7] != TRANSPORT_ZLIB
        || packed == 0
        || raw_bytes == 0
        || packed as usize > limit
        || raw_bytes as usize > limit
    {
        return Err(TransportError::Frame);
    }
    let mut bytes = vec![0; packed as usize];
    socket.read_exact(&mut bytes).await?;
    Ok((header[6], bytes, raw_bytes))
}

pub async fn write<T: Serialize + Send + 'static>(
    socket: &mut TransportConnection,
    kind: u8,
    message: T,
    limit: usize,
) -> TransportResult<()> {
    socket.require_live()?;
    let encoded = tokio::task::spawn_blocking(move || codec::encode(&message, limit))
        .await
        .map_err(|_| TransportError::Worker)??;
    write_encoded(socket, kind, encoded).await
}

pub async fn write_encoded(
    socket: &mut TransportConnection,
    kind: u8,
    encoded: EncodedFrame,
) -> TransportResult<()> {
    let size = u32::try_from(encoded.packed.len()).map_err(|_| TransportError::Frame)?;
    let mut header = [0; TRANSPORT_HEADER_BYTES];
    header[..4].copy_from_slice(&TRANSPORT_MAGIC);
    header[4..6].copy_from_slice(&TRANSPORT_VERSION.to_be_bytes());
    header[6] = kind;
    header[7] = TRANSPORT_ZLIB;
    header[8..12].copy_from_slice(&size.to_be_bytes());
    header[12..16].copy_from_slice(&encoded.raw_bytes.to_be_bytes());
    socket.write_all(&header).await?;
    socket.write_all(&encoded.packed).await?;
    Ok(())
}
