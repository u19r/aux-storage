use serde::{Serialize, de::DeserializeOwned};

use super::{
    codec::{self, EncodedFrame},
    connection::TransportConnection,
    error::{TransportError, TransportResult},
};
use crate::constants::{
    TRANSPORT_HEADER_BYTES, TRANSPORT_MAGIC, TRANSPORT_VERSION, TRANSPORT_ZLIB,
};

pub(super) async fn read<T: DeserializeOwned + Send + 'static>(
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

pub(super) enum IncomingRequest {
    Import(super::message::IncomingPage),
    Control(super::control_message::ControlMessage),
    Trim(super::trim_message::TrimMessage),
    Bootstrap(Box<super::bootstrap_message::BootstrapMessage>),
    Seal(Box<super::seal_message::SealMessage>),
    HistoryCapture(Box<super::history_message::HistoryCaptureMessage>),
}

impl IncomingRequest {
    fn identities(&self) -> (&super::message::WireIdentity, &super::message::WireIdentity) {
        match self {
            Self::Import(message) => (&message.from, &message.to),
            Self::Control(message) => (&message.from, &message.to),
            Self::Trim(message) => (&message.from, &message.to),
            Self::Bootstrap(message) => (&message.from, &message.to),
            Self::Seal(message) => (&message.from, &message.to),
            Self::HistoryCapture(message) => (&message.from, &message.to),
        }
    }
}

pub(super) async fn read_request(
    socket: &mut TransportConnection,
    resources: &super::resources::TransportResources,
    require_peer: impl FnOnce(
        &super::message::WireIdentity,
        &super::message::WireIdentity,
    ) -> TransportResult<()>,
) -> TransportResult<IncomingRequest> {
    let (kind, packed, raw_bytes) = read_bytes(socket, None, resources.limit).await?;
    let (kind, frame, identities) = if kind == crate::constants::TRANSPORT_CHUNK_DECLARATION_KIND {
        let declaration: super::chunk_message::ChunkDeclaration =
            tokio::task::spawn_blocking(move || codec::decode(packed, raw_bytes))
                .await
                .map_err(|_| TransportError::Worker)??;
        require_peer(&declaration.from, &declaration.to)?;
        declaration.require_request_kind()?;
        let frame = super::chunk_stream::read_chunks(socket, &declaration, resources).await?;
        (
            declaration.kind,
            frame,
            Some((declaration.from, declaration.to)),
        )
    } else {
        (kind, EncodedFrame { packed, raw_bytes }, None)
    };
    socket.require_live()?;
    tokio::task::spawn_blocking(move || {
        let request = decode_request(kind, frame)?;
        if let Some((from, to)) = identities
            && request.identities() != (&from, &to)
        {
            return Err(TransportError::Peer);
        }
        Ok(request)
    })
    .await
    .map_err(|_| TransportError::Worker)?
}

fn decode_request(kind: u8, frame: EncodedFrame) -> TransportResult<IncomingRequest> {
    let EncodedFrame { packed, raw_bytes } = frame;
    match kind {
        crate::constants::TRANSPORT_DATA_KIND => {
            codec::decode(packed, raw_bytes).map(IncomingRequest::Import)
        }
        crate::constants::TRANSPORT_TRIM_KIND => {
            codec::decode(packed, raw_bytes).map(IncomingRequest::Trim)
        }
        crate::constants::TRANSPORT_CONTROL_KIND => {
            codec::decode(packed, raw_bytes).map(IncomingRequest::Control)
        }
        crate::constants::TRANSPORT_BOOTSTRAP_KIND => {
            codec::decode(packed, raw_bytes).map(IncomingRequest::Bootstrap)
        }
        crate::constants::TRANSPORT_HISTORY_CAPTURE_KIND => {
            codec::decode(packed, raw_bytes).map(IncomingRequest::HistoryCapture)
        }
        crate::constants::TRANSPORT_SEAL_KIND => {
            codec::decode(packed, raw_bytes).map(IncomingRequest::Seal)
        }
        _ => Err(TransportError::Frame),
    }
}

pub(super) async fn read_bytes(
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
        || !matches!(header[6], 1..=9)
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

pub(super) async fn write<T: Serialize + Send + 'static>(
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

pub(super) async fn write_encoded(
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
