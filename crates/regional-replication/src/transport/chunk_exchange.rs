use serde::{Serialize, de::DeserializeOwned};

use super::{
    chunk_message::ChunkDeclaration,
    chunk_stream::{read_chunks, write_chunks},
    codec::{self, EncodedFrame},
    connection::TransportConnection,
    error::{TransportError, TransportResult},
    framing,
    message::WireIdentity,
    resources::TransportResources,
};
use crate::constants::TRANSPORT_CHUNK_DECLARATION_KIND;

pub(super) async fn write_message(
    socket: &mut TransportConnection,
    input: (u8, EncodedFrame),
    resources: &TransportResources,
    identities: (WireIdentity, WireIdentity),
) -> TransportResult<()> {
    let (kind, frame) = input;
    if frame.raw_bytes as usize <= resources.limit && frame.packed.len() <= resources.limit {
        ChunkDeclaration::require_message_kind(kind)?;
        return framing::write_encoded(socket, kind, frame).await;
    }
    let declaration = ChunkDeclaration::try_new((kind, &frame), identities)?;
    write_chunks(socket, frame, declaration, resources).await
}

pub(super) async fn read_message<T: DeserializeOwned + Send + 'static>(
    socket: &mut TransportConnection,
    kind: u8,
    resources: &TransportResources,
    identities: (WireIdentity, WireIdentity),
) -> TransportResult<T> {
    let (received, packed, raw_bytes) = framing::read_bytes(socket, None, resources.limit).await?;
    if received == kind {
        return tokio::task::spawn_blocking(move || codec::decode(packed, raw_bytes))
            .await
            .map_err(|_| TransportError::Worker)?;
    }
    if received != TRANSPORT_CHUNK_DECLARATION_KIND {
        return Err(TransportError::Frame);
    }
    let declaration: ChunkDeclaration =
        tokio::task::spawn_blocking(move || codec::decode(packed, raw_bytes))
            .await
            .map_err(|_| TransportError::Worker)??;
    if declaration.kind != kind {
        return Err(TransportError::Frame);
    }
    if declaration.from != identities.0 || declaration.to != identities.1 {
        return Err(TransportError::Peer);
    }
    let frame = read_chunks(socket, &declaration, resources).await?;
    tokio::task::spawn_blocking(move || codec::decode(frame.packed, frame.raw_bytes))
        .await
        .map_err(|_| TransportError::Worker)?
}

pub(super) async fn write_serialized<T: Serialize + Send + 'static>(
    socket: &mut TransportConnection,
    input: (u8, T),
    resources: &TransportResources,
    identities: (WireIdentity, WireIdentity),
) -> TransportResult<()> {
    socket.require_live()?;
    let limit = resources.semantic_limit;
    let (kind, value) = input;
    let frame = tokio::task::spawn_blocking(move || codec::encode(&value, limit))
        .await
        .map_err(|_| TransportError::Worker)??;
    write_message(socket, (kind, frame), resources, identities).await
}
