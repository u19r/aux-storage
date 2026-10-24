use replication_types::protocol::CanonicalDigest;

use super::{
    chunk_message::{ByteChunk, ChunkDeclaration, OutgoingChunk},
    codec::{self, EncodedFrame},
    connection::TransportConnection,
    error::{TransportError, TransportResult},
    framing,
    resources::TransportResources,
};
use crate::constants::{TRANSPORT_CHUNK_DECLARATION_KIND, TRANSPORT_CHUNK_KIND};

/// Byte assembly has no durable receipt and emits no partial acknowledgement.
/// A disconnect drops these bytes; the immutable native semantic source
/// remains.
pub(super) async fn read_chunks(
    socket: &mut TransportConnection,
    declaration: &ChunkDeclaration,
    resources: &TransportResources,
) -> TransportResult<EncodedFrame> {
    declaration.require_bound(resources.semantic_limit)?;
    socket.require_live()?;
    let count = plan_chunk_capacity(declaration.content, resources.limit)?;
    let mut packed = Vec::with_capacity(declaration.packed_bytes as usize);
    while packed.len() < declaration.packed_bytes as usize {
        let chunk: ByteChunk = framing::read(socket, TRANSPORT_CHUNK_KIND, resources.limit).await?;
        let expected = count.min(declaration.packed_bytes as usize - packed.len());
        if chunk.content != declaration.content
            || chunk.offset as usize != packed.len()
            || chunk.bytes.as_ref().len() != expected
        {
            return Err(TransportError::Frame);
        }
        packed.extend_from_slice(chunk.bytes.as_ref());
    }
    declaration.content.verify(&packed)?;
    Ok(EncodedFrame {
        raw_bytes: declaration.raw_bytes,
        packed,
    })
}

pub(super) async fn write_chunks(
    socket: &mut TransportConnection,
    frame: EncodedFrame,
    declaration: ChunkDeclaration,
    resources: &TransportResources,
) -> TransportResult<()> {
    declaration.require_bound(resources.semantic_limit)?;
    socket.require_live()?;
    let content = declaration.content;
    let count = plan_chunk_capacity(content, resources.limit)?;
    framing::write(
        socket,
        TRANSPORT_CHUNK_DECLARATION_KIND,
        declaration,
        resources.limit,
    )
    .await?;
    let mut packed = frame.packed;
    for start in (0..packed.len()).step_by(count) {
        socket.require_live()?;
        let offset = u32::try_from(start).map_err(|_| TransportError::Frame)?;
        let end = packed.len().min(start.saturating_add(count));
        let limit = resources.limit;
        let (returned, encoded) = tokio::task::spawn_blocking(move || {
            let encoded = codec::encode(
                &OutgoingChunk {
                    content,
                    offset,
                    bytes: &packed[start..end],
                },
                limit,
            )?;
            Ok::<_, TransportError>((packed, encoded))
        })
        .await
        .map_err(|_| TransportError::Worker)??;
        packed = returned;
        framing::write_encoded(socket, TRANSPORT_CHUNK_KIND, encoded).await?;
    }
    Ok(())
}

fn plan_chunk_capacity(content: CanonicalDigest, limit: usize) -> TransportResult<usize> {
    let overhead = codec::serialized_length(
        &OutgoingChunk {
            content,
            offset: u32::MAX,
            bytes: &[],
        },
        limit,
    )?;
    // JSON byte arrays use at most three decimal digits plus one separator.
    let count = limit
        .checked_sub(overhead)
        .ok_or(TransportError::Configuration)?
        / 4;
    if count == 0 {
        return Err(TransportError::Configuration);
    }
    Ok(count)
}
