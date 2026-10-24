use flate2::{Compression, write::ZlibEncoder};
use serde::{Serialize, de::DeserializeOwned};

use super::{
    error::{TransportError, TransportResult},
    frame_read::FrameReader,
    frame_write::{BoundedSerialization, FrameBuffer},
};

pub(super) struct EncodedFrame {
    pub raw_bytes: u32,
    pub packed: Vec<u8>,
}

pub(super) fn encode(value: &impl Serialize, limit: usize) -> TransportResult<EncodedFrame> {
    let limit = limit.min(u32::MAX as usize);
    let encoder = ZlibEncoder::new(FrameBuffer::new(limit), Compression::fast());
    let mut writer = BoundedSerialization::new(encoder, limit);
    serde_json::to_writer(&mut writer, value).map_err(classify_json_error)?;
    let (raw_bytes, encoder) = writer.into_parts();
    let raw_bytes = u32::try_from(raw_bytes).map_err(|_| TransportError::FrameTooLarge)?;
    Ok(EncodedFrame {
        raw_bytes,
        packed: encoder.finish().map_err(classify_write_error)?.into(),
    })
}

pub(super) fn decode<T: DeserializeOwned>(packed: Vec<u8>, raw_bytes: u32) -> TransportResult<T> {
    let mut reader = std::io::BufReader::new(FrameReader::new(&packed, raw_bytes));
    let value = serde_json::from_reader(&mut reader).map_err(|_| TransportError::Frame)?;
    reader.get_ref().require_complete()?;
    Ok(value)
}

pub(super) fn serialized_length(value: &impl Serialize, limit: usize) -> TransportResult<usize> {
    let mut writer = BoundedSerialization::new(std::io::sink(), limit);
    serde_json::to_writer(&mut writer, value).map_err(classify_json_error)?;
    Ok(writer.into_parts().0)
}

fn classify_json_error(error: serde_json::Error) -> TransportError {
    match error.io_error_kind() {
        Some(kind) => classify_write_error(std::io::Error::from(kind)),
        None => TransportError::Frame,
    }
}

fn classify_write_error(error: std::io::Error) -> TransportError {
    if error.kind() == std::io::ErrorKind::FileTooLarge {
        TransportError::FrameTooLarge
    } else {
        TransportError::Io(error)
    }
}
