pub(crate) const TRANSPORT_MAGIC: [u8; 4] = *b"AUXR";
pub(crate) const TRANSPORT_VERSION: u16 = 35;
pub(crate) const TRANSPORT_HEADER_BYTES: usize = 16;
pub(crate) const TRANSPORT_ZLIB: u8 = 1;
pub(crate) const FRAME_REQUEST: u8 = 1;
pub(crate) const FRAME_RESPONSE: u8 = 2;
