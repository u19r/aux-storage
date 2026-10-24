use storage_types::{StreamItemId, StreamName};

use crate::{
    CursorName, StreamItem, StreamResult,
    constants::{BOOTSTRAP_CURSOR_PREFIX, MAX_CURSOR_NAME_BYTES},
    validate_limit,
};

/// A canonical, bounded identity for a bootstrap retention cursor.
///
/// The encoded name is the durable session identity. It is deliberately not
/// constructible from an arbitrary `CursorName`, so ordinary cursors cannot be
/// presented as bootstrap retention authority.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BootstrapCursorName {
    cursor_name: CursorName,
    table_name: String,
    peer_region: String,
    generation: String,
}

impl BootstrapCursorName {
    pub fn try_new(
        table_name: impl AsRef<str>,
        peer_region: impl AsRef<str>,
        generation: impl AsRef<str>,
    ) -> StreamResult<Self> {
        let table_name = table_name.as_ref();
        let peer_region = peer_region.as_ref();
        let generation = generation.as_ref();
        validate_component("table name", table_name)?;
        validate_component("peer region", peer_region)?;
        validate_component("bootstrap generation", generation)?;
        let encoded = encode_identity(table_name, peer_region, generation)?;
        Ok(Self {
            cursor_name: CursorName::from(encoded),
            table_name: table_name.to_string(),
            peer_region: peer_region.to_string(),
            generation: generation.to_string(),
        })
    }

    pub fn parse(cursor_name: &CursorName) -> StreamResult<Self> {
        let encoded = cursor_name.as_str();
        let body = encoded
            .strip_prefix(BOOTSTRAP_CURSOR_PREFIX)
            .ok_or_else(|| {
                crate::StreamError::validation("cursor name is not a bootstrap identity")
            })?;
        let mut components = body.split(':');
        let (table_name, table_name_canonical) = decode_component(components.next(), "table name")?;
        let (peer_region, peer_region_canonical) =
            decode_component(components.next(), "peer region")?;
        let (generation, generation_canonical) =
            decode_component(components.next(), "bootstrap generation")?;
        if components.next().is_some() {
            return Err(crate::StreamError::validation(
                "bootstrap cursor identity has too many components",
            ));
        }
        validate_component("table name", &table_name)?;
        validate_component("peer region", &peer_region)?;
        validate_component("bootstrap generation", &generation)?;
        if encoded.len() > MAX_CURSOR_NAME_BYTES {
            return Err(crate::StreamError::validation(
                "bootstrap cursor identity exceeds the cursor name limit",
            ));
        }
        if !(table_name_canonical && peer_region_canonical && generation_canonical) {
            return Err(crate::StreamError::validation(
                "bootstrap cursor identity is not canonical",
            ));
        }
        Ok(Self {
            cursor_name: CursorName::from(encoded.to_string()),
            table_name,
            peer_region,
            generation,
        })
    }

    #[must_use]
    pub fn cursor_name(&self) -> &CursorName {
        &self.cursor_name
    }

    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    #[must_use]
    pub fn peer_region(&self) -> &str {
        &self.peer_region
    }

    #[must_use]
    pub fn generation(&self) -> &str {
        &self.generation
    }
}

fn validate_component(label: &str, value: &str) -> StreamResult<()> {
    if value.is_empty() {
        return Err(crate::StreamError::validation(format!(
            "bootstrap cursor {label} cannot be empty"
        )));
    }
    if value.len() > MAX_CURSOR_NAME_BYTES {
        return Err(crate::StreamError::validation(format!(
            "bootstrap cursor {label} exceeds the cursor name limit"
        )));
    }
    Ok(())
}

fn encode_identity(table_name: &str, peer_region: &str, generation: &str) -> StreamResult<String> {
    let encoded_len = BOOTSTRAP_CURSOR_PREFIX.len()
        + table_name.len() * 2
        + peer_region.len() * 2
        + generation.len() * 2
        + 2;
    if encoded_len > MAX_CURSOR_NAME_BYTES {
        return Err(crate::StreamError::validation(
            "bootstrap cursor identity exceeds the cursor name limit",
        ));
    }
    let mut encoded = String::with_capacity(encoded_len);
    encoded.push_str(BOOTSTRAP_CURSOR_PREFIX);
    append_encoded_component(&mut encoded, table_name);
    encoded.push(':');
    append_encoded_component(&mut encoded, peer_region);
    encoded.push(':');
    append_encoded_component(&mut encoded, generation);
    Ok(encoded)
}

fn append_encoded_component(encoded: &mut String, value: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in value.bytes() {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
}

fn decode_component(value: Option<&str>, label: &str) -> StreamResult<(String, bool)> {
    let value = value.ok_or_else(|| {
        crate::StreamError::validation(format!("bootstrap cursor is missing {label}"))
    })?;
    if value.is_empty() || value.len() % 2 != 0 {
        return Err(crate::StreamError::validation(format!(
            "bootstrap cursor {label} is not canonical hex"
        )));
    }
    let mut decoded = Vec::with_capacity(value.len() / 2);
    let mut canonical = true;
    let bytes = value.as_bytes();
    for pair in bytes.as_chunks::<2>().0 {
        let high = hex_value(pair[0]).ok_or_else(|| {
            crate::StreamError::validation(format!("bootstrap cursor {label} is not hex"))
        })?;
        let low = hex_value(pair[1]).ok_or_else(|| {
            crate::StreamError::validation(format!("bootstrap cursor {label} is not hex"))
        })?;
        canonical &= pair[0].is_ascii_lowercase() || !pair[0].is_ascii_alphabetic();
        canonical &= pair[1].is_ascii_lowercase() || !pair[1].is_ascii_alphabetic();
        decoded.push((high << 4) | low);
    }
    let decoded = String::from_utf8(decoded).map_err(|_| {
        crate::StreamError::validation(format!("bootstrap cursor {label} is not UTF-8"))
    })?;
    Ok((decoded, canonical))
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

/// A read request authenticated by a provider-owned named cursor.
///
/// `cursor_name` is the durable identity of the bootstrap session. Callers
/// must persist the same name and boundary for the lifetime of the session;
/// providers validate the cursor row, position, and protected floor together
/// with the returned page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtectedCursorReadRequest {
    stream_name: StreamName,
    cursor_name: BootstrapCursorName,
    protected_boundary: StreamItemId,
    expected_position: StreamItemId,
    limit: u32,
}

/// An authenticated monotonic advance for a bootstrap cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtectedCursorAdvanceRequest {
    stream_name: StreamName,
    cursor_name: BootstrapCursorName,
    protected_boundary: StreamItemId,
    expected_position: StreamItemId,
    target_position: StreamItemId,
}

impl ProtectedCursorAdvanceRequest {
    pub fn try_new(
        stream_name: StreamName,
        cursor_name: BootstrapCursorName,
        protected_boundary: StreamItemId,
        expected_position: StreamItemId,
        target_position: StreamItemId,
    ) -> StreamResult<Self> {
        Ok(Self {
            stream_name,
            cursor_name,
            protected_boundary,
            expected_position,
            target_position,
        })
    }

    #[must_use]
    pub fn stream_name(&self) -> &StreamName {
        &self.stream_name
    }

    #[must_use]
    pub fn cursor_name(&self) -> &BootstrapCursorName {
        &self.cursor_name
    }

    #[must_use]
    pub const fn protected_boundary(&self) -> StreamItemId {
        self.protected_boundary
    }

    #[must_use]
    pub const fn expected_position(&self) -> StreamItemId {
        self.expected_position
    }

    #[must_use]
    pub const fn target_position(&self) -> StreamItemId {
        self.target_position
    }
}

impl ProtectedCursorReadRequest {
    pub fn try_new(
        stream_name: StreamName,
        cursor_name: BootstrapCursorName,
        protected_boundary: StreamItemId,
        expected_position: StreamItemId,
        limit: u32,
    ) -> StreamResult<Self> {
        validate_limit(limit)?;
        Ok(Self {
            stream_name,
            cursor_name,
            protected_boundary,
            expected_position,
            limit,
        })
    }

    #[must_use]
    pub fn stream_name(&self) -> &StreamName {
        &self.stream_name
    }

    #[must_use]
    pub fn cursor_name(&self) -> &BootstrapCursorName {
        &self.cursor_name
    }

    #[must_use]
    pub const fn protected_boundary(&self) -> StreamItemId {
        self.protected_boundary
    }

    #[must_use]
    pub const fn expected_position(&self) -> StreamItemId {
        self.expected_position
    }

    #[must_use]
    pub const fn limit(&self) -> u32 {
        self.limit
    }
}

/// Whether a protected page proves that the authenticated cursor has reached
/// the current end of the retained stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtectedCursorPageEnd {
    More,
    Terminal,
}

impl ProtectedCursorPageEnd {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Terminal)
    }
}

/// A page returned only after the provider has authenticated cursor identity,
/// boundary, and resume position in one consistent/native read.
#[derive(Debug, Clone)]
pub struct ProtectedCursorPage {
    items: Vec<StreamItem>,
    cursor_position: StreamItemId,
    end: ProtectedCursorPageEnd,
}

impl ProtectedCursorPage {
    /// Construct a page after a provider has completed its native continuity
    /// check. Backends are the only callers of this constructor.
    #[doc(hidden)]
    #[must_use]
    pub fn from_provider(
        items: Vec<StreamItem>,
        cursor_position: StreamItemId,
        end: ProtectedCursorPageEnd,
    ) -> Self {
        Self {
            items,
            cursor_position,
            end,
        }
    }

    #[must_use]
    pub fn items(&self) -> &[StreamItem] {
        &self.items
    }

    #[must_use]
    pub const fn cursor_position(&self) -> StreamItemId {
        self.cursor_position
    }

    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.end.is_terminal()
    }

    #[must_use]
    pub fn into_items(self) -> Vec<StreamItem> {
        self.items
    }
}
