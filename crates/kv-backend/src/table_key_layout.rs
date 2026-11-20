//! Table-first physical keys: every record that one table incarnation owns.
//!
//! `TABLE_ROOT | table_id u32 BE | sub tag | rest`. One key range holds the
//! complete data of one table incarnation, so a range restore or a range
//! clear serves exactly one table. The stream part (`TABLE_RESERVED_SUB_TAG`
//! and above) holds stream history, which has its own reclaim phases.

use storage_types::{StorageError, StorageResult};

use crate::constants::{KV_CHUNK_KEY_FAMILY, MESSAGE_PREFIX, QUEUE_MESSAGE_CHUNK_PREFIX};

/// Root of all table-owned keys; apart from the `\0` and `\x7f` system roots
/// and from every global single-byte family.
pub(crate) const TABLE_ROOT: u8 = b'T';
/// Sub tags from here up are reserved for table-owned families that outlive
/// DeleteTable and have their own reclaim; the data range ends here.
pub(crate) const TABLE_RESERVED_SUB_TAG: u8 = 0x80;
/// Root, table id and sub tag.
pub(crate) const TABLE_KEY_PREFIX_BYTES: usize = 2 + size_of::<u32>();
/// Root and table id that a table chunk key omits from its owner.
const TABLE_OWNER_BYTES: usize = 1 + size_of::<u32>();

/// Families of one table's data range; each tag is below
/// `TABLE_RESERVED_SUB_TAG`.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TableSubFamily {
    GsiNameLock = b'B',
    RegionalMarker = b'D',
    RegionalProgress = b'P',
    GsiBuild = b'b',
    TtlConfig = b'c',
    TtlIndex = b'e',
    GsiEntry = b'g',
    Chunk = b'k',
    Catalog = b'm',
    Item = b'p',
}

impl TableSubFamily {
    pub(crate) fn from_tag(tag: u8) -> Option<Self> {
        Some(match tag {
            b'B' => Self::GsiNameLock,
            b'D' => Self::RegionalMarker,
            b'P' => Self::RegionalProgress,
            b'b' => Self::GsiBuild,
            b'c' => Self::TtlConfig,
            b'e' => Self::TtlIndex,
            b'g' => Self::GsiEntry,
            b'k' => Self::Chunk,
            b'm' => Self::Catalog,
            b'p' => Self::Item,
            _ => return None,
        })
    }
}

/// Families of one table's stream part; every tag is at or above
/// `TABLE_RESERVED_SUB_TAG`.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TableStreamFamily {
    Entry = 0x80,
    AppendRow = 0x81,
    AppendReceipt = 0x82,
    AppendSequence = 0x83,
    TrimState = 0x84,
    Chunk = 0x85,
}

impl TableStreamFamily {
    fn from_tag(tag: u8) -> Option<Self> {
        Some(match tag {
            0x80 => Self::Entry,
            0x81 => Self::AppendRow,
            0x82 => Self::AppendReceipt,
            0x83 => Self::AppendSequence,
            0x84 => Self::TrimState,
            0x85 => Self::Chunk,
            _ => return None,
        })
    }
}

fn root_prefix(table_id: u32, tag: u8) -> [u8; TABLE_KEY_PREFIX_BYTES] {
    let mut prefix = [0; TABLE_KEY_PREFIX_BYTES];
    prefix[0] = TABLE_ROOT;
    prefix[1..5].copy_from_slice(&table_id.to_be_bytes());
    prefix[5] = tag;
    prefix
}

pub(crate) fn table_prefix(table_id: u32, family: TableSubFamily) -> [u8; TABLE_KEY_PREFIX_BYTES] {
    root_prefix(table_id, family as u8)
}

pub(crate) fn table_stream_prefix(
    table_id: u32,
    family: TableStreamFamily,
) -> [u8; TABLE_KEY_PREFIX_BYTES] {
    root_prefix(table_id, family as u8)
}

/// The stream part of one table incarnation: `[root | id | 0x80, upper(root |
/// id))`.
pub(crate) fn table_stream_range(table_id: u32) -> (Vec<u8>, Vec<u8>) {
    let begin = root_prefix(table_id, TABLE_RESERVED_SUB_TAG).to_vec();
    let end = crate::range::prefix_upper_bound(&begin[..TABLE_OWNER_BYTES])
        .unwrap_or_else(|| vec![TABLE_ROOT + 1]); // never None: TABLE_ROOT != 0xff
    (begin, end)
}

/// The table and family of a stream-part key; `None` otherwise.
pub(crate) fn table_stream_family(key: &[u8]) -> Option<(u32, TableStreamFamily)> {
    if key.len() < TABLE_KEY_PREFIX_BYTES || key[0] != TABLE_ROOT {
        return None;
    }
    let table_id = u32::from_be_bytes([key[1], key[2], key[3], key[4]]);
    TableStreamFamily::from_tag(key[5]).map(|family| (table_id, family))
}

/// One table key from its family prefix and the concatenated parts.
pub(crate) fn table_key(
    table_id: u32,
    family: TableSubFamily,
    parts: &[&[u8]],
) -> StorageResult<Vec<u8>> {
    let bytes = parts
        .iter()
        .try_fold(TABLE_KEY_PREFIX_BYTES, |bytes, part| {
            bytes.checked_add(part.len())
        })
        .ok_or(storage_types::AttemptFailure::CAPACITY)?;
    let mut key = Vec::new();
    key.try_reserve_exact(bytes)
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    key.extend_from_slice(&table_prefix(table_id, family));
    for part in parts {
        key.extend_from_slice(part);
    }
    Ok(key)
}

/// The complete data range of one table incarnation: every family that
/// DeleteTable reclaims, and no reserved family.
pub(crate) fn table_data_range(table_id: u32) -> (Vec<u8>, Vec<u8>) {
    let mut begin = Vec::with_capacity(TABLE_KEY_PREFIX_BYTES);
    begin.push(TABLE_ROOT);
    begin.extend_from_slice(&table_id.to_be_bytes());
    let mut end = begin.clone();
    begin.push(0);
    end.push(TABLE_RESERVED_SUB_TAG);
    (begin, end)
}

/// The table and family of a table data key; `None` for a global key or a
/// reserved table family.
pub(crate) fn table_data_family(key: &[u8]) -> Option<(u32, TableSubFamily)> {
    if key.len() < TABLE_KEY_PREFIX_BYTES || key[0] != TABLE_ROOT {
        return None;
    }
    let table_id = u32::from_be_bytes([key[1], key[2], key[3], key[4]]);
    TableSubFamily::from_tag(key[5]).map(|family| (table_id, family))
}

/// Bytes of a queue message owner that its chunk home holds: prefix and
/// incarnation.
const QUEUE_MESSAGE_OWNER_BYTES: usize = MESSAGE_PREFIX.len() + 16;

enum ChunkHome {
    Table(u8),
    QueueMessage,
    Global,
}

impl ChunkHome {
    fn of(owner: &[u8]) -> Self {
        if table_data_family(owner).is_some_and(|(_, f)| f != TableSubFamily::Chunk) {
            return Self::Table(TableSubFamily::Chunk as u8);
        }
        if table_stream_family(owner).is_some_and(|(_, f)| f != TableStreamFamily::Chunk) {
            return Self::Table(TableStreamFamily::Chunk as u8);
        }
        if owner.len() >= QUEUE_MESSAGE_OWNER_BYTES && owner.starts_with(MESSAGE_PREFIX) {
            return Self::QueueMessage;
        }
        Self::Global
    }

    /// Owner bytes that the home already holds and the chunk key omits.
    fn omitted(&self) -> usize {
        match self {
            Self::Table(_) => TABLE_OWNER_BYTES,
            Self::QueueMessage => QUEUE_MESSAGE_OWNER_BYTES,
            Self::Global => 0,
        }
    }
}

/// The prefix of all chunks of one owner key; `owner` holds at least the
/// owner's first bytes and `owner_bytes` is the complete owner length.
/// Homes (the u16 length keeps each prefix-free):
/// - table data owner: `root | id | 'k' | u16 (len - 5) | owner[5..]`
/// - table stream owner: `root | id | 0x85 | u16 (len - 5) | owner[5..]`
/// - queue message owner: `\x7fq/k/ | incarnation | u16 (len - 21) |
///   owner[21..]`
/// - every other owner: `\0chunk/ | u16 len | owner`
pub(crate) fn chunk_prefix(owner: &[u8], owner_bytes: usize) -> StorageResult<Vec<u8>> {
    let home = ChunkHome::of(owner);
    let omitted = home.omitted();
    let tail = owner
        .get(omitted..)
        .ok_or_else(|| StorageError::internal("KV chunk owner is shorter than its home"))?;
    let length = owner_bytes
        .checked_sub(omitted)
        .and_then(|l| u16::try_from(l).ok())
        .ok_or_else(|| StorageError::internal("KV value key exceeds chunk layout"))?;
    // home (<= 21) + u16 length + tail + u16 chunk number appended by the
    // caller
    let bytes = omitted.max(KV_CHUNK_KEY_FAMILY.len()) + 1 + 2 * size_of::<u16>() + tail.len();
    let mut prefix = Vec::new();
    prefix
        .try_reserve_exact(bytes)
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    match home {
        ChunkHome::Table(tag) => {
            prefix.extend_from_slice(&owner[..TABLE_OWNER_BYTES]);
            prefix.push(tag);
        }
        ChunkHome::QueueMessage => {
            prefix.extend_from_slice(QUEUE_MESSAGE_CHUNK_PREFIX);
            prefix.extend_from_slice(&owner[MESSAGE_PREFIX.len()..QUEUE_MESSAGE_OWNER_BYTES]);
        }
        ChunkHome::Global => prefix.extend_from_slice(KV_CHUNK_KEY_FAMILY),
    }
    prefix.extend_from_slice(&length.to_be_bytes());
    prefix.extend_from_slice(tail);
    Ok(prefix)
}
