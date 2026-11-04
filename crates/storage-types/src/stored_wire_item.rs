use crate::WireItem;

/// A stored logical item and the indexer declaration persisted with it.
///
/// Native value codecs decode into this shape before shared read, write,
/// replication, GSI, or maintenance logic consumes the item.
pub struct StoredWireItem {
    pub item: WireItem,
    pub indexers: Vec<String>,
}
