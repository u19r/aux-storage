use crate::WireItem;

/// A stored logical item and the indexer declaration persisted with it.
///
/// Native value codecs decode into this shape before shared read, write,
/// replication, GSI, or maintenance logic consumes the item.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct StoredWireItem {
    #[serde(with = "crate::wire_item::dynamo_json")]
    pub item: WireItem,
    pub indexers: Vec<String>,
}
