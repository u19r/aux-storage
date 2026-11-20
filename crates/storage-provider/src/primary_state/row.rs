use storage_types::KeyAttributes;

use crate::ItemWriteObservation;

/// A native primary observation with its decoded logical address. This is
/// source data, not restore authority; the destination validates it against
/// its bound catalog before shared import or baseline acceptance.
pub struct PrimaryStateRow {
    pub key: KeyAttributes,
    pub state: ItemWriteObservation,
}
