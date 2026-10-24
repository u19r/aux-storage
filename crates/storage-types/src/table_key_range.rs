use serde::{Deserialize, Serialize};

use crate::{StorageError, StorageResult, TableKey};

#[derive(Clone, Serialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct TableKeyRange {
    pub start: Option<TableKey>,
    pub end: Option<TableKey>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
// This unchecked wire shape is deliberately distinct from `TableKeyRange`:
// only the validated type can cross the deserialization boundary.
struct UncheckedTableKeyRange {
    start: Option<TableKey>,
    end: Option<TableKey>,
}

impl<'de> Deserialize<'de> for TableKeyRange {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = UncheckedTableKeyRange::deserialize(deserializer)?;
        Self::try_from((wire.start, wire.end)).map_err(serde::de::Error::custom)
    }
}

impl PartialEq for TableKeyRange {
    fn eq(&self, other: &Self) -> bool {
        same_key(self.start.as_ref(), other.start.as_ref())
            && same_key(self.end.as_ref(), other.end.as_ref())
    }
}

impl Eq for TableKeyRange {}

impl TryFrom<(Option<TableKey>, Option<TableKey>)> for TableKeyRange {
    type Error = StorageError;

    fn try_from((start, end): (Option<TableKey>, Option<TableKey>)) -> StorageResult<Self> {
        if let (Some(start), Some(end)) = (&start, &end)
            && (start.table_name != end.table_name
                || start.canonical_cmp(end) != std::cmp::Ordering::Less)
        {
            return Err(StorageError::validation("invalid table key range"));
        }
        Ok(Self { start, end })
    }
}

fn same_key(left: Option<&TableKey>, right: Option<&TableKey>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.canonical_cmp(right).is_eq(),
        _ => false,
    }
}
