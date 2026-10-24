use storage_types::{StorageError, StorageResult};

/// Place one physical point result in request order, returning its slot count.
/// Missing results leave empty slots; repeated requests share cloned values.
pub fn place_value<Key, Value: Clone>(
    keys: &[Key],
    slots: &mut [Option<Value>],
    matches: impl Fn(&Key, &Value) -> bool,
    value: Value,
) -> StorageResult<usize> {
    PointPlacement {
        keys,
        slots,
        matches,
    }
    .place(value, |value| Ok(value.clone()))
}

/// Preserve request duplication while returning owned-copy failures unchanged.
pub fn try_place_value<Key, Value>(
    keys: &[Key],
    slots: &mut [Option<Value>],
    matches: impl Fn(&Key, &Value) -> bool,
    value: Value,
) -> StorageResult<usize>
where
    Value: for<'a> TryFrom<&'a Value, Error = StorageError>,
{
    PointPlacement {
        keys,
        slots,
        matches,
    }
    .place(value, |value| Value::try_from(value))
}

/// Ordered point association with a caller-owned, fallible duplicate-value
/// copy.
pub struct PointPlacement<'a, Key, Value, Matches> {
    keys: &'a [Key],
    slots: &'a mut [Option<Value>],
    matches: Matches,
}

impl<'a, Key, Value, Matches> PointPlacement<'a, Key, Value, Matches> {
    pub fn new(keys: &'a [Key], slots: &'a mut [Option<Value>], matches: Matches) -> Self {
        Self {
            keys,
            slots,
            matches,
        }
    }
}

impl<Key, Value, Matches: Fn(&Key, &Value) -> bool> PointPlacement<'_, Key, Value, Matches> {
    pub fn place(
        self,
        value: Value,
        copy: impl Fn(&Value) -> StorageResult<Value>,
    ) -> StorageResult<usize> {
        if self.slots.len() != self.keys.len() {
            return Err(StorageError::internal("point result slot count is invalid"));
        }
        let first = self
            .keys
            .iter()
            .position(|requested| (self.matches)(requested, &value))
            .ok_or_else(|| StorageError::internal("point result returned an unrequested key"))?;
        if self.slots[first].is_some() {
            return Err(duplicate_result());
        }
        let mut matched = 1;
        for (index, requested) in self.keys.iter().enumerate().skip(first + 1) {
            if !(self.matches)(requested, &value) {
                continue;
            }
            if self.slots[index].is_some() {
                return Err(duplicate_result());
            }
            self.slots[index] = Some(copy(&value)?);
            matched += 1;
        }
        self.slots[first] = Some(value);
        Ok(matched)
    }
}

fn duplicate_result() -> StorageError {
    StorageError::internal("point result returned a duplicate key")
}
