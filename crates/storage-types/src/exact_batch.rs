use crate::{StorageError, StorageResult};

/// One output for each input, in input order.
///
/// Only a driver primitive or a physical lowerer constructs it. The
/// constructor holds the one cardinality check, so callers above the
/// boundary index, zip, and destructure without re-checking.
#[derive(Clone, PartialEq, Eq)]
pub struct Exact<T>(Vec<T>);

impl<T> Exact<T> {
    /// The single internal cardinality error for a batch result.
    pub fn try_new(expected: usize, values: Vec<T>) -> StorageResult<Self> {
        if values.len() != expected {
            return Err(StorageError::internal("batch result count differs"));
        }
        Ok(Self(values))
    }

    /// The proof for a batch of no inputs.
    #[must_use]
    pub fn empty() -> Self {
        Self(Vec::new())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn as_slice(&self) -> &[T] {
        &self.0
    }

    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.0.iter()
    }

    #[must_use]
    pub fn first(&self) -> Option<&T> {
        self.0.first()
    }

    #[must_use]
    pub fn last(&self) -> Option<&T> {
        self.0.last()
    }

    /// Pair each output with its input. The input count is the proven count.
    pub fn zip<I>(self, inputs: impl IntoIterator<Item = I>) -> impl Iterator<Item = (I, T)> {
        inputs.into_iter().zip(self.0)
    }

    /// Map each output; the count proof carries over.
    pub fn map<U>(self, f: impl FnMut(T) -> U) -> Exact<U> {
        Exact(self.0.into_iter().map(f).collect())
    }

    /// Fallible `map`; the count proof carries over on success.
    pub fn try_map<U, E>(self, f: impl FnMut(T) -> Result<U, E>) -> Result<Exact<U>, E> {
        Ok(Exact(
            self.0.into_iter().map(f).collect::<Result<Vec<_>, E>>()?,
        ))
    }

    /// Pair each output with its input and map the pair; `inputs` are the
    /// inputs that produced this batch, so the count proof carries over.
    pub fn try_zip_map<I, U, E>(
        self,
        inputs: impl IntoIterator<Item = I>,
        mut f: impl FnMut(I, T) -> Result<U, E>,
    ) -> Result<Exact<U>, E> {
        Ok(Exact(
            inputs
                .into_iter()
                .zip(self.0)
                .map(|(input, value)| f(input, value))
                .collect::<Result<Vec<_>, E>>()?,
        ))
    }

    /// Undo a sort of the inputs: `order[position]` is the input index that
    /// produced the output at `position`. The result is in input order.
    #[must_use]
    pub fn restore_order(self, order: Vec<usize>) -> Self {
        let mut indexed = order.into_iter().zip(self.0).collect::<Vec<_>>();
        indexed.sort_unstable_by_key(|(index, _)| *index);
        Self(indexed.into_iter().map(|(_, value)| value).collect())
    }

    /// Key the outputs by the inputs that produced them. `keys` are those
    /// inputs, in order.
    pub fn keyed_by<K>(self, keys: Vec<K>) -> Aligned<K, T> {
        Aligned { keys, values: self }
    }

    /// The only output of a one-input batch; `None` unless exactly one.
    pub fn into_single(self) -> Option<T> {
        let mut values = self.0;
        match values.pop() {
            Some(value) if values.is_empty() => Some(value),
            _ => None,
        }
    }

    /// Split off the last output when the caller appended one more input
    /// for it. The proven count includes that input, so the tail exists.
    #[must_use]
    pub fn split_last_if(self, appended: bool) -> (Self, Option<T>) {
        let mut values = self.0;
        let tail = if appended { values.pop() } else { None };
        (Self(values), tail)
    }

    /// Fixed-width view for a pipeline with a statically known stage count.
    pub fn into_array<const N: usize>(self) -> StorageResult<[T; N]> {
        self.0
            .try_into()
            .map_err(|_| StorageError::internal("batch result count differs"))
    }

    #[must_use]
    pub fn into_vec(self) -> Vec<T> {
        self.0
    }
}

impl<T: PartialEq, const N: usize> PartialEq<[T; N]> for Exact<T> {
    fn eq(&self, other: &[T; N]) -> bool {
        self.0 == *other
    }
}

impl<T> IntoIterator for Exact<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a Exact<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<T> std::ops::Index<usize> for Exact<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        &self.0[index]
    }
}

/// Port results keyed by their inputs, for lookups by key.
pub struct Aligned<K, V> {
    keys: Vec<K>,
    values: Exact<V>,
}

impl<K, V> Aligned<K, V> {
    /// Fails once when the key and value counts differ.
    pub fn try_new(keys: Vec<K>, values: Vec<V>) -> StorageResult<Self> {
        let values = Exact::try_new(keys.len(), values)?;
        Ok(Self { keys, values })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.keys.iter().zip(self.values.iter())
    }

    pub fn into_pairs(self) -> impl Iterator<Item = (K, V)> {
        self.keys.into_iter().zip(self.values)
    }

    /// Replaces `position()` followed by an internal error.
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: std::borrow::Borrow<Q>,
        Q: PartialEq + ?Sized,
    {
        self.keys
            .iter()
            .position(|candidate| candidate.borrow() == key)
            .and_then(|index| self.values.as_slice().get(index))
    }
}
