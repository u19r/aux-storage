use std::{borrow::Borrow, collections::HashMap, fmt, hash::Hash};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{MapAccess, Visitor},
    ser::SerializeMap,
};

/// A small JSON object stored as ordered key/value entries. DynamoDB request
/// maps usually have only a handful of placeholders or table names.
#[derive(Debug, Clone)]
pub struct VecMap<K, V>(Vec<VecMapEntry<K, V>>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VecMapEntry<K, V> {
    pub key: K,
    pub value: V,
}

impl<K: PartialEq, V: PartialEq> PartialEq for VecMap<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .all(|(key, value)| other.get(key) == Some(value))
    }
}

impl<K: Eq, V: Eq> Eq for VecMap<K, V> {}

impl<K, V> Default for VecMap<K, V> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<K: PartialEq, V> VecMap<K, V> {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        if let Some(entry) = self.0.iter_mut().find(|entry| entry.key == key) {
            return Some(std::mem::replace(&mut entry.value, value));
        }
        self.0.push(VecMapEntry { key, value });
        None
    }

    pub fn get_or_insert_with(&mut self, key: K, value: impl FnOnce() -> V) -> &mut V {
        if let Some(index) = self.0.iter().position(|entry| entry.key == key) {
            return &mut self.0[index].value;
        }
        let index = self.0.len();
        self.0.push(VecMapEntry {
            key,
            value: value(),
        });
        &mut self.0[index].value
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: PartialEq + ?Sized,
    {
        self.0
            .iter()
            .find(|entry| entry.key.borrow() == key)
            .map(|entry| &entry.value)
    }

    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: PartialEq + ?Sized,
    {
        self.0
            .iter_mut()
            .find(|entry| entry.key.borrow() == key)
            .map(|entry| &mut entry.value)
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: PartialEq + ?Sized,
    {
        self.get(key).is_some()
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: PartialEq + ?Sized,
    {
        let index = self.0.iter().position(|entry| entry.key.borrow() == key)?;
        Some(self.0.remove(index).value)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.0.iter().map(|entry| (&entry.key, &entry.value))
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> {
        self.0
            .iter_mut()
            .map(|entry| (&entry.key, &mut entry.value))
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.0.iter().map(|entry| &entry.key)
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.0.iter().map(|entry| &entry.value)
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.0.iter_mut().map(|entry| &mut entry.value)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<K: PartialEq, V> FromIterator<(K, V)> for VecMap<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(items: I) -> Self {
        let items = items.into_iter();
        let mut map = Self::with_capacity(items.size_hint().0);
        for (key, value) in items {
            map.insert(key, value);
        }
        map
    }
}

impl<K: Eq + Hash, V> From<HashMap<K, V>> for VecMap<K, V> {
    fn from(map: HashMap<K, V>) -> Self {
        map.into_iter().collect()
    }
}

impl<K: PartialEq, V> IntoIterator for VecMap<K, V> {
    type Item = (K, V);
    type IntoIter =
        std::iter::Map<std::vec::IntoIter<VecMapEntry<K, V>>, fn(VecMapEntry<K, V>) -> (K, V)>;

    fn into_iter(self) -> Self::IntoIter {
        fn pair<K, V>(entry: VecMapEntry<K, V>) -> (K, V) {
            (entry.key, entry.value)
        }
        self.0.into_iter().map(pair)
    }
}

impl<'a, K: PartialEq, V> IntoIterator for &'a VecMap<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = std::iter::Map<
        std::slice::Iter<'a, VecMapEntry<K, V>>,
        fn(&'a VecMapEntry<K, V>) -> (&'a K, &'a V),
    >;

    fn into_iter(self) -> Self::IntoIter {
        fn pair<K, V>(entry: &VecMapEntry<K, V>) -> (&K, &V) {
            (&entry.key, &entry.value)
        }
        self.0.iter().map(pair)
    }
}

impl<'a, K: PartialEq, V> IntoIterator for &'a mut VecMap<K, V> {
    type Item = (&'a K, &'a mut V);
    type IntoIter = std::iter::Map<
        std::slice::IterMut<'a, VecMapEntry<K, V>>,
        fn(&'a mut VecMapEntry<K, V>) -> (&'a K, &'a mut V),
    >;

    fn into_iter(self) -> Self::IntoIter {
        fn pair<K, V>(entry: &mut VecMapEntry<K, V>) -> (&K, &mut V) {
            (&entry.key, &mut entry.value)
        }
        self.0.iter_mut().map(pair)
    }
}

impl<K: Serialize, V: Serialize> Serialize for VecMap<K, V> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for entry in &self.0 {
            map.serialize_entry(&entry.key, &entry.value)?;
        }
        map.end()
    }
}

impl<'de, K: Deserialize<'de> + PartialEq, V: Deserialize<'de>> Deserialize<'de> for VecMap<K, V> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct MapVisitor<K, V>(std::marker::PhantomData<(K, V)>);
        impl<'de, K: Deserialize<'de> + PartialEq, V: Deserialize<'de>> Visitor<'de> for MapVisitor<K, V> {
            type Value = VecMap<K, V>;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a map of small cardinality")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut access: M) -> Result<Self::Value, M::Error> {
                let mut map = VecMap::with_capacity(access.size_hint().unwrap_or(0));
                while let Some((key, value)) = access.next_entry()? {
                    map.insert(key, value);
                }
                Ok(map)
            }
        }
        deserializer.deserialize_map(MapVisitor(std::marker::PhantomData))
    }
}
