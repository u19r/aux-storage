use std::{borrow::Cow, collections::HashSet, fmt};

use serde::{Deserialize, Deserializer, de::Visitor};
use smallvec::SmallVec;

/// Borrow ordinary JSON text and allocate only when escape decoding requires
/// it.
pub(crate) struct BorrowedText<'a>(pub Cow<'a, str>);

impl<'de> Deserialize<'de> for BorrowedText<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        struct TextVisitor;
        impl<'de> Visitor<'de> for TextVisitor {
            type Value = BorrowedText<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E> {
                Ok(BorrowedText(Cow::Borrowed(value)))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(BorrowedText(Cow::Owned(value.to_owned())))
            }
        }
        deserializer.deserialize_str(TextVisitor)
    }
}

/// Bound the small-item comparison cost while hashing wider attribute sets.
#[derive(Default)]
pub(crate) struct AttributeNames<'a> {
    inline: SmallVec<[Cow<'a, str>; 32]>,
    hashed: Option<HashSet<Cow<'a, str>>>,
}

impl<'a> AttributeNames<'a> {
    pub(crate) fn insert(&mut self, name: Cow<'a, str>) -> bool {
        if let Some(names) = self.hashed.as_mut() {
            return names.insert(name);
        }
        if self.inline.len() == self.inline.inline_size() {
            let mut hashed = HashSet::with_capacity(self.inline.len() * 2);
            hashed.extend(self.inline.drain(..));
            let inserted = hashed.insert(name);
            self.hashed = Some(hashed);
            return inserted;
        }
        if self.inline.contains(&name) {
            return false;
        }
        self.inline.push(name);
        true
    }
}
