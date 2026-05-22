//! Backend-agnostic newtypes to improve semantic clarity & type-safety.
//! Introduce gradually; they intentionally implement common traits.
use std::fmt::{Display, Formatter};

macro_rules! simple_newtype {
    ($name:ident, $inner:ty) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
        pub struct $name(pub $inner);
        impl Display for $name { fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { self.0.fmt(f) } }
        impl From<$inner> for $name { fn from(v: $inner) -> Self { Self(v) } }
        impl From<$name> for $inner { fn from(v: $name) -> Self { v.0 } }
    };
}

simple_newtype!(PageToken, String);
simple_newtype!(IdempotencyToken, String);
simple_newtype!(BackfillCursor, String);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct JobIntervalMillis(pub u64);
impl Display for JobIntervalMillis {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl From<u64> for JobIntervalMillis {
    fn from(v: u64) -> Self {
        Self(v)
    }
}
impl From<JobIntervalMillis> for u64 {
    fn from(v: JobIntervalMillis) -> Self {
        v.0
    }
}
