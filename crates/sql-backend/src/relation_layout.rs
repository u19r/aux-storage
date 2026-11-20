//! Physical relation selection precedes SQL rendering. The ordinary engine
//! retains borrowed static statements; the opt-in layout qualifies names.
//! Neither layout chooses a generation or authorizes its publication.

use std::borrow::Cow;

use storage_types::{BoundGsi, TableHistoryId};

use crate::SqlEngine;

pub trait SqlLayout: Copy + Send + Sync {
    fn engine(self) -> SqlEngine;
    fn relation_name(self, name: &str) -> Cow<'_, str>;
    fn qualify_relation(self, name: String) -> String;
    fn render_static(
        self,
        ordinary: &'static str,
        qualified: impl FnOnce() -> String,
    ) -> Cow<'static, str>;

    fn item_table_name(self, history: TableHistoryId) -> String {
        self.qualify_relation(self.engine().item_table_name(history))
    }

    fn gsi_index_name(self, history: TableHistoryId, gsi: &BoundGsi) -> String {
        self.qualify_relation(self.engine().gsi_index_name(history, gsi))
    }
}

impl SqlLayout for SqlEngine {
    fn engine(self) -> SqlEngine {
        self
    }
    fn relation_name(self, name: &str) -> Cow<'_, str> {
        Cow::Borrowed(name)
    }
    fn qualify_relation(self, name: String) -> String {
        name
    }
    fn render_static(
        self,
        ordinary: &'static str,
        _qualified: impl FnOnce() -> String,
    ) -> Cow<'static, str> {
        Cow::Borrowed(ordinary)
    }
}
