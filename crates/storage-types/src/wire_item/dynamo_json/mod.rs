//! DynamoDB attribute serde for public responses and stored stream images.

mod item_json;
pub mod optional;
pub(crate) mod optional_items;
pub(crate) mod optional_table_items;

pub use item_json::ItemRef;
pub(crate) use item_json::{Item, deserialize, serialize};
