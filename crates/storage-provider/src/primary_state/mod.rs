//! Primary authority, native observations and validated upsert facts.

mod row;
mod upsert;

pub use row::PrimaryStateRow;
pub use upsert::UpsertImage;
