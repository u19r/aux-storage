//! DynamoDB Streams owns image selection and iterator semantics independently
//! of the snapshot stream extensions. Every request uses one native attempt.

mod description;
mod iterator;
pub(crate) mod read;
pub(crate) mod record;

pub use description::DescribeDynamoStream;
pub use iterator::GetDynamoShardIterator;
pub(crate) use read::read_dynamo_stream;
pub use read::{DynamoStreamRead, GetDynamoStreamRecords};
