//! DynamoDB Streams protocol uses the selected manager's read authority.
//! Ordinary and replicated assembly share iterator and record semantics.

mod description;
mod iterator;
mod read;
mod record;

pub(crate) use read::DynamoStreamReader;
