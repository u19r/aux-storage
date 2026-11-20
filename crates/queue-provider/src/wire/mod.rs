//! The one owner of the SQS wire protocols: protocol detection, request
//! decode, response encode, and the error shape, for JSON and for Query.

mod json;
mod query;
mod query_xml;
mod queue_wire;

pub use queue_wire::*;
