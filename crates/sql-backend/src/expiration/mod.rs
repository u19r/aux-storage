//! SQL physical TTL scan under the shared expiration workflow.

mod read;
mod sql_statements;
#[cfg(test)]
mod sql_statements_tests;
mod transaction;
