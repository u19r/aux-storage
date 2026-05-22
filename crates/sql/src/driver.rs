use std::future::Future;

use crate::sql_types::{SqlParam, SqlRow, SqlValue};

pub trait SqlDriver: Send + Sync {
    fn backend_name(&self) -> &'static str;
}

pub trait SqlExecutor: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn execute<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [SqlParam],
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send + 'a;

    fn query<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [SqlParam],
    ) -> impl Future<Output = Result<Vec<SqlRow>, Self::Error>> + Send + 'a;

    fn query_value<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [SqlParam],
    ) -> impl Future<Output = Result<Option<SqlValue>, Self::Error>> + Send + 'a;
}
