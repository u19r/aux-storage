#[cfg(test)]
use std::time::Instant;

use deadpool_postgres::GenericClient;
#[cfg(test)]
use storage_common::provider_perf;
use storage_types::{DurablePointReadGuard, KeyAttributes, StorageError, StorageResult, TableName};

use crate::backends::postgres::{PostgresStorageProvider, sql_statements};

impl PostgresStorageProvider {
    pub(super) async fn get_item_revision_with_client<C: GenericClient + Sync>(
        client: &C,
        table_name: &TableName,
        key_attributes: &KeyAttributes,
    ) -> StorageResult<i64> {
        let key_json = key_attributes.canonical_dynamo_json().map_err(|err| {
            StorageError::validation(format!("revision key must be Dynamo JSON encodable: {err}"))
        })?;
        let row = client
            .query_opt(
                sql_statements::get_item_revision(),
                &[&table_name.to_string(), &key_json],
            )
            .await
            .map_err(|err| Self::map_postgres_error("get item revision", err))?;
        row.map_or(Ok(0), |row| {
            row.try_get::<_, i64>(0)
                .map_err(|err| Self::map_postgres_error("decode item revision", err))
        })
    }

    pub(super) async fn bump_item_revision_with_client<C: GenericClient + Sync>(
        client: &C,
        table_name: &TableName,
        key_attributes: &KeyAttributes,
    ) -> StorageResult<i64> {
        let key_json = key_attributes.canonical_dynamo_json().map_err(|err| {
            StorageError::validation(format!("revision key must be Dynamo JSON encodable: {err}"))
        })?;
        #[cfg(test)]
        let started = Instant::now();
        let row = client
            .query_one(
                sql_statements::bump_item_revision(),
                &[&table_name.to_string(), &key_json],
            )
            .await
            .map_err(|err| {
                tracing::warn!(
                    backend = "postgres",
                    table = %table_name,
                    error = %err,
                    "item stream version allocation failed"
                );
                Self::map_postgres_write_error("bump item revision", err)
            })?;
        #[cfg(test)]
        provider_perf::record("postgres", "sql_execute_revision", started.elapsed());
        row.try_get::<_, i64>(0)
            .map_err(|err| Self::map_postgres_error("decode item revision", err))
    }

    pub(super) async fn validate_durable_guard_with_client<C: GenericClient + Sync>(
        client: &C,
        table_name: &TableName,
        key_attributes: &KeyAttributes,
        guard: &DurablePointReadGuard,
    ) -> StorageResult<()> {
        let expected_revision = match guard {
            DurablePointReadGuard::Present(revision) => {
                Self::revision_from_guard_bytes(revision.as_bytes())?
            }
            DurablePointReadGuard::Absent(proof) => {
                Self::revision_from_guard_bytes(proof.as_bytes())?
            }
        };
        let key_json = key_attributes.canonical_dynamo_json().map_err(|err| {
            StorageError::validation(format!("revision key must be Dynamo JSON encodable: {err}"))
        })?;
        client
            .execute(
                sql_statements::ensure_item_revision(),
                &[&table_name.to_string(), &key_json],
            )
            .await
            .map_err(|err| Self::map_postgres_write_error("create guard revision row", err))?;
        let row = client
            .query_one(
                sql_statements::lock_item_revision(),
                &[&table_name.to_string(), &key_json],
            )
            .await
            .map_err(|err| Self::map_postgres_error("lock guard revision", err))?;
        let current_revision = row
            .try_get::<_, i64>(0)
            .map_err(|err| Self::map_postgres_error("decode guard revision", err))?;
        if current_revision == expected_revision {
            return Ok(());
        }
        Err(StorageError::guard_conflict(&format!(
            "guard revision mismatch for {table_name}: expected {expected_revision}, got \
             {current_revision}"
        )))
    }

    fn revision_from_guard_bytes(bytes: &[u8]) -> StorageResult<i64> {
        let bytes: [u8; 8] = bytes
            .try_into()
            .map_err(|_| StorageError::validation("durable guard revision must be 8 bytes"))?;
        Ok(i64::from_be_bytes(bytes))
    }
}
