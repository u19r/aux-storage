use storage_types::{
    DurableAbsenceProof, DurableItemRevision, DurablePointReadGuard, DurablePointReadProof,
    DurablePointReadRequest, KeyAttributes, StorageError, StorageResult, TableName,
};

use crate::{
    SQLiteStorageProvider, dialect::SqliteDialect, error_handler::map_sqlite_error,
    provider_core::statements::durable_revision, utils::SqliteConn,
};

impl SQLiteStorageProvider {
    pub(crate) fn do_get_item_with_durable_proof(
        request: &DurablePointReadRequest,
        sqlite: &SqliteConn<'_>,
    ) -> StorageResult<DurablePointReadProof> {
        let item = Self::do_get_wire_item_with_indexers(&request.table_name, &request.key, sqlite)?;
        let revision = Self::do_get_item_revision(&request.table_name, &request.key, sqlite)?;

        Ok(match item {
            Some((item, indexers)) => DurablePointReadProof::Present {
                item: Box::new(item),
                indexers,
                revision: DurableItemRevision::new(revision.to_be_bytes().to_vec()),
            },
            None => DurablePointReadProof::Absent {
                proof: DurableAbsenceProof::new(revision.to_be_bytes().to_vec()),
            },
        })
    }

    pub(crate) fn do_get_item_revision(
        table_name: &TableName,
        key: &KeyAttributes,
        sqlite: &SqliteConn<'_>,
    ) -> StorageResult<i64> {
        let key_json = canonical_revision_key(key)?;
        let result = sqlite.query_row(
            durable_revision::get_item_revision(&SqliteDialect),
            (table_name.as_ref(), key_json.as_str()),
            |row| row.get::<_, i64>(0),
        );

        match result {
            Ok(revision) => Ok(revision),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(0),
            Err(error) => Err(map_sqlite_error(error)),
        }
    }

    pub(crate) fn do_bump_item_revision(
        table_name: &TableName,
        key: &KeyAttributes,
        sqlite: &SqliteConn<'_>,
    ) -> StorageResult<i64> {
        let key_json = canonical_revision_key(key)?;
        sqlite
            .query_row(
                durable_revision::bump_item_revision(&SqliteDialect),
                (table_name.as_ref(), key_json.as_str()),
                |row| row.get::<_, i64>(0),
            )
            .map_err(|error| {
                tracing::warn!(
                    backend = "sqlite",
                    table = %table_name,
                    error = %error,
                    "item stream version allocation failed"
                );
                map_sqlite_error(error)
            })
    }

    pub(crate) fn do_set_item_revision(
        table_name: &TableName,
        key: &KeyAttributes,
        revision: storage_types::ItemStreamVersion,
        sqlite: &SqliteConn<'_>,
    ) -> StorageResult<()> {
        let key_json = canonical_revision_key(key)?;
        let revision = i64::try_from(revision.get()).map_err(|_| {
            StorageError::validation("item stream version does not fit sqlite revision")
        })?;
        sqlite
            .execute(
                r"INSERT INTO item_revisions (table_name, key_json, revision)
                  VALUES (?1, ?2, ?3)
                  ON CONFLICT(table_name, key_json)
                  DO UPDATE SET revision = excluded.revision",
                (table_name.as_ref(), key_json.as_str(), revision),
            )
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    pub(crate) fn validate_durable_guard(
        table_name: &TableName,
        key: &KeyAttributes,
        guard: &DurablePointReadGuard,
        sqlite: &SqliteConn<'_>,
    ) -> StorageResult<()> {
        let current_revision = Self::do_get_item_revision(table_name, key, sqlite)?;
        let expected_revision = match guard {
            DurablePointReadGuard::Present(revision) => revision_i64(revision.as_bytes())?,
            DurablePointReadGuard::Absent(proof) => revision_i64(proof.as_bytes())?,
        };

        if current_revision == expected_revision {
            Ok(())
        } else {
            Err(StorageError::guard_conflict(
                "durable point-read guard does not match current revision",
            ))
        }
    }
}

fn revision_i64(bytes: &[u8]) -> StorageResult<i64> {
    let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
        StorageError::validation("durable revision/proof must be an 8-byte big-endian integer")
    })?;
    Ok(i64::from_be_bytes(bytes))
}

pub(crate) fn canonical_revision_key(key: &KeyAttributes) -> StorageResult<String> {
    if key.is_empty() {
        return Err(StorageError::invalid_or_missing_key());
    }
    key.canonical_dynamo_json().map_err(|error| {
        StorageError::validation(format!(
            "revision key must be Dynamo JSON encodable: {error}"
        ))
    })
}
