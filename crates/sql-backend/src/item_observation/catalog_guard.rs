use storage_types::StorageResult;

use crate::{SqlLayout, SqlStatement};

/// Guard every candidate read with the complete protected catalog vector.
pub(crate) fn guard_observation_statement(
    engine: impl SqlLayout,
    statement: SqlStatement,
    tables: &[(
        &storage_types::TableName,
        Option<storage_provider::TableReadCandidate<'_>>,
    )],
) -> StorageResult<SqlStatement> {
    let (sql, mut binds) = statement.into_parts();
    let mut predicates = Vec::new();
    for (name, candidate) in tables {
        let name_slot = binds.len() + 1;
        binds.push(crate::SqlValue::Text(name.to_string()));
        let relation = engine.relation_name("tables");
        predicates.push(match candidate {
            Some(candidate) => {
                let id_slot = binds.len() + 1;
                binds.push(crate::SqlValue::Text(
                    uuid::Uuid::from_bytes(*candidate.stamp.history_id.as_ref()).to_string(),
                ));
                let revision_slot = binds.len() + 1;
                binds.push(crate::SqlValue::Integer(
                    i64::try_from(candidate.stamp.revision.get()).map_err(|_| {
                        storage_types::StorageError::internal(
                            "catalog revision exceeds SQL integer",
                        )
                    })?,
                ));
                format!(
                    "EXISTS (SELECT 1 FROM {relation} WHERE table_name = ${name_slot} AND id = \
                     ${id_slot} AND catalog_revision = ${revision_slot} AND table_status <> \
                     'DELETING')"
                )
            }
            None => format!(
                "NOT EXISTS (SELECT 1 FROM {relation} WHERE table_name = ${name_slot} AND \
                 table_status <> 'DELETING')"
            ),
        });
    }
    let predicate = if predicates.is_empty() {
        "TRUE".to_owned()
    } else {
        predicates.join(" AND ")
    };
    Ok(SqlStatement::new(
        format!(
            "SELECT observed.* FROM (SELECT 1 AS matched WHERE {predicate}) AS authority CROSS \
             JOIN LATERAL ({sql} OFFSET authority.matched * 0) AS observed"
        ),
        binds,
    ))
}
