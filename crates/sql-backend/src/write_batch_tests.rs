use super::write_batch::*;
use crate::{SqlEngine, SqlValue};

fn shape(engine: SqlEngine) -> UpsertShape {
    UpsertShape::try_new(
        vec!["pk".to_owned(), "sk".to_owned(), "value".to_owned()],
        vec![false, engine == SqlEngine::Postgres, false],
        vec!["pk".to_owned(), "sk".to_owned()],
        true,
    )
    .unwrap()
}

fn row(pk: &str, sk: &str, value: &str) -> Vec<SqlValue> {
    vec![
        SqlValue::Text(pk.to_owned()),
        SqlValue::Text(sk.to_owned()),
        SqlValue::Text(value.to_owned()),
    ]
}

#[test]
fn given_two_rows_when_rendered_then_placeholders_and_binds_continue() {
    for (engine, first, second) in [
        (SqlEngine::Sqlite, "?1, ?2, ?3", "?4, ?5, ?6"),
        (SqlEngine::Turso, "?1, ?2, ?3", "?4, ?5, ?6"),
        (
            SqlEngine::Postgres,
            "$1, CAST($2 AS TEXT)::NUMERIC, $3",
            "$4, CAST($5 AS TEXT)::NUMERIC, $6",
        ),
    ] {
        let (sql, binds) = shape(engine)
            .render(
                engine,
                "relation",
                vec![row("a", "1", "one"), row("b", "2", "two")],
            )
            .unwrap()
            .into_parts();
        assert_eq!(binds.len(), 6);
        assert!(sql.contains(&format!("VALUES ({first}), ({second})")));
    }
}

#[test]
fn given_two_delete_rows_when_rendered_then_placeholders_and_binds_continue() {
    let shape =
        DeleteShape::try_new(vec!["pk".to_owned(), "sk".to_owned()], vec![false, false]).unwrap();
    let (sql, binds) = shape
        .render(
            SqlEngine::Sqlite,
            "relation",
            vec![
                vec![
                    SqlValue::Text("a".to_owned()),
                    SqlValue::Text("1".to_owned()),
                ],
                vec![
                    SqlValue::Text("b".to_owned()),
                    SqlValue::Text("2".to_owned()),
                ],
            ],
        )
        .unwrap()
        .into_parts();
    assert_eq!(
        sql,
        "DELETE FROM \"relation\" WHERE (\"pk\", \"sk\") IN (VALUES (?1, ?2), (?3, ?4))"
    );
    assert_eq!(binds.len(), 4);
}

#[test]
fn given_delete_rows_for_two_relations_when_staged_then_each_relation_is_grouped() {
    let engine = SqlEngine::Sqlite;
    let mut groups = DeleteGroups::default();
    for (relation, pk) in [("first", "a"), ("first", "b"), ("second", "c")] {
        groups
            .stage(PreparedDeleteRow {
                relation: relation.to_owned(),
                shape: DeleteShape::try_new(vec!["pk".to_owned()], vec![false]).unwrap(),
                binds: vec![SqlValue::Text(pk.to_owned())],
            })
            .unwrap();
    }
    let statements = groups.into_statements(engine).unwrap();
    assert_eq!(statements.len(), 2);
    assert_eq!(statements[0].1, 2);
    assert_eq!(statements[1].1, 1);
}

#[test]
fn given_two_relations_when_staged_then_each_relation_gets_its_own_statement() {
    let engine = SqlEngine::Sqlite;
    let mut groups = UpsertGroups::default();
    for (relation, pk) in [("first", "a"), ("first", "b"), ("second", "c")] {
        groups
            .stage(PreparedUpsertRow {
                relation: relation.to_owned(),
                shape: shape(engine),
                binds: row(pk, "1", "value"),
            })
            .unwrap();
    }
    let statements = groups.into_statements(engine).unwrap();
    assert_eq!(statements.len(), 2);
    assert_eq!(statements[0].1, 2);
    assert_eq!(statements[1].1, 1);
}

#[test]
fn given_more_rows_than_one_bind_budget_when_grouped_then_every_row_is_preserved_and_each_statement_is_bounded()
 {
    use crate::constants::MULTI_ROW_WRITE_BINDS;
    let row_count = MULTI_ROW_WRITE_BINDS / 3 + 1;
    let mut groups = UpsertGroups::default();
    for ordinal in 0..row_count {
        groups
            .stage(PreparedUpsertRow {
                relation: "items".to_owned(),
                shape: shape(SqlEngine::Postgres),
                binds: row(&ordinal.to_string(), "1", "value"),
            })
            .unwrap();
    }
    let statements = groups.into_statements(SqlEngine::Postgres).unwrap();
    assert_eq!(statements.len(), 2);
    let mut ordinal = 0;
    for (statement, count) in statements {
        let (sql, binds) = statement.into_parts();
        assert!(binds.len() <= MULTI_ROW_WRITE_BINDS);
        assert_eq!(binds.len(), count * 3);
        assert!(
            sql.contains("VALUES ($1,"),
            "each statement restarts its bind positions"
        );
        for values in binds.as_chunks::<3>().0 {
            assert!(matches!(&values[0], SqlValue::Text(key) if key == &ordinal.to_string()));
            ordinal += 1;
        }
    }
    assert_eq!(ordinal, row_count);
}
