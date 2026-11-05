use std::hint::black_box;

use alloc_counter::AllocationGuard;
use storage_backfill::{GsiActive, GsiBuild, GsiBuildTarget, GsiState};
use storage_types::{
    IndexName, SystemStreamCheckpoint, SystemStreamPosition, SystemStreamScope, TableName,
    maintenance::TargetIncarnation,
};

use super::write::transition_state_statement;
use crate::{SqlEngine, SqlValue, write_batch::UpsertShape};

fn build() -> GsiBuild<String> {
    let checkpoint = SystemStreamCheckpoint::try_new(
        SystemStreamScope::try_new("gsi-test", 1).unwrap(),
        vec![SystemStreamPosition::try_new("root", "root", "00000000000000000001").unwrap()],
    )
    .unwrap();
    GsiBuild {
        target: GsiBuildTarget {
            history_id: 1u32.into(),
            table_name: TableName::new("table"),
            index_name: IndexName::new("index"),
            incarnation: TargetIncarnation::from([1; 16]),
        },
        state: GsiState::Active(GsiActive { cursor: checkpoint }),
    }
}

#[test]
fn given_gsi_transition_when_rendered_then_preserve_sql_and_bind_slots() {
    let build = build();
    for (engine, prefix) in [
        (SqlEngine::Sqlite, "?"),
        (SqlEngine::Turso, "?"),
        (SqlEngine::Postgres, "$"),
    ] {
        let (sql, binds) = transition_state_statement(engine, &build, &build.state)
            .unwrap()
            .into_parts();
        assert_eq!(
            sql,
            format!(
                "UPDATE sys_gsi_builds SET phase = {prefix}1, state = {prefix}2 WHERE table_name \
                 = {prefix}3 AND index_name = {prefix}4 AND incarnation = {prefix}5 AND phase = \
                 {prefix}6 AND state = {prefix}7 AND table_id = {prefix}8"
            )
        );
        assert_eq!(binds.len(), 8);
        assert!(matches!(&binds[2], crate::SqlValue::Text(value) if value == "table"));
        assert!(matches!(&binds[3], crate::SqlValue::Text(value) if value == "index"));
        assert!(matches!(&binds[4], crate::SqlValue::Blob(value) if value == &[1;16]));
    }
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_gsi_statement_shapes_when_rendered_then_report_cpu_and_allocations() {
    let build = build();
    for (engine, name) in [
        (SqlEngine::Sqlite, "sqlite"),
        (SqlEngine::Turso, "turso"),
        (SqlEngine::Postgres, "postgres"),
    ] {
        report(&format!("{name}_transition"), || {
            drop(black_box(
                transition_state_statement(
                    black_box(engine),
                    black_box(&build),
                    black_box(&build.state),
                )
                .unwrap(),
            ));
        });
        for count in [2, 8, 32] {
            let columns = (0..count)
                .map(|index| format!("column_{index}"))
                .collect::<Vec<_>>();
            let mut numeric = vec![false; count];
            numeric[0] = true;
            numeric[1] = true;
            report(&format!("{name}_upsert_{count}"), || {
                let shape = UpsertShape::try_new(
                    black_box(columns.clone()),
                    black_box(numeric.clone()),
                    black_box(columns[..2].to_vec()),
                    true,
                )
                .unwrap();
                drop(black_box(
                    shape
                        .render(
                            black_box(engine),
                            "physical_table",
                            vec![(0..count).map(|_| SqlValue::Null).collect()],
                        )
                        .unwrap(),
                ));
            });
        }
    }
}

fn report(label: &str, mut render: impl FnMut()) {
    for _ in 0..100 {
        render();
    }
    let guard = AllocationGuard::start(module_path!(), "gsi_sql", file!(), line!(), Some(label));
    let started = cpu_time::ProcessTime::now();
    for _ in 0..100_000 {
        render();
    }
    let cpu = started.elapsed();
    alloc_counter::emit_report(&guard.finish()).unwrap();
    eprintln!(
        "gsi_sql label={label} iterations=100000 cpu_ns={}",
        cpu.as_nanos()
    );
}

#[test]
fn given_gsi_upsert_when_rendered_then_preserve_dynamic_columns_and_numeric_slots() {
    for (engine, prefix) in [
        (SqlEngine::Sqlite, "?"),
        (SqlEngine::Turso, "?"),
        (SqlEngine::Postgres, "$"),
    ] {
        let columns = vec!["pk".to_owned(), "sk".to_owned(), "value".to_owned()];
        let shape = UpsertShape::try_new(
            columns.clone(),
            vec![false, true, false],
            columns[..2].to_vec(),
            true,
        )
        .unwrap();
        let (sql, binds) = shape
            .render(
                engine,
                "physical",
                vec![vec![
                    SqlValue::Text("pk".into()),
                    SqlValue::Text("1".into()),
                    SqlValue::Text("value".into()),
                ]],
            )
            .unwrap()
            .into_parts();
        assert_eq!(binds.len(), 3);
        assert_eq!(
            sql,
            format!(
                "INSERT INTO \"physical\" (\"pk\", \"sk\", \"value\") VALUES ({prefix}1, \
                 CAST({prefix}2 AS TEXT)::NUMERIC, {prefix}3) ON CONFLICT (\"pk\", \"sk\") DO \
                 UPDATE SET \"value\" = excluded.\"value\" WHERE excluded.__aux_item_version > \
                 \"physical\".__aux_item_version"
            )
        );
        let columns = (1..=32)
            .map(|index| format!("column_{index}"))
            .collect::<Vec<_>>();
        let mut numeric = vec![false; columns.len()];
        for index in [20, 21, 32] {
            numeric[index - 1] = true;
        }
        let shape =
            UpsertShape::try_new(columns.clone(), numeric, columns[..2].to_vec(), true).unwrap();
        let (sql, _) = shape
            .render(
                engine,
                "physical",
                vec![(0..columns.len()).map(|_| SqlValue::Null).collect()],
            )
            .unwrap()
            .into_parts();
        for index in [20, 21, 32] {
            assert!(sql.contains(&format!("CAST({prefix}{index} AS TEXT)::NUMERIC")));
        }
    }
}
