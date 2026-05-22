use super::sql_statements::delete_gsi_row;

#[test]
fn delete_gsi_row_uses_full_gsi_primary_key() {
    let sql = delete_gsi_row(
        "gsi_table_idx",
        &[
            "gsi_pk".to_string(),
            "gsi_sk".to_string(),
            "table_pk".to_string(),
            "table_sk".to_string(),
        ],
    );

    assert_eq!(
        sql,
        "DELETE FROM \"gsi_table_idx\" WHERE gsi_pk = ?1 AND gsi_sk = ?2 AND table_pk = ?3 AND \
         table_sk = ?4"
    );
}
