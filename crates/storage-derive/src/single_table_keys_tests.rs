use crate::single_table_keys::parse_gsi_expressions;

#[test]
fn given_gsi_key_expressions_when_parsing_then_accepts_valid_rust_expressions() {
    let span = syn::parse_str::<syn::Ident>("Entity").expect("ident");

    let (pk, sk) = parse_gsi_expressions(
        &span,
        1,
        Some("Some(format!(\"USER#{}\", self.user_id))"),
        Some("self.created_at.to_string().into()"),
    )
    .expect("valid gsi expressions");

    assert_eq!(
        quote::quote!(#pk).to_string(),
        "Some (format ! (\"USER#{}\" , self . user_id))"
    );
    assert_eq!(
        quote::quote!(#sk).to_string(),
        "self . created_at . to_string () . into ()"
    );
}

#[test]
fn given_missing_gsi_key_expression_when_parsing_then_names_required_side() {
    let span = syn::parse_str::<syn::Ident>("Entity").expect("ident");

    let error = match parse_gsi_expressions(&span, 2, Some("Some(self.pk.clone())"), None) {
        Ok(_) => panic!("missing sk should fail"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("gsi2_sk_expr required"));
}

#[test]
fn given_invalid_gsi_expression_when_parsing_then_reports_index_and_side() {
    let span = syn::parse_str::<syn::Ident>("Entity").expect("ident");

    let error = match parse_gsi_expressions(&span, 3, Some("Some("), Some("Some(self.sk.clone())"))
    {
        Ok(_) => panic!("invalid pk should fail"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("invalid gsi3_pk_expr expression")
    );
}
