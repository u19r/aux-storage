use crate::single_table_keys::{parse_entity_indexers, parse_gsi_expressions};

#[test]
fn given_indexed_fields_when_parsing_then_uses_wire_names_in_ordinal_order() {
    let input = syn::parse_quote! {
        #[serde(rename_all = "PascalCase")]
        struct Order {
            #[single_table(indexer = 1)]
            #[wire_item(rename = "customer_id")]
            customer_id: String,
            #[single_table(indexer = 0)]
            order_id: String,
        }
    };

    let indexers = parse_entity_indexers(&input).expect("valid indexers");

    assert_eq!(indexers.len(), 2);
    assert_eq!(indexers[0].field, "order_id");
    assert_eq!(indexers[0].attribute_name, "OrderId");
    assert_eq!(indexers[0].ordinal, 0);
    assert_eq!(indexers[1].field, "customer_id");
    assert_eq!(indexers[1].attribute_name, "customer_id");
    assert_eq!(indexers[1].ordinal, 1);
}

#[test]
fn given_indexer_gap_when_parsing_then_reports_expected_ordinal() {
    let input = syn::parse_quote! {
        struct Order {
            #[single_table(indexer = 1)]
            customer_id: String,
        }
    };

    let error = parse_entity_indexers(&input).expect_err("gap must fail");

    assert!(error.to_string().contains("expected 0"));
}

#[test]
fn given_duplicate_wire_names_when_parsing_then_rejects_declaration() {
    let input = syn::parse_quote! {
        struct Order {
            #[single_table(indexer = 0)]
            #[serde(rename = "relationship_id")]
            customer_id: String,
            #[single_table(indexer = 1)]
            #[wire_item(rename = "relationship_id")]
            account_id: String,
        }
    };

    let error = parse_entity_indexers(&input).expect_err("duplicate must fail");

    assert!(error.to_string().contains("relationship_id` is duplicated"));
}

#[test]
fn given_duplicate_ordinals_when_parsing_then_rejects_declaration() {
    let input = syn::parse_quote! {
        struct Order {
            #[single_table(indexer = 0)]
            customer_id: String,
            #[single_table(indexer = 0)]
            account_id: String,
        }
    };

    let error = parse_entity_indexers(&input).expect_err("duplicate ordinal must fail");

    assert!(error.to_string().contains("expected 1"));
}

#[test]
fn given_more_than_maximum_indexers_when_parsing_then_rejects_declaration() {
    let fields = (0_u8..33).map(|ordinal| {
        let field = quote::format_ident!("field_{ordinal}");
        quote::quote! {
            #[single_table(indexer = #ordinal)]
            #field: String
        }
    });
    let input = syn::parse2(quote::quote! {
        struct Order {
            #(#fields),*
        }
    })
    .expect("test struct");

    let error = parse_entity_indexers(&input).expect_err("excess indexers must fail");

    assert!(error.to_string().contains("at most 32 indexers"));
}

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
