use syn::{Data, DeriveInput, Fields};

use crate::wire_item_encode::order_wire_fields;

fn wire_names(input: &DeriveInput) -> Result<Vec<String>, syn::Error> {
    let Data::Struct(data) = &input.data else {
        panic!("test struct")
    };
    let Fields::Named(fields) = &data.fields else {
        panic!("named test fields")
    };
    order_wire_fields(input, &fields.named, None).map(|fields| {
        fields
            .into_iter()
            .map(|(_, attributes)| attributes.wire_name)
            .collect()
    })
}

#[test]
fn given_scrambled_wire_fields_when_ordered_then_roles_and_wire_renames_determine_priority() {
    let input = syn::parse_quote! {
        struct Item {
            payload: String,
            #[single_table(indexer = 1)] second: String,
            #[wire_item(rename = "gsi5sk")] last_index_key: String,
            #[serde(rename = "pk")] partition: String,
            expires_at: i64,
            #[serde(rename = "gsi2pk")] index_partition: String,
            #[single_table(indexer = 0)] first: String,
            sk: String,
            ttl: i64,
        }
    };
    assert_eq!(
        wire_names(&input).unwrap(),
        [
            "pk", "sk", "gsi2pk", "gsi5sk", "e_at", "ttl", "first", "second", "payload"
        ]
    );
}

#[test]
fn given_duplicate_wire_names_when_ordered_then_encoding_is_rejected() {
    let input = syn::parse_quote! {
        struct Item { pk: String, #[wire_item(rename = "pk")] other: String }
    };
    assert!(
        wire_names(&input)
            .unwrap_err()
            .to_string()
            .contains("duplicate wire attribute `pk`")
    );
}

#[test]
fn given_serde_rename_and_skip_when_attribute_order_changes_then_wire_contract_is_identical() {
    for field in [
        syn::parse_quote! { #[serde(rename = "ttl", skip_serializing_if = "Option::is_none")] expires: Option<i64> },
        syn::parse_quote! { #[serde(skip_serializing_if = "Option::is_none", rename = "ttl")] expires: Option<i64> },
    ] {
        let attributes =
            crate::wire_item_helpers::parse_wire_item_field_attributes(&field, None).unwrap();
        assert_eq!(attributes.wire_name, "ttl");
        assert_eq!(
            attributes
                .skip_serializing_if
                .map(|predicate| quote::quote!(#predicate).to_string())
                .as_deref(),
            Some("Option :: is_none")
        );
    }
}
