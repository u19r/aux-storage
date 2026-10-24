use serde_json::{Value, json};

use crate::TransactWriteItemsRequest;

fn transaction_request(token: Option<&str>) -> Value {
    let mut request = json!({
        "TransactItems": [{
            "Put": {
                "TableName": "TokenValidation",
                "Item": {"id": {"S": "item"}}
            }
        }]
    });
    if let Some(token) = token {
        request["ClientRequestToken"] = json!(token);
    }
    request
}

#[test]
fn given_out_of_range_token_when_transaction_is_validated_then_request_is_rejected() {
    for token in [String::new(), "a".repeat(37), "界".repeat(37)] {
        let error = TransactWriteItemsRequest::try_from(transaction_request(Some(&token)))
            .expect_err("token length must be validated before execution");
        assert_eq!(
            error,
            "ClientRequestToken must contain between 1 and 36 characters"
        );
    }
}

#[test]
fn given_valid_token_when_transaction_is_validated_then_exact_value_is_preserved() {
    for token in ["a".to_string(), "a".repeat(36), "界".repeat(36)] {
        let request = TransactWriteItemsRequest::try_from(transaction_request(Some(&token)))
            .expect("valid token");
        assert_eq!(
            request.client_request_token.as_deref(),
            Some(token.as_str())
        );
    }
}

#[test]
fn given_omitted_token_when_transaction_is_validated_then_request_remains_untokened() {
    let request = TransactWriteItemsRequest::try_from(transaction_request(None))
        .expect("client token is optional");
    assert!(request.client_request_token.is_none());
}
