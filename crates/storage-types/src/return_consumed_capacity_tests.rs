use super::ReturnConsumedCapacity;

#[test]
fn consumed_capacity_modes_keep_the_dynamodb_wire_shape() {
    for (mode, wire) in [
        (ReturnConsumedCapacity::None, "NONE"),
        (ReturnConsumedCapacity::Total, "TOTAL"),
        (ReturnConsumedCapacity::Indexes, "INDEXES"),
    ] {
        assert_eq!(mode.as_str(), wire);
        assert_eq!(
            serde_json::to_string(&mode).expect("serialize mode"),
            format!("\"{wire}\"")
        );
        assert_eq!(
            serde_json::from_str::<ReturnConsumedCapacity>(&format!("\"{wire}\""))
                .expect("deserialize mode"),
            mode
        );
    }

    assert!(serde_json::from_str::<ReturnConsumedCapacity>("\"BOGUS\"").is_err());
}

#[test]
fn transact_get_request_conversion_preserves_each_capacity_mode() {
    for (mode, wire) in [
        (ReturnConsumedCapacity::None, "NONE"),
        (ReturnConsumedCapacity::Total, "TOTAL"),
        (ReturnConsumedCapacity::Indexes, "INDEXES"),
    ] {
        let request = crate::TransactGetItemsRequest::try_from(serde_json::json!({
            "TransactItems": [{
                "Get": {
                    "TableName": "users",
                    "Key": { "pk": { "S": "user#1" } }
                }
            }],
            "ReturnConsumedCapacity": wire
        }))
        .expect("valid wire capacity mode");

        assert_eq!(request.return_consumed_capacity, Some(mode));
    }
}
