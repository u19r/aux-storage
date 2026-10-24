use storage_types::{
    AttributeValue, KeyAttributes, ReturnValuesOldNewUpdated, TableName, TransactUpdateRequest,
};

use super::UpdateItemInput;

#[test]
fn given_an_update_that_returns_attributes_when_converted_to_a_transaction_then_it_is_rejected() {
    for return_values in [
        ReturnValuesOldNewUpdated::AllOld,
        ReturnValuesOldNewUpdated::AllNew,
        ReturnValuesOldNewUpdated::UpdatedOld,
        ReturnValuesOldNewUpdated::UpdatedNew,
    ] {
        let input = UpdateItemInput::builder()
            .table_name(TableName::new("items"))
            .key(KeyAttributes::from_iter([
                ("pk".to_string(), AttributeValue::S("pk".to_string())),
                ("sk".to_string(), AttributeValue::S("sk".to_string())),
            ]))
            .update_expression("SET #value = :value")
            .return_values(return_values)
            .build();
        assert!(TransactUpdateRequest::try_from(input).is_err());
    }
}
