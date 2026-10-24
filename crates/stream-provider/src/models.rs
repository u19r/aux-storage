use std::collections::HashMap;

use storage_types::AttributeValue;

#[derive(PartialEq)]
pub struct DynamoDbStreamImages {
    pub old: Option<HashMap<String, AttributeValue>>,
    pub new: Option<HashMap<String, AttributeValue>>,
}
