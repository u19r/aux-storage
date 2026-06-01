#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    NotExists {
        field: String,
    },
    Exists {
        field: String,
    },
    LessThan {
        field: String,
        value: String,
    },
    LessThanEqual {
        field: String,
        value: String,
    },
    GreaterThan {
        field: String,
        value: String,
    },
    GreaterThanEqual {
        field: String,
        value: String,
    },
    Equal {
        field: String,
        value: storage_types::AttributeValue,
    },
    NotEqual {
        field: String,
        value: storage_types::AttributeValue,
    },
    Between {
        field: String,
        min: String,
        max: String,
    },
    In {
        field: String,
        values: Vec<storage_types::AttributeValue>,
    },
    ValueEqual {
        left: storage_types::AttributeValue,
        right: storage_types::AttributeValue,
    },
    ValueNotEqual {
        left: storage_types::AttributeValue,
        right: storage_types::AttributeValue,
    },
    Contains {
        field: String,
        value: storage_types::AttributeValue,
    },
    BeginsWith {
        field: String,
        prefix: storage_types::AttributeValue,
    },
    AttributeType {
        field: String,
        attribute_type: String,
    },
    Size {
        field: String,
        size: usize,
    },
    SizeCompare {
        field: String,
        operator: SizeComparison,
        size: usize,
    },
    And {
        conditions: Vec<Condition>,
    },
    Or {
        conditions: Vec<Condition>,
    },
    Not {
        condition: Box<Condition>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SizeComparison {
    Equal,
    NotEqual,
    LessThan,
    LessThanEqual,
    GreaterThan,
    GreaterThanEqual,
}
