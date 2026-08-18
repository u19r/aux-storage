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

impl Condition {
    pub fn visit_attribute_paths<'a>(&'a self, visit: &mut impl FnMut(&'a str)) {
        match self {
            Self::NotExists { field }
            | Self::Exists { field }
            | Self::LessThan { field, .. }
            | Self::LessThanEqual { field, .. }
            | Self::GreaterThan { field, .. }
            | Self::GreaterThanEqual { field, .. }
            | Self::Equal { field, .. }
            | Self::NotEqual { field, .. }
            | Self::Between { field, .. }
            | Self::In { field, .. }
            | Self::Contains { field, .. }
            | Self::BeginsWith { field, .. }
            | Self::AttributeType { field, .. }
            | Self::Size { field, .. }
            | Self::SizeCompare { field, .. } => visit(field),
            Self::And { conditions } | Self::Or { conditions } => {
                for condition in conditions {
                    condition.visit_attribute_paths(visit);
                }
            }
            Self::Not { condition } => condition.visit_attribute_paths(visit),
            Self::ValueEqual { .. } | Self::ValueNotEqual { .. } => {}
        }
    }
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
