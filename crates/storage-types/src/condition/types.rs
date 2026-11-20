/// One element of a document path. An expression attribute name always
/// resolves to exactly one `Name`, whatever characters its value contains.
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub enum PathElement {
    Name(String),
    Index(usize),
}

/// A resolved document path; the first element is always a `Name`.
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct AttributePath(smallvec::SmallVec<[PathElement; 1]>);

impl AttributePath {
    #[must_use]
    pub fn top(name: String) -> Self {
        Self(smallvec::smallvec![PathElement::Name(name)])
    }

    pub fn push(&mut self, element: PathElement) {
        self.0.push(element);
    }

    #[must_use]
    pub fn elements(&self) -> &[PathElement] {
        &self.0
    }

    /// The top-level attribute this path starts at.
    #[must_use]
    pub fn root(&self) -> &str {
        match self.0.first() {
            Some(PathElement::Name(name)) => name,
            _ => "",
        }
    }

    /// `Some(name)` when the path is exactly one top-level attribute.
    #[must_use]
    pub fn top_level(&self) -> Option<&str> {
        (self.0.len() == 1).then(|| self.root())
    }

    pub fn lookup<'a>(
        &self,
        item: &'a impl crate::AttributeValueLookup,
    ) -> Option<&'a crate::AttributeValue> {
        let (first, rest) = self.0.split_first()?;
        let PathElement::Name(name) = first else {
            return None;
        };
        let mut current = item.get_attribute_value(name)?;
        for element in rest {
            current = match (element, current) {
                (PathElement::Name(name), crate::AttributeValue::M(map)) => map.get(name)?,
                (PathElement::Index(index), crate::AttributeValue::L(list)) => list.get(*index)?,
                _ => return None,
            };
        }
        Some(current)
    }
}

impl std::fmt::Display for AttributePath {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (position, element) in self.0.iter().enumerate() {
            match element {
                PathElement::Name(name) if position == 0 => formatter.write_str(name)?,
                PathElement::Name(name) => write!(formatter, ".{name}")?,
                PathElement::Index(index) => write!(formatter, "[{index}]")?,
            }
        }
        Ok(())
    }
}

/// A path equals a name only when it is that one top-level attribute.
impl PartialEq<str> for AttributePath {
    fn eq(&self, other: &str) -> bool {
        self.top_level() == Some(other)
    }
}

impl PartialEq<AttributePath> for String {
    fn eq(&self, other: &AttributePath) -> bool {
        other == self.as_str()
    }
}

#[derive(Clone, PartialEq)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub enum Condition {
    NotExists {
        field: AttributePath,
    },
    Exists {
        field: AttributePath,
    },
    LessThan {
        field: AttributePath,
        value: String,
    },
    LessThanEqual {
        field: AttributePath,
        value: String,
    },
    GreaterThan {
        field: AttributePath,
        value: String,
    },
    GreaterThanEqual {
        field: AttributePath,
        value: String,
    },
    Equal {
        field: AttributePath,
        value: crate::AttributeValue,
    },
    NotEqual {
        field: AttributePath,
        value: crate::AttributeValue,
    },
    Between {
        field: AttributePath,
        min: String,
        max: String,
    },
    In {
        field: AttributePath,
        values: Vec<crate::AttributeValue>,
    },
    ValueEqual {
        left: crate::AttributeValue,
        right: crate::AttributeValue,
    },
    ValueNotEqual {
        left: crate::AttributeValue,
        right: crate::AttributeValue,
    },
    Contains {
        field: AttributePath,
        value: crate::AttributeValue,
    },
    BeginsWith {
        field: AttributePath,
        prefix: crate::AttributeValue,
    },
    AttributeType {
        field: AttributePath,
        attribute_type: String,
    },
    /// `size(field) = operand`; `None` is a non-number operand.
    Size {
        field: AttributePath,
        size: Option<String>,
    },
    /// `size(field) <op> operand`; `None` is a non-number operand, which makes
    /// every operator false except `<>`.
    SizeCompare {
        field: AttributePath,
        operator: SizeComparison,
        size: Option<String>,
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
    pub fn visit_attribute_paths<'a>(&'a self, visit: &mut impl FnMut(&'a AttributePath)) {
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
