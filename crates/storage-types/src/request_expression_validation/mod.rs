use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    sync::LazyLock,
};

use smallvec::SmallVec;

use crate::{
    collect_expression_attribute_placeholder_refs,
    constants::{DYNAMODB_RESERVED_WORDS, MAX_EXPRESSION_SUBSTITUTION_BYTES},
};

pub(crate) mod expression_kind;
mod functions;
mod keys;
mod paths;
mod shape;
mod update;
mod validation;

use expression_kind::ExpressionKind;
use functions::*;
#[cfg(test)]
pub(crate) use keys::is_dynamodb_reserved_word;
use keys::*;
pub(crate) use keys::{
    validate_expression_attribute_name_keys, validate_expression_attribute_value_keys,
};
use paths::*;
use shape::*;
use update::*;
use validation::*;
pub(crate) use validation::{validate_expression_set, validate_expression_value_operands};

#[cfg(test)]
mod request_expression_validation_perf_tests;

#[cfg(test)]
mod functions_tests;

#[cfg(test)]
mod shape_tests;

#[cfg(test)]
mod expression_kind_tests;
