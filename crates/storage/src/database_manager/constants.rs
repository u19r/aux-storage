/// Optimistic-concurrency version attribute stored on guarded entities.
pub(crate) const OCC_VERSION_ATTR: &str = "_v";
pub(crate) const OCC_VERSION_NAME: &str = "#v";
pub(crate) const OCC_CREATE_CONDITION: &str = "attribute_not_exists(#v)";
pub(crate) const OCC_UPDATE_CONDITION: &str = "attribute_not_exists(#v) OR #v = :v";

/// Generic single-table conditions shared by capped entity helpers.
pub(crate) const ENTITY_ABSENT_CONDITION: &str =
    "attribute_not_exists(pk) AND attribute_not_exists(sk)";
pub(crate) const ENTITY_EXISTS_CONDITION: &str = "attribute_exists(pk)";

/// Synthetic route id used for writes to the manager's primary connection.
pub(crate) const ROUTED_DEFAULT_CONNECTION_ID: &str = "default";

/// Capped entity counter key shape, placeholders, and expressions.
pub(crate) const CAPPED_ENTITY_COUNTER_PK: &str = "COUNT";
pub(crate) const CAPPED_ENTITY_COUNTER_VALUE_ATTR: &str = "value";
pub(crate) const CAPPED_ENTITY_COUNTER_VALUE_NAME: &str = "#value";
pub(crate) const CAPPED_ENTITY_COUNTER_ENTITY_TYPE_NAME: &str = "#et";
pub(crate) const CAPPED_ENTITY_COUNTER_DELTA_VALUE: &str = ":delta";
pub(crate) const CAPPED_ENTITY_COUNTER_MAX_VALUE: &str = ":max";
pub(crate) const CAPPED_ENTITY_COUNTER_ZERO_VALUE: &str = ":zero";
pub(crate) const CAPPED_ENTITY_COUNTER_ENTITY_TYPE_VALUE: &str = ":entity_type";
pub(crate) const CAPPED_ENTITY_COUNTER_CREATE_CONDITION: &str =
    "attribute_not_exists(#value) OR #value < :max";
pub(crate) const CAPPED_ENTITY_COUNTER_DELETE_CONDITION: &str =
    "attribute_exists(#value) AND #value > :zero";
pub(crate) const CAPPED_ENTITY_COUNTER_UPDATE_EXPRESSION: &str =
    "ADD #value :delta SET #et = :entity_type";
