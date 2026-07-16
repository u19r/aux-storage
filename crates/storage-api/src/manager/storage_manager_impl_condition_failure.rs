use storage_types::{
    return_values_on_condition_check_failure_all_old,
};

pub(super) fn should_return_old_item_on_condition_failure(
    condition_expression: Option<&str>,
    return_values_on_condition_check_failure: Option<&String>,
) -> bool {
    condition_expression.is_some()
        && return_values_on_condition_check_failure_all_old(
            return_values_on_condition_check_failure,
        )
}
