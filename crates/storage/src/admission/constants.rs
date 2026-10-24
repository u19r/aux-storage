pub(super) const SERVICE_OUTCOMES: [&str; 4] = [
    "success",
    "success_after_pressure",
    "failure",
    "retryable_pressure",
];
pub(super) const PRESSURE_OUTCOMES: [&str; 4] = [
    "success_after_pressure",
    "retryable_pressure",
    "timeout",
    "explicit_throttle",
];
pub(super) const CONTROL_PRESSURE_OUTCOMES: [&str; 2] = ["provider_signal", "error"];
pub(super) const FIXED_INGRESS_MINIMUM: usize = 64;
pub(super) const FIXED_INGRESS_MAXIMUM: usize = 2_048;
