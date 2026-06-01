use super::gsi_lag::{
    GSI_LAG_CRITICAL_LIMIT_MS, GSI_LAG_HARD_LIMIT_MS, GSI_LAG_SOFT_LIMIT_MS,
    GsiPropagationGovernor, GsiWritePressure,
};

#[test]
fn given_lag_below_soft_when_checking_pressure_then_writes_are_allowed() {
    let governor = GsiPropagationGovernor::default();
    governor.observe_lag(GSI_LAG_SOFT_LIMIT_MS - 1, 10);

    assert_eq!(governor.write_pressure(10), GsiWritePressure::Allow);
}

#[test]
fn given_lag_between_soft_and_hard_when_checking_pressure_then_writes_are_delayed() {
    let governor = GsiPropagationGovernor::default();
    governor.observe_lag(GSI_LAG_SOFT_LIMIT_MS + 80, 10);

    assert!(matches!(
        governor.write_pressure(10),
        GsiWritePressure::Delay(delay) if delay.as_millis() > 0
    ));
}

#[test]
fn given_lag_above_hard_when_checking_pressure_then_some_writes_are_throttled() {
    let governor = GsiPropagationGovernor::default();
    governor.observe_lag(GSI_LAG_HARD_LIMIT_MS, 10);

    assert_eq!(governor.write_pressure(10), GsiWritePressure::Throttle);
}

#[test]
fn given_critical_lag_when_checking_pressure_then_most_writes_are_throttled() {
    let governor = GsiPropagationGovernor::default();
    governor.observe_lag(GSI_LAG_CRITICAL_LIMIT_MS, 10);

    let throttled = (0..100)
        .filter(|_| governor.write_pressure(10) == GsiWritePressure::Throttle)
        .count();

    assert_eq!(throttled, 90);
}

#[test]
fn given_caught_up_observation_when_checking_target_then_lag_is_reset() {
    let governor = GsiPropagationGovernor::default();
    governor.observe_lag(GSI_LAG_CRITICAL_LIMIT_MS, 10);
    governor.observe_caught_up();

    assert!(!governor.lag_above_target());
    assert_eq!(governor.write_pressure(2_000), GsiWritePressure::Allow);
}
