use std::time::Duration;

#[expect(
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
#[must_use]
pub fn jittered(base: Duration, percent: u8) -> Duration {
    if base.is_zero() || percent == 0 {
        return base;
    }

    let jitter = f64::from(percent.min(100)) / 100.0;
    let base_ms = base.as_millis() as i128;
    let min_ms = ((base_ms as f64) * (1.0 - jitter)).round().max(0.0) as i128;
    let max_ms = ((base_ms as f64) * (1.0 + jitter)).round().max(0.0) as i128;
    if max_ms <= min_ms {
        return Duration::from_millis(min_ms as u64);
    }

    let span = (max_ms - min_ms) as u128;
    let offset = rand::random::<u128>() % (span + 1);
    Duration::from_millis((min_ms as u128 + offset) as u64)
}
