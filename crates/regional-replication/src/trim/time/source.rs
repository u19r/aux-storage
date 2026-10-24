//! Bounded local monitoring acquisition outside native transaction ownership.
use std::{net::IpAddr, process::Stdio};

use replication_provider::runtime_configuration::time_trust::TimeTrustConfiguration;
use tokio::{
    io::AsyncReadExt,
    process::Command,
    time::{Instant, timeout_at},
};

use super::{
    clocks::ClockObservation,
    reports::{ReportSnapshot, Tracking, parse_contributors, parse_measurement},
};

#[derive(Clone)]
pub(super) struct QualifiedTimeSample {
    pub(super) clock: ClockObservation,
    pub(super) error: i128,
    pub(super) frequency: i128,
    pub(super) deadline: Instant,
    pub(super) fresh_until: Instant,
    pub(super) binding: replication_types::protocol::CanonicalDigest,
}

enum MonitoringCommand {
    Tracking,
    Sources,
    Authentication,
    Measurement(IpAddr),
}

struct ChronyAcquisition<'policy> {
    trust: &'policy TimeTrustConfiguration,
    deadline: Instant,
    remaining: usize,
}

impl ChronyAcquisition<'_> {
    async fn read_report(&mut self, command: MonitoringCommand) -> Option<String> {
        let mut invocation = Command::new(&self.trust.executable);
        invocation
            .arg("-n")
            .arg("-c")
            .arg("-h")
            .arg(&self.trust.socket);
        match command {
            MonitoringCommand::Tracking => {
                invocation.arg("tracking");
            }
            MonitoringCommand::Sources => {
                invocation.arg("sources");
            }
            MonitoringCommand::Authentication => {
                invocation.arg("authdata");
            }
            MonitoringCommand::Measurement(address) => {
                invocation.arg("ntpdata").arg(address.to_string());
            }
        }
        let mut child = invocation
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .ok()?;
        let limit = u64::try_from(self.remaining).ok()?.checked_add(1)?;
        let mut bytes = Vec::new();
        let result = timeout_at(self.deadline, async {
            child
                .stdout
                .take()?
                .take(limit)
                .read_to_end(&mut bytes)
                .await
                .ok()?;
            if bytes.len() > self.remaining {
                return None;
            }
            child.wait().await.ok()?.success().then_some(())
        })
        .await;
        if !matches!(result, Ok(Some(()))) {
            let _ = child.kill().await;
            return None;
        }
        self.remaining = self.remaining.checked_sub(bytes.len())?;
        String::from_utf8(bytes).ok()
    }

    async fn read_snapshot(&mut self) -> Option<ReportSnapshot> {
        let tracking = Tracking::parse(&self.read_report(MonitoringCommand::Tracking).await?)?;
        let contributors = parse_contributors(
            &self.read_report(MonitoringCommand::Sources).await?,
            self.trust,
        )?;
        let authentication = self.read_report(MonitoringCommand::Authentication).await?;
        let mut measurements = Vec::with_capacity(contributors.len());
        for source in &contributors {
            measurements.push(parse_measurement(
                &self
                    .read_report(MonitoringCommand::Measurement(source.address))
                    .await?,
                source,
            )?);
        }
        Some(ReportSnapshot {
            tracking,
            contributors,
            authentication,
            measurements,
        })
    }
}

impl QualifiedTimeSample {
    pub(super) async fn acquire(trust: &TimeTrustConfiguration, deadline: Instant) -> Option<Self> {
        let before_clock = ClockObservation::sample()?;
        let mut acquisition = ChronyAcquisition {
            trust,
            deadline: deadline.min(Instant::now().checked_add(
                std::time::Duration::from_millis(trust.acquisition_timeout_ms.get()),
            )?),
            remaining: trust.maximum_stdout_bytes.get(),
        };
        let before = acquisition.read_snapshot().await?;
        let after = acquisition.read_snapshot().await?;
        let observed_at = Instant::now();
        let clock = ClockObservation::sample()?;
        if Instant::now() > acquisition.deadline || !before.is_same_measurement(&after) {
            return None;
        }
        let before_error = before.require_sources(trust)?;
        let after_error = after.require_sources(trust)?;
        let upstream = before_error.0.max(after_error.0);
        let contributors = before_error.1.max(after_error.1);
        let (_, elapsed) = clock.elapsed_since(before_clock)?;
        let frequency = before.tracking.frequency.max(after.tracking.frequency);
        let drift = elapsed
            .checked_mul(frequency)?
            .checked_add(999_999_999)?
            .checked_div(1_000_000_000)?;
        let error = before
            .tracking
            .error
            .max(after.tracking.error)
            .checked_add(upstream)?
            .max(contributors)
            .checked_add(elapsed)?
            .checked_add(drift)?;
        clock.require_continuity(before_clock, error.checked_mul(2)?, frequency)?;
        let fresh_until = require_sample_deadline(
            &after,
            trust,
            (clock, observed_at),
            (error, frequency, elapsed),
        )?;
        let valid_until = deadline.min(fresh_until);
        let binding = replication_types::protocol::CanonicalDigest::of_json(&(
            trust,
            after.contributor_binding(),
        ))
        .ok()?;
        Some(Self {
            clock,
            error,
            frequency,
            binding,
            deadline: valid_until,
            fresh_until,
        })
    }
}

fn require_sample_deadline(
    report: &ReportSnapshot,
    trust: &TimeTrustConfiguration,
    observation: (ClockObservation, Instant),
    bounds: (i128, i128, i128),
) -> Option<Instant> {
    let (clock, observed_at) = observation;
    let (error, frequency, acquisition_elapsed) = bounds;
    // Native consumption permits continuity within two sample errors. Freshness
    // includes that whole envelope as well as the original qualified error.
    let error = error.checked_mul(3)?;
    let mut remaining = require_reference_age(clock.wall, report.tracking.reference, error, trust)?;
    for measurement in &report.measurements {
        remaining = remaining.min(require_reference_age(
            clock.wall,
            super::decimal::parse_seconds(&measurement[12])?,
            error,
            trust,
        )?);
    }
    let source_age = i128::from(report.maximum_source_age_ms()?)
        .checked_mul(1_000_000)?
        .checked_add(acquisition_elapsed)?
        .checked_add(error)?;
    remaining = remaining.min(
        i128::from(trust.maximum_measurement_age_ms.get())
            .checked_mul(1_000_000)?
            .checked_sub(source_age)?,
    );
    // A fast clock can exhaust wall freshness before nominal monotonic time.
    let remaining = remaining
        .checked_mul(1_000_000_000)?
        .checked_div(1_000_000_000i128.checked_add(frequency)?)?;
    if remaining <= 0 {
        return None;
    }
    let seconds = u64::try_from(remaining.checked_div(1_000_000_000)?).ok()?;
    let nanos = u32::try_from(remaining.checked_rem(1_000_000_000)?).ok()?;
    let deadline = observed_at.checked_add(std::time::Duration::new(seconds, nanos))?;
    (Instant::now() < deadline).then_some(deadline)
}

fn require_reference_age(
    wall: i128,
    reference: i128,
    error: i128,
    trust: &TimeTrustConfiguration,
) -> Option<i128> {
    let age = wall.checked_sub(reference)?;
    let maximum = i128::from(trust.maximum_measurement_age_ms.get()).checked_mul(1_000_000)?;
    if age < error.checked_neg()? || age.checked_add(error)? > maximum {
        return None;
    }
    maximum.checked_sub(age.checked_add(error)?)
}
