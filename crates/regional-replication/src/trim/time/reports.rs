//! Complete supported monitoring schemas; report data never grants retention.
use std::net::IpAddr;

use replication_provider::runtime_configuration::time_trust::TimeTrustConfiguration;

use super::{
    authentication::{require_authentication, require_report_row},
    decimal::{
        parse_error, parse_frequency_error, parse_nonnegative, parse_nonnegative_error,
        parse_scaled, parse_seconds,
    },
};

pub(super) struct Contributor {
    pub(super) address: IpAddr,
    pub(super) mode: String,
    identity: Vec<String>,
    error: i128,
    age_ms: u64,
}

pub(super) struct Tracking {
    pub(super) address: IpAddr,
    pub(super) reference: i128,
    pub(super) error: i128,
    pub(super) frequency: i128,
    identity: Vec<String>,
}

pub(super) struct ReportSnapshot {
    pub(super) tracking: Tracking,
    pub(super) contributors: Vec<Contributor>,
    pub(super) authentication: String,
    pub(super) measurements: Vec<Vec<String>>,
}

impl Tracking {
    pub(super) fn parse(report: &str) -> Option<Self> {
        let row = parse_single_row(report, 14)?;
        let reference_id = u32::from_str_radix(row[0], 16).ok()?;
        if reference_id == 0
            || reference_id == 0x7f7f0101
            || row[13] != "Normal"
            || !(1..16).contains(&row[2].parse::<u8>().ok()?)
        {
            return None;
        }
        let address = row[1].parse::<IpAddr>().ok()?;
        let reference = parse_nonnegative(row[3])?;
        for value in &row[4..6] {
            parse_seconds(value)?;
        }
        parse_nonnegative(row[6])?;
        let correction = parse_error(row[4])?;
        parse_scaled(row[9], 3).filter(|skew| *skew >= 0)?;
        let frequency = parse_frequency_error(row[7])?
            .checked_add(parse_frequency_error(row[8])?)?
            .checked_add(parse_frequency_error(row[9])?)?;
        let delay = parse_nonnegative_error(row[10])?;
        let dispersion = parse_nonnegative_error(row[11])?;
        if parse_nonnegative(row[12])? == 0 {
            return None;
        }
        let error = correction
            .checked_add(dispersion)?
            .checked_add(delay.checked_add(1)?.checked_div(2)?)?;
        let identity = [0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 12, 13]
            .into_iter()
            .map(|index| row[index].to_owned())
            .collect();
        Some(Self {
            address,
            reference,
            error,
            frequency,
            identity,
        })
    }
}

pub(super) fn parse_contributors(
    report: &str,
    trust: &TimeTrustConfiguration,
) -> Option<Vec<Contributor>> {
    let mut contributors = Vec::new();
    for line in report.lines() {
        let row = parse_row(line, 10)?;
        if !matches!(row[0], "^" | "=" | "#")
            || !matches!(row[1], "*" | "+" | "-" | "?" | "x" | "~")
        {
            return None;
        }
        row[3].parse::<u8>().ok()?;
        row[4].parse::<i16>().ok()?;
        let reachable = u8::from_str_radix(row[5], 8).ok()?;
        for value in &row[7..9] {
            parse_seconds(value)?;
        }
        parse_nonnegative(row[9])?;
        if !matches!(row[1], "*" | "+") {
            continue;
        }
        let address = row[2].parse::<IpAddr>().ok()?;
        if row[0] == "#" || reachable == 0 || !(1..16).contains(&row[3].parse::<u8>().ok()?) {
            return None;
        }
        let age = row[6]
            .parse::<u64>()
            .ok()?
            .checked_add(1)?
            .checked_mul(1000)?;
        if age > trust.maximum_measurement_age_ms.get()
            || !trust.sources.iter().any(|source| source.address == address)
        {
            return None;
        }
        let identity = [0, 1, 2, 3, 4, 5, 8, 9]
            .into_iter()
            .map(|index| row[index].to_owned())
            .collect();
        let error = parse_error(row[7])?.checked_add(parse_error(row[9])?)?;
        contributors.push(Contributor {
            address,
            mode: row[0].to_owned(),
            identity,
            error,
            age_ms: age,
        });
    }
    contributors.sort_by_key(|source| source.address);
    if contributors.is_empty()
        || contributors
            .windows(2)
            .any(|pair| pair[0].address == pair[1].address)
        || contributors
            .iter()
            .filter(|source| source.identity[1] == "*")
            .count()
            != 1
    {
        return None;
    }
    Some(contributors)
}

impl ReportSnapshot {
    pub(super) fn require_sources(&self, trust: &TimeTrustConfiguration) -> Option<(i128, i128)> {
        let best = self
            .contributors
            .iter()
            .find(|source| source.identity[1] == "*")?;
        if best.address != self.tracking.address
            || self.measurements.len() != self.contributors.len()
        {
            return None;
        }
        let mut upstream = 0;
        let mut contributor_error = 0;
        for contributor in &self.contributors {
            let source = trust
                .sources
                .iter()
                .find(|source| source.address == contributor.address)?;
            let mut matching = self.authentication.lines().filter_map(|line| {
                let row = parse_row(line, 10)?;
                (row[0].parse::<IpAddr>().ok()? == source.address).then_some(row)
            });
            require_authentication(&matching.next()?, source)?;
            if matching.next().is_some() {
                return None;
            }
            let approved_error = i128::from(source.upstream_accuracy_ns.get());
            upstream = upstream.max(approved_error);
            contributor_error =
                contributor_error.max(contributor.error.checked_add(approved_error)?);
        }
        // Invalid unselected rows also reject a truncated or unsupported
        // complete report.
        for line in self.authentication.lines() {
            require_report_row(&parse_row(line, 10)?)?;
        }
        Some((upstream, contributor_error))
    }

    pub(super) fn is_same_measurement(&self, other: &Self) -> bool {
        self.tracking.identity == other.tracking.identity
            && self.measurements == other.measurements
            && self.contributors.len() == other.contributors.len()
            && self
                .contributors
                .iter()
                .zip(&other.contributors)
                .all(|(left, right)| left.identity == right.identity)
    }

    pub(super) fn maximum_source_age_ms(&self) -> Option<u64> {
        self.contributors.iter().map(|source| source.age_ms).max()
    }

    pub(super) fn contributor_binding(&self) -> Vec<(IpAddr, &str)> {
        self.contributors
            .iter()
            .map(|source| (source.address, source.identity[1].as_str()))
            .collect()
    }
}

pub(super) fn parse_measurement(report: &str, source: &Contributor) -> Option<Vec<String>> {
    let row = parse_single_row(report, 38)?;
    if row[0].parse::<IpAddr>().ok()? != source.address
        || row[2].parse::<u16>().ok()? == 0
        || row[5] != "Normal"
        || !(3..=4).contains(&row[6].parse::<u8>().ok()?)
        || !(1..16).contains(&row[8].parse::<u8>().ok()?)
    {
        return None;
    }
    if (source.mode == "^" && row[7] != "Server")
        || (source.mode == "=" && !matches!(row[7], "Symmetric active" | "Symmetric passive"))
    {
        return None;
    }
    for index in [1, 4, 15] {
        u32::from_str_radix(row[index], 16).ok()?;
    }
    row[3].parse::<IpAddr>().ok()?;
    row[9].parse::<i16>().ok()?;
    row[11].parse::<i16>().ok()?;
    for index in [10, 12, 13, 14, 17, 19, 20, 21] {
        parse_nonnegative(row[index])?;
    }
    for index in [18, 22] {
        parse_seconds(row[index])?;
    }
    if row[23..26] != ["111", "111", "1111"] || row[27] != "Yes" || !matches!(row[26], "Yes" | "No")
    {
        return None;
    }
    for value in &row[28..30] {
        if !matches!(*value, "Daemon" | "Kernel" | "Hardware") {
            return None;
        }
    }
    for value in &row[30..38] {
        value.parse::<u64>().ok()?;
    }
    let good = row[33].parse::<u64>().ok()?;
    let valid = row[32].parse::<u64>().ok()?;
    let received = row[31].parse::<u64>().ok()?;
    if good == 0 || good > valid || valid > received {
        return None;
    }
    let identity = [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 13, 14, 15, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
        32, 33,
    ];
    Some(
        identity
            .into_iter()
            .map(|index| row[index].to_owned())
            .collect(),
    )
}

fn parse_single_row(report: &str, fields: usize) -> Option<Vec<&str>> {
    let mut lines = report.lines();
    let row = parse_row(lines.next()?, fields)?;
    if lines.next().is_some() {
        return None;
    }
    Some(row)
}

fn parse_row(line: &str, fields: usize) -> Option<Vec<&str>> {
    if line.contains(['\0', '"', '\r']) {
        return None;
    }
    let row: Vec<_> = line.split(',').collect();
    (row.len() == fields).then_some(row)
}
