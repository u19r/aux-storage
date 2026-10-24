//! The current measurement and daemon mechanism must both match operator trust.
use replication_provider::runtime_configuration::time_trust::{
    NtsAlgorithm, SourceAuthentication, SymmetricAlgorithm, TrustedTimeSource,
};

pub(super) fn require_authentication(row: &[&str], source: &TrustedTimeSource) -> Option<()> {
    require_report_row(row)?;
    if row[0].parse::<std::net::IpAddr>().ok()? != source.address {
        return None;
    }
    let key_id = row[2].parse::<u32>().ok()?;
    let algorithm = row[3].parse::<u16>().ok()?;
    let bits = row[4].parse::<u16>().ok()?;
    match &source.authentication {
        SourceAuthentication::Nts {
            algorithm: approved,
        } => {
            let expected = match approved {
                NtsAlgorithm::AesSivCmac256 => (15, 256),
                NtsAlgorithm::Aes128GcmSiv => (30, 128),
            };
            if row[1] != "NTS" || (algorithm, bits) != expected || row[8].parse::<u64>().ok()? == 0
            {
                return None;
            }
        }
        SourceAuthentication::SymmetricKey {
            key_id: expected,
            algorithm: approved,
        } => {
            let (expected_algorithm, minimum_bits) = require_symmetric_algorithm(*approved);
            if row[1] != "SK"
                || key_id != expected.get()
                || algorithm != expected_algorithm
                || bits < minimum_bits
            {
                return None;
            }
        }
    }
    Some(())
}

fn require_symmetric_algorithm(algorithm: SymmetricAlgorithm) -> (u16, u16) {
    match algorithm {
        SymmetricAlgorithm::Sha256 => (3, 256),
        SymmetricAlgorithm::Sha384 => (4, 384),
        SymmetricAlgorithm::Sha512 => (5, 512),
        SymmetricAlgorithm::Sha3_256 => (7, 256),
        SymmetricAlgorithm::Sha3_384 => (8, 384),
        SymmetricAlgorithm::Sha3_512 => (9, 512),
        SymmetricAlgorithm::Aes128 => (13, 128),
        SymmetricAlgorithm::Aes256 => (14, 256),
    }
}

pub(super) fn require_report_row(row: &[&str]) -> Option<()> {
    if row.len() != 10 || !matches!(row[1], "-" | "SK" | "NTS") {
        return None;
    }
    row[0].parse::<std::net::IpAddr>().ok()?;
    row[2].parse::<u32>().ok()?;
    row[3].parse::<u16>().ok()?;
    row[4].parse::<u16>().ok()?;
    for value in &row[5..] {
        value.parse::<u32>().ok()?;
    }
    Some(())
}
