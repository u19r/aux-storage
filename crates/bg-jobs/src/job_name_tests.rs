use std::str::FromStr;

use crate::{
    BackgroundJobGroup, BackgroundJobName, DatabaseJobKind, ImmediateJobKind, PeriodicJobKind,
};

#[test]
fn background_job_names_round_trip_between_registered_names_and_strings() {
    for job in BackgroundJobName::all() {
        let parsed =
            BackgroundJobName::from_str(job.as_str()).expect("registered job should parse");

        assert_eq!(parsed, *job);
        assert_eq!(job.to_string(), job.as_str());
    }
}

#[test]
fn background_job_names_report_group_and_lock_requirement() {
    let database = BackgroundJobName::Database {
        kind: DatabaseJobKind::GsiBackfill,
    };
    let periodic = BackgroundJobName::Periodic {
        kind: PeriodicJobKind::Maintenance,
    };
    let immediate = BackgroundJobName::Immediate {
        kind: ImmediateJobKind::Task,
    };

    assert_eq!(database.group(), BackgroundJobGroup::Database);
    assert_eq!(periodic.group(), BackgroundJobGroup::Periodic);
    assert_eq!(immediate.group(), BackgroundJobGroup::Immediate);
    assert!(database.requires_database_lock());
    assert!(periodic.requires_database_lock());
    assert!(!immediate.requires_database_lock());
}

#[test]
fn background_job_name_parse_trims_known_names_and_rejects_unknown_jobs() {
    let parsed = BackgroundJobName::from_str(" ttl-sweep ").expect("job should parse");
    let error = BackgroundJobName::from_str("unknown").expect_err("unknown job should fail");

    assert_eq!(
        parsed,
        BackgroundJobName::Database {
            kind: DatabaseJobKind::TtlSweep
        }
    );
    assert_eq!(error.to_string(), "unsupported background job 'unknown'");
}
