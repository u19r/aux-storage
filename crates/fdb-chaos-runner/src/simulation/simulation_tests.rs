use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use super::{first_workload_mut, materialize_test_file};
use crate::cli::CliArgs;

#[test]
fn materialization_keeps_runner_controls_out_of_reserved_simulation_options() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../run-artifacts/fdb-chaos-runner-tests")
        .join(format!(
            "materialization-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos()
        ));
    fs::create_dir_all(&root).expect("create materialization test directory");
    let destination = root.join("simulation.toml");
    let artifact_dir = root.join("artifact");
    let args = CliArgs {
        command: "run".to_string(),
        workload: "read_sequence_dag".to_string(),
        profile: "smoke".to_string(),
        seed: 2,
        buggify: "off".to_string(),
        fdbserver: "/usr/local/libexec/fdbserver".to_string(),
        test_file: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../fdb-chaos-workload/simulation/read_sequence_dag.toml"),
        artifact: None,
        artifact_root: PathBuf::from("run-artifacts/fdb-chaos"),
        library_path: "target/release".to_string(),
        library_name: "aux_storage_fdb_chaos".to_string(),
    };

    materialize_test_file(&args, &destination, &artifact_dir)
        .expect("materialize simulator workload");
    let rendered = fs::read_to_string(&destination).expect("read materialized workload");
    let mut parsed = toml::from_str::<toml::Table>(&rendered).expect("parse workload");
    let workload = first_workload_mut(&mut parsed).expect("workload table");
    assert_eq!(
        workload.get("readSequenceSeed"),
        Some(&toml::Value::Integer(2))
    );
    assert_eq!(
        workload.get("readSequenceBuggify"),
        Some(&toml::Value::String("off".to_string()))
    );
    assert!(!workload.contains_key("seed"));
    assert!(!workload.contains_key("buggify"));

    fs::remove_dir_all(root).expect("remove materialization test directory");
}

#[test]
fn materialization_does_not_add_read_sequence_options_to_other_workloads() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../run-artifacts/fdb-chaos-runner-tests")
        .join(format!(
            "noop-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos()
        ));
    fs::create_dir_all(&root).expect("create materialization test directory");
    let destination = root.join("simulation.toml");
    let artifact_dir = root.join("artifact");
    let args = CliArgs {
        command: "run".to_string(),
        workload: "noop".to_string(),
        profile: "smoke".to_string(),
        seed: 7,
        buggify: "off".to_string(),
        fdbserver: "/usr/local/libexec/fdbserver".to_string(),
        test_file: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../fdb-chaos-workload/simulation/noop.toml"),
        artifact: None,
        artifact_root: PathBuf::from("run-artifacts/fdb-chaos"),
        library_path: "target/release".to_string(),
        library_name: "aux_storage_fdb_chaos".to_string(),
    };

    materialize_test_file(&args, &destination, &artifact_dir)
        .expect("materialize simulator workload");
    let rendered = fs::read_to_string(&destination).expect("read materialized workload");
    let mut parsed = toml::from_str::<toml::Table>(&rendered).expect("parse workload");
    let workload = first_workload_mut(&mut parsed).expect("workload table");
    assert!(!workload.contains_key("readSequenceSeed"));
    assert!(!workload.contains_key("readSequenceBuggify"));

    fs::remove_dir_all(root).expect("remove materialization test directory");
}
