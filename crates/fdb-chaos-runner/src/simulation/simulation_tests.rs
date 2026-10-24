use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    cli::CliArgs,
    simulation::{
        cleanup_simulation_scratch, copy_new_trace_files, first_workload_mut,
        materialize_test_file, resolve_simulator_command, run_simulation, trace_files,
    },
};

#[test]
fn given_long_paths_when_simulator_runs_then_short_options_preserve_exact_filesystem_targets() {
    let parent_cwd = std::env::current_dir().unwrap();
    let root = PathBuf::from("run-artifacts/fdb-chaos-runner-tests").join(format!(
        "simulation 'quoted' \"double\" \\ directory-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&root).unwrap();
    let library = root.join("long-library-segment/".repeat(12));
    fs::create_dir_all(&library).unwrap();
    fs::write(library.join("library-marker"), "exact library").unwrap();
    let executable = root.join("simulator 'stub'");
    fs::write(
        &executable,
        r#"#!/bin/sh
set -eu
test "$(pwd -P)" = "$(dirname "$4")" || exit 91
printf '%s\n' "$@" > subprocess-arguments.txt
pwd -P > subprocess-directory.txt
mkdir simfdb
printf 'scratch' > simfdb/scratch
printf 'trace evidence' > trace.runner-test.xml
printf 'Metric stub 1\nstdout evidence\n'
printf 'stderr evidence\n' >&2
"#,
    )
    .unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
    let mut args = CliArgs::parse(std::iter::empty()).unwrap();
    args.test_file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../fdb-chaos-workload/simulation/noop.toml");
    args.fdbserver = executable.display().to_string();
    args.library_path = library.display().to_string();
    args.artifact_root = root.join("long-artifact-segment/".repeat(12));
    let profile_dir = args.artifact_root.join(&args.profile);

    let result = run_simulation(args);
    assert_eq!(std::env::current_dir().unwrap(), parent_cwd);
    result.expect("simulator must run inside its canonical artifact directory");
    let artifact_dir = fs::read_dir(&profile_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let artifact_dir = fs::canonicalize(artifact_dir).unwrap();
    let destination = artifact_dir.join("simulation.toml");
    let rendered = fs::read_to_string(&destination).unwrap();
    let mut parsed = toml::from_str::<toml::Table>(&rendered).unwrap();
    let workload = first_workload_mut(&mut parsed).unwrap();
    for (key, expected) in [("artifactRoot", "."), ("libraryPath", "workload-library")] {
        assert_eq!(workload[key].as_str(), Some(expected));
        let line = rendered
            .lines()
            .find(|line| line.starts_with(&format!("{key} = ")))
            .expect("path value must have a single-line assignment");
        assert_eq!(line, format!("{key} = \"{expected}\""));
    }
    assert_eq!(
        fs::canonicalize(artifact_dir.join("workload-library")).unwrap(),
        fs::canonicalize(&library).unwrap()
    );
    assert_eq!(
        fs::read_to_string(artifact_dir.join("workload-library/library-marker")).unwrap(),
        "exact library"
    );
    assert_eq!(
        fs::read_to_string(artifact_dir.join("subprocess-directory.txt"))
            .unwrap()
            .trim_end(),
        artifact_dir.to_str().unwrap()
    );
    let arguments = fs::read_to_string(artifact_dir.join("subprocess-arguments.txt")).unwrap();
    assert_eq!(arguments.lines().nth(3), destination.to_str());
    let output = fs::read_to_string(artifact_dir.join("fdbserver-output.log")).unwrap();
    assert!(output.contains("stdout evidence") && output.contains("stderr evidence"));
    assert_eq!(
        fs::read_to_string(artifact_dir.join("metrics.log")).unwrap(),
        "Metric stub 1\n"
    );
    assert_eq!(
        fs::read_to_string(artifact_dir.join("traces/trace.runner-test.xml")).unwrap(),
        "trace evidence"
    );
    assert!(!artifact_dir.join("trace.runner-test.xml").exists());
    assert!(!artifact_dir.join("simfdb").exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn given_existing_scratch_when_collecting_run_artifacts_then_only_new_traces_are_removed() {
    let root = std::env::temp_dir().join(format!(
        "fdb-chaos-scratch-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(root.join("simfdb")).unwrap();
    fs::write(root.join("simfdb/existing"), "preserved scratch").unwrap();
    fs::write(root.join("trace.existing.xml"), "preserved trace").unwrap();
    let traces_before = trace_files(&root).unwrap();
    fs::write(root.join("trace.new.xml"), "new trace").unwrap();

    copy_new_trace_files(&root, &traces_before).unwrap();
    cleanup_simulation_scratch(&root, &traces_before, true).unwrap();

    assert_eq!(
        fs::read_to_string(root.join("simfdb/existing")).unwrap(),
        "preserved scratch"
    );
    assert_eq!(
        fs::read_to_string(root.join("trace.existing.xml")).unwrap(),
        "preserved trace"
    );
    assert_eq!(
        fs::read_to_string(root.join("traces/trace.new.xml")).unwrap(),
        "new trace"
    );
    assert!(!root.join("trace.new.xml").exists());
    assert!(!root.join("traces/trace.existing.xml").exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn given_bare_simulator_command_when_resolving_then_path_lookup_is_preserved() {
    assert_eq!(
        resolve_simulator_command("fdbserver").unwrap(),
        PathBuf::from("fdbserver")
    );
}

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

    materialize_test_file(&args, &destination).expect("materialize simulator workload");
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

    materialize_test_file(&args, &destination).expect("materialize simulator workload");
    let rendered = fs::read_to_string(&destination).expect("read materialized workload");
    let mut parsed = toml::from_str::<toml::Table>(&rendered).expect("parse workload");
    let workload = first_workload_mut(&mut parsed).expect("workload table");
    assert!(!workload.contains_key("readSequenceSeed"));
    assert!(!workload.contains_key("readSequenceBuggify"));

    fs::remove_dir_all(root).expect("remove materialization test directory");
}
