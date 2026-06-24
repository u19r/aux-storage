use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    io::Write as _,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use fdb_chaos_model::{SimulationRunMetadata, SimulationRunMetadataInput};

use crate::{
    aggregate::post_process_artifacts, artifact_io::write_json, cli::CliArgs,
    profile_integer_overrides,
};

pub(crate) fn run_simulation(args: CliArgs) -> Result<(), String> {
    let artifact_dir = artifact_dir(&args)?;
    fs::create_dir_all(&artifact_dir).map_err(|err| {
        format!(
            "failed to create artifact directory {}: {err}",
            artifact_dir.display()
        )
    })?;

    let copied_test_file = artifact_dir.join("simulation.toml");
    materialize_test_file(&args, &copied_test_file, &artifact_dir)?;

    let metadata = SimulationRunMetadata::new(SimulationRunMetadataInput {
        workload: args.workload.clone(),
        profile: args.profile.clone(),
        seed: args.seed,
        buggify: args.buggify.clone(),
        test_file: copied_test_file.display().to_string(),
        library_path: args.library_path.clone(),
        library_name: args.library_name.clone(),
        rerun_command: args.rerun_command(),
        options: BTreeMap::from([
            (
                "artifact_root".to_string(),
                args.artifact_root.display().to_string(),
            ),
            (
                "artifact_dir".to_string(),
                artifact_dir.display().to_string(),
            ),
            ("fdbserver".to_string(), args.fdbserver.clone()),
        ]),
    });
    write_json(&artifact_dir.join("run-metadata.json"), &metadata)?;
    fs::write(
        artifact_dir.join("rerun.sh"),
        format!("{}\n", metadata.rerun_command),
    )
    .map_err(|err| format!("failed to write rerun command: {err}"))?;
    println!("fdb chaos artifact: {}", artifact_dir.display());
    io::stdout()
        .flush()
        .map_err(|err| format!("failed to flush artifact path: {err}"))?;

    let output_path = artifact_dir.join("fdbserver-output.log");
    let output = fs::File::create(&output_path)
        .map_err(|err| format!("failed to create {}: {err}", output_path.display()))?;
    let output_err = output
        .try_clone()
        .map_err(|err| format!("failed to clone simulation log handle: {err}"))?;

    let traces_before = trace_files()?;
    let simfdb_existed_before = Path::new("simfdb").exists();
    let status = Command::new(&args.fdbserver)
        .arg("-r")
        .arg("simulation")
        .arg("-f")
        .arg(&copied_test_file)
        .arg("--seed")
        .arg(args.seed.to_string())
        .arg("--buggify")
        .arg(&args.buggify)
        .stdout(Stdio::from(output))
        .stderr(Stdio::from(output_err))
        .status()
        .map_err(|err| map_command_error(&args.fdbserver, err))?;

    write_metric_lines(&output_path, &artifact_dir.join("metrics.log"))?;
    copy_new_trace_files(&artifact_dir, &traces_before)?;
    cleanup_simulation_scratch(&traces_before, simfdb_existed_before)?;
    let post_process_result = post_process_artifacts(&args, &artifact_dir);

    if status.success() {
        post_process_result?;
        Ok(())
    } else {
        Err(format!(
            "simulation failed with status {status}; see {}",
            output_path.display()
        ))
    }
}
fn artifact_dir(args: &CliArgs) -> Result<PathBuf, String> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("system clock is before UNIX_EPOCH: {err}"))?
        .as_secs();
    Ok(args
        .artifact_root
        .join(&args.profile)
        .join(format!("{}-seed-{}-{timestamp}", args.workload, args.seed)))
}
fn trace_files() -> Result<BTreeSet<PathBuf>, String> {
    let entries = fs::read_dir(".").map_err(|err| format!("failed to list trace files: {err}"))?;
    let mut paths = BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(|err| format!("failed to inspect trace file: {err}"))?;
        let path = entry.path();
        if is_trace_xml(&path) {
            paths.insert(path);
        }
    }
    Ok(paths)
}

fn copy_new_trace_files(
    artifact_dir: &Path,
    traces_before: &BTreeSet<PathBuf>,
) -> Result<(), String> {
    let traces_after = trace_files()?;
    let trace_dir = artifact_dir.join("traces");
    for path in traces_after.difference(traces_before) {
        fs::create_dir_all(&trace_dir).map_err(|err| {
            format!(
                "failed to create trace artifact directory {}: {err}",
                trace_dir.display()
            )
        })?;
        let Some(file_name) = path.file_name() else {
            continue;
        };
        fs::copy(path, trace_dir.join(file_name)).map_err(|err| {
            format!(
                "failed to copy trace file {} into {}: {err}",
                path.display(),
                trace_dir.display()
            )
        })?;
    }
    Ok(())
}

fn cleanup_simulation_scratch(
    traces_before: &BTreeSet<PathBuf>,
    simfdb_existed_before: bool,
) -> Result<(), String> {
    let traces_after = trace_files()?;
    for path in traces_after.difference(traces_before) {
        fs::remove_file(path)
            .map_err(|err| format!("failed to remove scratch trace {}: {err}", path.display()))?;
    }
    if !simfdb_existed_before && Path::new("simfdb").exists() {
        fs::remove_dir_all("simfdb")
            .map_err(|err| format!("failed to remove simulator scratch directory: {err}"))?;
    }
    Ok(())
}

fn is_trace_xml(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with("trace.") && name.ends_with(".xml"))
}

fn write_metric_lines(output_path: &Path, metrics_path: &Path) -> Result<(), String> {
    let output = fs::read_to_string(output_path)
        .map_err(|err| format!("failed to read {}: {err}", output_path.display()))?;
    let metrics = output
        .lines()
        .filter(|line| line.starts_with("Metric "))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(metrics_path, format!("{metrics}\n"))
        .map_err(|err| format!("failed to write {}: {err}", metrics_path.display()))
}

fn materialize_test_file(
    args: &CliArgs,
    destination: &Path,
    artifact_dir: &Path,
) -> Result<(), String> {
    let raw = fs::read_to_string(&args.test_file)
        .map_err(|err| format!("failed to read {}: {err}", args.test_file.display()))?;
    let mut value = toml::from_str::<toml::Table>(&raw)
        .map_err(|err| format!("failed to parse {}: {err}", args.test_file.display()))?;
    let workload = first_workload_mut(&mut value)?;
    workload.insert(
        "workloadName".to_string(),
        toml::Value::String(args.workload.clone()),
    );
    workload.insert(
        "libraryPath".to_string(),
        toml::Value::String(args.library_path.clone()),
    );
    workload.insert(
        "libraryName".to_string(),
        toml::Value::String(args.library_name.clone()),
    );
    workload.insert(
        "profile".to_string(),
        toml::Value::String(args.profile.clone()),
    );
    workload.insert(
        "artifactRoot".to_string(),
        toml::Value::String(artifact_dir.display().to_string()),
    );
    apply_profile_overrides(args, workload);
    let rendered = toml::to_string_pretty(&value)
        .map_err(|err| format!("failed to render simulation TOML: {err}"))?;
    fs::write(destination, rendered)
        .map_err(|err| format!("failed to write {}: {err}", destination.display()))
}

fn apply_profile_overrides(args: &CliArgs, workload: &mut toml::map::Map<String, toml::Value>) {
    for (key, value) in profile_integer_overrides(&args.profile, &args.workload) {
        workload.insert(key.to_string(), toml::Value::Integer(value));
    }
}

fn first_workload_mut(
    value: &mut toml::Table,
) -> Result<&mut toml::map::Map<String, toml::Value>, String> {
    let tests = value
        .get_mut("test")
        .and_then(toml::Value::as_array_mut)
        .ok_or_else(|| "simulation file must contain [[test]]".to_string())?;
    let first_test = tests
        .first_mut()
        .and_then(toml::Value::as_table_mut)
        .ok_or_else(|| "simulation file must contain a test table".to_string())?;
    let workloads = first_test
        .get_mut("workload")
        .and_then(toml::Value::as_array_mut)
        .ok_or_else(|| "simulation file must contain [[test.workload]]".to_string())?;
    workloads
        .first_mut()
        .and_then(toml::Value::as_table_mut)
        .ok_or_else(|| "simulation file must contain a workload table".to_string())
}

fn map_command_error(command: &str, err: io::Error) -> String {
    if err.kind() == io::ErrorKind::NotFound {
        format!("{command} was not found; install FoundationDB server or pass --fdbserver")
    } else {
        format!("failed to run {command}: {err}")
    }
}
