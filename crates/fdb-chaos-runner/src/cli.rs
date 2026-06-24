use std::path::PathBuf;

pub(crate) const DEFAULT_ARTIFACT_ROOT: &str = "run-artifacts/fdb-chaos";
pub(crate) const DEFAULT_BUGGIFY: &str = "on";
pub(crate) const DEFAULT_FDBSERVER: &str = "fdbserver";
pub(crate) const DEFAULT_LIBRARY_NAME: &str = "aux_storage_fdb_chaos";
pub(crate) const DEFAULT_LIBRARY_PATH: &str = "target/release";
pub(crate) const DEFAULT_PROFILE: &str = "smoke";
pub(crate) const DEFAULT_WORKLOAD: &str = "noop";

#[derive(Debug)]
pub(crate) struct CliArgs {
    pub(crate) command: String,
    pub(crate) workload: String,
    pub(crate) profile: String,
    pub(crate) seed: u64,
    pub(crate) buggify: String,
    pub(crate) fdbserver: String,
    pub(crate) test_file: PathBuf,
    pub(crate) artifact: Option<PathBuf>,
    pub(crate) artifact_root: PathBuf,
    pub(crate) library_path: String,
    pub(crate) library_name: String,
}

impl CliArgs {
    pub(crate) fn parse<I>(mut args: I) -> Result<Self, String>
    where I: Iterator<Item = String> {
        let command = args.next().unwrap_or_else(|| "run".to_string());
        let mut parsed = Self {
            command,
            workload: DEFAULT_WORKLOAD.to_string(),
            profile: DEFAULT_PROFILE.to_string(),
            seed: 1,
            buggify: DEFAULT_BUGGIFY.to_string(),
            fdbserver: DEFAULT_FDBSERVER.to_string(),
            test_file: PathBuf::from("crates/fdb-chaos-workload/simulation/noop.toml"),
            artifact: None,
            artifact_root: PathBuf::from(DEFAULT_ARTIFACT_ROOT),
            library_path: DEFAULT_LIBRARY_PATH.to_string(),
            library_name: DEFAULT_LIBRARY_NAME.to_string(),
        };

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--workload" => parsed.workload = next_arg(&mut args, "--workload")?,
                "--profile" => parsed.profile = next_arg(&mut args, "--profile")?,
                "--seed" => parsed.seed = parse_seed(&next_arg(&mut args, "--seed")?)?,
                "--buggify" => parsed.buggify = next_arg(&mut args, "--buggify")?,
                "--fdbserver" => parsed.fdbserver = next_arg(&mut args, "--fdbserver")?,
                "--test-file" => {
                    parsed.test_file = PathBuf::from(next_arg(&mut args, "--test-file")?)
                }
                "--artifact" => {
                    parsed.artifact = Some(PathBuf::from(next_arg(&mut args, "--artifact")?));
                }
                "--artifact-root" => {
                    parsed.artifact_root = PathBuf::from(next_arg(&mut args, "--artifact-root")?);
                }
                "--library-path" => parsed.library_path = next_arg(&mut args, "--library-path")?,
                "--library-name" => parsed.library_name = next_arg(&mut args, "--library-name")?,
                "--help" | "-h" => return Err(help_text()),
                other => return Err(format!("unknown argument '{other}'\n{}", help_text())),
            }
        }

        Ok(parsed)
    }

    pub(crate) fn rerun_command(&self) -> String {
        format!(
            "cargo run -p fdb-chaos-runner -- run --workload {} --profile {} --seed {} --buggify \
             {} --test-file {} --artifact-root {} --library-path {} --library-name {}",
            shell_word(&self.workload),
            shell_word(&self.profile),
            self.seed,
            shell_word(&self.buggify),
            shell_word(&self.test_file.display().to_string()),
            shell_word(&self.artifact_root.display().to_string()),
            shell_word(&self.library_path),
            shell_word(&self.library_name),
        )
    }
}
pub(crate) fn next_arg<I>(args: &mut I, flag: &str) -> Result<String, String>
where I: Iterator<Item = String> {
    args.next()
        .ok_or_else(|| format!("{flag} requires a value"))
}

pub(crate) fn parse_seed(value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|err| format!("--seed must be an unsigned integer: {err}"))
}
pub(crate) fn shell_word(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-'))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

pub(crate) fn help_text() -> String {
    "usage: fdb-chaos-runner run [--workload noop] [--profile smoke] [--seed 1] [--buggify on] \
     [--fdbserver fdbserver] [--test-file path] [--artifact-root path] [--library-path path] \
     [--library-name name]"
        .to_string()
}
