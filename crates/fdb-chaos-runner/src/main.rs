use std::{env, process::ExitCode};

fn main() -> ExitCode {
    match fdb_chaos_runner::run(env::args().skip(1)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("fdb-chaos-runner: {err}");
            ExitCode::FAILURE
        }
    }
}
