mod aggregate;
mod artifact_io;
mod cli;
mod minimize;
mod profiles;
mod simulation;

#[cfg(test)]
mod profile_tests;

use cli::CliArgs;
use minimize::minimize_history_command;
pub use profiles::profile_integer_overrides;
use simulation::run_simulation;

pub fn run<I>(args: I) -> Result<(), String>
where I: Iterator<Item = String> {
    let args = CliArgs::parse(args)?;
    match args.command.as_str() {
        "run" => run_simulation(args),
        "minimize-history" => minimize_history_command(args),
        "print-rerun" => {
            println!("{}", args.rerun_command());
            Ok(())
        }
        other => Err(format!(
            "unknown command '{other}'; expected 'run', 'minimize-history', or 'print-rerun'"
        )),
    }
}
