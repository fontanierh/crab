#![forbid(unsafe_code)]

use std::{ffi::OsString, path::PathBuf, process::ExitCode};

use crab_v2_runtime::ConfiguredRuntime;

const USAGE: &str = "usage: crab-v2 --config <runtime.json> --state-dir <directory>";

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = match parse_arguments(std::env::args_os().skip(1)) {
        Ok(Some(arguments)) => arguments,
        Ok(None) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(()) => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    let runtime = match ConfiguredRuntime::start_from_path(arguments.config, arguments.state).await
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("crab-v2: {error}");
            return ExitCode::from(2);
        }
    };
    match runtime.run_until_signal().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("crab-v2: {error}");
            ExitCode::FAILURE
        }
    }
}

struct Arguments {
    config: PathBuf,
    state: PathBuf,
}

fn parse_arguments(mut values: impl Iterator<Item = OsString>) -> Result<Option<Arguments>, ()> {
    let mut config = None;
    let mut state = None;
    while let Some(argument) = values.next() {
        match argument.to_str() {
            Some("--help" | "-h") if config.is_none() && state.is_none() => return Ok(None),
            Some("--config") if config.is_none() => config = values.next().map(PathBuf::from),
            Some("--state-dir") if state.is_none() => state = values.next().map(PathBuf::from),
            _ => return Err(()),
        }
    }
    match (config, state) {
        (Some(config), Some(state)) => Ok(Some(Arguments { config, state })),
        _ => Err(()),
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::parse_arguments;

    #[test]
    fn parser_requires_exact_explicit_paths() {
        let values = ["--state-dir", "state", "--config", "runtime.json"]
            .map(OsString::from)
            .into_iter();
        let arguments = parse_arguments(values)
            .expect("arguments parse")
            .expect("run requested");
        assert_eq!(arguments.state.to_string_lossy(), "state");
        assert_eq!(arguments.config.to_string_lossy(), "runtime.json");
        assert!(parse_arguments([OsString::from("--config")].into_iter()).is_err());
    }
}
