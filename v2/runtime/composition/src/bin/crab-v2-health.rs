#![forbid(unsafe_code)]

use std::{ffi::OsString, path::PathBuf, process::ExitCode};

use crab_v2_runtime::{ChannelIpcClient, RuntimeConfig, inspect_runtime_health};

const USAGE: &str =
    "usage: crab-v2-health --config <absolute-runtime-json> --state-dir <absolute-directory>";

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
    let config = match RuntimeConfig::load(&arguments.config) {
        Ok(config) => config,
        Err(error) => return failure(error),
    };
    let client = match ChannelIpcClient::from_state_directory(&arguments.state_directory) {
        Ok(client) => client,
        Err(error) => return failure(error),
    };
    let report = match inspect_runtime_health(&client, &config).await {
        Ok(report) => report,
        Err(error) => return failure(error),
    };
    match serde_json::to_string(&report) {
        Ok(value) => {
            println!("{value}");
            ExitCode::SUCCESS
        }
        Err(_) => failure("health output could not be encoded"),
    }
}

fn failure(error: impl std::fmt::Display) -> ExitCode {
    eprintln!("crab-v2-health: {error}");
    ExitCode::FAILURE
}

#[derive(Debug, PartialEq)]
struct Arguments {
    config: PathBuf,
    state_directory: PathBuf,
}

fn parse_arguments(values: impl IntoIterator<Item = OsString>) -> Result<Option<Arguments>, ()> {
    let mut values = values.into_iter();
    let Some(first) = values.next() else {
        return Ok(None);
    };
    if first == "--help" || first == "-h" {
        return values.next().is_none().then_some(None).ok_or(());
    }
    if first != "--config" {
        return Err(());
    }
    let config = absolute(values.next().ok_or(())?)?;
    if values.next().ok_or(())? != "--state-dir" {
        return Err(());
    }
    let state_directory = absolute(values.next().ok_or(())?)?;
    if values.next().is_some() {
        return Err(());
    }
    Ok(Some(Arguments {
        config,
        state_directory,
    }))
}

fn absolute(value: OsString) -> Result<PathBuf, ()> {
    let path = PathBuf::from(value);
    (path.is_absolute() && !path.as_os_str().is_empty())
        .then_some(path)
        .ok_or(())
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::{Arguments, parse_arguments};

    #[test]
    fn parser_requires_exact_absolute_owner_paths() {
        let parsed = parse_arguments(
            [
                "--config",
                "/private/runtime.json",
                "--state-dir",
                "/private/state",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .expect("arguments parse")
        .expect("health requested");
        assert_eq!(
            parsed,
            Arguments {
                config: "/private/runtime.json".into(),
                state_directory: "/private/state".into(),
            }
        );
        for invalid in [
            vec!["--config", "relative.json", "--state-dir", "/private/state"],
            vec![
                "--config",
                "/private/runtime.json",
                "--state-dir",
                "relative",
            ],
            vec!["--state-dir", "/private/state"],
        ] {
            assert!(parse_arguments(invalid.into_iter().map(OsString::from)).is_err());
        }
    }
}
