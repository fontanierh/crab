#![forbid(unsafe_code)]

use std::{ffi::OsString, path::PathBuf, process::ExitCode};

use crab_v2_runtime::{AcpChannelOptions, read_bootstrap_prompt_file, run_acp_channel_stdio};

const USAGE: &str = "usage: crab-v2-acp-channel --state-dir <directory> --agent <id> [--adapter <id>] [--bootstrap-file <path>]";

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
    let bootstrap_prompt = match arguments.bootstrap_file {
        Some(path) => match read_bootstrap_prompt_file(path) {
            Ok(prompt) => Some(prompt),
            Err(_) => {
                eprintln!("crab-v2-acp-channel: bootstrap file is unavailable");
                return ExitCode::from(2);
            }
        },
        None => None,
    };
    let options = AcpChannelOptions::new(arguments.state, arguments.agent)
        .adapter_id(arguments.adapter)
        .bootstrap_prompt(bootstrap_prompt);
    match run_acp_channel_stdio(options).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("crab-v2-acp-channel: {error}");
            ExitCode::FAILURE
        }
    }
}

struct Arguments {
    state: PathBuf,
    agent: String,
    adapter: String,
    bootstrap_file: Option<PathBuf>,
}

fn parse_arguments(mut values: impl Iterator<Item = OsString>) -> Result<Option<Arguments>, ()> {
    let mut state = None;
    let mut agent = None;
    let mut adapter = None;
    let mut bootstrap_file = None;
    while let Some(argument) = values.next() {
        match argument.to_str() {
            Some("--help" | "-h")
                if state.is_none()
                    && agent.is_none()
                    && adapter.is_none()
                    && bootstrap_file.is_none() =>
            {
                return Ok(None);
            }
            Some("--state-dir") if state.is_none() => state = values.next().map(PathBuf::from),
            Some("--agent") if agent.is_none() => {
                agent = values.next().and_then(|value| value.into_string().ok())
            }
            Some("--adapter") if adapter.is_none() => {
                adapter = values.next().and_then(|value| value.into_string().ok())
            }
            Some("--bootstrap-file") if bootstrap_file.is_none() => {
                bootstrap_file = values.next().map(PathBuf::from)
            }
            _ => return Err(()),
        }
    }
    match (state, agent) {
        (Some(state), Some(agent)) if !agent.trim().is_empty() => {
            let adapter = adapter.unwrap_or_else(|| "t3code".into());
            if adapter.trim().is_empty() {
                return Err(());
            }
            Ok(Some(Arguments {
                state,
                agent,
                adapter,
                bootstrap_file,
            }))
        }
        _ => Err(()),
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::parse_arguments;

    #[test]
    fn parser_accepts_only_explicit_unique_arguments() {
        let arguments = parse_arguments(
            [
                "--agent",
                "claude",
                "--state-dir",
                "/tmp/crab",
                "--adapter",
                "t3code",
                "--bootstrap-file",
                "prompt.md",
            ]
            .map(OsString::from)
            .into_iter(),
        )
        .expect("arguments parse")
        .expect("run requested");
        assert_eq!(arguments.agent, "claude");
        assert_eq!(arguments.adapter, "t3code");
        assert!(arguments.bootstrap_file.is_some());
        assert!(
            parse_arguments(
                [
                    "--agent",
                    "claude",
                    "--agent",
                    "other",
                    "--state-dir",
                    "state"
                ]
                .map(OsString::from)
                .into_iter()
            )
            .is_err()
        );
        assert!(parse_arguments([OsString::from("--state-dir")].into_iter()).is_err());
    }
}
