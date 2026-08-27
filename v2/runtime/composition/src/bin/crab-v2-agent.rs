#![forbid(unsafe_code)]

use std::{ffi::OsString, path::PathBuf, process::ExitCode};

use agent_host_contract::{
    AgentDiagnosticKind, AgentDiagnosticPage, AgentLifecycle, AgentSessionCatalog,
    ListAgentSessionsRequest, ReadAgentDiagnosticsRequest, SessionReference, SessionStatus,
};
use crab_v2_runtime::ChannelIpcClient;
use serde_json::{Value, json};

const USAGE: &str = "usage: crab-v2-agent --state-dir <directory> <list <limit> | status <session> | diagnostics <session> <after-sequence> <limit>>";
const MAX_DIAGNOSTIC_PAGE: u64 = 256;
const MAX_SESSION_CATALOG: u64 = 256;

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
    let client = match ChannelIpcClient::from_state_directory(&arguments.state_directory) {
        Ok(client) => client,
        Err(error) => return failure(error),
    };
    let result = match arguments.command {
        AgentCommand::List { limit } => client
            .list_agent_sessions(ListAgentSessionsRequest { limit })
            .await
            .map(catalog_json),
        AgentCommand::Status { session_id } => client
            .agent_session_status(SessionReference { session_id })
            .await
            .map(status_json),
        AgentCommand::Diagnostics {
            session_id,
            after_sequence,
            limit,
        } => client
            .read_agent_diagnostics(ReadAgentDiagnosticsRequest {
                session_id,
                after_sequence,
                limit,
            })
            .await
            .map(diagnostics_json),
    };
    match result {
        Ok(value) => match serde_json::to_string(&value) {
            Ok(value) => {
                println!("{value}");
                ExitCode::SUCCESS
            }
            Err(_) => failure("output could not be encoded"),
        },
        Err(error) => failure(error),
    }
}

fn failure(error: impl std::fmt::Display) -> ExitCode {
    eprintln!("crab-v2-agent: {error}");
    ExitCode::FAILURE
}

#[derive(Debug, PartialEq)]
struct Arguments {
    state_directory: PathBuf,
    command: AgentCommand,
}

#[derive(Debug, PartialEq)]
enum AgentCommand {
    List {
        limit: u64,
    },
    Status {
        session_id: String,
    },
    Diagnostics {
        session_id: String,
        after_sequence: u64,
        limit: u64,
    },
}

fn parse_arguments(values: impl IntoIterator<Item = OsString>) -> Result<Option<Arguments>, ()> {
    let mut values = values.into_iter();
    let Some(first) = values.next() else {
        return Ok(None);
    };
    if first == "--help" || first == "-h" {
        return values.next().is_none().then_some(None).ok_or(());
    }
    if first != "--state-dir" {
        return Err(());
    }
    let state_directory = PathBuf::from(values.next().ok_or(())?);
    if !state_directory.is_absolute() {
        return Err(());
    }
    let operation = text(values.next().ok_or(())?)?;
    let command = match operation.as_str() {
        "list" => {
            let limit = integer(values.next().ok_or(())?)?;
            if limit == 0 || limit > MAX_SESSION_CATALOG {
                return Err(());
            }
            AgentCommand::List { limit }
        }
        "status" => AgentCommand::Status {
            session_id: identifier(values.next().ok_or(())?)?,
        },
        "diagnostics" => {
            let session_id = identifier(values.next().ok_or(())?)?;
            let after_sequence = integer(values.next().ok_or(())?)?;
            let limit = integer(values.next().ok_or(())?)?;
            if limit == 0 || limit > MAX_DIAGNOSTIC_PAGE {
                return Err(());
            }
            AgentCommand::Diagnostics {
                session_id,
                after_sequence,
                limit,
            }
        }
        _ => return Err(()),
    };
    if values.next().is_some() {
        return Err(());
    }
    Ok(Some(Arguments {
        state_directory,
        command,
    }))
}

fn identifier(value: OsString) -> Result<String, ()> {
    let value = text(value)?;
    (!value.trim().is_empty()).then_some(value).ok_or(())
}

fn integer(value: OsString) -> Result<u64, ()> {
    text(value)?.parse().map_err(|_| ())
}

fn text(value: OsString) -> Result<String, ()> {
    value.into_string().map_err(|_| ())
}

fn status_json(status: SessionStatus) -> Value {
    json!({
        "sessionId": status.session_id,
        "lifecycle": lifecycle_name(&status.lifecycle),
        "lastEventSequence": status.last_sequence,
        "activeRunId": status.active_run_id,
    })
}

fn catalog_json(catalog: AgentSessionCatalog) -> Value {
    json!({
        "sessions": catalog.sessions.into_iter().map(|session| json!({
            "sessionId": session.session_id,
            "nativeSessionId": session.native_session_id,
            "agentId": session.agent_id,
            "workingDirectory": session.working_directory,
            "lifecycle": lifecycle_name(&session.lifecycle),
            "lastEventSequence": session.last_event_sequence,
            "lastDiagnosticSequence": session.last_diagnostic_sequence,
            "activeRunId": session.active_run_id,
            "updatedAtMs": session.updated_at_ms,
        })).collect::<Vec<_>>(),
        "totalSessions": catalog.total_sessions,
    })
}

fn diagnostics_json(page: AgentDiagnosticPage) -> Value {
    json!({
        "diagnostics": page.diagnostics.into_iter().map(|diagnostic| json!({
            "sessionId": diagnostic.session_id,
            "sequence": diagnostic.sequence,
            "observedAtMs": diagnostic.observed_at_ms,
            "kind": diagnostic_kind_name(&diagnostic.kind),
            "message": diagnostic.message,
        })).collect::<Vec<_>>(),
        "nextSequence": page.next_sequence,
        "caughtUp": page.caught_up,
        "oldestRetainedSequence": page.oldest_retained_sequence,
    })
}

fn lifecycle_name(value: &AgentLifecycle) -> &'static str {
    match value {
        AgentLifecycle::Discovered => "discovered",
        AgentLifecycle::Starting => "starting",
        AgentLifecycle::Ready => "ready",
        AgentLifecycle::Busy => "busy",
        AgentLifecycle::Detaching => "detaching",
        AgentLifecycle::Detached => "detached",
        AgentLifecycle::Stopping => "stopping",
        AgentLifecycle::Stopped => "stopped",
        AgentLifecycle::Failed => "failed",
        AgentLifecycle::Unknown { .. } => "unknown",
    }
}

fn diagnostic_kind_name(value: &AgentDiagnosticKind) -> &'static str {
    match value {
        AgentDiagnosticKind::AdapterStderr => "adapter-stderr",
        AgentDiagnosticKind::ActorFailure => "actor-failure",
        AgentDiagnosticKind::RestartInterruption => "restart-interruption",
        AgentDiagnosticKind::Unknown { .. } => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use agent_host_contract::{
        AgentDiagnostic, AgentDiagnosticKind, AgentDiagnosticPage, AgentLifecycle,
        AgentSessionCatalog, AgentSessionSummary,
    };

    use super::{AgentCommand, catalog_json, diagnostics_json, parse_arguments};

    #[test]
    fn parser_accepts_only_bounded_owner_operations() {
        let arguments = parse_arguments(
            [
                "--state-dir",
                "/tmp/crab-v2",
                "diagnostics",
                "session-1",
                "7",
                "100",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .expect("arguments parse")
        .expect("operation requested");
        assert_eq!(
            arguments.command,
            AgentCommand::Diagnostics {
                session_id: "session-1".into(),
                after_sequence: 7,
                limit: 100,
            }
        );
        let list = parse_arguments(
            ["--state-dir", "/tmp/crab-v2", "list", "25"]
                .into_iter()
                .map(OsString::from),
        )
        .expect("list arguments parse")
        .expect("list requested");
        assert_eq!(list.command, AgentCommand::List { limit: 25 });
        for invalid in [
            vec!["--state-dir", "relative", "status", "session-1"],
            vec!["--state-dir", "/tmp/crab-v2", "list", "257"],
            vec![
                "--state-dir",
                "/tmp/crab-v2",
                "diagnostics",
                "session-1",
                "0",
                "0",
            ],
            vec![
                "--state-dir",
                "/tmp/crab-v2",
                "diagnostics",
                "session-1",
                "0",
                "257",
            ],
        ] {
            assert!(parse_arguments(invalid.into_iter().map(OsString::from)).is_err());
        }
    }

    #[test]
    fn diagnostic_output_preserves_cursor_and_raw_private_message() {
        let output = diagnostics_json(AgentDiagnosticPage {
            diagnostics: vec![AgentDiagnostic {
                session_id: "session-1".into(),
                sequence: 9,
                observed_at_ms: 42,
                kind: AgentDiagnosticKind::AdapterStderr,
                message: "adapter detail".into(),
            }],
            next_sequence: 9,
            caught_up: true,
            oldest_retained_sequence: 9,
        });
        assert_eq!(output["diagnostics"][0]["kind"], "adapter-stderr");
        assert_eq!(output["diagnostics"][0]["message"], "adapter detail");
        assert_eq!(output["oldestRetainedSequence"], 9);
    }

    #[test]
    fn catalog_output_identifies_the_session_and_both_journal_cursors() {
        let output = catalog_json(AgentSessionCatalog {
            sessions: vec![AgentSessionSummary {
                session_id: "session-1".into(),
                native_session_id: Some("native-1".into()),
                agent_id: "claude".into(),
                working_directory: "/workspace".into(),
                lifecycle: AgentLifecycle::Failed,
                last_event_sequence: 12,
                last_diagnostic_sequence: 3,
                active_run_id: None,
                updated_at_ms: 42,
            }],
            total_sessions: 1,
        });
        assert_eq!(output["sessions"][0]["sessionId"], "session-1");
        assert_eq!(output["sessions"][0]["lastEventSequence"], 12);
        assert_eq!(output["sessions"][0]["lastDiagnosticSequence"], 3);
        assert_eq!(output["totalSessions"], 1);
    }
}
