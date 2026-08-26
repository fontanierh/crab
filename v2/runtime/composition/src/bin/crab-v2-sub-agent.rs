#![forbid(unsafe_code)]

use std::{ffi::OsString, io::Read as _, path::PathBuf, process::ExitCode};

use crab_v2_runtime::ChannelIpcClient;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sub_agent_host_contract::{
    ContextRealization, InputDisposition, InteractionReceipt, ReadSubAgentEventsRequest,
    SendToChildRequest, SendToParentRequest, SpawnSubAgentRequest, StopSubAgentRequest,
    SubAgentContextMode, SubAgentEventKind, SubAgentEventPage, SubAgentInputMode,
    SubAgentLifecycle, SubAgentReceipt, SubAgentRecord, SubAgentReference, SubAgentStatus,
};

const USAGE: &str = "usage: crab-v2-sub-agent --state-dir <directory> <spawn <client-id> <parent-session> <agent> <working-directory> <fresh|inherit> <none|through-sequence> <false|true> stdin | send-child <sub-agent> <message-id> <queue|steer|interrupt-and-steer> stdin | send-parent <sub-agent> <message-id> <queue|steer|interrupt-and-steer> stdin | events <sub-agent> <after-sequence> <limit> | status <sub-agent> | stop <sub-agent> <reason>>";
const MAX_STDIN_BYTES: u64 = 2 * 1024 * 1024;
const DEFAULT_CRASH_RESTART_LIMIT: u64 = 1;

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
    match execute(&client, arguments.command).await {
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
    eprintln!("crab-v2-sub-agent: {error}");
    ExitCode::FAILURE
}

async fn execute(
    client: &ChannelIpcClient,
    command: SubAgentCommand,
) -> Result<Value, SubAgentCliError> {
    match command {
        SubAgentCommand::Spawn {
            client_sub_agent_id,
            parent_session_id,
            agent_id,
            working_directory,
            context_mode,
            parent_context_through_sequence,
            allow_portable_snapshot,
        } => {
            let payload = spawn_payload(&read_stdin()?)?;
            client
                .spawn_sub_agent(SpawnSubAgentRequest {
                    client_sub_agent_id,
                    parent_session_id,
                    agent_id,
                    working_directory: working_directory.to_string_lossy().into_owned(),
                    context_mode,
                    parent_context_through_sequence,
                    allow_portable_snapshot,
                    native_task_prompt_json: payload.native_task_prompt_json,
                    metadata_json: payload.metadata_json,
                    crash_restart_limit: payload.crash_restart_limit,
                })
                .await
                .map(record_json)
                .map_err(Into::into)
        }
        SubAgentCommand::SendChild {
            sub_agent_id,
            client_message_id,
            mode,
        } => {
            let prompt = native_prompt(&read_stdin()?)?;
            client
                .send_to_child(SendToChildRequest {
                    sub_agent_id,
                    client_message_id,
                    mode,
                    native_prompt_json: prompt,
                })
                .await
                .map(interaction_json)
                .map_err(Into::into)
        }
        SubAgentCommand::SendParent {
            sub_agent_id,
            client_message_id,
            mode,
        } => {
            let message_json = structured_message(&read_stdin()?)?;
            client
                .send_to_parent(SendToParentRequest {
                    sub_agent_id,
                    client_message_id,
                    mode,
                    message_json,
                })
                .await
                .map(interaction_json)
                .map_err(Into::into)
        }
        SubAgentCommand::Events {
            sub_agent_id,
            after_sequence,
            limit,
        } => client
            .read_sub_agent_events(ReadSubAgentEventsRequest {
                sub_agent_id,
                after_sequence,
                limit,
            })
            .await
            .map(events_json)
            .map_err(Into::into),
        SubAgentCommand::Status { sub_agent_id } => client
            .sub_agent_status(SubAgentReference { sub_agent_id })
            .await
            .map(status_json)
            .map_err(Into::into),
        SubAgentCommand::Stop {
            sub_agent_id,
            reason,
        } => client
            .stop_sub_agent(StopSubAgentRequest {
                sub_agent_id,
                reason,
            })
            .await
            .map(receipt_json)
            .map_err(Into::into),
    }
}

#[derive(Debug)]
enum SubAgentCliError {
    Client(crab_v2_runtime::ChannelIpcClientError),
    InvalidStdin,
}

impl std::fmt::Display for SubAgentCliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Client(error) => error.fmt(formatter),
            Self::InvalidStdin => formatter.write_str(
                "stdin must contain one bounded JSON value of the required command shape",
            ),
        }
    }
}

impl From<crab_v2_runtime::ChannelIpcClientError> for SubAgentCliError {
    fn from(error: crab_v2_runtime::ChannelIpcClientError) -> Self {
        Self::Client(error)
    }
}

struct Arguments {
    state_directory: PathBuf,
    command: SubAgentCommand,
}

#[derive(Debug, PartialEq)]
enum SubAgentCommand {
    Spawn {
        client_sub_agent_id: String,
        parent_session_id: String,
        agent_id: String,
        working_directory: PathBuf,
        context_mode: SubAgentContextMode,
        parent_context_through_sequence: Option<u64>,
        allow_portable_snapshot: bool,
    },
    SendChild {
        sub_agent_id: String,
        client_message_id: String,
        mode: SubAgentInputMode,
    },
    SendParent {
        sub_agent_id: String,
        client_message_id: String,
        mode: SubAgentInputMode,
    },
    Events {
        sub_agent_id: String,
        after_sequence: u64,
        limit: u64,
    },
    Status {
        sub_agent_id: String,
    },
    Stop {
        sub_agent_id: String,
        reason: String,
    },
}

fn parse_arguments(mut values: impl Iterator<Item = OsString>) -> Result<Option<Arguments>, ()> {
    let first = next_text(&mut values).ok_or(())?;
    if first == "--help" || first == "-h" {
        return if values.next().is_none() {
            Ok(None)
        } else {
            Err(())
        };
    }
    if first != "--state-dir" {
        return Err(());
    }
    let state_directory = values.next().map(PathBuf::from).ok_or(())?;
    if state_directory.as_os_str().is_empty() {
        return Err(());
    }
    let command = match required_text(&mut values)?.as_str() {
        "spawn" => {
            let client_sub_agent_id = required_text(&mut values)?;
            let parent_session_id = required_text(&mut values)?;
            let agent_id = required_text(&mut values)?;
            let working_directory = PathBuf::from(required_text(&mut values)?);
            let context_mode = parse_context(&required_text(&mut values)?).ok_or(())?;
            let parent_context_through_sequence =
                parse_boundary(&required_text(&mut values)?).ok_or(())?;
            let allow_portable_snapshot = parse_bool(&required_text(&mut values)?).ok_or(())?;
            if required_text(&mut values)? != "stdin"
                || matches!(context_mode, SubAgentContextMode::Fresh)
                    && (parent_context_through_sequence.is_some() || allow_portable_snapshot)
                || matches!(context_mode, SubAgentContextMode::InheritParent)
                    && parent_context_through_sequence.is_none()
            {
                return Err(());
            }
            SubAgentCommand::Spawn {
                client_sub_agent_id,
                parent_session_id,
                agent_id,
                working_directory,
                context_mode,
                parent_context_through_sequence,
                allow_portable_snapshot,
            }
        }
        direction @ ("send-child" | "send-parent") => {
            let sub_agent_id = required_text(&mut values)?;
            let client_message_id = required_text(&mut values)?;
            let mode = parse_mode(&required_text(&mut values)?).ok_or(())?;
            if required_text(&mut values)? != "stdin" {
                return Err(());
            }
            if direction == "send-child" {
                SubAgentCommand::SendChild {
                    sub_agent_id,
                    client_message_id,
                    mode,
                }
            } else {
                SubAgentCommand::SendParent {
                    sub_agent_id,
                    client_message_id,
                    mode,
                }
            }
        }
        "events" => SubAgentCommand::Events {
            sub_agent_id: required_text(&mut values)?,
            after_sequence: required_text(&mut values)?.parse().map_err(|_| ())?,
            limit: required_text(&mut values)?.parse().map_err(|_| ())?,
        },
        "status" => SubAgentCommand::Status {
            sub_agent_id: required_text(&mut values)?,
        },
        "stop" => SubAgentCommand::Stop {
            sub_agent_id: required_text(&mut values)?,
            reason: required_text(&mut values)?,
        },
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

fn next_text(values: &mut impl Iterator<Item = OsString>) -> Option<String> {
    values.next()?.into_string().ok()
}

fn required_text(values: &mut impl Iterator<Item = OsString>) -> Result<String, ()> {
    next_text(values)
        .filter(|value| !value.trim().is_empty())
        .ok_or(())
}

fn parse_context(value: &str) -> Option<SubAgentContextMode> {
    match value {
        "fresh" => Some(SubAgentContextMode::Fresh),
        "inherit" => Some(SubAgentContextMode::InheritParent),
        _ => None,
    }
}

fn parse_boundary(value: &str) -> Option<Option<u64>> {
    if value == "none" {
        Some(None)
    } else {
        value.parse().ok().map(Some)
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn parse_mode(value: &str) -> Option<SubAgentInputMode> {
    match value {
        "queue" => Some(SubAgentInputMode::Queue),
        "steer" => Some(SubAgentInputMode::Steer),
        "interrupt-and-steer" => Some(SubAgentInputMode::InterruptAndSteer),
        _ => None,
    }
}

fn read_stdin() -> Result<Vec<u8>, SubAgentCliError> {
    let mut input = Vec::new();
    std::io::stdin()
        .take(MAX_STDIN_BYTES + 1)
        .read_to_end(&mut input)
        .map_err(|_| SubAgentCliError::InvalidStdin)?;
    if input.len() as u64 > MAX_STDIN_BYTES {
        return Err(SubAgentCliError::InvalidStdin);
    }
    Ok(input)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SpawnPayload {
    native_task_prompt: Vec<Value>,
    #[serde(default)]
    metadata: Map<String, Value>,
    #[serde(default = "default_crash_restart_limit")]
    crash_restart_limit: u64,
}

struct EncodedSpawnPayload {
    native_task_prompt_json: String,
    metadata_json: String,
    crash_restart_limit: u64,
}

const fn default_crash_restart_limit() -> u64 {
    DEFAULT_CRASH_RESTART_LIMIT
}

fn spawn_payload(input: &[u8]) -> Result<EncodedSpawnPayload, SubAgentCliError> {
    if input.len() as u64 > MAX_STDIN_BYTES {
        return Err(SubAgentCliError::InvalidStdin);
    }
    let payload = serde_json::from_slice::<SpawnPayload>(input)
        .map_err(|_| SubAgentCliError::InvalidStdin)?;
    Ok(EncodedSpawnPayload {
        native_task_prompt_json: serde_json::to_string(&payload.native_task_prompt)
            .map_err(|_| SubAgentCliError::InvalidStdin)?,
        metadata_json: serde_json::to_string(&payload.metadata)
            .map_err(|_| SubAgentCliError::InvalidStdin)?,
        crash_restart_limit: payload.crash_restart_limit,
    })
}

fn native_prompt(input: &[u8]) -> Result<String, SubAgentCliError> {
    if input.len() as u64 > MAX_STDIN_BYTES {
        return Err(SubAgentCliError::InvalidStdin);
    }
    let value =
        serde_json::from_slice::<Value>(input).map_err(|_| SubAgentCliError::InvalidStdin)?;
    value
        .is_array()
        .then(|| value.to_string())
        .ok_or(SubAgentCliError::InvalidStdin)
}

fn structured_message(input: &[u8]) -> Result<String, SubAgentCliError> {
    if input.len() as u64 > MAX_STDIN_BYTES {
        return Err(SubAgentCliError::InvalidStdin);
    }
    let value =
        serde_json::from_slice::<Value>(input).map_err(|_| SubAgentCliError::InvalidStdin)?;
    value
        .is_object()
        .then(|| value.to_string())
        .ok_or(SubAgentCliError::InvalidStdin)
}

fn record_json(record: SubAgentRecord) -> Value {
    json!({
        "subAgentId": record.sub_agent_id,
        "parentSessionId": record.parent_session_id,
        "childSessionId": record.child_session_id,
        "nativeChildSessionId": record.native_child_session_id,
        "agentId": record.agent_id,
        "lifecycle": lifecycle_name(&record.lifecycle),
        "contextMode": context_name(&record.context_mode),
        "contextRealization": realization_name(&record.context_realization),
        "contextThroughSequence": record.context_through_sequence,
        "processIdentity": record.process_identity,
        "startedAtMs": record.started_at_ms,
    })
}

fn interaction_json(receipt: InteractionReceipt) -> Value {
    json!({
        "subAgentId": receipt.sub_agent_id,
        "clientMessageId": receipt.client_message_id,
        "disposition": disposition_name(&receipt.disposition),
        "acceptedAtMs": receipt.accepted_at_ms,
    })
}

fn events_json(page: SubAgentEventPage) -> Value {
    json!({
        "events": page.events.into_iter().map(|event| json!({
            "subAgentId": event.sub_agent_id,
            "sequence": event.sequence,
            "observedAtMs": event.observed_at_ms,
            "kind": event_kind_name(&event.kind),
            "payload": embedded_json(&event.payload_json),
        })).collect::<Vec<_>>(),
        "nextSequence": page.next_sequence,
        "caughtUp": page.caught_up,
    })
}

fn status_json(status: SubAgentStatus) -> Value {
    json!({
        "record": record_json(status.record),
        "lastSequence": status.last_sequence,
        "pendingParentToChild": status.pending_parent_to_child,
        "pendingChildToParent": status.pending_child_to_parent,
        "restartCount": status.restart_count,
        "lastError": status.last_error,
    })
}

fn receipt_json(receipt: SubAgentReceipt) -> Value {
    json!({"accepted": receipt.accepted, "recordedAtMs": receipt.recorded_at_ms})
}

fn embedded_json(value: &str) -> Value {
    serde_json::from_str(value).unwrap_or(Value::Null)
}

fn lifecycle_name(value: &SubAgentLifecycle) -> &'static str {
    match value {
        SubAgentLifecycle::Starting => "starting",
        SubAgentLifecycle::Running => "running",
        SubAgentLifecycle::Idle => "idle",
        SubAgentLifecycle::Stopping => "stopping",
        SubAgentLifecycle::Completed => "completed",
        SubAgentLifecycle::Failed => "failed",
        SubAgentLifecycle::Unknown { .. } => "unknown",
    }
}

fn context_name(value: &SubAgentContextMode) -> &'static str {
    match value {
        SubAgentContextMode::Fresh => "fresh",
        SubAgentContextMode::InheritParent => "inherit-parent",
        SubAgentContextMode::Unknown { .. } => "unknown",
    }
}

fn realization_name(value: &ContextRealization) -> &'static str {
    match value {
        ContextRealization::FreshSession => "fresh-session",
        ContextRealization::NativeAcpFork => "native-acp-fork",
        ContextRealization::PortableSnapshot => "portable-snapshot",
        ContextRealization::Unknown { .. } => "unknown",
    }
}

fn disposition_name(value: &InputDisposition) -> &'static str {
    match value {
        InputDisposition::StartedForegroundWork => "started-foreground-work",
        InputDisposition::ContributedToActiveWork => "contributed-to-active-work",
        InputDisposition::QueuedForTurnBoundary => "queued-for-turn-boundary",
        InputDisposition::CancelRequestedThenQueued => "cancel-requested-then-queued",
        InputDisposition::Unknown { .. } => "unknown",
    }
}

fn event_kind_name(value: &SubAgentEventKind) -> &'static str {
    match value {
        SubAgentEventKind::Lifecycle => "lifecycle",
        SubAgentEventKind::NativeAcp => "native-acp",
        SubAgentEventKind::ParentToChild => "parent-to-child",
        SubAgentEventKind::ChildToParent => "child-to-parent",
        SubAgentEventKind::Compaction => "compaction",
        SubAgentEventKind::Failed => "failed",
        SubAgentEventKind::Unknown { .. } => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use sub_agent_host_contract::{SubAgentContextMode, SubAgentInputMode};

    use super::{
        MAX_STDIN_BYTES, SubAgentCommand, native_prompt, parse_arguments, spawn_payload,
        structured_message,
    };

    fn parse(values: &[&str]) -> SubAgentCommand {
        parse_arguments(values.iter().map(OsString::from))
            .expect("arguments parse")
            .expect("run requested")
            .command
    }

    #[test]
    fn parser_keeps_context_and_input_modes_explicit() {
        assert_eq!(
            parse(&[
                "--state-dir",
                "/tmp/crab",
                "spawn",
                "research-1",
                "session-1",
                "claude-opus",
                "/tmp/workspace",
                "inherit",
                "42",
                "true",
                "stdin",
            ]),
            SubAgentCommand::Spawn {
                client_sub_agent_id: "research-1".into(),
                parent_session_id: "session-1".into(),
                agent_id: "claude-opus".into(),
                working_directory: "/tmp/workspace".into(),
                context_mode: SubAgentContextMode::InheritParent,
                parent_context_through_sequence: Some(42),
                allow_portable_snapshot: true,
            }
        );
        assert_eq!(
            parse(&[
                "--state-dir",
                "/tmp/crab",
                "send-child",
                "subagent-1",
                "message-1",
                "interrupt-and-steer",
                "stdin",
            ]),
            SubAgentCommand::SendChild {
                sub_agent_id: "subagent-1".into(),
                client_message_id: "message-1".into(),
                mode: SubAgentInputMode::InterruptAndSteer,
            }
        );
    }

    #[test]
    fn parser_rejects_inline_payloads_and_incoherent_context() {
        for values in [
            vec!["--state-dir", "/tmp/crab", "status"],
            vec![
                "--state-dir",
                "/tmp/crab",
                "send-parent",
                "subagent-1",
                "message-1",
                "queue",
                r#"{"progress":"inline"}"#,
            ],
            vec![
                "--state-dir",
                "/tmp/crab",
                "spawn",
                "child",
                "parent",
                "agent",
                "/tmp",
                "fresh",
                "42",
                "false",
                "stdin",
            ],
        ] {
            assert!(parse_arguments(values.into_iter().map(OsString::from)).is_err());
        }
    }

    #[test]
    fn stdin_shapes_are_bounded_and_canonical() {
        let spawn = spawn_payload(
            br#"{"nativeTaskPrompt":[{"type":"text","text":"task"}],"metadata":{"role":"research"}}"#,
        )
        .expect("spawn payload");
        assert_eq!(
            spawn.native_task_prompt_json,
            r#"[{"type":"text","text":"task"}]"#
        );
        assert_eq!(spawn.metadata_json, r#"{"role":"research"}"#);
        assert_eq!(spawn.crash_restart_limit, 1);
        assert_eq!(
            spawn_payload(
                br#"{"nativeTaskPrompt":[{"type":"text","text":"task"}],"crashRestartLimit":0}"#,
            )
            .expect("disabled restart policy")
            .crash_restart_limit,
            0
        );
        assert_eq!(
            native_prompt(br#"[{"type":"text","text":"steer"}]"#).expect("prompt"),
            r#"[{"type":"text","text":"steer"}]"#
        );
        assert_eq!(
            structured_message(br#"{"progress":"halfway"}"#).expect("message"),
            r#"{"progress":"halfway"}"#
        );
        assert!(native_prompt(br#"{"not":"a prompt array"}"#).is_err());
        assert!(structured_message(b"[]").is_err());
        assert!(spawn_payload(&vec![b'x'; MAX_STDIN_BYTES as usize + 1]).is_err());
    }
}
