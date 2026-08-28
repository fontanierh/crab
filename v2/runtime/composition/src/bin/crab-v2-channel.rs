#![forbid(unsafe_code)]

use std::{
    ffi::OsString,
    path::PathBuf,
    process::ExitCode,
    time::{SystemTime, UNIX_EPOCH},
};

use crab_v2_runtime::ChannelIpcClient;
use native_channel_contract::{
    BindingReference, ChannelBindingCatalogPage, ChannelBindingSummary, ChannelLifecycle,
    InterruptReceipt, InterruptRequest, ListChannelBindingPageRequest, NativeEventDirection,
    NativeEventKind, PublishedEventPage, ReplayRequest,
};
use native_channel_implementation::MAX_BINDING_CATALOG_PAGE;
use serde_json::{Value, json};

const USAGE: &str = "usage: crab-v2-channel --state-dir <directory> <list [limit [active|all [after-binding-id]]] | status <binding> | events <binding> <after-sequence> <limit> | interrupt <binding> <expected-session> <reason>>";
const MAX_EVENT_PAGE: u64 = 256;

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
        ChannelCommand::List {
            after_binding_id,
            limit,
            include_detached,
        } => client
            .list_channel_binding_page(ListChannelBindingPageRequest {
                after_binding_id,
                limit,
                include_detached,
            })
            .await
            .map(catalog_page_json),
        ChannelCommand::Status { binding_id } => client
            .channel_binding_summary(BindingReference { binding_id })
            .await
            .map(summary_json),
        ChannelCommand::Events {
            binding_id,
            after_sequence,
            limit,
        } => client
            .replay_native_events(ReplayRequest {
                binding_id,
                after_sequence,
                limit,
            })
            .await
            .map(events_json),
        ChannelCommand::Interrupt {
            binding_id,
            expected_session_id,
            reason,
        } => {
            let requested_at_ms = match system_time_ms() {
                Ok(value) => value,
                Err(error) => return failure(error),
            };
            client
                .interrupt_and_drain(InterruptRequest {
                    binding_id,
                    expected_session_id,
                    requested_at_ms,
                    reason,
                })
                .await
                .map(interrupt_json)
        }
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
    eprintln!("crab-v2-channel: {error}");
    ExitCode::FAILURE
}

#[derive(Debug, PartialEq)]
struct Arguments {
    state_directory: PathBuf,
    command: ChannelCommand,
}

#[derive(Debug, PartialEq)]
enum ChannelCommand {
    List {
        after_binding_id: Option<String>,
        limit: u64,
        include_detached: bool,
    },
    Status {
        binding_id: String,
    },
    Events {
        binding_id: String,
        after_sequence: u64,
        limit: u64,
    },
    Interrupt {
        binding_id: String,
        expected_session_id: String,
        reason: String,
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
            let limit = match values.next() {
                Some(value) => integer(value)?,
                None => MAX_BINDING_CATALOG_PAGE,
            };
            if limit == 0 || limit > MAX_BINDING_CATALOG_PAGE {
                return Err(());
            }
            let include_detached = match values.next().map(text).transpose()?.as_deref() {
                Some("active") | None => false,
                Some("all") => true,
                Some(_) => return Err(()),
            };
            let after_binding_id = values.next().map(identifier).transpose()?;
            ChannelCommand::List {
                after_binding_id,
                limit,
                include_detached,
            }
        }
        "status" => ChannelCommand::Status {
            binding_id: identifier(values.next().ok_or(())?)?,
        },
        "events" => {
            let binding_id = identifier(values.next().ok_or(())?)?;
            let after_sequence = integer(values.next().ok_or(())?)?;
            let limit = integer(values.next().ok_or(())?)?;
            if limit == 0 || limit > MAX_EVENT_PAGE {
                return Err(());
            }
            ChannelCommand::Events {
                binding_id,
                after_sequence,
                limit,
            }
        }
        "interrupt" => ChannelCommand::Interrupt {
            binding_id: identifier(values.next().ok_or(())?)?,
            expected_session_id: identifier(values.next().ok_or(())?)?,
            reason: identifier(values.next().ok_or(())?)?,
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

fn system_time_ms() -> Result<u64, &'static str> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch")?;
    u64::try_from(duration.as_millis()).map_err(|_| "system clock is out of range")
}

fn catalog_page_json(catalog: ChannelBindingCatalogPage) -> Value {
    json!({
        "bindings": catalog.bindings.into_iter().map(summary_json).collect::<Vec<_>>(),
        "totalBindings": catalog.total_bindings,
        "nextAfterBindingId": catalog.next_after_binding_id,
    })
}

fn summary_json(summary: ChannelBindingSummary) -> Value {
    json!({
        "bindingId": summary.binding_id,
        "channelId": summary.channel_id,
        "adapterId": summary.adapter_id,
        "sessionId": summary.session_id,
        "lifecycle": lifecycle_name(&summary.lifecycle),
        "publishedSequence": summary.published_sequence,
        "pendingInputCount": summary.pending_input_count,
        "lastError": summary.last_error,
        "updatedAtMs": summary.updated_at_ms,
    })
}

fn events_json(page: PublishedEventPage) -> Value {
    json!({
        "events": page.events.into_iter().map(|event| json!({
            "bindingId": event.binding_id,
            "sessionId": event.session_id,
            "runId": event.run_id,
            "sequence": event.sequence,
            "observedAtMs": event.observed_at_ms,
            "kind": event_kind_name(&event.kind),
            "direction": event.direction.as_ref().map(direction_name),
            "nativeEventJson": event.native_event_json,
        })).collect::<Vec<_>>(),
        "nextSequence": page.next_sequence,
        "caughtUp": page.caught_up,
    })
}

fn interrupt_json(receipt: InterruptReceipt) -> Value {
    json!({
        "bindingId": receipt.binding_id,
        "sessionId": receipt.session_id,
        "cancelRequestedAtMs": receipt.cancel_requested_at_ms,
        "pendingInputCount": receipt.pending_input_count,
    })
}

fn lifecycle_name(value: &ChannelLifecycle) -> &'static str {
    match value {
        ChannelLifecycle::Binding => "binding",
        ChannelLifecycle::Attached => "attached",
        ChannelLifecycle::Replaying => "replaying",
        ChannelLifecycle::Detached => "detached",
        ChannelLifecycle::Failed => "failed",
        ChannelLifecycle::Unknown { .. } => "unknown",
    }
}

fn event_kind_name(value: &NativeEventKind) -> &'static str {
    match value {
        NativeEventKind::Message => "message",
        NativeEventKind::Thought => "thought",
        NativeEventKind::Plan => "plan",
        NativeEventKind::ToolCall => "tool-call",
        NativeEventKind::ToolResult => "tool-result",
        NativeEventKind::Terminal => "terminal",
        NativeEventKind::FileDiff => "file-diff",
        NativeEventKind::PermissionRequest => "permission-request",
        NativeEventKind::Usage => "usage",
        NativeEventKind::Compaction => "compaction",
        NativeEventKind::SessionState => "session-state",
        NativeEventKind::RunFinished => "run-finished",
        NativeEventKind::Other | NativeEventKind::Unknown { .. } => "other",
    }
}

fn direction_name(value: &NativeEventDirection) -> &'static str {
    match value {
        NativeEventDirection::ClientToAgent => "client-to-agent",
        NativeEventDirection::AgentToClient => "agent-to-client",
        NativeEventDirection::Other | NativeEventDirection::Unknown { .. } => "other",
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use native_channel_contract::{
        ChannelBindingCatalogPage, ChannelBindingSummary, ChannelLifecycle, NativeChannelEvent,
        NativeEventDirection, NativeEventKind, PublishedEventPage,
    };

    use super::{ChannelCommand, catalog_page_json, events_json, parse_arguments};

    #[test]
    fn parser_accepts_only_bounded_owner_operations() {
        let list = parse_arguments(
            [
                "--state-dir",
                "/tmp/crab-v2",
                "list",
                "100",
                "all",
                "binding-100",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .expect("list arguments parse")
        .expect("list requested");
        assert_eq!(
            list.command,
            ChannelCommand::List {
                after_binding_id: Some("binding-100".into()),
                limit: 100,
                include_detached: true,
            }
        );
        let default_list = parse_arguments(
            ["--state-dir", "/tmp/crab-v2", "list"]
                .into_iter()
                .map(OsString::from),
        )
        .expect("default list arguments parse")
        .expect("default list requested");
        assert_eq!(
            default_list.command,
            ChannelCommand::List {
                after_binding_id: None,
                limit: 256,
                include_detached: false,
            }
        );
        let events = parse_arguments(
            [
                "--state-dir",
                "/tmp/crab-v2",
                "events",
                "binding-1",
                "7",
                "100",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .expect("arguments parse")
        .expect("operation requested");
        assert_eq!(
            events.command,
            ChannelCommand::Events {
                binding_id: "binding-1".into(),
                after_sequence: 7,
                limit: 100,
            }
        );
        let interrupt = parse_arguments(
            [
                "--state-dir",
                "/tmp/crab-v2",
                "interrupt",
                "binding-1",
                "session-1",
                "operator requested",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .expect("interrupt arguments parse")
        .expect("interrupt requested");
        assert_eq!(
            interrupt.command,
            ChannelCommand::Interrupt {
                binding_id: "binding-1".into(),
                expected_session_id: "session-1".into(),
                reason: "operator requested".into(),
            }
        );
        for invalid in [
            vec!["--state-dir", "relative", "status", "binding-1"],
            vec!["--state-dir", "/tmp/crab-v2", "list", "257"],
            vec![
                "--state-dir",
                "/tmp/crab-v2",
                "events",
                "binding-1",
                "0",
                "0",
            ],
            vec![
                "--state-dir",
                "/tmp/crab-v2",
                "interrupt",
                "binding-1",
                "session-1",
                "",
            ],
        ] {
            assert!(parse_arguments(invalid.into_iter().map(OsString::from)).is_err());
        }
    }

    #[test]
    fn catalog_output_is_non_secret_and_reports_pending_work() {
        let output = catalog_page_json(ChannelBindingCatalogPage {
            bindings: vec![ChannelBindingSummary {
                binding_id: "binding-1".into(),
                channel_id: "channel-1".into(),
                adapter_id: "t3code".into(),
                session_id: "session-1".into(),
                lifecycle: ChannelLifecycle::Failed,
                published_sequence: 12,
                pending_input_count: 2,
                last_error: Some("adapter unavailable".into()),
                updated_at_ms: 42,
            }],
            total_bindings: 1,
            next_after_binding_id: Some("binding-1".into()),
        });
        assert_eq!(output["bindings"][0]["pendingInputCount"], 2);
        assert_eq!(output["bindings"][0]["lifecycle"], "failed");
        assert!(output["bindings"][0].get("nativeChannelJson").is_none());
        assert_eq!(output["totalBindings"], 1);
        assert_eq!(output["nextAfterBindingId"], "binding-1");
    }

    #[test]
    fn event_output_preserves_the_exact_native_payload_and_cursor() {
        let output = events_json(PublishedEventPage {
            events: vec![NativeChannelEvent {
                binding_id: "binding-1".into(),
                session_id: "session-1".into(),
                run_id: Some("run-1".into()),
                sequence: 9,
                observed_at_ms: 42,
                kind: NativeEventKind::ToolCall,
                direction: Some(NativeEventDirection::AgentToClient),
                native_event_json: r#"{"method":"session/update"}"#.into(),
            }],
            next_sequence: 9,
            caught_up: true,
        });
        assert_eq!(output["events"][0]["kind"], "tool-call");
        assert_eq!(
            output["events"][0]["nativeEventJson"],
            r#"{"method":"session/update"}"#
        );
        assert_eq!(output["nextSequence"], 9);
    }
}
