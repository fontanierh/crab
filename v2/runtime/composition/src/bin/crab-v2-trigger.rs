#![forbid(unsafe_code)]

use std::{ffi::OsString, path::PathBuf, process::ExitCode};

use crab_v2_runtime::ChannelIpcClient;
use serde_json::json;
use trigger_inbox_contract::{EnqueueTrigger, TriggerMode, TriggerSource, TriggerState};

const USAGE: &str = "usage: crab-v2-trigger --state-dir <directory> --channel <id> --lane <lane> --source <bridge|scheduler|self-work|operator> --source-id <id> --dedupe-key <key> [--mode <queue|steer|interrupt-and-steer>] [--not-before-ms <unix-ms>] (--message <text> | --message-json <json>)";

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
        Err(error) => {
            eprintln!("crab-v2-trigger: {error}");
            return ExitCode::FAILURE;
        }
    };
    let receipt = match client
        .enqueue_trigger(EnqueueTrigger {
            source: arguments.source,
            source_id: arguments.source_id,
            deduplication_key: arguments.deduplication_key,
            target_channel_id: arguments.channel_id,
            lane: arguments.lane,
            mode: arguments.mode,
            not_before_ms: arguments.not_before_ms,
            message_json: arguments.message_json,
            attachments: Vec::new(),
        })
        .await
    {
        Ok(receipt) => receipt,
        Err(error) => {
            eprintln!("crab-v2-trigger: {error}");
            return ExitCode::FAILURE;
        }
    };
    println!(
        "trigger_id={} state={} deduplicated={} recorded_at_ms={}",
        receipt.trigger_id,
        state_name(&receipt.state),
        receipt.deduplicated,
        receipt.recorded_at_ms,
    );
    ExitCode::SUCCESS
}

struct Arguments {
    state_directory: PathBuf,
    channel_id: String,
    lane: String,
    source: TriggerSource,
    source_id: String,
    deduplication_key: String,
    mode: TriggerMode,
    not_before_ms: u64,
    message_json: String,
}

fn parse_arguments(mut values: impl Iterator<Item = OsString>) -> Result<Option<Arguments>, ()> {
    let mut state_directory = None;
    let mut channel_id = None;
    let mut lane = None;
    let mut source = None;
    let mut source_id = None;
    let mut deduplication_key = None;
    let mut mode = None;
    let mut not_before_ms = None;
    let mut message_json = None;
    while let Some(argument) = values.next() {
        match argument.to_str() {
            Some("--help" | "-h")
                if state_directory.is_none()
                    && channel_id.is_none()
                    && lane.is_none()
                    && source.is_none()
                    && source_id.is_none()
                    && deduplication_key.is_none()
                    && mode.is_none()
                    && not_before_ms.is_none()
                    && message_json.is_none() =>
            {
                return Ok(None);
            }
            Some("--state-dir") if state_directory.is_none() => {
                state_directory = values.next().map(PathBuf::from)
            }
            Some("--channel") if channel_id.is_none() => channel_id = next_text(&mut values),
            Some("--lane") if lane.is_none() => lane = next_text(&mut values),
            Some("--source") if source.is_none() => {
                let value = next_text(&mut values).ok_or(())?;
                source = Some(parse_source(&value).ok_or(())?);
            }
            Some("--source-id") if source_id.is_none() => source_id = next_text(&mut values),
            Some("--dedupe-key") if deduplication_key.is_none() => {
                deduplication_key = next_text(&mut values)
            }
            Some("--mode") if mode.is_none() => {
                let value = next_text(&mut values).ok_or(())?;
                mode = Some(parse_mode(&value).ok_or(())?);
            }
            Some("--not-before-ms") if not_before_ms.is_none() => {
                let value = next_text(&mut values).ok_or(())?;
                not_before_ms = Some(value.parse().map_err(|_| ())?);
            }
            Some("--message") if message_json.is_none() => {
                let text = next_text(&mut values)
                    .filter(|value| !value.trim().is_empty())
                    .ok_or(())?;
                message_json = Some(serde_json::to_string(&json!({"text": text})).map_err(|_| ())?);
            }
            Some("--message-json") if message_json.is_none() => {
                let value = next_text(&mut values).ok_or(())?;
                message_json = Some(canonical_json(value).ok_or(())?);
            }
            _ => return Err(()),
        }
    }
    let required = (
        state_directory,
        non_empty(channel_id),
        non_empty(lane),
        source,
        non_empty(source_id),
        non_empty(deduplication_key),
        message_json,
    );
    match required {
        (
            Some(state_directory),
            Some(channel_id),
            Some(lane),
            Some(source),
            Some(source_id),
            Some(deduplication_key),
            Some(message_json),
        ) => Ok(Some(Arguments {
            state_directory,
            channel_id,
            lane,
            source,
            source_id,
            deduplication_key,
            mode: mode.unwrap_or(TriggerMode::Queue),
            not_before_ms: not_before_ms.unwrap_or(0),
            message_json,
        })),
        _ => Err(()),
    }
}

fn next_text(values: &mut impl Iterator<Item = OsString>) -> Option<String> {
    values.next()?.into_string().ok()
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

fn canonical_json(value: String) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(&value)
        .ok()
        .and_then(|value| serde_json::to_string(&value).ok())
}

fn parse_source(value: &str) -> Option<TriggerSource> {
    match value {
        "bridge" => Some(TriggerSource::Bridge),
        "scheduler" => Some(TriggerSource::Scheduler),
        "self-work" => Some(TriggerSource::SelfWork),
        "operator" => Some(TriggerSource::Operator),
        _ => None,
    }
}

fn parse_mode(value: &str) -> Option<TriggerMode> {
    match value {
        "queue" => Some(TriggerMode::Queue),
        "steer" => Some(TriggerMode::Steer),
        "interrupt-and-steer" => Some(TriggerMode::InterruptAndSteer),
        _ => None,
    }
}

fn state_name(state: &TriggerState) -> &'static str {
    match state {
        TriggerState::Pending => "pending",
        TriggerState::Leased => "leased",
        TriggerState::Completed => "completed",
        TriggerState::RetryScheduled => "retry-scheduled",
        TriggerState::DeadLettered => "dead-lettered",
        TriggerState::Unknown { .. } => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use trigger_inbox_contract::{TriggerMode, TriggerSource};

    use super::parse_arguments;

    #[test]
    fn parser_requires_idempotent_explicit_ingress_and_canonicalizes_payloads() {
        let arguments = parse_arguments(
            [
                "--source",
                "self-work",
                "--state-dir",
                "/tmp/crab-v2",
                "--channel",
                "primary",
                "--lane",
                "primary",
                "--source-id",
                "jim",
                "--dedupe-key",
                "follow-up-1",
                "--mode",
                "steer",
                "--not-before-ms",
                "42",
                "--message-json",
                "{ \"text\": \"continue\" }",
            ]
            .map(OsString::from)
            .into_iter(),
        )
        .expect("arguments parse")
        .expect("run requested");
        assert!(matches!(arguments.source, TriggerSource::SelfWork));
        assert!(matches!(arguments.mode, TriggerMode::Steer));
        assert_eq!(arguments.not_before_ms, 42);
        assert_eq!(arguments.message_json, r#"{"text":"continue"}"#);

        let plain = parse_arguments(
            [
                "--state-dir",
                "/tmp/crab-v2",
                "--channel",
                "primary",
                "--lane",
                "primary",
                "--source",
                "operator",
                "--source-id",
                "henry",
                "--dedupe-key",
                "plain-1",
                "--message",
                "hello",
            ]
            .map(OsString::from)
            .into_iter(),
        )
        .expect("plain arguments parse")
        .expect("plain run requested");
        assert!(matches!(plain.mode, TriggerMode::Queue));
        assert_eq!(plain.message_json, r#"{"text":"hello"}"#);

        assert!(
            parse_arguments(
                [
                    "--state-dir",
                    "/tmp/crab-v2",
                    "--channel",
                    "primary",
                    "--lane",
                    "primary",
                    "--source",
                    "operator",
                    "--source-id",
                    "henry",
                    "--message",
                    "missing dedupe key",
                ]
                .map(OsString::from)
                .into_iter(),
            )
            .is_err()
        );
        assert!(
            parse_arguments(
                [
                    "--state-dir",
                    "/tmp/crab-v2",
                    "--channel",
                    "primary",
                    "--lane",
                    "primary",
                    "--source",
                    "operator",
                    "--source-id",
                    "henry",
                    "--dedupe-key",
                    "bad-json",
                    "--message-json",
                    "{",
                ]
                .map(OsString::from)
                .into_iter(),
            )
            .is_err()
        );
    }
}
