use std::collections::BTreeMap;

use crab_core::{CrabError, CrabResult};

use crate::{ensure_non_empty_field, BackendEvent, BackendEventKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCodeTokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenCodeTurnState {
    Completed,
    Interrupted,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenCodeRawEvent {
    AssistantDelta {
        sequence: u64,
        text: String,
    },
    ToolCall {
        sequence: u64,
        tool_name: String,
        call_id: String,
        arguments_json: String,
    },
    ToolResult {
        sequence: u64,
        call_id: String,
        output: String,
        is_error: bool,
    },
    ApprovalRequest {
        sequence: u64,
        request_kind: String,
        reason: String,
    },
    TurnFinished {
        sequence: u64,
        turn_id: String,
        state: OpenCodeTurnState,
        message: Option<String>,
        usage: Option<OpenCodeTokenUsage>,
    },
}

pub fn normalize_opencode_events(raw_events: &[OpenCodeRawEvent]) -> CrabResult<Vec<BackendEvent>> {
    let mut normalized = Vec::with_capacity(raw_events.len());
    for raw_event in raw_events {
        normalized.push(normalize_one(raw_event)?);
    }
    Ok(normalized)
}

fn normalize_one(raw_event: &OpenCodeRawEvent) -> CrabResult<BackendEvent> {
    match raw_event {
        OpenCodeRawEvent::AssistantDelta { sequence, text } => {
            ensure_sequence("opencode_event_assistant_delta", *sequence)?;
            ensure_non_empty_field("opencode_event_assistant_delta", "text", text)?;
            Ok(BackendEvent {
                sequence: *sequence,
                kind: BackendEventKind::TextDelta,
                payload: BTreeMap::from([("delta".to_string(), text.clone())]),
            })
        }
        OpenCodeRawEvent::ToolCall {
            sequence,
            tool_name,
            call_id,
            arguments_json,
        } => {
            ensure_sequence("opencode_event_tool_call", *sequence)?;
            ensure_non_empty_field("opencode_event_tool_call", "tool_name", tool_name)?;
            ensure_non_empty_field("opencode_event_tool_call", "call_id", call_id)?;
            ensure_non_empty_field("opencode_event_tool_call", "arguments_json", arguments_json)?;
            Ok(BackendEvent {
                sequence: *sequence,
                kind: BackendEventKind::ToolCall,
                payload: BTreeMap::from([
                    ("tool_name".to_string(), tool_name.clone()),
                    ("call_id".to_string(), call_id.clone()),
                    ("arguments_json".to_string(), arguments_json.clone()),
                ]),
            })
        }
        OpenCodeRawEvent::ToolResult {
            sequence,
            call_id,
            output,
            is_error,
        } => {
            ensure_sequence("opencode_event_tool_result", *sequence)?;
            ensure_non_empty_field("opencode_event_tool_result", "call_id", call_id)?;
            ensure_non_empty_field("opencode_event_tool_result", "output", output)?;
            Ok(BackendEvent {
                sequence: *sequence,
                kind: BackendEventKind::ToolResult,
                payload: BTreeMap::from([
                    ("call_id".to_string(), call_id.clone()),
                    ("output".to_string(), output.clone()),
                    (
                        "result_kind".to_string(),
                        if *is_error { "error" } else { "ok" }.to_string(),
                    ),
                ]),
            })
        }
        OpenCodeRawEvent::ApprovalRequest {
            sequence,
            request_kind,
            reason,
        } => {
            ensure_sequence("opencode_event_approval_request", *sequence)?;
            ensure_non_empty_field(
                "opencode_event_approval_request",
                "request_kind",
                request_kind,
            )?;
            ensure_non_empty_field("opencode_event_approval_request", "reason", reason)?;
            Ok(BackendEvent {
                sequence: *sequence,
                kind: BackendEventKind::RunNote,
                payload: BTreeMap::from([
                    ("note_type".to_string(), "approval.requested".to_string()),
                    ("request_kind".to_string(), request_kind.clone()),
                    ("reason".to_string(), reason.clone()),
                ]),
            })
        }
        OpenCodeRawEvent::TurnFinished {
            sequence,
            turn_id,
            state,
            message,
            usage,
        } => {
            ensure_sequence("opencode_event_turn_finished", *sequence)?;
            ensure_non_empty_field("opencode_event_turn_finished", "turn_id", turn_id)?;
            if let Some(message) = message {
                ensure_non_empty_field("opencode_event_turn_finished", "message", message)?;
            }
            if let Some(usage) = usage {
                validate_usage("opencode_event_turn_finished", usage)?;
            }
            let mut payload = BTreeMap::from([("turn_id".to_string(), turn_id.clone())]);
            if let Some(message) = message {
                payload.insert("message".to_string(), message.clone());
            }
            if let Some(usage) = usage {
                payload.insert("input_tokens".to_string(), usage.input_tokens.to_string());
                payload.insert("output_tokens".to_string(), usage.output_tokens.to_string());
                payload.insert("total_tokens".to_string(), usage.total_tokens.to_string());
            }

            match state {
                OpenCodeTurnState::Completed => Ok(BackendEvent {
                    sequence: *sequence,
                    kind: BackendEventKind::TurnCompleted,
                    payload,
                }),
                OpenCodeTurnState::Interrupted => Ok(BackendEvent {
                    sequence: *sequence,
                    kind: BackendEventKind::TurnInterrupted,
                    payload,
                }),
                OpenCodeTurnState::Failed => Ok(BackendEvent {
                    sequence: *sequence,
                    kind: BackendEventKind::Error,
                    payload,
                }),
            }
        }
    }
}

fn ensure_sequence(context: &'static str, sequence: u64) -> CrabResult<()> {
    if sequence == 0 {
        return Err(CrabError::InvariantViolation {
            context,
            message: "sequence must be non-zero".to_string(),
        });
    }
    Ok(())
}

fn validate_usage(context: &'static str, usage: &OpenCodeTokenUsage) -> CrabResult<()> {
    if usage.input_tokens + usage.output_tokens != usage.total_tokens {
        return Err(CrabError::InvariantViolation {
            context,
            message: "total_tokens must equal input_tokens + output_tokens".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crab_core::CrabError;

    use super::{
        normalize_opencode_events, OpenCodeRawEvent, OpenCodeTokenUsage, OpenCodeTurnState,
    };
    use crate::{BackendEvent, BackendEventKind};

    #[test]
    fn normalizes_full_event_stream_snapshot() {
        let normalized = normalize_opencode_events(&[
            OpenCodeRawEvent::AssistantDelta {
                sequence: 1,
                text: "hi".to_string(),
            },
            OpenCodeRawEvent::ToolCall {
                sequence: 2,
                tool_name: "shell".to_string(),
                call_id: "tool-1".to_string(),
                arguments_json: "{\"cmd\":\"pwd\"}".to_string(),
            },
            OpenCodeRawEvent::ToolResult {
                sequence: 3,
                call_id: "tool-1".to_string(),
                output: "/repo".to_string(),
                is_error: false,
            },
            OpenCodeRawEvent::ApprovalRequest {
                sequence: 4,
                request_kind: "command".to_string(),
                reason: "rm -rf".to_string(),
            },
            OpenCodeRawEvent::TurnFinished {
                sequence: 5,
                turn_id: "turn-5".to_string(),
                state: OpenCodeTurnState::Completed,
                message: Some("done".to_string()),
                usage: Some(OpenCodeTokenUsage {
                    input_tokens: 10,
                    output_tokens: 15,
                    total_tokens: 25,
                }),
            },
        ])
        .expect("stream should normalize");

        assert_eq!(
            normalized,
            vec![
                BackendEvent {
                    sequence: 1,
                    kind: BackendEventKind::TextDelta,
                    payload: BTreeMap::from([("delta".to_string(), "hi".to_string())]),
                },
                BackendEvent {
                    sequence: 2,
                    kind: BackendEventKind::ToolCall,
                    payload: BTreeMap::from([
                        ("tool_name".to_string(), "shell".to_string()),
                        ("call_id".to_string(), "tool-1".to_string()),
                        (
                            "arguments_json".to_string(),
                            "{\"cmd\":\"pwd\"}".to_string()
                        ),
                    ]),
                },
                BackendEvent {
                    sequence: 3,
                    kind: BackendEventKind::ToolResult,
                    payload: BTreeMap::from([
                        ("call_id".to_string(), "tool-1".to_string()),
                        ("output".to_string(), "/repo".to_string()),
                        ("result_kind".to_string(), "ok".to_string()),
                    ]),
                },
                BackendEvent {
                    sequence: 4,
                    kind: BackendEventKind::RunNote,
                    payload: BTreeMap::from([
                        ("note_type".to_string(), "approval.requested".to_string()),
                        ("request_kind".to_string(), "command".to_string()),
                        ("reason".to_string(), "rm -rf".to_string()),
                    ]),
                },
                BackendEvent {
                    sequence: 5,
                    kind: BackendEventKind::TurnCompleted,
                    payload: BTreeMap::from([
                        ("turn_id".to_string(), "turn-5".to_string()),
                        ("message".to_string(), "done".to_string()),
                        ("input_tokens".to_string(), "10".to_string()),
                        ("output_tokens".to_string(), "15".to_string()),
                        ("total_tokens".to_string(), "25".to_string()),
                    ]),
                },
            ]
        );
    }

    #[test]
    fn turn_finished_state_mapping_and_optional_fields() {
        let interrupted = normalize_opencode_events(&[OpenCodeRawEvent::TurnFinished {
            sequence: 9,
            turn_id: "turn-9".to_string(),
            state: OpenCodeTurnState::Interrupted,
            message: None,
            usage: None,
        }])
        .expect("interrupted turn should normalize");
        assert_eq!(
            interrupted,
            vec![BackendEvent {
                sequence: 9,
                kind: BackendEventKind::TurnInterrupted,
                payload: BTreeMap::from([("turn_id".to_string(), "turn-9".to_string())]),
            }]
        );

        let failed = normalize_opencode_events(&[OpenCodeRawEvent::TurnFinished {
            sequence: 10,
            turn_id: "turn-10".to_string(),
            state: OpenCodeTurnState::Failed,
            message: Some("boom".to_string()),
            usage: None,
        }])
        .expect("failed turn should normalize");
        assert_eq!(
            failed,
            vec![BackendEvent {
                sequence: 10,
                kind: BackendEventKind::Error,
                payload: BTreeMap::from([
                    ("turn_id".to_string(), "turn-10".to_string()),
                    ("message".to_string(), "boom".to_string()),
                ]),
            }]
        );
    }

    #[test]
    fn tool_result_error_flag_is_preserved() {
        let normalized = normalize_opencode_events(&[OpenCodeRawEvent::ToolResult {
            sequence: 7,
            call_id: "tool-7".to_string(),
            output: "permission denied".to_string(),
            is_error: true,
        }])
        .expect("tool result should normalize");
        assert_eq!(
            normalized,
            vec![BackendEvent {
                sequence: 7,
                kind: BackendEventKind::ToolResult,
                payload: BTreeMap::from([
                    ("call_id".to_string(), "tool-7".to_string()),
                    ("output".to_string(), "permission denied".to_string()),
                    ("result_kind".to_string(), "error".to_string()),
                ]),
            }]
        );
    }

    #[test]
    fn rejects_invalid_payloads() {
        let invalid_cases = vec![
            (
                OpenCodeRawEvent::AssistantDelta {
                    sequence: 0,
                    text: "hi".to_string(),
                },
                CrabError::InvariantViolation {
                    context: "opencode_event_assistant_delta",
                    message: "sequence must be non-zero".to_string(),
                },
            ),
            (
                OpenCodeRawEvent::AssistantDelta {
                    sequence: 1,
                    text: " ".to_string(),
                },
                CrabError::InvariantViolation {
                    context: "opencode_event_assistant_delta",
                    message: "text must not be empty".to_string(),
                },
            ),
            (
                OpenCodeRawEvent::ToolCall {
                    sequence: 0,
                    tool_name: "shell".to_string(),
                    call_id: "call".to_string(),
                    arguments_json: "{}".to_string(),
                },
                CrabError::InvariantViolation {
                    context: "opencode_event_tool_call",
                    message: "sequence must be non-zero".to_string(),
                },
            ),
            (
                OpenCodeRawEvent::ToolCall {
                    sequence: 1,
                    tool_name: " ".to_string(),
                    call_id: "call".to_string(),
                    arguments_json: "{}".to_string(),
                },
                CrabError::InvariantViolation {
                    context: "opencode_event_tool_call",
                    message: "tool_name must not be empty".to_string(),
                },
            ),
            (
                OpenCodeRawEvent::ToolCall {
                    sequence: 1,
                    tool_name: "shell".to_string(),
                    call_id: " ".to_string(),
                    arguments_json: "{}".to_string(),
                },
                CrabError::InvariantViolation {
                    context: "opencode_event_tool_call",
                    message: "call_id must not be empty".to_string(),
                },
            ),
            (
                OpenCodeRawEvent::ToolCall {
                    sequence: 1,
                    tool_name: "shell".to_string(),
                    call_id: "call".to_string(),
                    arguments_json: " ".to_string(),
                },
                CrabError::InvariantViolation {
                    context: "opencode_event_tool_call",
                    message: "arguments_json must not be empty".to_string(),
                },
            ),
            (
                OpenCodeRawEvent::ToolResult {
                    sequence: 0,
                    call_id: "call".to_string(),
                    output: "ok".to_string(),
                    is_error: false,
                },
                CrabError::InvariantViolation {
                    context: "opencode_event_tool_result",
                    message: "sequence must be non-zero".to_string(),
                },
            ),
            (
                OpenCodeRawEvent::ToolResult {
                    sequence: 1,
                    call_id: " ".to_string(),
                    output: "ok".to_string(),
                    is_error: false,
                },
                CrabError::InvariantViolation {
                    context: "opencode_event_tool_result",
                    message: "call_id must not be empty".to_string(),
                },
            ),
            (
                OpenCodeRawEvent::ToolResult {
                    sequence: 1,
                    call_id: "call".to_string(),
                    output: " ".to_string(),
                    is_error: false,
                },
                CrabError::InvariantViolation {
                    context: "opencode_event_tool_result",
                    message: "output must not be empty".to_string(),
                },
            ),
            (
                OpenCodeRawEvent::ApprovalRequest {
                    sequence: 0,
                    request_kind: "command".to_string(),
                    reason: "danger".to_string(),
                },
                CrabError::InvariantViolation {
                    context: "opencode_event_approval_request",
                    message: "sequence must be non-zero".to_string(),
                },
            ),
            (
                OpenCodeRawEvent::ApprovalRequest {
                    sequence: 1,
                    request_kind: " ".to_string(),
                    reason: "danger".to_string(),
                },
                CrabError::InvariantViolation {
                    context: "opencode_event_approval_request",
                    message: "request_kind must not be empty".to_string(),
                },
            ),
            (
                OpenCodeRawEvent::ApprovalRequest {
                    sequence: 1,
                    request_kind: "command".to_string(),
                    reason: " ".to_string(),
                },
                CrabError::InvariantViolation {
                    context: "opencode_event_approval_request",
                    message: "reason must not be empty".to_string(),
                },
            ),
            (
                OpenCodeRawEvent::TurnFinished {
                    sequence: 0,
                    turn_id: "turn".to_string(),
                    state: OpenCodeTurnState::Completed,
                    message: None,
                    usage: None,
                },
                CrabError::InvariantViolation {
                    context: "opencode_event_turn_finished",
                    message: "sequence must be non-zero".to_string(),
                },
            ),
            (
                OpenCodeRawEvent::TurnFinished {
                    sequence: 1,
                    turn_id: " ".to_string(),
                    state: OpenCodeTurnState::Completed,
                    message: None,
                    usage: None,
                },
                CrabError::InvariantViolation {
                    context: "opencode_event_turn_finished",
                    message: "turn_id must not be empty".to_string(),
                },
            ),
            (
                OpenCodeRawEvent::TurnFinished {
                    sequence: 1,
                    turn_id: "turn".to_string(),
                    state: OpenCodeTurnState::Completed,
                    message: Some(" ".to_string()),
                    usage: None,
                },
                CrabError::InvariantViolation {
                    context: "opencode_event_turn_finished",
                    message: "message must not be empty".to_string(),
                },
            ),
            (
                OpenCodeRawEvent::TurnFinished {
                    sequence: 1,
                    turn_id: "turn".to_string(),
                    state: OpenCodeTurnState::Completed,
                    message: None,
                    usage: Some(OpenCodeTokenUsage {
                        input_tokens: 1,
                        output_tokens: 2,
                        total_tokens: 4,
                    }),
                },
                CrabError::InvariantViolation {
                    context: "opencode_event_turn_finished",
                    message: "total_tokens must equal input_tokens + output_tokens".to_string(),
                },
            ),
        ];

        for (raw_event, expected_error) in invalid_cases {
            let error = normalize_opencode_events(&[raw_event])
                .expect_err("invalid event should return invariant error");
            assert_eq!(error, expected_error);
        }
    }
}
