use std::collections::BTreeMap;

use crab_core::{CrabError, CrabResult};

use crate::{ensure_non_empty_field, BackendEvent, BackendEventKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexRawEvent {
    Notification(CodexNotification),
    Request(CodexRequest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexNotification {
    ThreadStarted {
        thread_id: String,
    },
    TurnStarted {
        thread_id: String,
        turn_id: String,
    },
    AgentMessageDelta {
        thread_id: String,
        turn_id: String,
        item_id: String,
        delta: String,
    },
    ItemCompleted {
        thread_id: String,
        turn_id: String,
        item: CodexCompletedItem,
    },
    TurnCompleted {
        thread_id: String,
        turn_id: String,
        status: CodexTurnStatus,
        error_message: Option<String>,
    },
    TokenUsageUpdated {
        thread_id: String,
        turn_id: String,
        input_tokens: u64,
        output_tokens: u64,
        total_tokens: u64,
    },
    Error {
        thread_id: String,
        turn_id: String,
        message: String,
        will_retry: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexCompletedItem {
    AgentMessage {
        item_id: String,
        text: String,
    },
    ToolCall {
        item_id: String,
        tool_name: String,
        input_summary: String,
    },
    ToolResult {
        item_id: String,
        tool_name: String,
        output_summary: String,
        is_error: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexTurnStatus {
    Completed,
    Interrupted,
    Failed,
    InProgress,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexRequest {
    CommandExecutionApproval {
        request_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        command: Option<String>,
        reason: Option<String>,
    },
    FileChangeApproval {
        request_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        grant_root: Option<String>,
        reason: Option<String>,
    },
    ToolUserInput {
        request_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        question_count: usize,
    },
}

pub fn normalize_codex_events(raw_events: Vec<CodexRawEvent>) -> CrabResult<Vec<BackendEvent>> {
    let mut normalized = Vec::with_capacity(raw_events.len());
    for (index, raw_event) in raw_events.into_iter().enumerate() {
        normalized.push(normalize_codex_event(sequence_number(index), raw_event)?);
    }
    Ok(normalized)
}

fn normalize_codex_event(sequence: u64, raw_event: CodexRawEvent) -> CrabResult<BackendEvent> {
    match raw_event {
        CodexRawEvent::Notification(notification) => {
            normalize_codex_notification(sequence, notification)
        }
        CodexRawEvent::Request(request) => normalize_codex_request(sequence, request),
    }
}

fn normalize_codex_notification(
    sequence: u64,
    notification: CodexNotification,
) -> CrabResult<BackendEvent> {
    let (kind, payload) = match notification {
        CodexNotification::ThreadStarted { thread_id } => {
            ensure_non_empty_field("codex_event_thread_started", "thread_id", &thread_id)?;
            (
                BackendEventKind::RunNote,
                BTreeMap::from([
                    ("event".to_string(), "thread.started".to_string()),
                    ("thread_id".to_string(), thread_id),
                ]),
            )
        }
        CodexNotification::TurnStarted { thread_id, turn_id } => {
            ensure_non_empty_field("codex_event_turn_started", "thread_id", &thread_id)?;
            ensure_non_empty_field("codex_event_turn_started", "turn_id", &turn_id)?;
            (
                BackendEventKind::RunNote,
                BTreeMap::from([
                    ("event".to_string(), "turn.started".to_string()),
                    ("thread_id".to_string(), thread_id),
                    ("turn_id".to_string(), turn_id),
                ]),
            )
        }
        CodexNotification::AgentMessageDelta {
            thread_id,
            turn_id,
            item_id,
            delta,
        } => {
            ensure_non_empty_field("codex_event_agent_message_delta", "thread_id", &thread_id)?;
            ensure_non_empty_field("codex_event_agent_message_delta", "turn_id", &turn_id)?;
            ensure_non_empty_field("codex_event_agent_message_delta", "item_id", &item_id)?;
            ensure_non_empty_field("codex_event_agent_message_delta", "delta", &delta)?;
            (
                BackendEventKind::TextDelta,
                BTreeMap::from([("delta".to_string(), delta)]),
            )
        }
        CodexNotification::ItemCompleted {
            thread_id,
            turn_id,
            item,
        } => {
            ensure_non_empty_field("codex_event_item_completed", "thread_id", &thread_id)?;
            ensure_non_empty_field("codex_event_item_completed", "turn_id", &turn_id)?;
            normalize_completed_item(item)?
        }
        CodexNotification::TurnCompleted {
            thread_id,
            turn_id,
            status,
            error_message,
        } => {
            ensure_non_empty_field("codex_event_turn_completed", "thread_id", &thread_id)?;
            ensure_non_empty_field("codex_event_turn_completed", "turn_id", &turn_id)?;
            match status {
                CodexTurnStatus::Completed => (
                    BackendEventKind::TurnCompleted,
                    BTreeMap::from([
                        ("stop_reason".to_string(), "completed".to_string()),
                        ("thread_id".to_string(), thread_id),
                        ("turn_id".to_string(), turn_id),
                    ]),
                ),
                CodexTurnStatus::Interrupted => (
                    BackendEventKind::TurnInterrupted,
                    BTreeMap::from([(
                        "reason".to_string(),
                        error_message.unwrap_or_else(|| "interrupted".to_string()),
                    )]),
                ),
                CodexTurnStatus::Failed => (
                    BackendEventKind::Error,
                    BTreeMap::from([(
                        "message".to_string(),
                        error_message.unwrap_or_else(|| "turn failed".to_string()),
                    )]),
                ),
                CodexTurnStatus::InProgress => {
                    return Err(CrabError::InvariantViolation {
                        context: "codex_event_turn_completed",
                        message: "turn/completed received inProgress status".to_string(),
                    });
                }
            }
        }
        CodexNotification::TokenUsageUpdated {
            thread_id,
            turn_id,
            input_tokens,
            output_tokens,
            total_tokens,
        } => {
            ensure_non_empty_field("codex_event_token_usage", "thread_id", &thread_id)?;
            ensure_non_empty_field("codex_event_token_usage", "turn_id", &turn_id)?;
            let minimum_total = input_tokens.checked_add(output_tokens).ok_or_else(|| {
                CrabError::InvariantViolation {
                    context: "codex_event_token_usage",
                    message: "input/output token addition overflow".to_string(),
                }
            })?;
            if total_tokens < minimum_total {
                return Err(CrabError::InvariantViolation {
                    context: "codex_event_token_usage",
                    message: format!(
                        "total_tokens {total_tokens} is lower than input+output {minimum_total}"
                    ),
                });
            }
            (
                BackendEventKind::RunNote,
                BTreeMap::from([
                    (
                        "event".to_string(),
                        "thread.token_usage.updated".to_string(),
                    ),
                    ("thread_id".to_string(), thread_id),
                    ("turn_id".to_string(), turn_id),
                    ("usage_input_tokens".to_string(), input_tokens.to_string()),
                    ("usage_output_tokens".to_string(), output_tokens.to_string()),
                    ("usage_total_tokens".to_string(), total_tokens.to_string()),
                    ("usage_source".to_string(), "codex".to_string()),
                ]),
            )
        }
        CodexNotification::Error {
            thread_id,
            turn_id,
            message,
            will_retry,
        } => {
            ensure_non_empty_field("codex_event_error", "thread_id", &thread_id)?;
            ensure_non_empty_field("codex_event_error", "turn_id", &turn_id)?;
            ensure_non_empty_field("codex_event_error", "message", &message)?;
            (
                BackendEventKind::Error,
                BTreeMap::from([
                    ("message".to_string(), message),
                    ("thread_id".to_string(), thread_id),
                    ("turn_id".to_string(), turn_id),
                    ("will_retry".to_string(), will_retry.to_string()),
                ]),
            )
        }
    };

    Ok(BackendEvent {
        sequence,
        kind,
        payload,
    })
}

fn normalize_completed_item(
    item: CodexCompletedItem,
) -> CrabResult<(BackendEventKind, BTreeMap<String, String>)> {
    match item {
        CodexCompletedItem::AgentMessage { item_id, text } => {
            ensure_non_empty_field("codex_event_item_completed", "item_id", &item_id)?;
            ensure_non_empty_field("codex_event_item_completed", "text", &text)?;
            Ok((
                BackendEventKind::TextDelta,
                BTreeMap::from([("delta".to_string(), text)]),
            ))
        }
        CodexCompletedItem::ToolCall {
            item_id,
            tool_name,
            input_summary,
        } => {
            ensure_non_empty_field("codex_event_item_completed", "item_id", &item_id)?;
            ensure_non_empty_field("codex_event_item_completed", "tool_name", &tool_name)?;
            ensure_non_empty_field(
                "codex_event_item_completed",
                "input_summary",
                &input_summary,
            )?;
            Ok((
                BackendEventKind::ToolCall,
                BTreeMap::from([
                    ("tool_call_id".to_string(), item_id),
                    ("tool_name".to_string(), tool_name),
                    ("input_json".to_string(), input_summary),
                ]),
            ))
        }
        CodexCompletedItem::ToolResult {
            item_id,
            tool_name,
            output_summary,
            is_error,
        } => {
            ensure_non_empty_field("codex_event_item_completed", "item_id", &item_id)?;
            ensure_non_empty_field("codex_event_item_completed", "tool_name", &tool_name)?;
            ensure_non_empty_field(
                "codex_event_item_completed",
                "output_summary",
                &output_summary,
            )?;
            Ok((
                BackendEventKind::ToolResult,
                BTreeMap::from([
                    ("tool_call_id".to_string(), item_id),
                    ("tool_name".to_string(), tool_name),
                    ("output".to_string(), output_summary),
                    ("is_error".to_string(), is_error.to_string()),
                ]),
            ))
        }
    }
}

fn normalize_codex_request(sequence: u64, request: CodexRequest) -> CrabResult<BackendEvent> {
    let payload = match request {
        CodexRequest::CommandExecutionApproval {
            request_id,
            thread_id,
            turn_id,
            item_id,
            command,
            reason,
        } => {
            ensure_non_empty_field(
                "codex_event_request_command_approval",
                "request_id",
                &request_id,
            )?;
            ensure_non_empty_field(
                "codex_event_request_command_approval",
                "thread_id",
                &thread_id,
            )?;
            ensure_non_empty_field("codex_event_request_command_approval", "turn_id", &turn_id)?;
            ensure_non_empty_field("codex_event_request_command_approval", "item_id", &item_id)?;

            let mut payload = BTreeMap::from([
                ("event".to_string(), "approval.requested".to_string()),
                (
                    "request_method".to_string(),
                    "item/commandExecution/requestApproval".to_string(),
                ),
                ("request_id".to_string(), request_id),
                ("thread_id".to_string(), thread_id),
                ("turn_id".to_string(), turn_id),
                ("item_id".to_string(), item_id),
            ]);
            if let Some(command) = command {
                payload.insert("command".to_string(), command);
            }
            if let Some(reason) = reason {
                payload.insert("reason".to_string(), reason);
            }
            payload
        }
        CodexRequest::FileChangeApproval {
            request_id,
            thread_id,
            turn_id,
            item_id,
            grant_root,
            reason,
        } => {
            ensure_non_empty_field(
                "codex_event_request_file_approval",
                "request_id",
                &request_id,
            )?;
            ensure_non_empty_field("codex_event_request_file_approval", "thread_id", &thread_id)?;
            ensure_non_empty_field("codex_event_request_file_approval", "turn_id", &turn_id)?;
            ensure_non_empty_field("codex_event_request_file_approval", "item_id", &item_id)?;

            let mut payload = BTreeMap::from([
                ("event".to_string(), "approval.requested".to_string()),
                (
                    "request_method".to_string(),
                    "item/fileChange/requestApproval".to_string(),
                ),
                ("request_id".to_string(), request_id),
                ("thread_id".to_string(), thread_id),
                ("turn_id".to_string(), turn_id),
                ("item_id".to_string(), item_id),
            ]);
            if let Some(grant_root) = grant_root {
                payload.insert("grant_root".to_string(), grant_root);
            }
            if let Some(reason) = reason {
                payload.insert("reason".to_string(), reason);
            }
            payload
        }
        CodexRequest::ToolUserInput {
            request_id,
            thread_id,
            turn_id,
            item_id,
            question_count,
        } => {
            ensure_non_empty_field("codex_event_request_user_input", "request_id", &request_id)?;
            ensure_non_empty_field("codex_event_request_user_input", "thread_id", &thread_id)?;
            ensure_non_empty_field("codex_event_request_user_input", "turn_id", &turn_id)?;
            ensure_non_empty_field("codex_event_request_user_input", "item_id", &item_id)?;
            if question_count == 0 {
                return Err(CrabError::InvariantViolation {
                    context: "codex_event_request_user_input",
                    message: "question_count must be greater than zero".to_string(),
                });
            }

            BTreeMap::from([
                ("event".to_string(), "user_input.requested".to_string()),
                (
                    "request_method".to_string(),
                    "item/tool/requestUserInput".to_string(),
                ),
                ("request_id".to_string(), request_id),
                ("thread_id".to_string(), thread_id),
                ("turn_id".to_string(), turn_id),
                ("item_id".to_string(), item_id),
                ("question_count".to_string(), question_count.to_string()),
            ])
        }
    };

    Ok(BackendEvent {
        sequence,
        kind: BackendEventKind::RunNote,
        payload,
    })
}

fn sequence_number(index: usize) -> u64 {
    let base = u64::try_from(index).unwrap_or(u64::MAX);
    base.saturating_add(1)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crab_core::CrabError;

    use super::{
        normalize_codex_events, CodexCompletedItem, CodexNotification, CodexRawEvent, CodexRequest,
        CodexTurnStatus,
    };
    use crate::{BackendEvent, BackendEventKind};

    fn fixture_raw_events() -> Vec<CodexRawEvent> {
        vec![
            CodexRawEvent::Notification(CodexNotification::ThreadStarted {
                thread_id: "thread-1".to_string(),
            }),
            CodexRawEvent::Notification(CodexNotification::TurnStarted {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
            }),
            CodexRawEvent::Notification(CodexNotification::AgentMessageDelta {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                item_id: "item-1".to_string(),
                delta: "working".to_string(),
            }),
            CodexRawEvent::Notification(CodexNotification::ItemCompleted {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                item: CodexCompletedItem::ToolCall {
                    item_id: "call-1".to_string(),
                    tool_name: "bash".to_string(),
                    input_summary: "{\"cmd\":\"ls\"}".to_string(),
                },
            }),
            CodexRawEvent::Request(CodexRequest::CommandExecutionApproval {
                request_id: "req-1".to_string(),
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                item_id: "call-1".to_string(),
                command: Some("ls".to_string()),
                reason: Some("needs read access".to_string()),
            }),
            CodexRawEvent::Request(CodexRequest::FileChangeApproval {
                request_id: "req-2".to_string(),
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                item_id: "patch-1".to_string(),
                grant_root: Some("/repo".to_string()),
                reason: None,
            }),
            CodexRawEvent::Request(CodexRequest::ToolUserInput {
                request_id: "req-3".to_string(),
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                item_id: "tool-1".to_string(),
                question_count: 2,
            }),
            CodexRawEvent::Notification(CodexNotification::ItemCompleted {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                item: CodexCompletedItem::ToolResult {
                    item_id: "call-1".to_string(),
                    tool_name: "bash".to_string(),
                    output_summary: "ok".to_string(),
                    is_error: false,
                },
            }),
            CodexRawEvent::Notification(CodexNotification::ItemCompleted {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                item: CodexCompletedItem::AgentMessage {
                    item_id: "item-2".to_string(),
                    text: "done".to_string(),
                },
            }),
            CodexRawEvent::Notification(CodexNotification::TokenUsageUpdated {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                input_tokens: 7,
                output_tokens: 5,
                total_tokens: 12,
            }),
            CodexRawEvent::Notification(CodexNotification::TurnCompleted {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                status: CodexTurnStatus::Completed,
                error_message: None,
            }),
        ]
    }

    fn snapshot(events: &[BackendEvent]) -> String {
        events
            .iter()
            .map(|event| {
                let payload = event
                    .payload
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect::<Vec<_>>()
                    .join(";");
                format!("{}|{:?}|{}", event.sequence, event.kind, payload)
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn codex_event_normalization_snapshot() {
        let events = normalize_codex_events(fixture_raw_events())
            .expect("fixture normalization should pass");

        assert_eq!(
            snapshot(&events),
            "1|RunNote|event=thread.started;thread_id=thread-1\n\
2|RunNote|event=turn.started;thread_id=thread-1;turn_id=turn-1\n\
3|TextDelta|delta=working\n\
4|ToolCall|input_json={\"cmd\":\"ls\"};tool_call_id=call-1;tool_name=bash\n\
5|RunNote|command=ls;event=approval.requested;item_id=call-1;reason=needs read access;request_id=req-1;request_method=item/commandExecution/requestApproval;thread_id=thread-1;turn_id=turn-1\n\
6|RunNote|event=approval.requested;grant_root=/repo;item_id=patch-1;request_id=req-2;request_method=item/fileChange/requestApproval;thread_id=thread-1;turn_id=turn-1\n\
7|RunNote|event=user_input.requested;item_id=tool-1;question_count=2;request_id=req-3;request_method=item/tool/requestUserInput;thread_id=thread-1;turn_id=turn-1\n\
8|ToolResult|is_error=false;output=ok;tool_call_id=call-1;tool_name=bash\n\
9|TextDelta|delta=done\n\
10|RunNote|event=thread.token_usage.updated;thread_id=thread-1;turn_id=turn-1;usage_input_tokens=7;usage_output_tokens=5;usage_source=codex;usage_total_tokens=12\n\
11|TurnCompleted|stop_reason=completed;thread_id=thread-1;turn_id=turn-1"
        );
    }

    #[test]
    fn turn_completed_status_mappings_are_deterministic() {
        let interrupted = normalize_codex_events(vec![CodexRawEvent::Notification(
            CodexNotification::TurnCompleted {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                status: CodexTurnStatus::Interrupted,
                error_message: None,
            },
        )])
        .expect("interrupted should normalize");
        assert_eq!(
            interrupted,
            vec![BackendEvent {
                sequence: 1,
                kind: BackendEventKind::TurnInterrupted,
                payload: BTreeMap::from([("reason".to_string(), "interrupted".to_string())]),
            }]
        );

        let failed = normalize_codex_events(vec![CodexRawEvent::Notification(
            CodexNotification::TurnCompleted {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                status: CodexTurnStatus::Failed,
                error_message: None,
            },
        )])
        .expect("failed should normalize");
        assert_eq!(
            failed,
            vec![BackendEvent {
                sequence: 1,
                kind: BackendEventKind::Error,
                payload: BTreeMap::from([("message".to_string(), "turn failed".to_string())]),
            }]
        );
    }

    #[test]
    fn error_notification_maps_to_error_event() {
        let events = normalize_codex_events(vec![CodexRawEvent::Notification(
            CodexNotification::Error {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-9".to_string(),
                message: "backend stream lost".to_string(),
                will_retry: true,
            },
        )])
        .expect("error notification should normalize");

        assert_eq!(
            events,
            vec![BackendEvent {
                sequence: 1,
                kind: BackendEventKind::Error,
                payload: BTreeMap::from([
                    ("message".to_string(), "backend stream lost".to_string()),
                    ("thread_id".to_string(), "thread-1".to_string()),
                    ("turn_id".to_string(), "turn-9".to_string()),
                    ("will_retry".to_string(), "true".to_string()),
                ]),
            }]
        );
    }

    #[test]
    fn request_payloads_skip_missing_optional_fields() {
        let events = normalize_codex_events(vec![
            CodexRawEvent::Request(CodexRequest::CommandExecutionApproval {
                request_id: "req-1".to_string(),
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                item_id: "item-1".to_string(),
                command: None,
                reason: None,
            }),
            CodexRawEvent::Request(CodexRequest::FileChangeApproval {
                request_id: "req-2".to_string(),
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                item_id: "item-2".to_string(),
                grant_root: None,
                reason: Some("touches protected paths".to_string()),
            }),
        ])
        .expect("requests should normalize");

        assert!(!events[0].payload.contains_key("command"));
        assert!(!events[0].payload.contains_key("reason"));
        assert!(!events[1].payload.contains_key("grant_root"));
        assert_eq!(
            events[1].payload.get("reason"),
            Some(&"touches protected paths".to_string())
        );
    }

    #[test]
    fn token_usage_validation_rejects_invalid_totals() {
        let lower_total = normalize_codex_events(vec![CodexRawEvent::Notification(
            CodexNotification::TokenUsageUpdated {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                input_tokens: 5,
                output_tokens: 5,
                total_tokens: 9,
            },
        )])
        .expect_err("total lower than input+output should fail");
        assert_eq!(
            lower_total,
            CrabError::InvariantViolation {
                context: "codex_event_token_usage",
                message: "total_tokens 9 is lower than input+output 10".to_string(),
            }
        );

        let overflow = normalize_codex_events(vec![CodexRawEvent::Notification(
            CodexNotification::TokenUsageUpdated {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                input_tokens: u64::MAX,
                output_tokens: 1,
                total_tokens: u64::MAX,
            },
        )])
        .expect_err("overflow should fail");
        assert_eq!(
            overflow,
            CrabError::InvariantViolation {
                context: "codex_event_token_usage",
                message: "input/output token addition overflow".to_string(),
            }
        );
    }

    #[test]
    fn codex_event_normalization_rejects_invalid_payloads() {
        let invalid_cases = vec![
            (
                CodexRawEvent::Notification(CodexNotification::ThreadStarted {
                    thread_id: " ".to_string(),
                }),
                "codex_event_thread_started",
                "thread_id must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::TurnStarted {
                    thread_id: "thread-1".to_string(),
                    turn_id: " ".to_string(),
                }),
                "codex_event_turn_started",
                "turn_id must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::AgentMessageDelta {
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-1".to_string(),
                    delta: " ".to_string(),
                }),
                "codex_event_agent_message_delta",
                "delta must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::ItemCompleted {
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item: CodexCompletedItem::ToolCall {
                        item_id: "item-1".to_string(),
                        tool_name: " ".to_string(),
                        input_summary: "{}".to_string(),
                    },
                }),
                "codex_event_item_completed",
                "tool_name must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::TurnCompleted {
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    status: CodexTurnStatus::InProgress,
                    error_message: None,
                }),
                "codex_event_turn_completed",
                "turn/completed received inProgress status",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::Error {
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    message: " ".to_string(),
                    will_retry: false,
                }),
                "codex_event_error",
                "message must not be empty",
            ),
            (
                CodexRawEvent::Request(CodexRequest::CommandExecutionApproval {
                    request_id: " ".to_string(),
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-1".to_string(),
                    command: None,
                    reason: None,
                }),
                "codex_event_request_command_approval",
                "request_id must not be empty",
            ),
            (
                CodexRawEvent::Request(CodexRequest::FileChangeApproval {
                    request_id: "req-1".to_string(),
                    thread_id: "thread-1".to_string(),
                    turn_id: " ".to_string(),
                    item_id: "item-1".to_string(),
                    grant_root: None,
                    reason: None,
                }),
                "codex_event_request_file_approval",
                "turn_id must not be empty",
            ),
            (
                CodexRawEvent::Request(CodexRequest::ToolUserInput {
                    request_id: "req-1".to_string(),
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-1".to_string(),
                    question_count: 0,
                }),
                "codex_event_request_user_input",
                "question_count must be greater than zero",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::TurnStarted {
                    thread_id: " ".to_string(),
                    turn_id: "turn-1".to_string(),
                }),
                "codex_event_turn_started",
                "thread_id must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::AgentMessageDelta {
                    thread_id: " ".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-1".to_string(),
                    delta: "ok".to_string(),
                }),
                "codex_event_agent_message_delta",
                "thread_id must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::AgentMessageDelta {
                    thread_id: "thread-1".to_string(),
                    turn_id: " ".to_string(),
                    item_id: "item-1".to_string(),
                    delta: "ok".to_string(),
                }),
                "codex_event_agent_message_delta",
                "turn_id must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::AgentMessageDelta {
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: " ".to_string(),
                    delta: "ok".to_string(),
                }),
                "codex_event_agent_message_delta",
                "item_id must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::ItemCompleted {
                    thread_id: " ".to_string(),
                    turn_id: "turn-1".to_string(),
                    item: CodexCompletedItem::AgentMessage {
                        item_id: "item-1".to_string(),
                        text: "ok".to_string(),
                    },
                }),
                "codex_event_item_completed",
                "thread_id must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::ItemCompleted {
                    thread_id: "thread-1".to_string(),
                    turn_id: " ".to_string(),
                    item: CodexCompletedItem::AgentMessage {
                        item_id: "item-1".to_string(),
                        text: "ok".to_string(),
                    },
                }),
                "codex_event_item_completed",
                "turn_id must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::TurnCompleted {
                    thread_id: " ".to_string(),
                    turn_id: "turn-1".to_string(),
                    status: CodexTurnStatus::Completed,
                    error_message: None,
                }),
                "codex_event_turn_completed",
                "thread_id must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::TurnCompleted {
                    thread_id: "thread-1".to_string(),
                    turn_id: " ".to_string(),
                    status: CodexTurnStatus::Completed,
                    error_message: None,
                }),
                "codex_event_turn_completed",
                "turn_id must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::TokenUsageUpdated {
                    thread_id: " ".to_string(),
                    turn_id: "turn-1".to_string(),
                    input_tokens: 1,
                    output_tokens: 1,
                    total_tokens: 2,
                }),
                "codex_event_token_usage",
                "thread_id must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::TokenUsageUpdated {
                    thread_id: "thread-1".to_string(),
                    turn_id: " ".to_string(),
                    input_tokens: 1,
                    output_tokens: 1,
                    total_tokens: 2,
                }),
                "codex_event_token_usage",
                "turn_id must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::Error {
                    thread_id: " ".to_string(),
                    turn_id: "turn-1".to_string(),
                    message: "boom".to_string(),
                    will_retry: false,
                }),
                "codex_event_error",
                "thread_id must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::Error {
                    thread_id: "thread-1".to_string(),
                    turn_id: " ".to_string(),
                    message: "boom".to_string(),
                    will_retry: false,
                }),
                "codex_event_error",
                "turn_id must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::ItemCompleted {
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item: CodexCompletedItem::AgentMessage {
                        item_id: " ".to_string(),
                        text: "ok".to_string(),
                    },
                }),
                "codex_event_item_completed",
                "item_id must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::ItemCompleted {
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item: CodexCompletedItem::AgentMessage {
                        item_id: "item-1".to_string(),
                        text: " ".to_string(),
                    },
                }),
                "codex_event_item_completed",
                "text must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::ItemCompleted {
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item: CodexCompletedItem::ToolCall {
                        item_id: " ".to_string(),
                        tool_name: "bash".to_string(),
                        input_summary: "{}".to_string(),
                    },
                }),
                "codex_event_item_completed",
                "item_id must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::ItemCompleted {
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item: CodexCompletedItem::ToolCall {
                        item_id: "item-1".to_string(),
                        tool_name: "bash".to_string(),
                        input_summary: " ".to_string(),
                    },
                }),
                "codex_event_item_completed",
                "input_summary must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::ItemCompleted {
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item: CodexCompletedItem::ToolResult {
                        item_id: " ".to_string(),
                        tool_name: "bash".to_string(),
                        output_summary: "ok".to_string(),
                        is_error: false,
                    },
                }),
                "codex_event_item_completed",
                "item_id must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::ItemCompleted {
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item: CodexCompletedItem::ToolResult {
                        item_id: "item-1".to_string(),
                        tool_name: " ".to_string(),
                        output_summary: "ok".to_string(),
                        is_error: false,
                    },
                }),
                "codex_event_item_completed",
                "tool_name must not be empty",
            ),
            (
                CodexRawEvent::Notification(CodexNotification::ItemCompleted {
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item: CodexCompletedItem::ToolResult {
                        item_id: "item-1".to_string(),
                        tool_name: "bash".to_string(),
                        output_summary: " ".to_string(),
                        is_error: false,
                    },
                }),
                "codex_event_item_completed",
                "output_summary must not be empty",
            ),
            (
                CodexRawEvent::Request(CodexRequest::CommandExecutionApproval {
                    request_id: "req-1".to_string(),
                    thread_id: " ".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-1".to_string(),
                    command: None,
                    reason: None,
                }),
                "codex_event_request_command_approval",
                "thread_id must not be empty",
            ),
            (
                CodexRawEvent::Request(CodexRequest::CommandExecutionApproval {
                    request_id: "req-1".to_string(),
                    thread_id: "thread-1".to_string(),
                    turn_id: " ".to_string(),
                    item_id: "item-1".to_string(),
                    command: None,
                    reason: None,
                }),
                "codex_event_request_command_approval",
                "turn_id must not be empty",
            ),
            (
                CodexRawEvent::Request(CodexRequest::CommandExecutionApproval {
                    request_id: "req-1".to_string(),
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: " ".to_string(),
                    command: None,
                    reason: None,
                }),
                "codex_event_request_command_approval",
                "item_id must not be empty",
            ),
            (
                CodexRawEvent::Request(CodexRequest::FileChangeApproval {
                    request_id: " ".to_string(),
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-1".to_string(),
                    grant_root: None,
                    reason: None,
                }),
                "codex_event_request_file_approval",
                "request_id must not be empty",
            ),
            (
                CodexRawEvent::Request(CodexRequest::FileChangeApproval {
                    request_id: "req-1".to_string(),
                    thread_id: " ".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-1".to_string(),
                    grant_root: None,
                    reason: None,
                }),
                "codex_event_request_file_approval",
                "thread_id must not be empty",
            ),
            (
                CodexRawEvent::Request(CodexRequest::FileChangeApproval {
                    request_id: "req-1".to_string(),
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: " ".to_string(),
                    grant_root: None,
                    reason: None,
                }),
                "codex_event_request_file_approval",
                "item_id must not be empty",
            ),
            (
                CodexRawEvent::Request(CodexRequest::ToolUserInput {
                    request_id: " ".to_string(),
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-1".to_string(),
                    question_count: 1,
                }),
                "codex_event_request_user_input",
                "request_id must not be empty",
            ),
            (
                CodexRawEvent::Request(CodexRequest::ToolUserInput {
                    request_id: "req-1".to_string(),
                    thread_id: " ".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-1".to_string(),
                    question_count: 1,
                }),
                "codex_event_request_user_input",
                "thread_id must not be empty",
            ),
            (
                CodexRawEvent::Request(CodexRequest::ToolUserInput {
                    request_id: "req-1".to_string(),
                    thread_id: "thread-1".to_string(),
                    turn_id: " ".to_string(),
                    item_id: "item-1".to_string(),
                    question_count: 1,
                }),
                "codex_event_request_user_input",
                "turn_id must not be empty",
            ),
            (
                CodexRawEvent::Request(CodexRequest::ToolUserInput {
                    request_id: "req-1".to_string(),
                    thread_id: "thread-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: " ".to_string(),
                    question_count: 1,
                }),
                "codex_event_request_user_input",
                "item_id must not be empty",
            ),
        ];

        for (raw_event, context, message) in invalid_cases {
            let error = normalize_codex_events(vec![raw_event])
                .expect_err("invalid raw event should fail normalization");
            assert_eq!(
                error,
                CrabError::InvariantViolation {
                    context,
                    message: message.to_string(),
                }
            );
        }
    }
}
