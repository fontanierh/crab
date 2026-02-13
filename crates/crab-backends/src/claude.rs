use std::collections::BTreeMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use crab_core::{BackendKind, CrabError, CrabResult, PhysicalSession};
use futures_core::Stream;

use crate::{
    ensure_non_empty_field, Backend, BackendEvent, BackendEventKind, BackendEventStream,
    SessionContext, TurnInput,
};

pub type ClaudeRawEventStream = Pin<Box<dyn Stream<Item = ClaudeRawEvent> + Send>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaudeRawEvent {
    TextDelta {
        text: String,
    },
    ToolCall {
        tool_call_id: String,
        tool_name: String,
        input_json: String,
    },
    ToolResult {
        tool_call_id: String,
        tool_name: String,
        output: String,
        is_error: bool,
    },
    Usage {
        input_tokens: u64,
        output_tokens: u64,
        total_tokens: u64,
    },
    RunNote {
        note: String,
    },
    TurnCompleted {
        stop_reason: String,
    },
    TurnInterrupted {
        reason: String,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UsageAccounting {
    input_tokens: u64,
    output_tokens: u64,
    total_tokens: u64,
}

pub trait ClaudeProcess: Send + Sync {
    fn create_session(&self, context: &SessionContext) -> CrabResult<String>;

    fn send_turn(
        &self,
        backend_session_id: &str,
        input: &TurnInput,
    ) -> CrabResult<ClaudeRawEventStream>;

    fn interrupt_turn(&self, backend_session_id: &str, turn_id: &str) -> CrabResult<()>;

    fn end_session(&self, backend_session_id: &str) -> CrabResult<()>;
}

#[derive(Debug, Clone)]
pub struct ClaudeBackend<P: ClaudeProcess> {
    process: P,
}

impl<P: ClaudeProcess> ClaudeBackend<P> {
    #[must_use]
    pub fn new(process: P) -> Self {
        Self { process }
    }
}

#[async_trait]
impl<P: ClaudeProcess> Backend for ClaudeBackend<P> {
    async fn create_session(&self, context: &SessionContext) -> CrabResult<PhysicalSession> {
        let backend_session_id = self.process.create_session(context)?;
        ensure_non_empty_field(
            "claude_backend_create_session",
            "backend_session_id",
            &backend_session_id,
        )?;

        Ok(PhysicalSession {
            id: format!("claude:{backend_session_id}"),
            logical_session_id: context.logical_session_id.clone(),
            backend: BackendKind::Claude,
            backend_session_id,
            created_at_epoch_ms: unix_epoch_ms(),
            last_turn_id: None,
        })
    }

    async fn send_turn(
        &self,
        session: &mut PhysicalSession,
        input: TurnInput,
    ) -> CrabResult<BackendEventStream> {
        ensure_claude_session("claude_backend_send_turn", session)?;
        let raw_events = self
            .process
            .send_turn(&session.backend_session_id, &input)?;
        session.last_turn_id = Some(input.turn_id);
        Ok(Box::pin(ClaudeNormalizeStream::new(raw_events)))
    }

    async fn interrupt_turn(&self, session: &PhysicalSession, turn_id: &str) -> CrabResult<()> {
        ensure_claude_session("claude_backend_interrupt_turn", session)?;
        self.process
            .interrupt_turn(&session.backend_session_id, turn_id)
    }

    async fn end_session(&self, session: &PhysicalSession) -> CrabResult<()> {
        ensure_claude_session("claude_backend_end_session", session)?;
        self.process.end_session(&session.backend_session_id)
    }
}

fn normalize_claude_event(sequence: u64, raw_event: ClaudeRawEvent) -> CrabResult<BackendEvent> {
    let (kind, payload) = match raw_event {
        ClaudeRawEvent::TextDelta { text } => {
            ensure_non_empty_field("claude_event_text_delta", "text", &text)?;
            (
                BackendEventKind::TextDelta,
                BTreeMap::from([("delta".to_string(), text)]),
            )
        }
        ClaudeRawEvent::ToolCall {
            tool_call_id,
            tool_name,
            input_json,
        } => {
            ensure_non_empty_field("claude_event_tool_call", "tool_call_id", &tool_call_id)?;
            ensure_non_empty_field("claude_event_tool_call", "tool_name", &tool_name)?;
            (
                BackendEventKind::ToolCall,
                BTreeMap::from([
                    ("tool_call_id".to_string(), tool_call_id),
                    ("tool_name".to_string(), tool_name),
                    ("input_json".to_string(), input_json),
                ]),
            )
        }
        ClaudeRawEvent::ToolResult {
            tool_call_id,
            tool_name,
            output,
            is_error,
        } => {
            ensure_non_empty_field("claude_event_tool_result", "tool_call_id", &tool_call_id)?;
            ensure_non_empty_field("claude_event_tool_result", "tool_name", &tool_name)?;
            (
                BackendEventKind::ToolResult,
                BTreeMap::from([
                    ("tool_call_id".to_string(), tool_call_id),
                    ("tool_name".to_string(), tool_name),
                    ("output".to_string(), output),
                    ("is_error".to_string(), is_error.to_string()),
                ]),
            )
        }
        ClaudeRawEvent::Usage {
            input_tokens,
            output_tokens,
            total_tokens,
        } => {
            let minimum_total = input_tokens.checked_add(output_tokens).ok_or_else(|| {
                CrabError::InvariantViolation {
                    context: "claude_event_usage",
                    message: "input/output token addition overflow".to_string(),
                }
            })?;
            if total_tokens < minimum_total {
                return Err(CrabError::InvariantViolation {
                    context: "claude_event_usage",
                    message: format!(
                        "total_tokens {total_tokens} is lower than input+output {minimum_total}"
                    ),
                });
            }
            (
                BackendEventKind::RunNote,
                BTreeMap::from([
                    ("usage_input_tokens".to_string(), input_tokens.to_string()),
                    ("usage_output_tokens".to_string(), output_tokens.to_string()),
                    ("usage_total_tokens".to_string(), total_tokens.to_string()),
                ]),
            )
        }
        ClaudeRawEvent::RunNote { note } => {
            ensure_non_empty_field("claude_event_run_note", "note", &note)?;
            (
                BackendEventKind::RunNote,
                BTreeMap::from([("note".to_string(), note)]),
            )
        }
        ClaudeRawEvent::TurnCompleted { stop_reason } => {
            ensure_non_empty_field("claude_event_turn_completed", "stop_reason", &stop_reason)?;
            (
                BackendEventKind::TurnCompleted,
                BTreeMap::from([("stop_reason".to_string(), stop_reason)]),
            )
        }
        ClaudeRawEvent::TurnInterrupted { reason } => {
            ensure_non_empty_field("claude_event_turn_interrupted", "reason", &reason)?;
            (
                BackendEventKind::TurnInterrupted,
                BTreeMap::from([("reason".to_string(), reason)]),
            )
        }
        ClaudeRawEvent::Error { message } => {
            ensure_non_empty_field("claude_event_error", "message", &message)?;
            (
                BackendEventKind::Error,
                BTreeMap::from([("message".to_string(), message)]),
            )
        }
    };

    Ok(BackendEvent {
        sequence,
        kind,
        payload,
    })
}

struct ClaudeNormalizeStream {
    inner: ClaudeRawEventStream,
    next_sequence: u64,
    usage: Option<UsageAccounting>,
    emitted_any: bool,
    saw_terminal: bool,
    done: bool,
}

impl ClaudeNormalizeStream {
    fn new(inner: ClaudeRawEventStream) -> Self {
        Self {
            inner,
            next_sequence: 1,
            usage: None,
            emitted_any: false,
            saw_terminal: false,
            done: false,
        }
    }

    fn make_event(
        &mut self,
        kind: BackendEventKind,
        payload: BTreeMap<String, String>,
    ) -> BackendEvent {
        let event = BackendEvent {
            sequence: self.next_sequence,
            kind,
            payload,
        };
        self.next_sequence = self.next_sequence.saturating_add(1);
        event
    }

    fn make_error_event(&mut self, message: String) -> BackendEvent {
        self.saw_terminal = true;
        self.emitted_any = true;
        self.make_event(
            BackendEventKind::Error,
            BTreeMap::from([("message".to_string(), message)]),
        )
    }

    fn make_turn_completed_fallback(&mut self) -> BackendEvent {
        self.saw_terminal = true;
        self.emitted_any = true;
        self.make_event(
            BackendEventKind::TurnCompleted,
            BTreeMap::from([("stop_reason".to_string(), "completed".to_string())]),
        )
    }

    fn make_run_usage_note(&mut self, usage: UsageAccounting) -> BackendEvent {
        self.make_event(BackendEventKind::RunNote, run_usage_payload(usage))
    }
}

fn run_usage_payload(usage: UsageAccounting) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "run_usage_input_tokens".to_string(),
            usage.input_tokens.to_string(),
        ),
        (
            "run_usage_output_tokens".to_string(),
            usage.output_tokens.to_string(),
        ),
        (
            "run_usage_total_tokens".to_string(),
            usage.total_tokens.to_string(),
        ),
        ("run_usage_source".to_string(), "claude".to_string()),
    ])
}

impl Stream for ClaudeNormalizeStream {
    type Item = BackendEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.done {
                if !self.emitted_any {
                    return Poll::Ready(Some(self.make_error_event(
                        "claude stream produced no assistant/result events".to_string(),
                    )));
                }
                if !self.saw_terminal {
                    return Poll::Ready(Some(self.make_turn_completed_fallback()));
                }
                if let Some(usage) = self.usage.take() {
                    return Poll::Ready(Some(self.make_run_usage_note(usage)));
                }
                return Poll::Ready(None);
            }

            match self.inner.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.done = true;
                    continue;
                }
                Poll::Ready(Some(raw_event)) => match raw_event {
                    ClaudeRawEvent::Usage {
                        input_tokens,
                        output_tokens,
                        total_tokens,
                    } => {
                        let usage = UsageAccounting {
                            input_tokens,
                            output_tokens,
                            total_tokens,
                        };
                        match normalize_claude_event(
                            self.next_sequence,
                            ClaudeRawEvent::Usage {
                                input_tokens,
                                output_tokens,
                                total_tokens,
                            },
                        ) {
                            Ok(event) => {
                                self.usage = Some(usage);
                                self.emitted_any = true;
                                self.next_sequence = self.next_sequence.saturating_add(1);
                                return Poll::Ready(Some(event));
                            }
                            Err(error) => {
                                self.done = true;
                                self.usage = None;
                                return Poll::Ready(Some(self.make_error_event(error.to_string())));
                            }
                        }
                    }
                    ClaudeRawEvent::TurnCompleted { .. }
                    | ClaudeRawEvent::TurnInterrupted { .. }
                    | ClaudeRawEvent::Error { .. } => {
                        self.saw_terminal = true;
                        match normalize_claude_event(self.next_sequence, raw_event) {
                            Ok(event) => {
                                self.emitted_any = true;
                                self.next_sequence = self.next_sequence.saturating_add(1);
                                return Poll::Ready(Some(event));
                            }
                            Err(error) => {
                                self.done = true;
                                self.usage = None;
                                return Poll::Ready(Some(self.make_error_event(error.to_string())));
                            }
                        }
                    }
                    other => match normalize_claude_event(self.next_sequence, other) {
                        Ok(event) => {
                            self.emitted_any = true;
                            self.next_sequence = self.next_sequence.saturating_add(1);
                            return Poll::Ready(Some(event));
                        }
                        Err(error) => {
                            self.done = true;
                            self.usage = None;
                            return Poll::Ready(Some(self.make_error_event(error.to_string())));
                        }
                    },
                },
            }
        }
    }
}

fn ensure_claude_session(context: &'static str, session: &PhysicalSession) -> CrabResult<()> {
    if session.backend != BackendKind::Claude {
        return Err(CrabError::InvariantViolation {
            context,
            message: format!("expected Claude session backend, got {:?}", session.backend),
        });
    }
    ensure_non_empty_field(context, "backend_session_id", &session.backend_session_id)
}

fn unix_epoch_ms() -> u64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};

    use crab_core::{
        BackendKind, CrabError, CrabResult, InferenceProfile, PhysicalSession, ReasoningLevel,
    };
    use futures::executor::block_on;
    use futures::stream;
    use futures::StreamExt;
    use futures_core::Stream;

    use crate::claude::{ClaudeProcess, ClaudeRawEvent, ClaudeRawEventStream};
    use crate::{Backend, BackendEvent, BackendEventKind, SessionContext, TurnInput};

    use super::ClaudeBackend;

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    struct FakeProcessStats {
        create_calls: usize,
        send_calls: usize,
        interrupt_calls: usize,
        end_calls: usize,
        last_send_session_id: Option<String>,
        last_interrupt_turn_id: Option<String>,
    }

    #[derive(Debug, Clone)]
    struct FakeProcess {
        state: Arc<Mutex<FakeProcessState>>,
    }

    #[derive(Debug, Clone)]
    struct FakeProcessState {
        create_session_id: String,
        send_events: Vec<ClaudeRawEvent>,
        stats: FakeProcessStats,
        create_error: Option<CrabError>,
        send_error: Option<CrabError>,
        interrupt_error: Option<CrabError>,
        end_error: Option<CrabError>,
    }

    impl FakeProcess {
        fn new(create_session_id: &str, send_events: Vec<ClaudeRawEvent>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeProcessState {
                    create_session_id: create_session_id.to_string(),
                    send_events,
                    stats: FakeProcessStats::default(),
                    create_error: None,
                    send_error: None,
                    interrupt_error: None,
                    end_error: None,
                })),
            }
        }

        fn stats(&self) -> FakeProcessStats {
            self.state
                .lock()
                .expect("lock should succeed")
                .stats
                .clone()
        }

        fn set_create_error(&self, error: CrabError) {
            self.state.lock().expect("lock should succeed").create_error = Some(error);
        }

        fn set_send_error(&self, error: CrabError) {
            self.state.lock().expect("lock should succeed").send_error = Some(error);
        }

        fn set_interrupt_error(&self, error: CrabError) {
            self.state
                .lock()
                .expect("lock should succeed")
                .interrupt_error = Some(error);
        }

        fn set_end_error(&self, error: CrabError) {
            self.state.lock().expect("lock should succeed").end_error = Some(error);
        }
    }

    impl ClaudeProcess for FakeProcess {
        fn create_session(&self, _context: &SessionContext) -> CrabResult<String> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.create_calls += 1;
            maybe_fail(&state.create_error, state.create_session_id.clone())
        }

        fn send_turn(
            &self,
            backend_session_id: &str,
            _input: &TurnInput,
        ) -> CrabResult<ClaudeRawEventStream> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.send_calls += 1;
            state.stats.last_send_session_id = Some(backend_session_id.to_string());
            let events = maybe_fail(&state.send_error, state.send_events.clone())?;
            Ok(Box::pin(stream::iter(events)))
        }

        fn interrupt_turn(&self, _backend_session_id: &str, turn_id: &str) -> CrabResult<()> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.interrupt_calls += 1;
            state.stats.last_interrupt_turn_id = Some(turn_id.to_string());
            maybe_fail_unit(&state.interrupt_error)
        }

        fn end_session(&self, _backend_session_id: &str) -> CrabResult<()> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.end_calls += 1;
            maybe_fail_unit(&state.end_error)
        }
    }

    fn maybe_fail<T>(error: &Option<CrabError>, success_value: T) -> CrabResult<T> {
        match error.clone() {
            Some(error) => Err(error),
            None => Ok(success_value),
        }
    }

    fn maybe_fail_unit(error: &Option<CrabError>) -> CrabResult<()> {
        maybe_fail(error, ())
    }

    fn session_context() -> SessionContext {
        SessionContext {
            logical_session_id: "discord:channel:abc".to_string(),
            profile: InferenceProfile {
                backend: BackendKind::Claude,
                model: "claude-sonnet".to_string(),
                reasoning_level: ReasoningLevel::Medium,
            },
        }
    }

    fn claude_session() -> PhysicalSession {
        PhysicalSession {
            id: "claude:resume-1".to_string(),
            logical_session_id: "discord:channel:abc".to_string(),
            backend: BackendKind::Claude,
            backend_session_id: "resume-1".to_string(),
            created_at_epoch_ms: 1,
            last_turn_id: None,
        }
    }

    fn codex_session() -> PhysicalSession {
        PhysicalSession {
            backend: BackendKind::Codex,
            ..claude_session()
        }
    }

    fn turn_input() -> TurnInput {
        TurnInput {
            run_id: "run-1".to_string(),
            turn_id: "turn-7".to_string(),
            user_input: "status".to_string(),
        }
    }

    fn normalized_fixture_events() -> Vec<BackendEvent> {
        vec![
            BackendEvent {
                sequence: 1,
                kind: BackendEventKind::TextDelta,
                payload: BTreeMap::from([("delta".to_string(), "hello".to_string())]),
            },
            BackendEvent {
                sequence: 2,
                kind: BackendEventKind::ToolCall,
                payload: BTreeMap::from([
                    ("tool_call_id".to_string(), "call-1".to_string()),
                    ("tool_name".to_string(), "bash".to_string()),
                    ("input_json".to_string(), "{\"cmd\":\"ls\"}".to_string()),
                ]),
            },
            BackendEvent {
                sequence: 3,
                kind: BackendEventKind::ToolResult,
                payload: BTreeMap::from([
                    ("tool_call_id".to_string(), "call-1".to_string()),
                    ("tool_name".to_string(), "bash".to_string()),
                    ("output".to_string(), "ok".to_string()),
                    ("is_error".to_string(), "false".to_string()),
                ]),
            },
            BackendEvent {
                sequence: 4,
                kind: BackendEventKind::RunNote,
                payload: BTreeMap::from([
                    ("usage_input_tokens".to_string(), "7".to_string()),
                    ("usage_output_tokens".to_string(), "5".to_string()),
                    ("usage_total_tokens".to_string(), "12".to_string()),
                ]),
            },
            BackendEvent {
                sequence: 5,
                kind: BackendEventKind::TurnCompleted,
                payload: BTreeMap::from([("stop_reason".to_string(), "end_turn".to_string())]),
            },
            BackendEvent {
                sequence: 6,
                kind: BackendEventKind::RunNote,
                payload: run_usage_payload(7, 5, 12),
            },
        ]
    }

    fn fixture_raw_events() -> Vec<ClaudeRawEvent> {
        vec![
            ClaudeRawEvent::TextDelta {
                text: "hello".to_string(),
            },
            ClaudeRawEvent::ToolCall {
                tool_call_id: "call-1".to_string(),
                tool_name: "bash".to_string(),
                input_json: "{\"cmd\":\"ls\"}".to_string(),
            },
            ClaudeRawEvent::ToolResult {
                tool_call_id: "call-1".to_string(),
                tool_name: "bash".to_string(),
                output: "ok".to_string(),
                is_error: false,
            },
            ClaudeRawEvent::Usage {
                input_tokens: 7,
                output_tokens: 5,
                total_tokens: 12,
            },
            ClaudeRawEvent::TurnCompleted {
                stop_reason: "end_turn".to_string(),
            },
        ]
    }

    fn assert_send_turn_error(raw_event: ClaudeRawEvent, expected_error: CrabError) {
        let events = send_turn_events(vec![raw_event]);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, BackendEventKind::Error);
        assert_eq!(
            events[0].payload.get("message"),
            Some(&expected_error.to_string())
        );
    }

    fn send_turn_events(raw_events: Vec<ClaudeRawEvent>) -> Vec<BackendEvent> {
        let process = FakeProcess::new("resume-1", raw_events);
        let backend = ClaudeBackend::new(process);
        let mut session = claude_session();
        let stream =
            block_on(backend.send_turn(&mut session, turn_input())).expect("send should succeed");
        block_on(stream.collect::<Vec<_>>())
    }

    fn run_usage_payload(
        input_tokens: u64,
        output_tokens: u64,
        total_tokens: u64,
    ) -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                "run_usage_input_tokens".to_string(),
                input_tokens.to_string(),
            ),
            (
                "run_usage_output_tokens".to_string(),
                output_tokens.to_string(),
            ),
            (
                "run_usage_total_tokens".to_string(),
                total_tokens.to_string(),
            ),
            ("run_usage_source".to_string(), "claude".to_string()),
        ])
    }

    #[test]
    fn claude_backend_lifecycle_uses_fixture_streams() {
        let process = FakeProcess::new("resume-1", fixture_raw_events());
        let backend = ClaudeBackend::new(process.clone());

        let mut session = block_on(backend.create_session(&session_context()))
            .expect("create session should succeed");
        assert_eq!(session.id, "claude:resume-1");
        assert_eq!(session.backend_session_id, "resume-1");
        assert_eq!(session.backend, BackendKind::Claude);
        assert!(session.created_at_epoch_ms > 0);

        let stream =
            block_on(backend.send_turn(&mut session, turn_input())).expect("send should succeed");
        let events = block_on(stream.collect::<Vec<_>>());
        assert_eq!(events, normalized_fixture_events());
        assert_eq!(session.last_turn_id, Some("turn-7".to_string()));

        block_on(backend.interrupt_turn(&session, "turn-7")).expect("interrupt should succeed");
        block_on(backend.end_session(&session)).expect("end should succeed");

        let stats = process.stats();
        assert_eq!(stats.create_calls, 1);
        assert_eq!(stats.send_calls, 1);
        assert_eq!(stats.interrupt_calls, 1);
        assert_eq!(stats.end_calls, 1);
        assert_eq!(stats.last_send_session_id, Some("resume-1".to_string()));
        assert_eq!(stats.last_interrupt_turn_id, Some("turn-7".to_string()));
    }

    #[test]
    fn claude_event_normalization_snapshot() {
        let mut fixture_events = fixture_raw_events();
        fixture_events.insert(
            4,
            ClaudeRawEvent::RunNote {
                note: "checkpoint created".to_string(),
            },
        );
        fixture_events.push(ClaudeRawEvent::TurnInterrupted {
            reason: "cancelled".to_string(),
        });
        fixture_events.push(ClaudeRawEvent::Error {
            message: "backend failed".to_string(),
        });
        let events = send_turn_events(fixture_events);

        let snapshot = render_snapshot(&events);
        assert_eq!(
            snapshot,
            "1|TextDelta|delta=hello\n\
2|ToolCall|input_json={\"cmd\":\"ls\"};tool_call_id=call-1;tool_name=bash\n\
3|ToolResult|is_error=false;output=ok;tool_call_id=call-1;tool_name=bash\n\
4|RunNote|usage_input_tokens=7;usage_output_tokens=5;usage_total_tokens=12\n\
5|RunNote|note=checkpoint created\n\
6|TurnCompleted|stop_reason=end_turn\n\
7|TurnInterrupted|reason=cancelled\n\
8|Error|message=backend failed\n\
9|RunNote|run_usage_input_tokens=7;run_usage_output_tokens=5;run_usage_source=claude;run_usage_total_tokens=12"
        );
    }

    #[test]
    fn run_usage_metadata_is_not_emitted_without_usage_events() {
        let raw_events = vec![
            ClaudeRawEvent::TextDelta {
                text: "hello".to_string(),
            },
            ClaudeRawEvent::TurnCompleted {
                stop_reason: "end_turn".to_string(),
            },
        ];
        let events = send_turn_events(raw_events);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, BackendEventKind::TextDelta);
        assert_eq!(events[1].kind, BackendEventKind::TurnCompleted);
    }

    #[test]
    fn run_usage_metadata_uses_latest_usage_event() {
        let raw_events = vec![
            ClaudeRawEvent::Usage {
                input_tokens: 3,
                output_tokens: 2,
                total_tokens: 5,
            },
            ClaudeRawEvent::Usage {
                input_tokens: 10,
                output_tokens: 6,
                total_tokens: 16,
            },
        ];
        let events = send_turn_events(raw_events);
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].kind, BackendEventKind::RunNote);
        assert_eq!(events[1].kind, BackendEventKind::RunNote);
        assert_eq!(events[2].kind, BackendEventKind::TurnCompleted);
        assert_eq!(events[3].kind, BackendEventKind::RunNote);
        assert_eq!(events[3].payload, run_usage_payload(10, 6, 16));
    }

    fn render_snapshot(events: &[BackendEvent]) -> String {
        let mut lines = Vec::with_capacity(events.len());
        for event in events {
            let payload = event
                .payload
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(";");
            lines.push(format!("{}|{:?}|{}", event.sequence, event.kind, payload));
        }
        lines.join("\n")
    }

    #[test]
    fn claude_event_normalization_rejects_invalid_payloads() {
        let invalid_field_cases = vec![
            (
                ClaudeRawEvent::ToolCall {
                    tool_call_id: " ".to_string(),
                    tool_name: "bash".to_string(),
                    input_json: "{}".to_string(),
                },
                "claude_event_tool_call",
                "tool_call_id must not be empty",
            ),
            (
                ClaudeRawEvent::ToolCall {
                    tool_call_id: "call-1".to_string(),
                    tool_name: " ".to_string(),
                    input_json: "{}".to_string(),
                },
                "claude_event_tool_call",
                "tool_name must not be empty",
            ),
            (
                ClaudeRawEvent::ToolResult {
                    tool_call_id: " ".to_string(),
                    tool_name: "bash".to_string(),
                    output: "ok".to_string(),
                    is_error: false,
                },
                "claude_event_tool_result",
                "tool_call_id must not be empty",
            ),
            (
                ClaudeRawEvent::ToolResult {
                    tool_call_id: "call-1".to_string(),
                    tool_name: " ".to_string(),
                    output: "ok".to_string(),
                    is_error: false,
                },
                "claude_event_tool_result",
                "tool_name must not be empty",
            ),
            (
                ClaudeRawEvent::RunNote {
                    note: " ".to_string(),
                },
                "claude_event_run_note",
                "note must not be empty",
            ),
            (
                ClaudeRawEvent::TurnCompleted {
                    stop_reason: " ".to_string(),
                },
                "claude_event_turn_completed",
                "stop_reason must not be empty",
            ),
            (
                ClaudeRawEvent::TurnInterrupted {
                    reason: " ".to_string(),
                },
                "claude_event_turn_interrupted",
                "reason must not be empty",
            ),
            (
                ClaudeRawEvent::Error {
                    message: " ".to_string(),
                },
                "claude_event_error",
                "message must not be empty",
            ),
        ];
        for (event, context, message) in invalid_field_cases {
            assert_send_turn_error(
                event,
                CrabError::InvariantViolation {
                    context,
                    message: message.to_string(),
                },
            );
        }

        assert_send_turn_error(
            ClaudeRawEvent::TextDelta {
                text: " ".to_string(),
            },
            CrabError::InvariantViolation {
                context: "claude_event_text_delta",
                message: "text must not be empty".to_string(),
            },
        );

        assert_send_turn_error(
            ClaudeRawEvent::Usage {
                input_tokens: 10,
                output_tokens: 5,
                total_tokens: 12,
            },
            CrabError::InvariantViolation {
                context: "claude_event_usage",
                message: "total_tokens 12 is lower than input+output 15".to_string(),
            },
        );

        assert_send_turn_error(
            ClaudeRawEvent::Usage {
                input_tokens: u64::MAX,
                output_tokens: 1,
                total_tokens: u64::MAX,
            },
            CrabError::InvariantViolation {
                context: "claude_event_usage",
                message: "input/output token addition overflow".to_string(),
            },
        );
    }

    #[test]
    fn create_session_requires_backend_session_id_from_process() {
        let process = FakeProcess::new(" ", vec![]);
        let backend = ClaudeBackend::new(process);

        let err = block_on(backend.create_session(&session_context()))
            .expect_err("blank backend session id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "claude_backend_create_session",
                message: "backend_session_id must not be empty".to_string()
            }
        );
    }

    #[test]
    fn send_turn_requires_claude_session_backend() {
        let process = FakeProcess::new("resume-1", vec![]);
        let backend = ClaudeBackend::new(process);
        let mut session = codex_session();

        let err = block_on(backend.send_turn(&mut session, turn_input()))
            .err()
            .expect("non-claude session should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "claude_backend_send_turn",
                message: "expected Claude session backend, got Codex".to_string()
            }
        );
    }

    #[test]
    fn interrupt_turn_requires_claude_session_backend() {
        let process = FakeProcess::new("resume-1", vec![]);
        let backend = ClaudeBackend::new(process);
        let session = codex_session();

        let err = block_on(backend.interrupt_turn(&session, "turn-7"))
            .expect_err("non-claude session should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "claude_backend_interrupt_turn",
                message: "expected Claude session backend, got Codex".to_string()
            }
        );
    }

    #[test]
    fn end_session_requires_claude_session_backend() {
        let process = FakeProcess::new("resume-1", vec![]);
        let backend = ClaudeBackend::new(process);
        let session = codex_session();

        let err =
            block_on(backend.end_session(&session)).expect_err("non-claude session should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "claude_backend_end_session",
                message: "expected Claude session backend, got Codex".to_string()
            }
        );
    }

    #[test]
    fn claude_process_errors_propagate() {
        let process = FakeProcess::new("resume-1", vec![]);
        process.set_create_error(CrabError::InvariantViolation {
            context: "fake_claude_create",
            message: "boom".to_string(),
        });
        let backend = ClaudeBackend::new(process.clone());
        let create_err = block_on(backend.create_session(&session_context()))
            .expect_err("create error should propagate");
        assert_eq!(
            create_err,
            CrabError::InvariantViolation {
                context: "fake_claude_create",
                message: "boom".to_string()
            }
        );

        let process = FakeProcess::new("resume-1", vec![]);
        process.set_send_error(CrabError::InvariantViolation {
            context: "fake_claude_send",
            message: "boom".to_string(),
        });
        let backend = ClaudeBackend::new(process);
        let mut session = claude_session();
        let send_err = block_on(backend.send_turn(&mut session, turn_input()))
            .err()
            .expect("send error should propagate");
        assert_eq!(
            send_err,
            CrabError::InvariantViolation {
                context: "fake_claude_send",
                message: "boom".to_string()
            }
        );

        let process = FakeProcess::new("resume-1", vec![]);
        process.set_interrupt_error(CrabError::InvariantViolation {
            context: "fake_claude_interrupt",
            message: "boom".to_string(),
        });
        let backend = ClaudeBackend::new(process);
        let interrupt_err = block_on(backend.interrupt_turn(&claude_session(), "turn-7"))
            .expect_err("interrupt error should propagate");
        assert_eq!(
            interrupt_err,
            CrabError::InvariantViolation {
                context: "fake_claude_interrupt",
                message: "boom".to_string()
            }
        );

        let process = FakeProcess::new("resume-1", vec![]);
        process.set_end_error(CrabError::InvariantViolation {
            context: "fake_claude_end",
            message: "boom".to_string(),
        });
        let backend = ClaudeBackend::new(process);
        let end_err = block_on(backend.end_session(&claude_session()))
            .expect_err("end error should propagate");
        assert_eq!(
            end_err,
            CrabError::InvariantViolation {
                context: "fake_claude_end",
                message: "boom".to_string()
            }
        );
    }

    #[test]
    fn claude_normalization_emits_error_when_stream_produces_no_events() {
        let events = send_turn_events(vec![]);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, BackendEventKind::Error);
        assert_eq!(
            events[0].payload.get("message"),
            Some(&"claude stream produced no assistant/result events".to_string())
        );
    }

    #[test]
    fn claude_normalization_handles_pending_from_inner_stream() {
        #[derive(Debug)]
        struct PendingOnceRawStream {
            polled: bool,
        }

        impl Stream for PendingOnceRawStream {
            type Item = ClaudeRawEvent;

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                if !self.polled {
                    self.polled = true;
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(None)
            }
        }

        #[derive(Debug)]
        struct PendingProcess;

        impl ClaudeProcess for PendingProcess {
            fn create_session(&self, _context: &SessionContext) -> CrabResult<String> {
                Ok("resume-1".to_string())
            }

            fn send_turn(
                &self,
                _backend_session_id: &str,
                _input: &TurnInput,
            ) -> CrabResult<ClaudeRawEventStream> {
                Ok(Box::pin(PendingOnceRawStream { polled: false }))
            }

            fn interrupt_turn(&self, _backend_session_id: &str, _turn_id: &str) -> CrabResult<()> {
                Ok(())
            }

            fn end_session(&self, _backend_session_id: &str) -> CrabResult<()> {
                Ok(())
            }
        }

        let process = PendingProcess;
        let backend_session_id = process
            .create_session(&session_context())
            .expect("create session should succeed");
        process
            .interrupt_turn(&backend_session_id, "turn-1")
            .expect("interrupt should succeed");
        process
            .end_session(&backend_session_id)
            .expect("end should succeed");

        let backend = ClaudeBackend::new(process);
        let mut session = claude_session();
        let stream =
            block_on(backend.send_turn(&mut session, turn_input())).expect("send should succeed");
        let events = block_on(stream.collect::<Vec<_>>());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, BackendEventKind::Error);
    }

    #[test]
    fn send_turn_requires_backend_session_id() {
        let process = FakeProcess::new("resume-1", vec![]);
        let backend = ClaudeBackend::new(process);
        let mut session = claude_session();
        session.backend_session_id = " ".to_string();

        let err = block_on(backend.send_turn(&mut session, turn_input()))
            .err()
            .expect("blank backend session id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "claude_backend_send_turn",
                message: "backend_session_id must not be empty".to_string()
            }
        );
    }
}
