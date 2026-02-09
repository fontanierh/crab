use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use crab_core::{BackendKind, CrabError, CrabResult, PhysicalSession};
use futures::stream;

use crate::{
    ensure_non_empty_field, Backend, BackendEvent, BackendEventStream, SessionContext, TurnInput,
};

pub trait ClaudeProcess: Send + Sync {
    fn create_session(&self, context: &SessionContext) -> CrabResult<String>;

    fn send_turn(
        &self,
        backend_session_id: &str,
        input: &TurnInput,
    ) -> CrabResult<Vec<BackendEvent>>;

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
        let events = self
            .process
            .send_turn(&session.backend_session_id, &input)?;
        session.last_turn_id = Some(input.turn_id);
        Ok(Box::pin(stream::iter(events)))
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
    use std::sync::{Arc, Mutex};

    use crab_core::{
        BackendKind, CrabError, CrabResult, InferenceProfile, PhysicalSession, ReasoningLevel,
    };
    use futures::executor::block_on;
    use futures::StreamExt;

    use crate::claude::ClaudeProcess;
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
        send_events: Vec<BackendEvent>,
        stats: FakeProcessStats,
        create_error: Option<CrabError>,
        send_error: Option<CrabError>,
        interrupt_error: Option<CrabError>,
        end_error: Option<CrabError>,
    }

    impl FakeProcess {
        fn new(create_session_id: &str, send_events: Vec<BackendEvent>) -> Self {
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
        ) -> CrabResult<Vec<BackendEvent>> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.send_calls += 1;
            state.stats.last_send_session_id = Some(backend_session_id.to_string());
            maybe_fail(&state.send_error, state.send_events.clone())
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

    #[test]
    fn claude_backend_lifecycle_uses_fixture_streams() {
        let fixture_events = vec![
            BackendEvent {
                sequence: 1,
                kind: BackendEventKind::TextDelta,
                payload: BTreeMap::from([("delta".to_string(), "a".to_string())]),
            },
            BackendEvent {
                sequence: 2,
                kind: BackendEventKind::TextDelta,
                payload: BTreeMap::from([("delta".to_string(), "b".to_string())]),
            },
        ];
        let process = FakeProcess::new("resume-1", fixture_events.clone());
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
        assert_eq!(events, fixture_events);
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
