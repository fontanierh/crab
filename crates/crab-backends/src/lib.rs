//! Unified backend contract and adapter harness for Crab.

use std::collections::BTreeMap;
use std::pin::Pin;

use async_trait::async_trait;
use crab_core::{CrabError, CrabResult, InferenceProfile, PhysicalSession};
use futures_core::Stream;

pub mod claude;
pub mod profile_mapping;

pub use claude::{ClaudeBackend, ClaudeProcess};
pub use profile_mapping::{
    map_claude_inference_profile, ClaudeInferenceConfig, ClaudeThinkingMode,
};

pub type BackendEventStream = Pin<Box<dyn Stream<Item = BackendEvent> + Send>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendEventKind {
    TextDelta,
    ToolCall,
    ToolResult,
    RunNote,
    TurnCompleted,
    TurnInterrupted,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendEvent {
    pub sequence: u64,
    pub kind: BackendEventKind,
    pub payload: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionContext {
    pub logical_session_id: String,
    pub profile: InferenceProfile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnInput {
    pub run_id: String,
    pub turn_id: String,
    pub user_input: String,
}

#[async_trait]
pub trait Backend: Send + Sync {
    async fn create_session(&self, context: &SessionContext) -> CrabResult<PhysicalSession>;

    async fn send_turn(
        &self,
        session: &mut PhysicalSession,
        input: TurnInput,
    ) -> CrabResult<BackendEventStream>;

    async fn interrupt_turn(&self, session: &PhysicalSession, turn_id: &str) -> CrabResult<()>;

    async fn end_session(&self, session: &PhysicalSession) -> CrabResult<()>;
}

#[derive(Debug, Clone)]
pub struct BackendHarness<B: Backend> {
    backend: B,
}

impl<B: Backend> BackendHarness<B> {
    #[must_use]
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub async fn create_session(&self, context: &SessionContext) -> CrabResult<PhysicalSession> {
        validate_session_context(context)?;
        let session = self.backend.create_session(context).await?;
        validate_physical_session(&session)?;
        if session.logical_session_id != context.logical_session_id {
            return Err(CrabError::InvariantViolation {
                context: "backend_contract_create_session",
                message: format!(
                    "session logical_session_id {} does not match context {}",
                    session.logical_session_id, context.logical_session_id
                ),
            });
        }
        if session.backend != context.profile.backend {
            return Err(CrabError::InvariantViolation {
                context: "backend_contract_create_session",
                message: format!(
                    "session backend {:?} does not match profile backend {:?}",
                    session.backend, context.profile.backend
                ),
            });
        }
        Ok(session)
    }

    pub async fn send_turn(
        &self,
        session: &mut PhysicalSession,
        input: TurnInput,
    ) -> CrabResult<BackendEventStream> {
        validate_physical_session(session)?;
        validate_turn_input(&input)?;
        self.backend.send_turn(session, input).await
    }

    pub async fn interrupt_turn(&self, session: &PhysicalSession, turn_id: &str) -> CrabResult<()> {
        validate_physical_session(session)?;
        ensure_non_empty_field("backend_interrupt_turn", "turn_id", turn_id)?;
        self.backend.interrupt_turn(session, turn_id).await
    }

    pub async fn end_session(&self, session: &PhysicalSession) -> CrabResult<()> {
        validate_physical_session(session)?;
        self.backend.end_session(session).await
    }
}

fn validate_session_context(context: &SessionContext) -> CrabResult<()> {
    ensure_non_empty_field(
        "backend_session_context_validate",
        "logical_session_id",
        &context.logical_session_id,
    )
}

fn validate_physical_session(session: &PhysicalSession) -> CrabResult<()> {
    ensure_non_empty_field("backend_physical_session_validate", "id", &session.id)?;
    ensure_non_empty_field(
        "backend_physical_session_validate",
        "logical_session_id",
        &session.logical_session_id,
    )?;
    ensure_non_empty_field(
        "backend_physical_session_validate",
        "backend_session_id",
        &session.backend_session_id,
    )?;
    Ok(())
}

fn validate_turn_input(input: &TurnInput) -> CrabResult<()> {
    ensure_non_empty_field("backend_turn_input_validate", "run_id", &input.run_id)?;
    ensure_non_empty_field("backend_turn_input_validate", "turn_id", &input.turn_id)?;
    ensure_non_empty_field(
        "backend_turn_input_validate",
        "user_input",
        &input.user_input,
    )?;
    Ok(())
}

pub(crate) fn ensure_non_empty_field(
    context: &'static str,
    field_name: &'static str,
    value: &str,
) -> CrabResult<()> {
    if value.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context,
            message: format!("{field_name} must not be empty"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use crab_core::{BackendKind, CrabError, InferenceProfile, PhysicalSession, ReasoningLevel};
    use futures::executor::block_on;
    use futures::stream;
    use futures::StreamExt;

    use super::{
        Backend, BackendEvent, BackendEventKind, BackendEventStream, BackendHarness,
        SessionContext, TurnInput,
    };

    #[derive(Debug, Clone)]
    struct FakeBackend {
        state: Arc<Mutex<FakeState>>,
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    struct FakeStats {
        create_calls: usize,
        send_calls: usize,
        interrupt_calls: usize,
        end_calls: usize,
        last_interrupted_turn_id: Option<String>,
    }

    #[derive(Debug, Clone)]
    struct FakeState {
        session: PhysicalSession,
        events: Vec<BackendEvent>,
        stats: FakeStats,
        create_error: Option<CrabError>,
        send_error: Option<CrabError>,
        interrupt_error: Option<CrabError>,
        end_error: Option<CrabError>,
    }

    impl FakeBackend {
        fn new(session: PhysicalSession, events: Vec<BackendEvent>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeState {
                    session,
                    events,
                    stats: FakeStats::default(),
                    create_error: None,
                    send_error: None,
                    interrupt_error: None,
                    end_error: None,
                })),
            }
        }

        fn set_create_error(&self, error: CrabError) {
            let mut state = self.state.lock().expect("lock should succeed");
            state.create_error = Some(error);
        }

        fn set_send_error(&self, error: CrabError) {
            let mut state = self.state.lock().expect("lock should succeed");
            state.send_error = Some(error);
        }

        fn set_interrupt_error(&self, error: CrabError) {
            let mut state = self.state.lock().expect("lock should succeed");
            state.interrupt_error = Some(error);
        }

        fn set_end_error(&self, error: CrabError) {
            let mut state = self.state.lock().expect("lock should succeed");
            state.end_error = Some(error);
        }

        fn stats(&self) -> FakeStats {
            self.state
                .lock()
                .expect("lock should succeed")
                .stats
                .clone()
        }
    }

    #[async_trait]
    impl Backend for FakeBackend {
        async fn create_session(
            &self,
            _context: &SessionContext,
        ) -> crab_core::CrabResult<PhysicalSession> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.create_calls += 1;
            if let Some(error) = state.create_error.clone() {
                return Err(error);
            }
            Ok(state.session.clone())
        }

        async fn send_turn(
            &self,
            session: &mut PhysicalSession,
            input: TurnInput,
        ) -> crab_core::CrabResult<BackendEventStream> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.send_calls += 1;
            if let Some(error) = state.send_error.clone() {
                return Err(error);
            }
            session.last_turn_id = Some(input.turn_id);
            let events = state.events.clone();
            Ok(Box::pin(stream::iter(events)))
        }

        async fn interrupt_turn(
            &self,
            _session: &PhysicalSession,
            turn_id: &str,
        ) -> crab_core::CrabResult<()> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.interrupt_calls += 1;
            state.stats.last_interrupted_turn_id = Some(turn_id.to_string());
            if let Some(error) = state.interrupt_error.clone() {
                return Err(error);
            }
            Ok(())
        }

        async fn end_session(&self, _session: &PhysicalSession) -> crab_core::CrabResult<()> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.end_calls += 1;
            if let Some(error) = state.end_error.clone() {
                return Err(error);
            }
            Ok(())
        }
    }

    fn run<T>(future: impl Future<Output = T>) -> T {
        block_on(future)
    }

    fn profile() -> InferenceProfile {
        InferenceProfile {
            backend: BackendKind::Claude,
            model: "claude-sonnet".to_string(),
            reasoning_level: ReasoningLevel::High,
        }
    }

    fn session_context() -> SessionContext {
        SessionContext {
            logical_session_id: "discord:channel:abc".to_string(),
            profile: profile(),
        }
    }

    fn physical_session() -> PhysicalSession {
        PhysicalSession {
            id: "physical-1".to_string(),
            logical_session_id: "discord:channel:abc".to_string(),
            backend: BackendKind::Claude,
            backend_session_id: "thread-1".to_string(),
            created_at_epoch_ms: 1_000,
            last_turn_id: None,
        }
    }

    fn turn_input() -> TurnInput {
        TurnInput {
            run_id: "run-1".to_string(),
            turn_id: "turn-1".to_string(),
            user_input: "hello".to_string(),
        }
    }

    fn harness_with_fake(events: Vec<BackendEvent>) -> (FakeBackend, BackendHarness<FakeBackend>) {
        let backend = FakeBackend::new(physical_session(), events);
        let harness = BackendHarness::new(backend.clone());
        (backend, harness)
    }

    fn invalid_session_id() -> PhysicalSession {
        let mut session = physical_session();
        session.id = " ".to_string();
        session
    }

    fn assert_invariant_error(error: CrabError, context: &'static str, message: &str) {
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context,
                message: message.to_string()
            }
        );
    }

    fn assert_create_session_contract_error(
        mutator: impl FnOnce(&mut PhysicalSession),
        expected_message: &str,
    ) {
        let mut mismatched = physical_session();
        mutator(&mut mismatched);
        let backend = FakeBackend::new(mismatched, vec![]);
        let harness = BackendHarness::new(backend);

        let err = run(harness.create_session(&session_context()))
            .expect_err("contract mismatch should fail");
        assert_invariant_error(err, "backend_contract_create_session", expected_message);
    }

    fn text_delta_event(sequence: u64, text: &str) -> BackendEvent {
        BackendEvent {
            sequence,
            kind: BackendEventKind::TextDelta,
            payload: BTreeMap::from([("text".to_string(), text.to_string())]),
        }
    }

    #[test]
    fn create_session_delegates_when_contract_is_valid() {
        let backend = FakeBackend::new(physical_session(), vec![]);
        let harness = BackendHarness::new(backend.clone());
        let context = session_context();

        let created =
            run(harness.create_session(&context)).expect("session creation should succeed");
        assert_eq!(created, physical_session());
        assert_eq!(backend.stats().create_calls, 1);
    }

    #[test]
    fn create_session_requires_context_logical_session_id() {
        let backend = FakeBackend::new(physical_session(), vec![]);
        let harness = BackendHarness::new(backend);
        let mut context = session_context();
        context.logical_session_id = " ".to_string();

        let err = run(harness.create_session(&context)).expect_err("blank context id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "backend_session_context_validate",
                message: "logical_session_id must not be empty".to_string()
            }
        );
    }

    #[test]
    fn create_session_rejects_logical_session_mismatch() {
        assert_create_session_contract_error(
            |session| session.logical_session_id = "discord:channel:other".to_string(),
            "session logical_session_id discord:channel:other does not match context discord:channel:abc",
        );
    }

    #[test]
    fn send_turn_delegates_and_streams_events() {
        let events = vec![text_delta_event(1, "a"), text_delta_event(2, "b")];
        let (backend, harness) = harness_with_fake(events.clone());
        let mut session = physical_session();
        let input = turn_input();

        let stream = run(harness.send_turn(&mut session, input)).expect("send turn should succeed");
        let streamed_events = run(stream.collect::<Vec<_>>());
        assert_eq!(streamed_events, events);
        assert_eq!(session.last_turn_id, Some("turn-1".to_string()));
        assert_eq!(backend.stats().send_calls, 1);
    }

    #[test]
    fn send_turn_requires_non_empty_fields() {
        let backend = FakeBackend::new(physical_session(), vec![]);
        let harness = BackendHarness::new(backend);
        let mut session = physical_session();

        let mut input = turn_input();
        input.run_id = " ".to_string();
        let run_err = run(harness.send_turn(&mut session, input))
            .err()
            .expect("run id should be required");
        assert_eq!(
            run_err,
            CrabError::InvariantViolation {
                context: "backend_turn_input_validate",
                message: "run_id must not be empty".to_string()
            }
        );

        let mut input = turn_input();
        input.turn_id = " ".to_string();
        let turn_err = run(harness.send_turn(&mut session, input))
            .err()
            .expect("turn id should be required");
        assert_eq!(
            turn_err,
            CrabError::InvariantViolation {
                context: "backend_turn_input_validate",
                message: "turn_id must not be empty".to_string()
            }
        );

        let mut input = turn_input();
        input.user_input = " ".to_string();
        let user_err = run(harness.send_turn(&mut session, input))
            .err()
            .expect("user input should be required");
        assert_eq!(
            user_err,
            CrabError::InvariantViolation {
                context: "backend_turn_input_validate",
                message: "user_input must not be empty".to_string()
            }
        );
    }

    #[test]
    fn send_turn_validates_physical_session_shape() {
        let (_, harness) = harness_with_fake(vec![]);
        let mut session = invalid_session_id();

        let err = run(harness.send_turn(&mut session, turn_input()))
            .err()
            .expect("blank session id should fail");
        assert_invariant_error(
            err,
            "backend_physical_session_validate",
            "id must not be empty",
        );
    }

    #[test]
    fn send_turn_requires_physical_session_logical_session_id() {
        let backend = FakeBackend::new(physical_session(), vec![]);
        let harness = BackendHarness::new(backend);
        let mut session = physical_session();
        session.logical_session_id = " ".to_string();

        let err = run(harness.send_turn(&mut session, turn_input()))
            .err()
            .expect("blank logical session id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "backend_physical_session_validate",
                message: "logical_session_id must not be empty".to_string()
            }
        );
    }

    #[test]
    fn send_turn_requires_physical_session_backend_session_id() {
        let backend = FakeBackend::new(physical_session(), vec![]);
        let harness = BackendHarness::new(backend);
        let mut session = physical_session();
        session.backend_session_id = " ".to_string();

        let err = run(harness.send_turn(&mut session, turn_input()))
            .err()
            .expect("blank backend session id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "backend_physical_session_validate",
                message: "backend_session_id must not be empty".to_string()
            }
        );
    }

    #[test]
    fn interrupt_turn_delegates() {
        let (backend, harness) = harness_with_fake(vec![]);
        let session = physical_session();

        run(harness.interrupt_turn(&session, "turn-7")).expect("interrupt should succeed");
        let stats = backend.stats();
        assert_eq!(stats.interrupt_calls, 1);
        assert_eq!(stats.last_interrupted_turn_id, Some("turn-7".to_string()));
    }

    #[test]
    fn interrupt_turn_requires_turn_id() {
        let backend = FakeBackend::new(physical_session(), vec![]);
        let harness = BackendHarness::new(backend);
        let session = physical_session();

        let err =
            run(harness.interrupt_turn(&session, " ")).expect_err("turn id should be required");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "backend_interrupt_turn",
                message: "turn_id must not be empty".to_string()
            }
        );
    }

    #[test]
    fn interrupt_turn_validates_physical_session_shape() {
        let (_, harness) = harness_with_fake(vec![]);
        let session = invalid_session_id();

        let err = run(harness.interrupt_turn(&session, "turn-1"))
            .expect_err("invalid physical session should fail");
        assert_invariant_error(
            err,
            "backend_physical_session_validate",
            "id must not be empty",
        );
    }

    #[test]
    fn end_session_delegates() {
        let (backend, harness) = harness_with_fake(vec![]);
        let session = physical_session();

        run(harness.end_session(&session)).expect("end session should succeed");
        assert_eq!(backend.stats().end_calls, 1);
    }

    #[test]
    fn end_session_validates_physical_session_shape() {
        let (_, harness) = harness_with_fake(vec![]);
        let session = invalid_session_id();

        let err = run(harness.end_session(&session)).expect_err("invalid session should fail");
        assert_invariant_error(
            err,
            "backend_physical_session_validate",
            "id must not be empty",
        );
    }

    #[test]
    fn backend_errors_propagate_through_harness() {
        let backend = FakeBackend::new(physical_session(), vec![]);
        backend.set_create_error(CrabError::InvariantViolation {
            context: "fake_create",
            message: "boom".to_string(),
        });
        let harness = BackendHarness::new(backend.clone());
        let context = session_context();
        let create_err =
            run(harness.create_session(&context)).expect_err("create error should propagate");
        assert_eq!(
            create_err,
            CrabError::InvariantViolation {
                context: "fake_create",
                message: "boom".to_string()
            }
        );

        let backend = FakeBackend::new(physical_session(), vec![]);
        backend.set_send_error(CrabError::InvariantViolation {
            context: "fake_send",
            message: "boom".to_string(),
        });
        let harness = BackendHarness::new(backend);
        let mut session = physical_session();
        let send_err = run(harness.send_turn(&mut session, turn_input()))
            .err()
            .expect("send error should propagate");
        assert_eq!(
            send_err,
            CrabError::InvariantViolation {
                context: "fake_send",
                message: "boom".to_string()
            }
        );

        let backend = FakeBackend::new(physical_session(), vec![]);
        backend.set_interrupt_error(CrabError::InvariantViolation {
            context: "fake_interrupt",
            message: "boom".to_string(),
        });
        let harness = BackendHarness::new(backend);
        let interrupt_err = run(harness.interrupt_turn(&physical_session(), "turn-1"))
            .expect_err("interrupt error should propagate");
        assert_eq!(
            interrupt_err,
            CrabError::InvariantViolation {
                context: "fake_interrupt",
                message: "boom".to_string()
            }
        );

        let backend = FakeBackend::new(physical_session(), vec![]);
        backend.set_end_error(CrabError::InvariantViolation {
            context: "fake_end",
            message: "boom".to_string(),
        });
        let harness = BackendHarness::new(backend);
        let end_err =
            run(harness.end_session(&physical_session())).expect_err("end error should propagate");
        assert_eq!(
            end_err,
            CrabError::InvariantViolation {
                context: "fake_end",
                message: "boom".to_string()
            }
        );
    }

    #[test]
    fn create_session_validates_backend_session_shape() {
        let mut bad_session = physical_session();
        bad_session.id = " ".to_string();
        let backend = FakeBackend::new(bad_session, vec![]);
        let harness = BackendHarness::new(backend);

        let err = run(harness.create_session(&session_context()))
            .expect_err("blank returned session id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "backend_physical_session_validate",
                message: "id must not be empty".to_string()
            }
        );
    }
}
