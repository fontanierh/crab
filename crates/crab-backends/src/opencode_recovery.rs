use crab_core::CrabResult;

use crate::{
    ensure_non_empty_field, OpenCodeApiTransport, OpenCodeManager, OpenCodeProtocol,
    OpenCodeServerProcess, OpenCodeSessionConfig, OpenCodeTurnConfig,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OpenCodeRecoveryPlan {
    pub session_config: OpenCodeSessionConfig,
    pub checkpoint_prompt: Option<String>,
    pub checkpoint_turn_config: OpenCodeTurnConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCodeRecoveryOutcome {
    pub new_session_id: String,
    pub checkpoint_turn_id: Option<String>,
    pub previous_session_end_error: Option<String>,
}

pub trait OpenCodeRecoveryRuntime: Send + Sync {
    fn create_session(&self, config: OpenCodeSessionConfig) -> CrabResult<String>;
    fn end_session(&self, session_id: &str) -> CrabResult<()>;
    fn send_prompt(
        &self,
        session_id: &str,
        prompt: &str,
        config: OpenCodeTurnConfig,
    ) -> CrabResult<String>;
}

impl<T: OpenCodeApiTransport> OpenCodeRecoveryRuntime for OpenCodeProtocol<T> {
    fn create_session(&self, config: OpenCodeSessionConfig) -> CrabResult<String> {
        OpenCodeProtocol::create_session(self, config)
    }

    fn end_session(&self, session_id: &str) -> CrabResult<()> {
        OpenCodeProtocol::end_session(self, session_id)
    }

    fn send_prompt(
        &self,
        session_id: &str,
        prompt: &str,
        config: OpenCodeTurnConfig,
    ) -> CrabResult<String> {
        OpenCodeProtocol::send_prompt(self, session_id, prompt, config)
    }
}

pub fn recover_opencode_session<P: OpenCodeServerProcess, R: OpenCodeRecoveryRuntime>(
    manager: &mut OpenCodeManager<P>,
    runtime: &R,
    previous_session_id: Option<&str>,
    plan: &OpenCodeRecoveryPlan,
) -> CrabResult<OpenCodeRecoveryOutcome> {
    if let Some(previous_session_id) = previous_session_id {
        ensure_non_empty_field(
            "opencode_recovery_input",
            "previous_session_id",
            previous_session_id,
        )?;
    }
    if let Some(prompt) = plan.checkpoint_prompt.as_deref() {
        ensure_non_empty_field("opencode_recovery_input", "checkpoint_prompt", prompt)?;
    }

    manager.ensure_running()?;

    let previous_session_end_error = if let Some(previous_session_id) = previous_session_id {
        match runtime.end_session(previous_session_id) {
            Ok(()) => None,
            Err(error) => Some(error.to_string()),
        }
    } else {
        None
    };

    let new_session_id = runtime.create_session(plan.session_config.clone())?;
    ensure_non_empty_field(
        "opencode_recovery_output",
        "new_session_id",
        &new_session_id,
    )?;

    let checkpoint_turn_id = if let Some(prompt) = plan.checkpoint_prompt.as_deref() {
        let turn_id =
            runtime.send_prompt(&new_session_id, prompt, plan.checkpoint_turn_config.clone())?;
        ensure_non_empty_field("opencode_recovery_output", "checkpoint_turn_id", &turn_id)?;
        Some(turn_id)
    } else {
        None
    };

    Ok(OpenCodeRecoveryOutcome {
        new_session_id,
        checkpoint_turn_id,
        previous_session_end_error,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use crate::{
        OpenCodeApiRequest, OpenCodeApiResponse, OpenCodeApiTransport, OpenCodeProtocol,
        OpenCodeServerHandle, OpenCodeServerProcess,
    };

    use super::{
        recover_opencode_session, OpenCodeManager, OpenCodeRecoveryOutcome, OpenCodeRecoveryPlan,
        OpenCodeRecoveryRuntime, OpenCodeSessionConfig, OpenCodeTurnConfig,
    };
    use crab_core::{CrabError, CrabResult};

    #[derive(Debug, Clone)]
    struct FakeRuntime {
        state: Arc<Mutex<FakeRuntimeState>>,
    }

    #[derive(Debug, Clone)]
    struct FakeRuntimeState {
        end_results: VecDeque<CrabResult<()>>,
        create_results: VecDeque<CrabResult<String>>,
        send_results: VecDeque<CrabResult<String>>,
        calls: Vec<String>,
    }

    impl FakeRuntime {
        fn new(
            end_results: Vec<CrabResult<()>>,
            create_results: Vec<CrabResult<String>>,
            send_results: Vec<CrabResult<String>>,
        ) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeRuntimeState {
                    end_results: VecDeque::from(end_results),
                    create_results: VecDeque::from(create_results),
                    send_results: VecDeque::from(send_results),
                    calls: Vec::new(),
                })),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.state
                .lock()
                .expect("lock should succeed")
                .calls
                .clone()
        }

        fn missing_script(context: &'static str) -> CrabError {
            CrabError::InvariantViolation {
                context,
                message: "missing scripted response".to_string(),
            }
        }
    }

    impl OpenCodeRecoveryRuntime for FakeRuntime {
        fn create_session(&self, _config: OpenCodeSessionConfig) -> CrabResult<String> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.calls.push("create_session".to_string());
            match state.create_results.pop_front() {
                Some(result) => result,
                None => Err(Self::missing_script("fake_runtime_create")),
            }
        }

        fn end_session(&self, session_id: &str) -> CrabResult<()> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.calls.push(format!("end_session:{session_id}"));
            match state.end_results.pop_front() {
                Some(result) => result,
                None => Err(Self::missing_script("fake_runtime_end")),
            }
        }

        fn send_prompt(
            &self,
            session_id: &str,
            prompt: &str,
            _config: OpenCodeTurnConfig,
        ) -> CrabResult<String> {
            let mut state = self.state.lock().expect("lock should succeed");
            state
                .calls
                .push(format!("send_prompt:{session_id}:{prompt}"));
            match state.send_results.pop_front() {
                Some(result) => result,
                None => Err(Self::missing_script("fake_runtime_send")),
            }
        }
    }

    #[derive(Debug, Clone)]
    struct FakeProcess {
        state: Arc<Mutex<FakeProcessState>>,
    }

    #[derive(Debug, Clone)]
    struct FakeProcessState {
        starts: VecDeque<CrabResult<OpenCodeServerHandle>>,
        health: BTreeMap<u32, bool>,
    }

    impl FakeProcess {
        fn from_starts(starts: Vec<CrabResult<OpenCodeServerHandle>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeProcessState {
                    starts: VecDeque::from(starts),
                    health: BTreeMap::new(),
                })),
            }
        }
    }

    impl OpenCodeServerProcess for FakeProcess {
        fn spawn_server(&self) -> CrabResult<OpenCodeServerHandle> {
            let mut state = self.state.lock().expect("lock should succeed");
            let next = state
                .starts
                .pop_front()
                .ok_or_else(|| CrabError::InvariantViolation {
                    context: "fake_process_spawn",
                    message: "missing scripted start".to_string(),
                })?;
            if let Ok(handle) = &next {
                state.health.entry(handle.process_id).or_insert(true);
            }
            next
        }

        fn is_server_healthy(&self, handle: &OpenCodeServerHandle) -> bool {
            self.state
                .lock()
                .expect("lock should succeed")
                .health
                .get(&handle.process_id)
                .copied()
                .unwrap_or(false)
        }

        fn terminate_server(&self, _handle: &OpenCodeServerHandle) -> CrabResult<()> {
            Ok(())
        }
    }

    fn handle(process_id: u32) -> OpenCodeServerHandle {
        OpenCodeServerHandle {
            process_id,
            started_at_epoch_ms: u64::from(process_id),
            server_base_url: format!("http://127.0.0.1:{process_id}"),
        }
    }

    #[derive(Debug, Clone)]
    struct ForwardTransport {
        state: Arc<Mutex<VecDeque<CrabResult<OpenCodeApiResponse>>>>,
    }

    impl ForwardTransport {
        fn with_results(results: Vec<CrabResult<OpenCodeApiResponse>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(VecDeque::from(results))),
            }
        }
    }

    impl OpenCodeApiTransport for ForwardTransport {
        fn execute(&self, _request: OpenCodeApiRequest) -> CrabResult<OpenCodeApiResponse> {
            match self.state.lock().expect("lock should succeed").pop_front() {
                Some(result) => result,
                None => Err(CrabError::InvariantViolation {
                    context: "forward_transport",
                    message: "missing scripted response".to_string(),
                }),
            }
        }
    }

    #[test]
    fn protocol_runtime_impl_forwards_calls() {
        let protocol = OpenCodeProtocol::new(ForwardTransport::with_results(vec![
            Ok(OpenCodeApiResponse::SessionCreated {
                session_id: "session-rt".to_string(),
            }),
            Ok(OpenCodeApiResponse::Accepted),
            Ok(OpenCodeApiResponse::TurnAccepted {
                turn_id: "turn-rt".to_string(),
            }),
        ]));

        let session_id =
            OpenCodeRecoveryRuntime::create_session(&protocol, OpenCodeSessionConfig::default())
                .expect("create should forward");
        OpenCodeRecoveryRuntime::end_session(&protocol, &session_id).expect("end should forward");
        let turn_id = OpenCodeRecoveryRuntime::send_prompt(
            &protocol,
            &session_id,
            "restore",
            OpenCodeTurnConfig::default(),
        )
        .expect("send should forward");
        assert_eq!(session_id, "session-rt");
        assert_eq!(turn_id, "turn-rt");
    }

    #[test]
    fn protocol_runtime_forwarder_reports_missing_scripted_response() {
        let protocol = OpenCodeProtocol::new(ForwardTransport::with_results(Vec::new()));
        let error =
            OpenCodeRecoveryRuntime::create_session(&protocol, OpenCodeSessionConfig::default())
                .expect_err("missing scripted response should fail");
        let rendered = error.to_string();
        assert!(rendered.contains("forward_transport"));
        assert!(rendered.contains("missing scripted response"));
    }

    #[test]
    fn recovers_by_rotating_to_new_session() {
        let mut manager = OpenCodeManager::new(FakeProcess::from_starts(vec![Ok(handle(9001))]));
        let runtime = FakeRuntime::new(Vec::new(), vec![Ok("session-new".to_string())], Vec::new());

        let outcome = recover_opencode_session(
            &mut manager,
            &runtime,
            None,
            &OpenCodeRecoveryPlan::default(),
        )
        .expect("recovery should create a new session");
        assert_eq!(
            outcome,
            OpenCodeRecoveryOutcome {
                new_session_id: "session-new".to_string(),
                checkpoint_turn_id: None,
                previous_session_end_error: None,
            }
        );
        assert_eq!(runtime.calls(), vec!["create_session".to_string()]);
    }

    #[test]
    fn recovery_end_previous_is_best_effort() {
        let mut manager = OpenCodeManager::new(FakeProcess::from_starts(vec![Ok(handle(9100))]));
        let runtime = FakeRuntime::new(
            vec![Err(CrabError::InvariantViolation {
                context: "runtime_end",
                message: "session missing".to_string(),
            })],
            vec![Ok("session-fresh".to_string())],
            Vec::new(),
        );

        let outcome = recover_opencode_session(
            &mut manager,
            &runtime,
            Some("session-old"),
            &OpenCodeRecoveryPlan::default(),
        )
        .expect("recovery should continue even if previous end fails");
        assert_eq!(outcome.new_session_id, "session-fresh");
        assert_eq!(outcome.checkpoint_turn_id, None);
        assert_eq!(
            outcome.previous_session_end_error,
            Some("runtime_end invariant violation: session missing".to_string())
        );
        assert_eq!(
            runtime.calls(),
            vec![
                "end_session:session-old".to_string(),
                "create_session".to_string()
            ]
        );
    }

    #[test]
    fn recovery_can_replay_checkpoint_prompt() {
        let mut manager = OpenCodeManager::new(FakeProcess::from_starts(vec![Ok(handle(9200))]));
        let runtime = FakeRuntime::new(
            vec![Ok(())],
            vec![Ok("session-42".to_string())],
            vec![Ok("turn-restore".to_string())],
        );
        let plan = OpenCodeRecoveryPlan {
            session_config: OpenCodeSessionConfig {
                model: Some("gpt-5".to_string()),
                reasoning_level: None,
            },
            checkpoint_prompt: Some("restore from checkpoint".to_string()),
            checkpoint_turn_config: OpenCodeTurnConfig {
                model: None,
                reasoning_level: Some("medium".to_string()),
            },
        };

        let outcome = recover_opencode_session(&mut manager, &runtime, Some("session-old"), &plan)
            .expect("recovery with checkpoint replay should succeed");
        assert_eq!(outcome.new_session_id, "session-42");
        assert_eq!(outcome.checkpoint_turn_id, Some("turn-restore".to_string()));
        assert_eq!(outcome.previous_session_end_error, None);
        assert_eq!(
            runtime.calls(),
            vec![
                "end_session:session-old".to_string(),
                "create_session".to_string(),
                "send_prompt:session-42:restore from checkpoint".to_string()
            ]
        );
    }

    #[test]
    fn recovery_validates_inputs_and_propagates_failures() {
        let mut manager = OpenCodeManager::new(FakeProcess::from_starts(vec![Ok(handle(9300))]));
        let runtime = FakeRuntime::new(Vec::new(), vec![Ok("session".to_string())], Vec::new());

        let blank_previous_error = recover_opencode_session(
            &mut manager,
            &runtime,
            Some(" "),
            &OpenCodeRecoveryPlan::default(),
        )
        .expect_err("blank previous session should fail");
        assert_eq!(
            blank_previous_error,
            CrabError::InvariantViolation {
                context: "opencode_recovery_input",
                message: "previous_session_id must not be empty".to_string(),
            }
        );

        let blank_checkpoint_error = recover_opencode_session(
            &mut manager,
            &runtime,
            None,
            &OpenCodeRecoveryPlan {
                checkpoint_prompt: Some(" ".to_string()),
                ..OpenCodeRecoveryPlan::default()
            },
        )
        .expect_err("blank checkpoint prompt should fail");
        assert_eq!(
            blank_checkpoint_error,
            CrabError::InvariantViolation {
                context: "opencode_recovery_input",
                message: "checkpoint_prompt must not be empty".to_string(),
            }
        );

        let mut manager = OpenCodeManager::new(FakeProcess::from_starts(vec![Err(
            CrabError::InvariantViolation {
                context: "fake_process_spawn",
                message: "boom".to_string(),
            },
        )]));
        let runtime = FakeRuntime::new(Vec::new(), vec![Ok("session".to_string())], Vec::new());
        let start_error = recover_opencode_session(
            &mut manager,
            &runtime,
            None,
            &OpenCodeRecoveryPlan::default(),
        )
        .expect_err("manager start failure should propagate");
        assert_eq!(
            start_error,
            CrabError::InvariantViolation {
                context: "fake_process_spawn",
                message: "boom".to_string(),
            }
        );

        let mut manager = OpenCodeManager::new(FakeProcess::from_starts(Vec::new()));
        let runtime = FakeRuntime::new(Vec::new(), vec![Ok("session".to_string())], Vec::new());
        let missing_start_error = recover_opencode_session(
            &mut manager,
            &runtime,
            None,
            &OpenCodeRecoveryPlan::default(),
        )
        .expect_err("missing scripted process start should fail");
        assert_eq!(
            missing_start_error,
            CrabError::InvariantViolation {
                context: "fake_process_spawn",
                message: "missing scripted start".to_string(),
            }
        );

        let mut manager = OpenCodeManager::new(FakeProcess::from_starts(vec![Ok(handle(9400))]));
        let runtime = FakeRuntime::new(
            Vec::new(),
            vec![Err(CrabError::InvariantViolation {
                context: "runtime_create",
                message: "boom".to_string(),
            })],
            Vec::new(),
        );
        let create_error = recover_opencode_session(
            &mut manager,
            &runtime,
            None,
            &OpenCodeRecoveryPlan::default(),
        )
        .expect_err("create session failure should propagate");
        assert_eq!(
            create_error,
            CrabError::InvariantViolation {
                context: "runtime_create",
                message: "boom".to_string(),
            }
        );

        let mut manager = OpenCodeManager::new(FakeProcess::from_starts(vec![Ok(handle(9500))]));
        let runtime = FakeRuntime::new(
            Vec::new(),
            vec![Ok("session-9500".to_string())],
            vec![Err(CrabError::InvariantViolation {
                context: "runtime_send",
                message: "boom".to_string(),
            })],
        );
        let send_error = recover_opencode_session(
            &mut manager,
            &runtime,
            None,
            &OpenCodeRecoveryPlan {
                checkpoint_prompt: Some("restore".to_string()),
                ..OpenCodeRecoveryPlan::default()
            },
        )
        .expect_err("checkpoint send failure should propagate");
        assert_eq!(
            send_error,
            CrabError::InvariantViolation {
                context: "runtime_send",
                message: "boom".to_string(),
            }
        );
    }

    #[test]
    fn recovery_output_validation_and_fake_guards_are_actionable() {
        let mut manager = OpenCodeManager::new(FakeProcess::from_starts(vec![Ok(handle(9600))]));
        let runtime = FakeRuntime::new(Vec::new(), vec![Ok(" ".to_string())], Vec::new());
        let session_id_error = recover_opencode_session(
            &mut manager,
            &runtime,
            None,
            &OpenCodeRecoveryPlan::default(),
        )
        .expect_err("blank new session id should fail");
        assert_eq!(
            session_id_error,
            CrabError::InvariantViolation {
                context: "opencode_recovery_output",
                message: "new_session_id must not be empty".to_string(),
            }
        );

        let mut manager = OpenCodeManager::new(FakeProcess::from_starts(vec![Ok(handle(9700))]));
        let runtime = FakeRuntime::new(
            Vec::new(),
            vec![Ok("session-9700".to_string())],
            vec![Ok(" ".to_string())],
        );
        let turn_id_error = recover_opencode_session(
            &mut manager,
            &runtime,
            None,
            &OpenCodeRecoveryPlan {
                checkpoint_prompt: Some("restore".to_string()),
                ..OpenCodeRecoveryPlan::default()
            },
        )
        .expect_err("blank checkpoint turn id should fail");
        assert_eq!(
            turn_id_error,
            CrabError::InvariantViolation {
                context: "opencode_recovery_output",
                message: "checkpoint_turn_id must not be empty".to_string(),
            }
        );

        let mut manager = OpenCodeManager::new(FakeProcess::from_starts(vec![Ok(handle(9800))]));
        let runtime = FakeRuntime::new(Vec::new(), Vec::new(), Vec::new());
        let guard_error = recover_opencode_session(
            &mut manager,
            &runtime,
            None,
            &OpenCodeRecoveryPlan::default(),
        )
        .expect_err("missing scripted create response should fail");
        assert_eq!(
            guard_error,
            CrabError::InvariantViolation {
                context: "fake_runtime_create",
                message: "missing scripted response".to_string(),
            }
        );

        let mut manager = OpenCodeManager::new(FakeProcess::from_starts(vec![Ok(handle(9810))]));
        let runtime =
            FakeRuntime::new(Vec::new(), vec![Ok("session-9810".to_string())], Vec::new());
        let missing_end_error = recover_opencode_session(
            &mut manager,
            &runtime,
            Some("session-old"),
            &OpenCodeRecoveryPlan::default(),
        )
        .expect("missing end script should be captured and recovery should continue");
        assert_eq!(
            missing_end_error.previous_session_end_error,
            Some("fake_runtime_end invariant violation: missing scripted response".to_string())
        );

        let mut manager = OpenCodeManager::new(FakeProcess::from_starts(vec![Ok(handle(9820))]));
        let runtime =
            FakeRuntime::new(Vec::new(), vec![Ok("session-9820".to_string())], Vec::new());
        let missing_send_error = recover_opencode_session(
            &mut manager,
            &runtime,
            None,
            &OpenCodeRecoveryPlan {
                checkpoint_prompt: Some("restore".to_string()),
                ..OpenCodeRecoveryPlan::default()
            },
        )
        .expect_err("missing send script should fail");
        assert_eq!(
            missing_send_error,
            CrabError::InvariantViolation {
                context: "fake_runtime_send",
                message: "missing scripted response".to_string(),
            }
        );
    }

    #[test]
    fn fake_process_health_and_terminate_paths_are_exercised() {
        let process = FakeProcess::from_starts(vec![Ok(handle(9900))]);
        let started = process
            .spawn_server()
            .expect("spawn should return scripted server");
        assert!(process.is_server_healthy(&started));
        assert!(!process.is_server_healthy(&handle(9999)));
        process
            .terminate_server(&started)
            .expect("terminate should succeed");
    }
}
