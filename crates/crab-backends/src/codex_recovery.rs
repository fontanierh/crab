use crab_core::{CrabError, CrabResult};

use crate::{ensure_non_empty_field, CodexLifecycleManager, CodexProtocol, CodexRpcTransport};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexRotationReason {
    NoPreviousThread,
    ResumeFailed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexRecoveryOutcome {
    Resumed {
        thread_id: String,
    },
    Rotated {
        thread_id: String,
        reason: CodexRotationReason,
    },
}

pub fn recover_codex_session<M: CodexLifecycleManager + ?Sized, T: CodexRpcTransport>(
    manager: &mut M,
    protocol: &CodexProtocol<T>,
    previous_thread_id: Option<&str>,
) -> CrabResult<CodexRecoveryOutcome> {
    manager.restart()?;

    match previous_thread_id {
        Some(thread_id) => {
            ensure_non_empty_field("codex_recovery", "previous_thread_id", thread_id)?;
            match protocol.thread_resume(thread_id) {
                Ok(resumed_thread_id) => Ok(CodexRecoveryOutcome::Resumed {
                    thread_id: resumed_thread_id,
                }),
                Err(resume_error) => match protocol.thread_start() {
                    Ok(new_thread_id) => Ok(CodexRecoveryOutcome::Rotated {
                        thread_id: new_thread_id,
                        reason: CodexRotationReason::ResumeFailed,
                    }),
                    Err(start_error) => Err(CrabError::InvariantViolation {
                        context: "codex_recovery",
                        message: format!(
                            "thread/resume failed: {resume_error}; thread/start failed: {start_error}"
                        ),
                    }),
                },
            }
        }
        None => {
            let new_thread_id = protocol.thread_start()?;
            Ok(CodexRecoveryOutcome::Rotated {
                thread_id: new_thread_id,
                reason: CodexRotationReason::NoPreviousThread,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use crab_core::{CrabError, CrabResult};

    use super::{recover_codex_session, CodexRecoveryOutcome, CodexRotationReason};
    use crate::{
        CodexAppServerProcess, CodexManager, CodexProcessHandle, CodexProtocol, CodexRpcRequest,
        CodexRpcResponse, CodexRpcTransport,
    };

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    struct ProcessStats {
        spawn_calls: usize,
        terminate_calls: usize,
    }

    #[derive(Debug, Clone)]
    struct FakeProcess {
        state: Arc<Mutex<FakeProcessState>>,
    }

    #[derive(Debug, Clone)]
    struct FakeProcessState {
        scripted_spawns: VecDeque<CrabResult<CodexProcessHandle>>,
        terminate_error: Option<CrabError>,
        stats: ProcessStats,
    }

    impl FakeProcess {
        fn with_spawns(spawns: Vec<CrabResult<CodexProcessHandle>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeProcessState {
                    scripted_spawns: VecDeque::from(spawns),
                    terminate_error: None,
                    stats: ProcessStats::default(),
                })),
            }
        }

        fn set_terminate_error(&self, error: CrabError) {
            self.state
                .lock()
                .expect("lock should succeed")
                .terminate_error = Some(error);
        }

        fn stats(&self) -> ProcessStats {
            self.state
                .lock()
                .expect("lock should succeed")
                .stats
                .clone()
        }
    }

    impl CodexAppServerProcess for FakeProcess {
        fn spawn_app_server(&self) -> CrabResult<CodexProcessHandle> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.spawn_calls += 1;
            state.scripted_spawns.pop_front().unwrap_or_else(|| {
                Err(CrabError::InvariantViolation {
                    context: "fake_recovery_spawn",
                    message: "missing scripted spawn".to_string(),
                })
            })
        }

        fn is_healthy(&self, _handle: &CodexProcessHandle) -> bool {
            true
        }

        fn terminate_app_server(&self, _handle: &CodexProcessHandle) -> CrabResult<()> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.terminate_calls += 1;
            if let Some(error) = state.terminate_error.clone() {
                return Err(error);
            }
            Ok(())
        }
    }

    #[derive(Debug, Clone)]
    struct FakeTransport {
        state: Arc<Mutex<FakeTransportState>>,
    }

    #[derive(Debug, Clone)]
    struct FakeTransportState {
        scripted_resume: VecDeque<CrabResult<String>>,
        scripted_start: VecDeque<CrabResult<String>>,
        methods_called: Vec<String>,
    }

    impl FakeTransport {
        fn new(resume: Vec<CrabResult<String>>, start: Vec<CrabResult<String>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeTransportState {
                    scripted_resume: VecDeque::from(resume),
                    scripted_start: VecDeque::from(start),
                    methods_called: Vec::new(),
                })),
            }
        }

        fn methods_called(&self) -> Vec<String> {
            self.state
                .lock()
                .expect("lock should succeed")
                .methods_called
                .clone()
        }

        fn pop_thread_result(
            queue: &mut VecDeque<CrabResult<String>>,
            missing_message: &'static str,
        ) -> CrabResult<String> {
            match queue.pop_front() {
                Some(result) => result,
                None => Err(CrabError::InvariantViolation {
                    context: "fake_recovery_transport",
                    message: missing_message.to_string(),
                }),
            }
        }

        fn thread_response(thread_id: String) -> CodexRpcResponse {
            let mut fields = BTreeMap::new();
            fields.insert("threadId".to_string(), thread_id);
            CodexRpcResponse { fields }
        }
    }

    impl CodexRpcTransport for FakeTransport {
        fn call(&self, request: CodexRpcRequest) -> CrabResult<CodexRpcResponse> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.methods_called.push(request.method.clone());
            match request.method.as_str() {
                "thread/resume" => FakeTransport::pop_thread_result(
                    &mut state.scripted_resume,
                    "missing scripted thread/resume result",
                )
                .map(FakeTransport::thread_response),
                "thread/start" => FakeTransport::pop_thread_result(
                    &mut state.scripted_start,
                    "missing scripted thread/start result",
                )
                .map(FakeTransport::thread_response),
                _ => Err(CrabError::InvariantViolation {
                    context: "fake_recovery_transport",
                    message: format!("unexpected method {}", request.method),
                }),
            }
        }
    }

    fn handle(process_id: u32) -> CodexProcessHandle {
        CodexProcessHandle {
            process_id,
            started_at_epoch_ms: u64::from(process_id),
        }
    }

    fn started_manager() -> (CodexManager<FakeProcess>, FakeProcess) {
        let process = FakeProcess::with_spawns(vec![Ok(handle(101)), Ok(handle(202))]);
        let mut manager = CodexManager::new(process.clone());
        manager
            .ensure_started()
            .expect("initial process start should succeed");
        (manager, process)
    }

    fn recovery_boom(context: &'static str) -> CrabError {
        CrabError::InvariantViolation {
            context,
            message: "boom".to_string(),
        }
    }

    #[test]
    fn recover_resumes_thread_after_restart() {
        let (mut manager, process) = started_manager();
        let transport = FakeTransport::new(vec![Ok("thread-7".to_string())], Vec::new());
        let protocol = CodexProtocol::new(transport.clone());

        let outcome = recover_codex_session(&mut manager, &protocol, Some("thread-7"))
            .expect("resume recovery should succeed");
        assert_eq!(
            outcome,
            CodexRecoveryOutcome::Resumed {
                thread_id: "thread-7".to_string(),
            }
        );
        assert_eq!(manager.state().restart_count, 1);
        assert_eq!(manager.state().active_process_id, Some(202));
        assert_eq!(process.stats().terminate_calls, 1);
        assert_eq!(
            transport.methods_called(),
            vec!["thread/resume".to_string()]
        );
    }

    #[test]
    fn recover_rotates_when_previous_thread_is_missing() {
        let (mut manager, process) = started_manager();
        let transport = FakeTransport::new(Vec::new(), vec![Ok("thread-new".to_string())]);
        let protocol = CodexProtocol::new(transport.clone());

        let outcome = recover_codex_session(&mut manager, &protocol, None)
            .expect("missing previous thread should rotate");
        assert_eq!(
            outcome,
            CodexRecoveryOutcome::Rotated {
                thread_id: "thread-new".to_string(),
                reason: CodexRotationReason::NoPreviousThread,
            }
        );
        assert_eq!(manager.state().restart_count, 1);
        assert_eq!(process.stats().terminate_calls, 1);
        assert_eq!(transport.methods_called(), vec!["thread/start".to_string()]);
    }

    #[test]
    fn recover_without_previous_thread_propagates_thread_start_error() {
        let (mut manager, _) = started_manager();
        let transport = FakeTransport::new(Vec::new(), vec![Err(recovery_boom("start_fail"))]);
        let protocol = CodexProtocol::new(transport.clone());

        let error = recover_codex_session(&mut manager, &protocol, None)
            .expect_err("thread/start error should propagate");
        assert_eq!(error, recovery_boom("start_fail"));
        assert_eq!(transport.methods_called(), vec!["thread/start".to_string()]);
    }

    #[test]
    fn recover_rotates_when_resume_fails() {
        let (mut manager, _) = started_manager();
        let transport = FakeTransport::new(
            vec![Err(recovery_boom("resume_fail"))],
            vec![Ok("thread-fresh".to_string())],
        );
        let protocol = CodexProtocol::new(transport.clone());

        let outcome = recover_codex_session(&mut manager, &protocol, Some("thread-old"))
            .expect("resume failure should rotate");
        assert_eq!(
            outcome,
            CodexRecoveryOutcome::Rotated {
                thread_id: "thread-fresh".to_string(),
                reason: CodexRotationReason::ResumeFailed,
            }
        );
        assert_eq!(
            transport.methods_called(),
            vec!["thread/resume".to_string(), "thread/start".to_string()]
        );
    }

    #[test]
    fn recover_returns_actionable_error_when_resume_and_start_fail() {
        let (mut manager, _) = started_manager();
        let transport = FakeTransport::new(
            vec![Err(recovery_boom("resume_fail"))],
            vec![Err(recovery_boom("start_fail"))],
        );
        let protocol = CodexProtocol::new(transport);

        let error = recover_codex_session(&mut manager, &protocol, Some("thread-old"))
            .expect_err("double failure should surface");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "codex_recovery",
                message: "thread/resume failed: resume_fail invariant violation: boom; thread/start failed: start_fail invariant violation: boom".to_string(),
            }
        );
    }

    #[test]
    fn recover_validates_previous_thread_id_when_provided() {
        let (mut manager, _) = started_manager();
        let transport = FakeTransport::new(Vec::new(), Vec::new());
        let protocol = CodexProtocol::new(transport);

        let error = recover_codex_session(&mut manager, &protocol, Some(" "))
            .expect_err("blank previous thread id should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "codex_recovery",
                message: "previous_thread_id must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn recover_propagates_restart_errors() {
        let (mut manager, process) = started_manager();
        process.set_terminate_error(recovery_boom("terminate_fail"));
        let transport = FakeTransport::new(Vec::new(), vec![Ok("thread-new".to_string())]);
        let protocol = CodexProtocol::new(transport.clone());

        let error = recover_codex_session(&mut manager, &protocol, Some("thread-1"))
            .expect_err("restart failure should propagate");
        assert_eq!(error, recovery_boom("terminate_fail"));
        assert_eq!(transport.methods_called().len(), 0);
    }

    #[test]
    fn fake_process_and_transport_guards_are_actionable() {
        let process = FakeProcess::with_spawns(Vec::new());
        let mut manager = CodexManager::new(process.clone());
        let start_error = manager
            .ensure_started()
            .expect_err("missing scripted spawn should fail");
        assert_eq!(
            start_error,
            CrabError::InvariantViolation {
                context: "fake_recovery_spawn",
                message: "missing scripted spawn".to_string(),
            }
        );

        let process = FakeProcess::with_spawns(vec![Ok(handle(900))]);
        let mut manager = CodexManager::new(process);
        let first = manager
            .ensure_started()
            .expect("first start should use scripted spawn");
        let second = manager
            .ensure_started()
            .expect("second start should call is_healthy");
        assert_eq!(first, second);

        let transport = FakeTransport::new(Vec::new(), Vec::new());
        let protocol = CodexProtocol::new(transport.clone());
        let resume_error = protocol
            .thread_resume("thread-x")
            .expect_err("missing scripted thread/resume should fail");
        assert_eq!(
            resume_error,
            CrabError::InvariantViolation {
                context: "fake_recovery_transport",
                message: "missing scripted thread/resume result".to_string(),
            }
        );

        let start_error = protocol
            .thread_start()
            .expect_err("missing scripted thread/start should fail");
        assert_eq!(
            start_error,
            CrabError::InvariantViolation {
                context: "fake_recovery_transport",
                message: "missing scripted thread/start result".to_string(),
            }
        );

        let unexpected_method_error = transport
            .call(CodexRpcRequest {
                method: "unexpected/method".to_string(),
                params: BTreeMap::new(),
                input: Vec::new(),
            })
            .expect_err("unexpected method should fail");
        assert_eq!(
            unexpected_method_error,
            CrabError::InvariantViolation {
                context: "fake_recovery_transport",
                message: "unexpected method unexpected/method".to_string(),
            }
        );
    }
}
