use crate::{CheckpointTurnDocument, CrabError, CrabResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotationSequenceOutcome {
    pub checkpoint_id: String,
    pub used_fallback_checkpoint: bool,
    pub memory_flush_error: Option<String>,
    pub checkpoint_turn_error: Option<String>,
}

pub trait RotationSequenceRuntime {
    fn run_hidden_memory_flush(&mut self) -> CrabResult<()>;
    fn run_hidden_checkpoint_turn(&mut self) -> CrabResult<CheckpointTurnDocument>;
    fn build_fallback_checkpoint(&mut self) -> CrabResult<CheckpointTurnDocument>;
    fn persist_checkpoint(&mut self, checkpoint: &CheckpointTurnDocument) -> CrabResult<String>;
    fn end_physical_session(&mut self) -> CrabResult<()>;
    fn clear_active_physical_session(&mut self) -> CrabResult<()>;
}

pub fn execute_rotation_sequence<R: RotationSequenceRuntime>(
    runtime: &mut R,
) -> CrabResult<RotationSequenceOutcome> {
    let memory_flush_error = runtime
        .run_hidden_memory_flush()
        .err()
        .map(|error| format!("hidden memory flush failed and was ignored for rotation: {error}"));

    let (checkpoint_document, used_fallback_checkpoint, checkpoint_turn_error) =
        resolve_checkpoint_document(runtime)?;

    let checkpoint_id = runtime.persist_checkpoint(&checkpoint_document)?;
    if checkpoint_id.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "rotation_sequence",
            message: "persist_checkpoint returned an empty checkpoint_id".to_string(),
        });
    }

    runtime.end_physical_session()?;
    runtime.clear_active_physical_session()?;

    Ok(RotationSequenceOutcome {
        checkpoint_id,
        used_fallback_checkpoint,
        memory_flush_error,
        checkpoint_turn_error,
    })
}

fn resolve_checkpoint_document<R: RotationSequenceRuntime>(
    runtime: &mut R,
) -> CrabResult<(CheckpointTurnDocument, bool, Option<String>)> {
    match runtime.run_hidden_checkpoint_turn() {
        Ok(document) => Ok((document, false, None)),
        Err(checkpoint_error) => match runtime.build_fallback_checkpoint() {
            Ok(fallback_document) => Ok((fallback_document, true, Some(checkpoint_error.to_string()))),
            Err(fallback_error) => Err(CrabError::InvariantViolation {
                context: "rotation_sequence",
                message: format!(
                    "hidden checkpoint turn failed: {checkpoint_error}; fallback checkpoint generation failed: {fallback_error}"
                ),
            }),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use crate::{CheckpointTurnArtifact, CrabError, CrabResult};

    use super::{execute_rotation_sequence, RotationSequenceOutcome, RotationSequenceRuntime};
    use crate::CheckpointTurnDocument;

    #[derive(Debug, Clone)]
    struct FakeRuntime {
        run_hidden_memory_flush_result: CrabResult<()>,
        run_hidden_checkpoint_turn_result: CrabResult<CheckpointTurnDocument>,
        build_fallback_checkpoint_result: CrabResult<CheckpointTurnDocument>,
        persist_checkpoint_results: VecDeque<CrabResult<String>>,
        end_physical_session_result: CrabResult<()>,
        clear_active_physical_session_result: CrabResult<()>,
        calls: Vec<String>,
        persisted_summaries: Vec<String>,
    }

    impl FakeRuntime {
        fn successful() -> Self {
            Self {
                run_hidden_memory_flush_result: Ok(()),
                run_hidden_checkpoint_turn_result: Ok(checkpoint_document("primary checkpoint")),
                build_fallback_checkpoint_result: Ok(checkpoint_document("fallback checkpoint")),
                persist_checkpoint_results: VecDeque::from(vec![Ok("ckpt-1".to_string())]),
                end_physical_session_result: Ok(()),
                clear_active_physical_session_result: Ok(()),
                calls: Vec::new(),
                persisted_summaries: Vec::new(),
            }
        }
    }

    impl RotationSequenceRuntime for FakeRuntime {
        fn run_hidden_memory_flush(&mut self) -> CrabResult<()> {
            self.calls.push("memory_flush".to_string());
            self.run_hidden_memory_flush_result.clone()
        }

        fn run_hidden_checkpoint_turn(&mut self) -> CrabResult<CheckpointTurnDocument> {
            self.calls.push("checkpoint_turn".to_string());
            self.run_hidden_checkpoint_turn_result.clone()
        }

        fn build_fallback_checkpoint(&mut self) -> CrabResult<CheckpointTurnDocument> {
            self.calls.push("checkpoint_fallback".to_string());
            self.build_fallback_checkpoint_result.clone()
        }

        fn persist_checkpoint(
            &mut self,
            checkpoint: &CheckpointTurnDocument,
        ) -> CrabResult<String> {
            self.calls.push("checkpoint_persist".to_string());
            self.persisted_summaries.push(checkpoint.summary.clone());
            match self.persist_checkpoint_results.pop_front() {
                Some(result) => result,
                None => Err(CrabError::InvariantViolation {
                    context: "fake_rotation_runtime",
                    message: "missing scripted persist result".to_string(),
                }),
            }
        }

        fn end_physical_session(&mut self) -> CrabResult<()> {
            self.calls.push("session_end".to_string());
            self.end_physical_session_result.clone()
        }

        fn clear_active_physical_session(&mut self) -> CrabResult<()> {
            self.calls.push("session_clear".to_string());
            self.clear_active_physical_session_result.clone()
        }
    }

    fn checkpoint_document(summary: &str) -> CheckpointTurnDocument {
        CheckpointTurnDocument {
            summary: summary.to_string(),
            decisions: vec!["decision".to_string()],
            open_questions: vec!["question".to_string()],
            next_actions: vec!["next".to_string()],
            artifacts: vec![CheckpointTurnArtifact {
                path: "path".to_string(),
                note: "note".to_string(),
            }],
        }
    }

    fn invariant_boom(context: &'static str) -> CrabError {
        CrabError::InvariantViolation {
            context,
            message: "boom".to_string(),
        }
    }

    #[test]
    fn executes_primary_checkpoint_rotation_sequence() {
        let mut runtime = FakeRuntime::successful();
        let outcome = execute_rotation_sequence(&mut runtime).expect("rotation should succeed");

        assert_eq!(
            outcome,
            RotationSequenceOutcome {
                checkpoint_id: "ckpt-1".to_string(),
                used_fallback_checkpoint: false,
                memory_flush_error: None,
                checkpoint_turn_error: None,
            }
        );
        assert_eq!(
            runtime.calls,
            vec![
                "memory_flush",
                "checkpoint_turn",
                "checkpoint_persist",
                "session_end",
                "session_clear",
            ]
        );
        assert_eq!(runtime.persisted_summaries, vec!["primary checkpoint"]);
    }

    #[test]
    fn memory_flush_failures_are_non_blocking() {
        let mut runtime = FakeRuntime::successful();
        runtime.run_hidden_memory_flush_result = Err(invariant_boom("memory_flush"));

        let outcome = execute_rotation_sequence(&mut runtime).expect("rotation should continue");
        let memory_flush_error = outcome
            .memory_flush_error
            .expect("flush errors should be included");
        assert!(memory_flush_error.contains("hidden memory flush failed and was ignored"));
        assert!(memory_flush_error.contains("memory_flush"));
        assert!(memory_flush_error.contains("boom"));
        assert!(!outcome.used_fallback_checkpoint);
    }

    #[test]
    fn checkpoint_turn_failure_uses_fallback_checkpoint() {
        let mut runtime = FakeRuntime::successful();
        runtime.run_hidden_checkpoint_turn_result = Err(invariant_boom("checkpoint_turn"));
        runtime.persist_checkpoint_results = VecDeque::from(vec![Ok("ckpt-fallback".to_string())]);

        let outcome = execute_rotation_sequence(&mut runtime)
            .expect("rotation should succeed with fallback checkpoint");
        assert_eq!(outcome.checkpoint_id, "ckpt-fallback");
        assert!(outcome.used_fallback_checkpoint);
        let checkpoint_turn_error = outcome
            .checkpoint_turn_error
            .expect("checkpoint failure should be included");
        assert!(checkpoint_turn_error.contains("checkpoint_turn"));
        assert!(checkpoint_turn_error.contains("boom"));
        assert_eq!(runtime.persisted_summaries, vec!["fallback checkpoint"]);
        assert_eq!(
            runtime.calls,
            vec![
                "memory_flush",
                "checkpoint_turn",
                "checkpoint_fallback",
                "checkpoint_persist",
                "session_end",
                "session_clear",
            ]
        );
    }

    #[test]
    fn fails_if_both_checkpoint_and_fallback_generation_fail() {
        let mut runtime = FakeRuntime::successful();
        runtime.run_hidden_checkpoint_turn_result = Err(invariant_boom("checkpoint_turn"));
        runtime.build_fallback_checkpoint_result = Err(invariant_boom("checkpoint_fallback"));

        let error = execute_rotation_sequence(&mut runtime)
            .expect_err("dual checkpoint failure should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "rotation_sequence",
                ..
            }
        ));
        let message = error.to_string();
        assert!(message.contains("hidden checkpoint turn failed"));
        assert!(message.contains("checkpoint_turn"));
        assert!(message.contains("fallback checkpoint generation failed"));
        assert!(message.contains("checkpoint_fallback"));
        assert_eq!(
            runtime.calls,
            vec!["memory_flush", "checkpoint_turn", "checkpoint_fallback"]
        );
    }

    #[test]
    fn fails_when_persist_returns_empty_checkpoint_id() {
        let mut runtime = FakeRuntime::successful();
        runtime.persist_checkpoint_results = VecDeque::from(vec![Ok("   ".to_string())]);

        let error = execute_rotation_sequence(&mut runtime)
            .expect_err("empty checkpoint id should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "rotation_sequence",
                message: "persist_checkpoint returned an empty checkpoint_id".to_string(),
            }
        );
        assert_eq!(
            runtime.calls,
            vec!["memory_flush", "checkpoint_turn", "checkpoint_persist"]
        );
    }

    #[test]
    fn propagates_persist_errors() {
        let mut runtime = FakeRuntime::successful();
        runtime.persist_checkpoint_results =
            VecDeque::from(vec![Err(invariant_boom("checkpoint_persist"))]);

        let error =
            execute_rotation_sequence(&mut runtime).expect_err("persist errors should propagate");
        assert_eq!(error, invariant_boom("checkpoint_persist"));
        assert_eq!(
            runtime.calls,
            vec!["memory_flush", "checkpoint_turn", "checkpoint_persist"]
        );
    }

    #[test]
    fn propagates_end_session_errors_without_clearing_handle() {
        let mut runtime = FakeRuntime::successful();
        runtime.end_physical_session_result = Err(invariant_boom("session_end"));

        let error = execute_rotation_sequence(&mut runtime)
            .expect_err("end session errors should propagate");
        assert_eq!(error, invariant_boom("session_end"));
        assert_eq!(
            runtime.calls,
            vec![
                "memory_flush",
                "checkpoint_turn",
                "checkpoint_persist",
                "session_end",
            ]
        );
    }

    #[test]
    fn propagates_clear_handle_errors_after_end_session() {
        let mut runtime = FakeRuntime::successful();
        runtime.clear_active_physical_session_result = Err(invariant_boom("session_clear"));

        let error = execute_rotation_sequence(&mut runtime)
            .expect_err("clear handle errors should propagate");
        assert_eq!(error, invariant_boom("session_clear"));
        assert_eq!(
            runtime.calls,
            vec![
                "memory_flush",
                "checkpoint_turn",
                "checkpoint_persist",
                "session_end",
                "session_clear",
            ]
        );
    }

    #[test]
    fn fake_runtime_requires_scripted_persist_result() {
        let mut runtime = FakeRuntime::successful();
        runtime.persist_checkpoint_results = VecDeque::new();

        let error = execute_rotation_sequence(&mut runtime)
            .expect_err("missing scripted persist result should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "fake_rotation_runtime",
                message: "missing scripted persist result".to_string(),
            }
        );
    }
}
