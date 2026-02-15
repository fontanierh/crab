use crate::{CheckpointTurnDocument, CrabError, CrabResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotationSequenceOutcome {
    pub checkpoint_id: String,
}

pub trait RotationSequenceRuntime {
    fn persist_checkpoint(&mut self, checkpoint: &CheckpointTurnDocument) -> CrabResult<String>;
    fn end_physical_session(&mut self) -> CrabResult<()>;
    fn clear_active_physical_session(&mut self) -> CrabResult<()>;
}

pub fn execute_rotation_sequence<R: RotationSequenceRuntime>(
    runtime: &mut R,
    checkpoint: &CheckpointTurnDocument,
) -> CrabResult<RotationSequenceOutcome> {
    let checkpoint_id = runtime.persist_checkpoint(checkpoint)?;
    if checkpoint_id.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "rotation_sequence",
            message: "persist_checkpoint returned an empty checkpoint_id".to_string(),
        });
    }

    runtime.end_physical_session()?;
    runtime.clear_active_physical_session()?;

    Ok(RotationSequenceOutcome { checkpoint_id })
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use crate::{CheckpointTurnArtifact, CrabError, CrabResult};

    use super::{execute_rotation_sequence, RotationSequenceOutcome, RotationSequenceRuntime};
    use crate::CheckpointTurnDocument;

    #[derive(Debug, Clone)]
    struct FakeRuntime {
        persist_checkpoint_results: VecDeque<CrabResult<String>>,
        end_physical_session_result: CrabResult<()>,
        clear_active_physical_session_result: CrabResult<()>,
        calls: Vec<String>,
        persisted_summaries: Vec<String>,
    }

    impl FakeRuntime {
        fn successful() -> Self {
            Self {
                persist_checkpoint_results: VecDeque::from(vec![Ok("ckpt-1".to_string())]),
                end_physical_session_result: Ok(()),
                clear_active_physical_session_result: Ok(()),
                calls: Vec::new(),
                persisted_summaries: Vec::new(),
            }
        }
    }

    impl RotationSequenceRuntime for FakeRuntime {
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
    fn executes_rotation_sequence_with_provided_checkpoint() {
        let mut runtime = FakeRuntime::successful();
        let checkpoint = checkpoint_document("agent checkpoint");
        let outcome =
            execute_rotation_sequence(&mut runtime, &checkpoint).expect("rotation should succeed");

        assert_eq!(
            outcome,
            RotationSequenceOutcome {
                checkpoint_id: "ckpt-1".to_string(),
            }
        );
        assert_eq!(
            runtime.calls,
            vec!["checkpoint_persist", "session_end", "session_clear",]
        );
        assert_eq!(runtime.persisted_summaries, vec!["agent checkpoint"]);
    }

    #[test]
    fn fails_when_persist_returns_empty_checkpoint_id() {
        let mut runtime = FakeRuntime::successful();
        runtime.persist_checkpoint_results = VecDeque::from(vec![Ok("   ".to_string())]);

        let checkpoint = checkpoint_document("agent checkpoint");
        let error = execute_rotation_sequence(&mut runtime, &checkpoint)
            .expect_err("empty checkpoint id should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "rotation_sequence",
                message: "persist_checkpoint returned an empty checkpoint_id".to_string(),
            }
        );
        assert_eq!(runtime.calls, vec!["checkpoint_persist"]);
    }

    #[test]
    fn propagates_persist_errors() {
        let mut runtime = FakeRuntime::successful();
        runtime.persist_checkpoint_results =
            VecDeque::from(vec![Err(invariant_boom("checkpoint_persist"))]);

        let checkpoint = checkpoint_document("agent checkpoint");
        let error = execute_rotation_sequence(&mut runtime, &checkpoint)
            .expect_err("persist errors should propagate");
        assert_eq!(error, invariant_boom("checkpoint_persist"));
        assert_eq!(runtime.calls, vec!["checkpoint_persist"]);
    }

    #[test]
    fn propagates_end_session_errors_without_clearing_handle() {
        let mut runtime = FakeRuntime::successful();
        runtime.end_physical_session_result = Err(invariant_boom("session_end"));

        let checkpoint = checkpoint_document("agent checkpoint");
        let error = execute_rotation_sequence(&mut runtime, &checkpoint)
            .expect_err("end session errors should propagate");
        assert_eq!(error, invariant_boom("session_end"));
        assert_eq!(runtime.calls, vec!["checkpoint_persist", "session_end",]);
    }

    #[test]
    fn propagates_clear_handle_errors_after_end_session() {
        let mut runtime = FakeRuntime::successful();
        runtime.clear_active_physical_session_result = Err(invariant_boom("session_clear"));

        let checkpoint = checkpoint_document("agent checkpoint");
        let error = execute_rotation_sequence(&mut runtime, &checkpoint)
            .expect_err("clear handle errors should propagate");
        assert_eq!(error, invariant_boom("session_clear"));
        assert_eq!(
            runtime.calls,
            vec!["checkpoint_persist", "session_end", "session_clear",]
        );
    }

    #[test]
    fn fake_runtime_requires_scripted_persist_result() {
        let mut runtime = FakeRuntime::successful();
        runtime.persist_checkpoint_results = VecDeque::new();

        let checkpoint = checkpoint_document("agent checkpoint");
        let error = execute_rotation_sequence(&mut runtime, &checkpoint)
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
