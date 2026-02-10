use serde::{Deserialize, Serialize};

use crate::validation::validate_non_empty_text;
use crate::{CrabError, CrabResult};

const ONBOARDING_STATE_MACHINE_CONTEXT: &str = "onboarding_state_machine";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingState {
    Pending,
    InProgress,
    Completed,
    Skipped,
}

impl OnboardingState {
    #[must_use]
    pub const fn as_token(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Skipped => "skipped",
        }
    }

    #[must_use]
    pub fn parse_token(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "in_progress" => Some(Self::InProgress),
            "completed" => Some(Self::Completed),
            "skipped" => Some(Self::Skipped),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OnboardingLifecycle {
    pub state: OnboardingState,
    pub onboarding_session_id: Option<String>,
}

impl OnboardingLifecycle {
    #[must_use]
    pub fn pending() -> Self {
        Self {
            state: OnboardingState::Pending,
            onboarding_session_id: None,
        }
    }

    #[must_use]
    pub fn should_resume_after_restart(&self) -> bool {
        self.state == OnboardingState::InProgress
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OnboardingTransition {
    Start { onboarding_session_id: String },
    ResumeAfterRestart,
    Complete,
    Skip,
    Reset,
}

pub fn apply_onboarding_transition(
    current: &OnboardingLifecycle,
    transition: OnboardingTransition,
) -> CrabResult<OnboardingLifecycle> {
    match transition {
        OnboardingTransition::Start {
            onboarding_session_id,
        } => {
            if current.state != OnboardingState::Pending {
                return Err(invalid_transition(current.state, "start"));
            }
            Ok(build_lifecycle(
                OnboardingState::InProgress,
                Some(normalize_session_id(&onboarding_session_id)?),
            ))
        }
        OnboardingTransition::ResumeAfterRestart => {
            if current.state != OnboardingState::InProgress {
                return Err(invalid_transition(current.state, "resume_after_restart"));
            }
            let session_id =
                current
                    .onboarding_session_id
                    .as_deref()
                    .ok_or(CrabError::InvariantViolation {
                        context: ONBOARDING_STATE_MACHINE_CONTEXT,
                        message: "state in_progress requires onboarding_session_id".to_string(),
                    })?;
            let normalized_session_id = session_id.trim();
            if normalized_session_id.is_empty() {
                return Err(CrabError::InvariantViolation {
                    context: ONBOARDING_STATE_MACHINE_CONTEXT,
                    message: "state in_progress requires onboarding_session_id".to_string(),
                });
            }
            Ok(build_lifecycle(
                OnboardingState::InProgress,
                Some(normalized_session_id.to_string()),
            ))
        }
        OnboardingTransition::Complete => {
            if current.state != OnboardingState::InProgress {
                return Err(invalid_transition(current.state, "complete"));
            }
            Ok(build_lifecycle(OnboardingState::Completed, None))
        }
        OnboardingTransition::Skip => {
            if current.state != OnboardingState::Pending
                && current.state != OnboardingState::InProgress
            {
                return Err(invalid_transition(current.state, "skip"));
            }
            Ok(build_lifecycle(OnboardingState::Skipped, None))
        }
        OnboardingTransition::Reset => {
            validate_lifecycle_invariants(current)?;
            Ok(build_lifecycle(OnboardingState::Pending, None))
        }
    }
}

fn build_lifecycle(
    state: OnboardingState,
    onboarding_session_id: Option<String>,
) -> OnboardingLifecycle {
    OnboardingLifecycle {
        state,
        onboarding_session_id,
    }
}

fn normalize_session_id(onboarding_session_id: &str) -> CrabResult<String> {
    validate_non_empty_text(
        ONBOARDING_STATE_MACHINE_CONTEXT,
        "onboarding_session_id",
        onboarding_session_id,
    )?;
    Ok(onboarding_session_id.trim().to_string())
}

fn validate_lifecycle_invariants(lifecycle: &OnboardingLifecycle) -> CrabResult<()> {
    let normalized_session_id = lifecycle
        .onboarding_session_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    match lifecycle.state {
        OnboardingState::Pending | OnboardingState::Completed | OnboardingState::Skipped => {
            if normalized_session_id.is_some() {
                return Err(CrabError::InvariantViolation {
                    context: ONBOARDING_STATE_MACHINE_CONTEXT,
                    message: format!(
                        "state {} must not carry onboarding_session_id",
                        lifecycle.state.as_token()
                    ),
                });
            }
        }
        OnboardingState::InProgress => {
            if normalized_session_id.is_none() {
                return Err(CrabError::InvariantViolation {
                    context: ONBOARDING_STATE_MACHINE_CONTEXT,
                    message: "state in_progress requires onboarding_session_id".to_string(),
                });
            }
        }
    }

    Ok(())
}

fn invalid_transition(state: OnboardingState, transition: &'static str) -> CrabError {
    CrabError::InvariantViolation {
        context: ONBOARDING_STATE_MACHINE_CONTEXT,
        message: format!(
            "transition {transition} is not allowed from state {}",
            state.as_token()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_onboarding_transition, OnboardingLifecycle, OnboardingState, OnboardingTransition,
    };
    use crate::CrabError;

    fn lifecycle(
        state: OnboardingState,
        onboarding_session_id: Option<&str>,
    ) -> OnboardingLifecycle {
        OnboardingLifecycle {
            state,
            onboarding_session_id: onboarding_session_id.map(str::to_string),
        }
    }

    fn assert_invariant_error(result: crate::CrabResult<OnboardingLifecycle>, message: &str) {
        let error = result.expect_err("expected invariant violation");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "onboarding_state_machine",
                message: message.to_string(),
            }
        );
    }

    #[test]
    fn onboarding_state_token_mapping_is_stable() {
        assert_eq!(OnboardingState::Pending.as_token(), "pending");
        assert_eq!(OnboardingState::InProgress.as_token(), "in_progress");
        assert_eq!(OnboardingState::Completed.as_token(), "completed");
        assert_eq!(OnboardingState::Skipped.as_token(), "skipped");

        assert_eq!(
            OnboardingState::parse_token("pending"),
            Some(OnboardingState::Pending)
        );
        assert_eq!(
            OnboardingState::parse_token("in_progress"),
            Some(OnboardingState::InProgress)
        );
        assert_eq!(
            OnboardingState::parse_token("completed"),
            Some(OnboardingState::Completed)
        );
        assert_eq!(
            OnboardingState::parse_token("skipped"),
            Some(OnboardingState::Skipped)
        );
        assert_eq!(OnboardingState::parse_token("unknown"), None);
    }

    #[test]
    fn pending_lifecycle_is_bootstrap_default() {
        let lifecycle = OnboardingLifecycle::pending();
        assert_eq!(lifecycle.state, OnboardingState::Pending);
        assert_eq!(lifecycle.onboarding_session_id, None);
        assert!(!lifecycle.should_resume_after_restart());
    }

    #[test]
    fn in_progress_lifecycle_requires_resume_after_restart() {
        let lifecycle = lifecycle(OnboardingState::InProgress, Some("session-1"));
        assert!(lifecycle.should_resume_after_restart());
    }

    #[test]
    fn start_transition_enters_in_progress_and_normalizes_session_id() {
        let next = apply_onboarding_transition(
            &OnboardingLifecycle::pending(),
            OnboardingTransition::Start {
                onboarding_session_id: "  session-42  ".to_string(),
            },
        )
        .expect("start transition should succeed");

        assert_eq!(
            next,
            lifecycle(OnboardingState::InProgress, Some("session-42"))
        );
    }

    #[test]
    fn start_transition_rejects_blank_session_id() {
        assert_invariant_error(
            apply_onboarding_transition(
                &OnboardingLifecycle::pending(),
                OnboardingTransition::Start {
                    onboarding_session_id: "   ".to_string(),
                },
            ),
            "onboarding_session_id must not be empty",
        );
    }

    #[test]
    fn start_transition_rejects_non_pending_states() {
        for state in [
            OnboardingState::InProgress,
            OnboardingState::Completed,
            OnboardingState::Skipped,
        ] {
            let current = lifecycle(
                state,
                if state == OnboardingState::InProgress {
                    Some("session-1")
                } else {
                    None
                },
            );
            let expected = format!(
                "transition start is not allowed from state {}",
                state.as_token()
            );
            assert_invariant_error(
                apply_onboarding_transition(
                    &current,
                    OnboardingTransition::Start {
                        onboarding_session_id: "session-2".to_string(),
                    },
                ),
                &expected,
            );
        }
    }

    #[test]
    fn resume_after_restart_is_idempotent_for_in_progress_state() {
        let current = lifecycle(OnboardingState::InProgress, Some(" session-1 "));
        let next = apply_onboarding_transition(&current, OnboardingTransition::ResumeAfterRestart)
            .expect("resume should succeed");
        assert_eq!(
            next,
            lifecycle(OnboardingState::InProgress, Some("session-1"))
        );
    }

    #[test]
    fn resume_after_restart_rejects_non_in_progress_states() {
        for state in [
            OnboardingState::Pending,
            OnboardingState::Completed,
            OnboardingState::Skipped,
        ] {
            let expected = format!(
                "transition resume_after_restart is not allowed from state {}",
                state.as_token()
            );
            assert_invariant_error(
                apply_onboarding_transition(
                    &lifecycle(state, None),
                    OnboardingTransition::ResumeAfterRestart,
                ),
                &expected,
            );
        }
    }

    #[test]
    fn complete_transition_moves_in_progress_to_completed() {
        let next = apply_onboarding_transition(
            &lifecycle(OnboardingState::InProgress, Some("session-3")),
            OnboardingTransition::Complete,
        )
        .expect("complete transition should succeed");
        assert_eq!(next, lifecycle(OnboardingState::Completed, None));
    }

    #[test]
    fn complete_transition_rejects_non_in_progress_states() {
        for state in [
            OnboardingState::Pending,
            OnboardingState::Completed,
            OnboardingState::Skipped,
        ] {
            let expected = format!(
                "transition complete is not allowed from state {}",
                state.as_token()
            );
            assert_invariant_error(
                apply_onboarding_transition(
                    &lifecycle(state, None),
                    OnboardingTransition::Complete,
                ),
                &expected,
            );
        }
    }

    #[test]
    fn skip_transition_supports_pending_and_in_progress() {
        let from_pending = apply_onboarding_transition(
            &lifecycle(OnboardingState::Pending, None),
            OnboardingTransition::Skip,
        )
        .expect("pending skip should succeed");
        assert_eq!(from_pending, lifecycle(OnboardingState::Skipped, None));

        let from_in_progress = apply_onboarding_transition(
            &lifecycle(OnboardingState::InProgress, Some("session-9")),
            OnboardingTransition::Skip,
        )
        .expect("in-progress skip should succeed");
        assert_eq!(from_in_progress, lifecycle(OnboardingState::Skipped, None));
    }

    #[test]
    fn skip_transition_rejects_completed_and_skipped() {
        for state in [OnboardingState::Completed, OnboardingState::Skipped] {
            let expected = format!(
                "transition skip is not allowed from state {}",
                state.as_token()
            );
            assert_invariant_error(
                apply_onboarding_transition(&lifecycle(state, None), OnboardingTransition::Skip),
                &expected,
            );
        }
    }

    #[test]
    fn reset_transition_returns_pending_from_any_state() {
        let pending = apply_onboarding_transition(
            &lifecycle(OnboardingState::Pending, None),
            OnboardingTransition::Reset,
        )
        .expect("pending reset should succeed");
        assert_eq!(pending, lifecycle(OnboardingState::Pending, None));

        let in_progress = apply_onboarding_transition(
            &lifecycle(OnboardingState::InProgress, Some("session-10")),
            OnboardingTransition::Reset,
        )
        .expect("in-progress reset should succeed");
        assert_eq!(in_progress, lifecycle(OnboardingState::Pending, None));

        let completed = apply_onboarding_transition(
            &lifecycle(OnboardingState::Completed, None),
            OnboardingTransition::Reset,
        )
        .expect("completed reset should succeed");
        assert_eq!(completed, lifecycle(OnboardingState::Pending, None));

        let skipped = apply_onboarding_transition(
            &lifecycle(OnboardingState::Skipped, None),
            OnboardingTransition::Reset,
        )
        .expect("skipped reset should succeed");
        assert_eq!(skipped, lifecycle(OnboardingState::Pending, None));
    }

    #[test]
    fn transition_rejects_invalid_current_lifecycle_shape() {
        assert_invariant_error(
            apply_onboarding_transition(
                &lifecycle(OnboardingState::Pending, Some("session-1")),
                OnboardingTransition::Reset,
            ),
            "state pending must not carry onboarding_session_id",
        );

        assert_invariant_error(
            apply_onboarding_transition(
                &lifecycle(OnboardingState::InProgress, None),
                OnboardingTransition::Reset,
            ),
            "state in_progress requires onboarding_session_id",
        );

        assert_invariant_error(
            apply_onboarding_transition(
                &lifecycle(OnboardingState::InProgress, None),
                OnboardingTransition::ResumeAfterRestart,
            ),
            "state in_progress requires onboarding_session_id",
        );

        assert_invariant_error(
            apply_onboarding_transition(
                &lifecycle(OnboardingState::InProgress, Some("   ")),
                OnboardingTransition::ResumeAfterRestart,
            ),
            "state in_progress requires onboarding_session_id",
        );
    }

    #[test]
    fn lifecycle_rejects_unknown_fields_from_serialized_payload() {
        let error = serde_json::from_str::<OnboardingLifecycle>(
            r#"{
                "state": "pending",
                "onboarding_session_id": null,
                "extra": "unexpected"
            }"#,
        )
        .expect_err("unknown fields must be rejected");
        assert_eq!(
            error.to_string(),
            "unknown field `extra`, expected `state` or `onboarding_session_id` at line 4 column 23"
        );
    }

    #[test]
    fn restart_resume_round_trip_is_stable_after_serialization() {
        let current = lifecycle(OnboardingState::InProgress, Some("session-777"));
        let encoded = serde_json::to_string(&current).expect("encoding should succeed");
        let decoded: OnboardingLifecycle =
            serde_json::from_str(&encoded).expect("decoding should succeed");

        let resumed =
            apply_onboarding_transition(&decoded, OnboardingTransition::ResumeAfterRestart)
                .expect("resume after restart should succeed");
        assert_eq!(resumed, current);
    }
}
