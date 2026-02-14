use crate::{CrabError, CrabResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManualRotationRequest {
    Compact,
    Reset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationTrigger {
    ManualCompact,
    ManualReset,
    BackendCompaction,
    InactivityTimeout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotationTriggerInput {
    pub now_epoch_ms: u64,
    pub last_activity_epoch_ms: u64,
    pub lane_is_idle: bool,
    pub backend_compaction_signaled: bool,
    pub inactivity_timeout_secs: u64,
    pub manual_request: Option<ManualRotationRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotationTriggerDecision {
    pub should_rotate: bool,
    pub triggers: Vec<RotationTrigger>,
}

pub fn evaluate_rotation_triggers(
    input: &RotationTriggerInput,
) -> CrabResult<RotationTriggerDecision> {
    validate_rotation_trigger_input(input)?;

    let mut triggers = Vec::new();

    if let Some(manual_request) = input.manual_request {
        triggers.push(match manual_request {
            ManualRotationRequest::Compact => RotationTrigger::ManualCompact,
            ManualRotationRequest::Reset => RotationTrigger::ManualReset,
        });
    }

    if input.backend_compaction_signaled {
        triggers.push(RotationTrigger::BackendCompaction);
    }

    if input.lane_is_idle {
        let inactivity_timeout_ms =
            input
                .inactivity_timeout_secs
                .checked_mul(1_000)
                .ok_or(CrabError::InvalidConfig {
                    key: "CRAB_INACTIVITY_TIMEOUT_SECS",
                    value: input.inactivity_timeout_secs.to_string(),
                    reason: "must fit in milliseconds as a u64",
                })?;
        let elapsed_ms = input.now_epoch_ms - input.last_activity_epoch_ms;
        if elapsed_ms >= inactivity_timeout_ms {
            triggers.push(RotationTrigger::InactivityTimeout);
        }
    }

    Ok(RotationTriggerDecision {
        should_rotate: !triggers.is_empty(),
        triggers,
    })
}

fn validate_rotation_trigger_input(input: &RotationTriggerInput) -> CrabResult<()> {
    if input.inactivity_timeout_secs == 0 {
        return Err(CrabError::InvalidConfig {
            key: "CRAB_INACTIVITY_TIMEOUT_SECS",
            value: input.inactivity_timeout_secs.to_string(),
            reason: "must be greater than 0",
        });
    }

    if input.now_epoch_ms < input.last_activity_epoch_ms {
        return Err(CrabError::InvariantViolation {
            context: "rotation_trigger_evaluator",
            message: format!(
                "now_epoch_ms {} must be greater than or equal to last_activity_epoch_ms {}",
                input.now_epoch_ms, input.last_activity_epoch_ms
            ),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::CrabError;

    use super::{
        evaluate_rotation_triggers, ManualRotationRequest, RotationTrigger, RotationTriggerInput,
    };

    fn input() -> RotationTriggerInput {
        RotationTriggerInput {
            now_epoch_ms: 10_000,
            last_activity_epoch_ms: 9_500,
            lane_is_idle: false,
            backend_compaction_signaled: false,
            inactivity_timeout_secs: 60,
            manual_request: None,
        }
    }

    #[test]
    fn returns_no_rotation_when_no_triggers_fire() {
        let decision = evaluate_rotation_triggers(&input()).expect("evaluation should succeed");
        assert!(!decision.should_rotate);
        assert!(decision.triggers.is_empty());
        assert!(!decision
            .triggers
            .contains(&RotationTrigger::BackendCompaction));
    }

    #[test]
    fn triggers_manual_compact() {
        let mut value = input();
        value.manual_request = Some(ManualRotationRequest::Compact);

        let decision = evaluate_rotation_triggers(&value).expect("evaluation should succeed");
        assert!(decision.should_rotate);
        assert_eq!(decision.triggers, vec![RotationTrigger::ManualCompact]);
        assert!(decision.triggers.contains(&RotationTrigger::ManualCompact));
    }

    #[test]
    fn triggers_manual_reset() {
        let mut value = input();
        value.manual_request = Some(ManualRotationRequest::Reset);

        let decision = evaluate_rotation_triggers(&value).expect("evaluation should succeed");
        assert!(decision.should_rotate);
        assert_eq!(decision.triggers, vec![RotationTrigger::ManualReset]);
        assert!(decision.triggers.contains(&RotationTrigger::ManualReset));
    }

    #[test]
    fn triggers_backend_compaction_when_signaled() {
        let mut value = input();
        value.backend_compaction_signaled = true;

        let decision = evaluate_rotation_triggers(&value).expect("evaluation should succeed");
        assert!(decision.should_rotate);
        assert_eq!(decision.triggers, vec![RotationTrigger::BackendCompaction]);
        assert!(decision
            .triggers
            .contains(&RotationTrigger::BackendCompaction));
    }

    #[test]
    fn ignores_backend_compaction_when_not_signaled() {
        let value = input();

        let decision = evaluate_rotation_triggers(&value).expect("evaluation should succeed");
        assert!(!decision.should_rotate);
        assert!(decision.triggers.is_empty());
    }

    #[test]
    fn inactivity_requires_idle_lane() {
        let mut value = input();
        value.now_epoch_ms = 100_000;
        value.last_activity_epoch_ms = 0;
        value.lane_is_idle = false;

        let decision = evaluate_rotation_triggers(&value).expect("evaluation should succeed");
        assert!(!decision.should_rotate);
        assert!(decision.triggers.is_empty());
    }

    #[test]
    fn triggers_inactivity_when_idle_and_timeout_elapsed() {
        let mut value = input();
        value.lane_is_idle = true;
        value.now_epoch_ms = 10_000;
        value.last_activity_epoch_ms = 4_000;
        value.inactivity_timeout_secs = 6;

        let decision = evaluate_rotation_triggers(&value).expect("evaluation should succeed");
        assert!(decision.should_rotate);
        assert_eq!(decision.triggers, vec![RotationTrigger::InactivityTimeout]);
        assert!(decision
            .triggers
            .contains(&RotationTrigger::InactivityTimeout));
    }

    #[test]
    fn does_not_trigger_inactivity_before_timeout_window() {
        let mut value = input();
        value.lane_is_idle = true;
        value.last_activity_epoch_ms = 9_500;
        value.now_epoch_ms = 10_000;
        value.inactivity_timeout_secs = 1;

        let decision = evaluate_rotation_triggers(&value).expect("evaluation should succeed");
        assert!(!decision.should_rotate);
        assert!(!decision
            .triggers
            .contains(&RotationTrigger::InactivityTimeout));
    }

    #[test]
    fn includes_multiple_triggers_in_stable_priority_order() {
        let mut value = input();
        value.manual_request = Some(ManualRotationRequest::Reset);
        value.backend_compaction_signaled = true;
        value.lane_is_idle = true;
        value.last_activity_epoch_ms = 0;
        value.now_epoch_ms = 120_000;
        value.inactivity_timeout_secs = 60;

        let decision = evaluate_rotation_triggers(&value).expect("evaluation should succeed");
        assert!(decision.should_rotate);
        assert_eq!(
            decision.triggers,
            vec![
                RotationTrigger::ManualReset,
                RotationTrigger::BackendCompaction,
                RotationTrigger::InactivityTimeout,
            ]
        );
    }

    #[test]
    fn rejects_zero_inactivity_timeout() {
        let mut value = input();
        value.inactivity_timeout_secs = 0;

        let error =
            evaluate_rotation_triggers(&value).expect_err("zero inactivity timeout should fail");
        assert_eq!(
            error,
            CrabError::InvalidConfig {
                key: "CRAB_INACTIVITY_TIMEOUT_SECS",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );
    }

    #[test]
    fn rejects_clock_skew_where_now_precedes_last_activity() {
        let mut value = input();
        value.now_epoch_ms = 4_999;
        value.last_activity_epoch_ms = 5_000;

        let error = evaluate_rotation_triggers(&value).expect_err("clock skew should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "rotation_trigger_evaluator",
                message:
                    "now_epoch_ms 4999 must be greater than or equal to last_activity_epoch_ms 5000"
                        .to_string(),
            }
        );
    }

    #[test]
    fn rejects_inactivity_timeout_that_overflows_millisecond_conversion() {
        let mut value = input();
        value.lane_is_idle = true;
        value.inactivity_timeout_secs = u64::MAX;

        let error =
            evaluate_rotation_triggers(&value).expect_err("overflowing timeout should fail");
        assert_eq!(
            error,
            CrabError::InvalidConfig {
                key: "CRAB_INACTIVITY_TIMEOUT_SECS",
                value: u64::MAX.to_string(),
                reason: "must fit in milliseconds as a u64",
            }
        );
    }
}
