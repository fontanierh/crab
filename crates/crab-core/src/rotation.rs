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
    TokenCompaction,
    InactivityTimeout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotationTriggerInput {
    pub now_epoch_ms: u64,
    pub last_activity_epoch_ms: u64,
    pub lane_is_idle: bool,
    pub token_usage_total: Option<u64>,
    pub compaction_token_threshold: u64,
    pub inactivity_timeout_secs: u64,
    pub manual_request: Option<ManualRotationRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotationTriggerDecision {
    pub should_rotate: bool,
    pub triggers: Vec<RotationTrigger>,
}

impl RotationTriggerDecision {
    #[must_use]
    pub fn triggered_by(&self, trigger: RotationTrigger) -> bool {
        self.triggers.contains(&trigger)
    }
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

    if input
        .token_usage_total
        .is_some_and(|usage| usage >= input.compaction_token_threshold)
    {
        triggers.push(RotationTrigger::TokenCompaction);
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
    if input.compaction_token_threshold == 0 {
        return Err(CrabError::InvalidConfig {
            key: "CRAB_COMPACTION_TOKEN_THRESHOLD",
            value: input.compaction_token_threshold.to_string(),
            reason: "must be greater than 0",
        });
    }

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
            token_usage_total: Some(1_000),
            compaction_token_threshold: 2_000,
            inactivity_timeout_secs: 60,
            manual_request: None,
        }
    }

    #[test]
    fn returns_no_rotation_when_no_triggers_fire() {
        let decision = evaluate_rotation_triggers(&input()).expect("evaluation should succeed");
        assert!(!decision.should_rotate);
        assert!(decision.triggers.is_empty());
        assert!(!decision.triggered_by(RotationTrigger::TokenCompaction));
    }

    #[test]
    fn triggers_manual_compact() {
        let mut value = input();
        value.manual_request = Some(ManualRotationRequest::Compact);

        let decision = evaluate_rotation_triggers(&value).expect("evaluation should succeed");
        assert!(decision.should_rotate);
        assert_eq!(decision.triggers, vec![RotationTrigger::ManualCompact]);
        assert!(decision.triggered_by(RotationTrigger::ManualCompact));
    }

    #[test]
    fn triggers_manual_reset() {
        let mut value = input();
        value.manual_request = Some(ManualRotationRequest::Reset);

        let decision = evaluate_rotation_triggers(&value).expect("evaluation should succeed");
        assert!(decision.should_rotate);
        assert_eq!(decision.triggers, vec![RotationTrigger::ManualReset]);
        assert!(decision.triggered_by(RotationTrigger::ManualReset));
    }

    #[test]
    fn triggers_compaction_when_usage_reaches_threshold() {
        let mut value = input();
        value.token_usage_total = Some(2_000);

        let decision = evaluate_rotation_triggers(&value).expect("evaluation should succeed");
        assert!(decision.should_rotate);
        assert_eq!(decision.triggers, vec![RotationTrigger::TokenCompaction]);
        assert!(decision.triggered_by(RotationTrigger::TokenCompaction));
    }

    #[test]
    fn ignores_compaction_when_usage_is_unavailable() {
        let mut value = input();
        value.token_usage_total = None;

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
        assert!(decision.triggered_by(RotationTrigger::InactivityTimeout));
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
        assert!(!decision.triggered_by(RotationTrigger::InactivityTimeout));
    }

    #[test]
    fn includes_multiple_triggers_in_stable_priority_order() {
        let mut value = input();
        value.manual_request = Some(ManualRotationRequest::Reset);
        value.token_usage_total = Some(4_000);
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
                RotationTrigger::TokenCompaction,
                RotationTrigger::InactivityTimeout,
            ]
        );
    }

    #[test]
    fn rejects_zero_compaction_threshold() {
        let mut value = input();
        value.compaction_token_threshold = 0;

        let error =
            evaluate_rotation_triggers(&value).expect_err("zero compaction threshold should fail");
        assert_eq!(
            error,
            CrabError::InvalidConfig {
                key: "CRAB_COMPACTION_TOKEN_THRESHOLD",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
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
