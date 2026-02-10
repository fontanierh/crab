use crate::{validation::validate_non_empty_text, CrabError, CrabResult};

pub const MEMORY_FLUSH_NO_REPLY_TOKEN: &str = "NO_REPLY";
pub const MEMORY_FLUSH_DONE_TOKEN: &str = "MEMORY_FLUSH_DONE";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryFlushAck {
    NoReply,
    Done,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HiddenMemoryFlushOutcome {
    pub ack: MemoryFlushAck,
    pub suppress_user_output: bool,
}

pub fn build_memory_flush_prompt(memory_flush_cycle_id: &str) -> CrabResult<String> {
    validate_non_empty_text(
        "memory_flush_prompt",
        "memory_flush_cycle_id",
        memory_flush_cycle_id,
    )?;

    Ok(format!(
        "Memory flush cycle: {memory_flush_cycle_id}\n\
You are running a hidden memory flush turn. Persist durable facts to memory files when needed.\n\
Do not produce normal assistant chat output. Respond with exactly one token:\n\
- {MEMORY_FLUSH_NO_REPLY_TOKEN}\n\
- {MEMORY_FLUSH_DONE_TOKEN}\n\
No extra text."
    ))
}

pub fn should_run_memory_flush_cycle(
    memory_flush_cycle_id: &str,
    last_completed_cycle_id: Option<&str>,
) -> CrabResult<bool> {
    validate_non_empty_text(
        "memory_flush_cycle_check",
        "memory_flush_cycle_id",
        memory_flush_cycle_id,
    )?;

    if let Some(last_completed_cycle_id) = last_completed_cycle_id {
        validate_non_empty_text(
            "memory_flush_cycle_check",
            "last_completed_cycle_id",
            last_completed_cycle_id,
        )?;
        if last_completed_cycle_id == memory_flush_cycle_id {
            return Ok(false);
        }
    }

    Ok(true)
}

pub fn finalize_hidden_memory_flush(raw_output: &str) -> CrabResult<HiddenMemoryFlushOutcome> {
    let ack = parse_memory_flush_ack(raw_output)?;
    Ok(HiddenMemoryFlushOutcome {
        ack,
        suppress_user_output: true,
    })
}

fn parse_memory_flush_ack(raw_output: &str) -> CrabResult<MemoryFlushAck> {
    let normalized = raw_output.trim();
    if normalized == MEMORY_FLUSH_NO_REPLY_TOKEN {
        return Ok(MemoryFlushAck::NoReply);
    }
    if normalized == MEMORY_FLUSH_DONE_TOKEN {
        return Ok(MemoryFlushAck::Done);
    }

    Err(CrabError::InvariantViolation {
        context: "memory_flush_hidden_turn",
        message: format!(
            "expected one of [{MEMORY_FLUSH_NO_REPLY_TOKEN}, {MEMORY_FLUSH_DONE_TOKEN}], got {:?}",
            raw_output
        ),
    })
}

#[cfg(test)]
mod tests {
    use crate::CrabError;

    use super::{
        build_memory_flush_prompt, finalize_hidden_memory_flush, should_run_memory_flush_cycle,
        MemoryFlushAck, MEMORY_FLUSH_DONE_TOKEN, MEMORY_FLUSH_NO_REPLY_TOKEN,
    };

    #[test]
    fn build_prompt_includes_cycle_id_and_expected_tokens() {
        let prompt =
            build_memory_flush_prompt("cycle-7").expect("prompt builder should accept cycle id");
        assert!(prompt.contains("Memory flush cycle: cycle-7"));
        assert!(prompt.contains(MEMORY_FLUSH_NO_REPLY_TOKEN));
        assert!(prompt.contains(MEMORY_FLUSH_DONE_TOKEN));
        assert!(prompt.contains("hidden memory flush turn"));
    }

    #[test]
    fn build_prompt_rejects_blank_cycle_id() {
        let error = build_memory_flush_prompt(" ").expect_err("blank cycle id should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "memory_flush_prompt",
                message: "memory_flush_cycle_id must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn cycle_gate_runs_when_no_previous_cycle_is_recorded() {
        let should_run = should_run_memory_flush_cycle("cycle-1", None)
            .expect("cycle gate should accept new cycle");
        assert!(should_run);
    }

    #[test]
    fn cycle_gate_skips_duplicate_cycle() {
        let should_run = should_run_memory_flush_cycle("cycle-2", Some("cycle-2"))
            .expect("cycle gate should compare current and previous cycle");
        assert!(!should_run);
    }

    #[test]
    fn cycle_gate_runs_when_cycle_changes() {
        let should_run = should_run_memory_flush_cycle("cycle-3", Some("cycle-2"))
            .expect("cycle gate should allow changed cycle");
        assert!(should_run);
    }

    #[test]
    fn cycle_gate_rejects_blank_current_cycle() {
        let error =
            should_run_memory_flush_cycle(" ", Some("cycle-1")).expect_err("blank cycle fails");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "memory_flush_cycle_check",
                message: "memory_flush_cycle_id must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn cycle_gate_rejects_blank_last_completed_cycle() {
        let error =
            should_run_memory_flush_cycle("cycle-2", Some(" ")).expect_err("blank last fails");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "memory_flush_cycle_check",
                message: "last_completed_cycle_id must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn finalize_accepts_no_reply_and_suppresses_output() {
        let outcome =
            finalize_hidden_memory_flush("NO_REPLY").expect("NO_REPLY marker should be accepted");
        assert_eq!(outcome.ack, MemoryFlushAck::NoReply);
        assert!(outcome.suppress_user_output);
    }

    #[test]
    fn finalize_accepts_done_with_whitespace() {
        let outcome = finalize_hidden_memory_flush("  MEMORY_FLUSH_DONE \n")
            .expect("MEMORY_FLUSH_DONE marker should be accepted");
        assert_eq!(outcome.ack, MemoryFlushAck::Done);
        assert!(outcome.suppress_user_output);
    }

    #[test]
    fn finalize_rejects_unexpected_output() {
        let error =
            finalize_hidden_memory_flush("completed").expect_err("unexpected output should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "memory_flush_hidden_turn",
                message: "expected one of [NO_REPLY, MEMORY_FLUSH_DONE], got \"completed\""
                    .to_string(),
            }
        );
    }

    #[test]
    fn finalize_rejects_empty_output() {
        let error = finalize_hidden_memory_flush("   ").expect_err("blank output should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "memory_flush_hidden_turn",
                message: "expected one of [NO_REPLY, MEMORY_FLUSH_DONE], got \"   \"".to_string(),
            }
        );
    }
}
