use std::collections::{BTreeMap, BTreeSet};

use crab_core::{CrabError, CrabResult};

use crate::ensure_non_empty_field;

const DEFAULT_USER_INPUT_REJECTION_MESSAGE: &str =
    "Crab is running unattended and cannot answer interactive questions.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexApprovalPolicy {
    AutoApprove,
    AutoApproveForSession,
    Decline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexApprovalDecision {
    Accept,
    AcceptForSession,
    Decline,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexUnattendedPolicy {
    pub approval_policy: CodexApprovalPolicy,
    pub user_input_rejection_message: String,
}

impl Default for CodexUnattendedPolicy {
    fn default() -> Self {
        Self {
            approval_policy: CodexApprovalPolicy::AutoApprove,
            user_input_rejection_message: DEFAULT_USER_INPUT_REJECTION_MESSAGE.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexUserInputQuestion {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexInteractiveRequest {
    CommandExecutionApproval {
        request_id: String,
    },
    FileChangeApproval {
        request_id: String,
    },
    ToolUserInput {
        request_id: String,
        questions: Vec<CodexUserInputQuestion>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexRequestResponse {
    CommandExecutionApproval {
        request_id: String,
        decision: CodexApprovalDecision,
    },
    FileChangeApproval {
        request_id: String,
        decision: CodexApprovalDecision,
    },
    ToolUserInput {
        request_id: String,
        answers: BTreeMap<String, Vec<String>>,
    },
}

pub fn decide_unattended_response(
    policy: &CodexUnattendedPolicy,
    request: &CodexInteractiveRequest,
) -> CrabResult<CodexRequestResponse> {
    ensure_non_empty_field(
        "codex_unattended_policy",
        "user_input_rejection_message",
        &policy.user_input_rejection_message,
    )?;

    match request {
        CodexInteractiveRequest::CommandExecutionApproval { request_id } => {
            ensure_non_empty_field("codex_unattended_policy", "request_id", request_id)?;
            Ok(CodexRequestResponse::CommandExecutionApproval {
                request_id: request_id.clone(),
                decision: approval_decision(policy.approval_policy),
            })
        }
        CodexInteractiveRequest::FileChangeApproval { request_id } => {
            ensure_non_empty_field("codex_unattended_policy", "request_id", request_id)?;
            Ok(CodexRequestResponse::FileChangeApproval {
                request_id: request_id.clone(),
                decision: approval_decision(policy.approval_policy),
            })
        }
        CodexInteractiveRequest::ToolUserInput {
            request_id,
            questions,
        } => {
            ensure_non_empty_field("codex_unattended_policy", "request_id", request_id)?;
            if questions.is_empty() {
                return Err(CrabError::InvariantViolation {
                    context: "codex_unattended_policy",
                    message: "tool user input request must include at least one question"
                        .to_string(),
                });
            }

            let mut seen_question_ids = BTreeSet::new();
            let mut answers = BTreeMap::new();

            for question in questions {
                ensure_non_empty_field("codex_unattended_policy", "question.id", &question.id)?;
                if !seen_question_ids.insert(question.id.clone()) {
                    return Err(CrabError::InvariantViolation {
                        context: "codex_unattended_policy",
                        message: format!("duplicate question id {}", question.id),
                    });
                }
                answers.insert(
                    question.id.clone(),
                    vec![policy.user_input_rejection_message.clone()],
                );
            }

            Ok(CodexRequestResponse::ToolUserInput {
                request_id: request_id.clone(),
                answers,
            })
        }
    }
}

fn approval_decision(policy: CodexApprovalPolicy) -> CodexApprovalDecision {
    match policy {
        CodexApprovalPolicy::AutoApprove => CodexApprovalDecision::Accept,
        CodexApprovalPolicy::AutoApproveForSession => CodexApprovalDecision::AcceptForSession,
        CodexApprovalPolicy::Decline => CodexApprovalDecision::Decline,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crab_core::CrabError;

    use super::{
        decide_unattended_response, CodexApprovalDecision, CodexApprovalPolicy,
        CodexInteractiveRequest, CodexRequestResponse, CodexUnattendedPolicy,
        CodexUserInputQuestion,
    };

    #[test]
    fn default_policy_auto_approves_command_and_file_requests() {
        let policy = CodexUnattendedPolicy::default();

        let command = decide_unattended_response(
            &policy,
            &CodexInteractiveRequest::CommandExecutionApproval {
                request_id: "req-cmd".to_string(),
            },
        )
        .expect("command request should succeed");
        assert_eq!(
            command,
            CodexRequestResponse::CommandExecutionApproval {
                request_id: "req-cmd".to_string(),
                decision: CodexApprovalDecision::Accept,
            }
        );

        let file = decide_unattended_response(
            &policy,
            &CodexInteractiveRequest::FileChangeApproval {
                request_id: "req-file".to_string(),
            },
        )
        .expect("file request should succeed");
        assert_eq!(
            file,
            CodexRequestResponse::FileChangeApproval {
                request_id: "req-file".to_string(),
                decision: CodexApprovalDecision::Accept,
            }
        );
    }

    #[test]
    fn policy_can_auto_approve_for_session_or_decline() {
        let session_policy = CodexUnattendedPolicy {
            approval_policy: CodexApprovalPolicy::AutoApproveForSession,
            ..CodexUnattendedPolicy::default()
        };
        let session_decision = decide_unattended_response(
            &session_policy,
            &CodexInteractiveRequest::CommandExecutionApproval {
                request_id: "req-1".to_string(),
            },
        )
        .expect("session auto approval should succeed");
        assert_eq!(
            session_decision,
            CodexRequestResponse::CommandExecutionApproval {
                request_id: "req-1".to_string(),
                decision: CodexApprovalDecision::AcceptForSession,
            }
        );

        let decline_policy = CodexUnattendedPolicy {
            approval_policy: CodexApprovalPolicy::Decline,
            ..CodexUnattendedPolicy::default()
        };
        let decline_decision = decide_unattended_response(
            &decline_policy,
            &CodexInteractiveRequest::FileChangeApproval {
                request_id: "req-2".to_string(),
            },
        )
        .expect("decline policy should succeed");
        assert_eq!(
            decline_decision,
            CodexRequestResponse::FileChangeApproval {
                request_id: "req-2".to_string(),
                decision: CodexApprovalDecision::Decline,
            }
        );
    }

    #[test]
    fn tool_user_input_is_rejected_with_deterministic_answers() {
        let policy = CodexUnattendedPolicy {
            approval_policy: CodexApprovalPolicy::AutoApprove,
            user_input_rejection_message: "unattended mode: request rejected".to_string(),
        };

        let response = decide_unattended_response(
            &policy,
            &CodexInteractiveRequest::ToolUserInput {
                request_id: "req-ui".to_string(),
                questions: vec![
                    CodexUserInputQuestion {
                        id: "q-1".to_string(),
                    },
                    CodexUserInputQuestion {
                        id: "q-2".to_string(),
                    },
                ],
            },
        )
        .expect("user input request should succeed");

        assert_eq!(
            response,
            CodexRequestResponse::ToolUserInput {
                request_id: "req-ui".to_string(),
                answers: BTreeMap::from([
                    (
                        "q-1".to_string(),
                        vec!["unattended mode: request rejected".to_string()],
                    ),
                    (
                        "q-2".to_string(),
                        vec!["unattended mode: request rejected".to_string()],
                    ),
                ]),
            }
        );
    }

    #[test]
    fn unattended_policy_validates_request_and_policy_shape() {
        let blank_policy_message = CodexUnattendedPolicy {
            approval_policy: CodexApprovalPolicy::AutoApprove,
            user_input_rejection_message: " ".to_string(),
        };
        let blank_policy_err = decide_unattended_response(
            &blank_policy_message,
            &CodexInteractiveRequest::CommandExecutionApproval {
                request_id: "req-1".to_string(),
            },
        )
        .expect_err("blank rejection message should fail");
        assert_eq!(
            blank_policy_err,
            CrabError::InvariantViolation {
                context: "codex_unattended_policy",
                message: "user_input_rejection_message must not be empty".to_string(),
            }
        );

        let policy = CodexUnattendedPolicy::default();
        let blank_request_id = decide_unattended_response(
            &policy,
            &CodexInteractiveRequest::FileChangeApproval {
                request_id: " ".to_string(),
            },
        )
        .expect_err("blank request id should fail");
        assert_eq!(
            blank_request_id,
            CrabError::InvariantViolation {
                context: "codex_unattended_policy",
                message: "request_id must not be empty".to_string(),
            }
        );

        let blank_command_request_id = decide_unattended_response(
            &policy,
            &CodexInteractiveRequest::CommandExecutionApproval {
                request_id: " ".to_string(),
            },
        )
        .expect_err("blank command request id should fail");
        assert_eq!(
            blank_command_request_id,
            CrabError::InvariantViolation {
                context: "codex_unattended_policy",
                message: "request_id must not be empty".to_string(),
            }
        );

        let empty_questions = decide_unattended_response(
            &policy,
            &CodexInteractiveRequest::ToolUserInput {
                request_id: "req-ui".to_string(),
                questions: Vec::new(),
            },
        )
        .expect_err("empty question list should fail");
        assert_eq!(
            empty_questions,
            CrabError::InvariantViolation {
                context: "codex_unattended_policy",
                message: "tool user input request must include at least one question".to_string(),
            }
        );

        let blank_question_id = decide_unattended_response(
            &policy,
            &CodexInteractiveRequest::ToolUserInput {
                request_id: "req-ui".to_string(),
                questions: vec![CodexUserInputQuestion {
                    id: " ".to_string(),
                }],
            },
        )
        .expect_err("blank question id should fail");
        assert_eq!(
            blank_question_id,
            CrabError::InvariantViolation {
                context: "codex_unattended_policy",
                message: "question.id must not be empty".to_string(),
            }
        );

        let blank_tool_request_id = decide_unattended_response(
            &policy,
            &CodexInteractiveRequest::ToolUserInput {
                request_id: " ".to_string(),
                questions: vec![CodexUserInputQuestion {
                    id: "q-1".to_string(),
                }],
            },
        )
        .expect_err("blank tool request id should fail");
        assert_eq!(
            blank_tool_request_id,
            CrabError::InvariantViolation {
                context: "codex_unattended_policy",
                message: "request_id must not be empty".to_string(),
            }
        );

        let duplicate_question_id = decide_unattended_response(
            &policy,
            &CodexInteractiveRequest::ToolUserInput {
                request_id: "req-ui".to_string(),
                questions: vec![
                    CodexUserInputQuestion {
                        id: "dup".to_string(),
                    },
                    CodexUserInputQuestion {
                        id: "dup".to_string(),
                    },
                ],
            },
        )
        .expect_err("duplicate question ids should fail");
        assert_eq!(
            duplicate_question_id,
            CrabError::InvariantViolation {
                context: "codex_unattended_policy",
                message: "duplicate question id dup".to_string(),
            }
        );
    }
}
