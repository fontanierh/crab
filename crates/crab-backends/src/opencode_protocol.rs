use crab_core::{CrabError, CrabResult};

use crate::ensure_non_empty_field;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OpenCodeSessionConfig {
    pub model: Option<String>,
    pub reasoning_level: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OpenCodeTurnConfig {
    pub model: Option<String>,
    pub reasoning_level: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenCodeApiRequest {
    CreateSession {
        config: OpenCodeSessionConfig,
    },
    SendPrompt {
        session_id: String,
        prompt: String,
        config: OpenCodeTurnConfig,
    },
    InterruptTurn {
        session_id: String,
        turn_id: String,
    },
    EndSession {
        session_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenCodeApiResponse {
    SessionCreated { session_id: String },
    TurnAccepted { turn_id: String },
    Accepted,
}

pub trait OpenCodeApiTransport: Send + Sync {
    fn execute(&self, request: OpenCodeApiRequest) -> CrabResult<OpenCodeApiResponse>;
}

#[derive(Debug, Clone)]
pub struct OpenCodeProtocol<T: OpenCodeApiTransport> {
    transport: T,
}

impl<T: OpenCodeApiTransport> OpenCodeProtocol<T> {
    #[must_use]
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    pub fn create_session(&self, config: OpenCodeSessionConfig) -> CrabResult<String> {
        validate_session_config("opencode_create_session_input", &config)?;
        match self
            .transport
            .execute(OpenCodeApiRequest::CreateSession { config })?
        {
            OpenCodeApiResponse::SessionCreated { session_id } => {
                validate_response_id("opencode_create_session_response", "session_id", session_id)
            }
            response => Err(unexpected_response(
                "opencode_create_session_response",
                "SessionCreated",
                &response,
            )),
        }
    }

    pub fn send_prompt(
        &self,
        session_id: &str,
        prompt: &str,
        config: OpenCodeTurnConfig,
    ) -> CrabResult<String> {
        ensure_non_empty_field("opencode_send_prompt_input", "session_id", session_id)?;
        ensure_non_empty_field("opencode_send_prompt_input", "prompt", prompt)?;
        validate_turn_config("opencode_send_prompt_input", &config)?;
        match self.transport.execute(OpenCodeApiRequest::SendPrompt {
            session_id: session_id.to_string(),
            prompt: prompt.to_string(),
            config,
        })? {
            OpenCodeApiResponse::TurnAccepted { turn_id } => {
                validate_response_id("opencode_send_prompt_response", "turn_id", turn_id)
            }
            response => Err(unexpected_response(
                "opencode_send_prompt_response",
                "TurnAccepted",
                &response,
            )),
        }
    }

    pub fn interrupt_turn(&self, session_id: &str, turn_id: &str) -> CrabResult<()> {
        ensure_non_empty_field("opencode_interrupt_input", "session_id", session_id)?;
        ensure_non_empty_field("opencode_interrupt_input", "turn_id", turn_id)?;
        match self.transport.execute(OpenCodeApiRequest::InterruptTurn {
            session_id: session_id.to_string(),
            turn_id: turn_id.to_string(),
        })? {
            OpenCodeApiResponse::Accepted => Ok(()),
            response => Err(unexpected_response(
                "opencode_interrupt_response",
                "Accepted",
                &response,
            )),
        }
    }

    pub fn end_session(&self, session_id: &str) -> CrabResult<()> {
        ensure_non_empty_field("opencode_end_session_input", "session_id", session_id)?;
        match self.transport.execute(OpenCodeApiRequest::EndSession {
            session_id: session_id.to_string(),
        })? {
            OpenCodeApiResponse::Accepted => Ok(()),
            response => Err(unexpected_response(
                "opencode_end_session_response",
                "Accepted",
                &response,
            )),
        }
    }
}

fn validate_session_config(
    context: &'static str,
    config: &OpenCodeSessionConfig,
) -> CrabResult<()> {
    validate_optional_non_empty(context, "model", config.model.as_deref())?;
    validate_optional_non_empty(
        context,
        "reasoning_level",
        config.reasoning_level.as_deref(),
    )
}

fn validate_turn_config(context: &'static str, config: &OpenCodeTurnConfig) -> CrabResult<()> {
    validate_optional_non_empty(context, "model", config.model.as_deref())?;
    validate_optional_non_empty(
        context,
        "reasoning_level",
        config.reasoning_level.as_deref(),
    )
}

fn validate_optional_non_empty(
    context: &'static str,
    field_name: &'static str,
    value: Option<&str>,
) -> CrabResult<()> {
    if let Some(value) = value {
        ensure_non_empty_field(context, field_name, value)?;
    }
    Ok(())
}

fn validate_response_id(
    context: &'static str,
    field_name: &'static str,
    value: String,
) -> CrabResult<String> {
    ensure_non_empty_field(context, field_name, &value)?;
    Ok(value)
}

fn unexpected_response(
    context: &'static str,
    expected: &'static str,
    response: &OpenCodeApiResponse,
) -> CrabError {
    CrabError::InvariantViolation {
        context,
        message: format!("expected {expected} response, got {response:?}"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crab_core::{CrabError, CrabResult};

    use super::{
        OpenCodeApiRequest, OpenCodeApiResponse, OpenCodeApiTransport, OpenCodeProtocol,
        OpenCodeSessionConfig, OpenCodeTurnConfig,
    };

    #[derive(Debug, Clone)]
    struct FakeTransport {
        state: Arc<Mutex<FakeTransportState>>,
    }

    #[derive(Debug, Clone)]
    struct FakeTransportState {
        scripted: Vec<CrabResult<OpenCodeApiResponse>>,
        seen_requests: Vec<OpenCodeApiRequest>,
        cursor: usize,
    }

    impl FakeTransport {
        fn with_scripted(scripted: Vec<CrabResult<OpenCodeApiResponse>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeTransportState {
                    scripted,
                    seen_requests: Vec::new(),
                    cursor: 0,
                })),
            }
        }

        fn seen(&self) -> Vec<OpenCodeApiRequest> {
            self.state
                .lock()
                .expect("lock should succeed")
                .seen_requests
                .clone()
        }
    }

    impl OpenCodeApiTransport for FakeTransport {
        fn execute(&self, request: OpenCodeApiRequest) -> CrabResult<OpenCodeApiResponse> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.seen_requests.push(request);
            let index = state.cursor;
            state.cursor += 1;
            match state.scripted.get(index) {
                Some(result) => result.clone(),
                None => Err(CrabError::InvariantViolation {
                    context: "fake_opencode_protocol_transport",
                    message: "missing scripted response".to_string(),
                }),
            }
        }
    }

    fn protocol_with(
        scripted: Vec<CrabResult<OpenCodeApiResponse>>,
    ) -> (OpenCodeProtocol<FakeTransport>, FakeTransport) {
        let transport = FakeTransport::with_scripted(scripted);
        (OpenCodeProtocol::new(transport.clone()), transport)
    }

    #[test]
    fn create_session_sends_config_and_returns_session_id() {
        let (protocol, transport) = protocol_with(vec![Ok(OpenCodeApiResponse::SessionCreated {
            session_id: "session-1".to_string(),
        })]);
        let session_id = protocol
            .create_session(OpenCodeSessionConfig {
                model: Some("gpt-5".to_string()),
                reasoning_level: Some("high".to_string()),
            })
            .expect("create session should return id");
        assert_eq!(session_id, "session-1");
        assert_eq!(
            transport.seen(),
            vec![OpenCodeApiRequest::CreateSession {
                config: OpenCodeSessionConfig {
                    model: Some("gpt-5".to_string()),
                    reasoning_level: Some("high".to_string()),
                },
            }]
        );
    }

    #[test]
    fn send_prompt_sends_turn_request_and_returns_turn_id() {
        let (protocol, transport) = protocol_with(vec![Ok(OpenCodeApiResponse::TurnAccepted {
            turn_id: "turn-5".to_string(),
        })]);
        let turn_id = protocol
            .send_prompt(
                "session-2",
                "Ship it",
                OpenCodeTurnConfig {
                    model: Some("o3".to_string()),
                    reasoning_level: None,
                },
            )
            .expect("send_prompt should return turn id");
        assert_eq!(turn_id, "turn-5");
        assert_eq!(
            transport.seen(),
            vec![OpenCodeApiRequest::SendPrompt {
                session_id: "session-2".to_string(),
                prompt: "Ship it".to_string(),
                config: OpenCodeTurnConfig {
                    model: Some("o3".to_string()),
                    reasoning_level: None,
                },
            }]
        );
    }

    #[test]
    fn interrupt_and_end_session_require_accepted_response() {
        let (protocol, transport) = protocol_with(vec![
            Ok(OpenCodeApiResponse::Accepted),
            Ok(OpenCodeApiResponse::Accepted),
        ]);

        protocol
            .interrupt_turn("session-9", "turn-7")
            .expect("interrupt should accept");
        protocol
            .end_session("session-9")
            .expect("end_session should accept");

        assert_eq!(
            transport.seen(),
            vec![
                OpenCodeApiRequest::InterruptTurn {
                    session_id: "session-9".to_string(),
                    turn_id: "turn-7".to_string(),
                },
                OpenCodeApiRequest::EndSession {
                    session_id: "session-9".to_string(),
                },
            ]
        );
    }

    #[test]
    fn protocol_validates_inputs() {
        let (protocol, transport) = protocol_with(Vec::new());

        let create_error = protocol
            .create_session(OpenCodeSessionConfig {
                model: Some(" ".to_string()),
                reasoning_level: None,
            })
            .expect_err("blank model should fail");
        assert_eq!(
            create_error,
            CrabError::InvariantViolation {
                context: "opencode_create_session_input",
                message: "model must not be empty".to_string(),
            }
        );

        let prompt_error = protocol
            .send_prompt("session-a", " ", OpenCodeTurnConfig::default())
            .expect_err("blank prompt should fail");
        assert_eq!(
            prompt_error,
            CrabError::InvariantViolation {
                context: "opencode_send_prompt_input",
                message: "prompt must not be empty".to_string(),
            }
        );

        let prompt_session_error = protocol
            .send_prompt(" ", "ok", OpenCodeTurnConfig::default())
            .expect_err("blank session id should fail");
        assert_eq!(
            prompt_session_error,
            CrabError::InvariantViolation {
                context: "opencode_send_prompt_input",
                message: "session_id must not be empty".to_string(),
            }
        );

        let model_error = protocol
            .send_prompt(
                "session-a",
                "ok",
                OpenCodeTurnConfig {
                    model: Some(" ".to_string()),
                    reasoning_level: None,
                },
            )
            .expect_err("blank model should fail");
        assert_eq!(
            model_error,
            CrabError::InvariantViolation {
                context: "opencode_send_prompt_input",
                message: "model must not be empty".to_string(),
            }
        );

        let reason_error = protocol
            .send_prompt(
                "session-a",
                "ok",
                OpenCodeTurnConfig {
                    model: None,
                    reasoning_level: Some(" ".to_string()),
                },
            )
            .expect_err("blank reasoning should fail");
        assert_eq!(
            reason_error,
            CrabError::InvariantViolation {
                context: "opencode_send_prompt_input",
                message: "reasoning_level must not be empty".to_string(),
            }
        );

        let interrupt_error = protocol
            .interrupt_turn(" ", "turn-1")
            .expect_err("blank session id should fail");
        assert_eq!(
            interrupt_error,
            CrabError::InvariantViolation {
                context: "opencode_interrupt_input",
                message: "session_id must not be empty".to_string(),
            }
        );

        let interrupt_turn_error = protocol
            .interrupt_turn("session-a", " ")
            .expect_err("blank turn id should fail");
        assert_eq!(
            interrupt_turn_error,
            CrabError::InvariantViolation {
                context: "opencode_interrupt_input",
                message: "turn_id must not be empty".to_string(),
            }
        );

        let end_error = protocol
            .end_session(" ")
            .expect_err("blank session id should fail");
        assert_eq!(
            end_error,
            CrabError::InvariantViolation {
                context: "opencode_end_session_input",
                message: "session_id must not be empty".to_string(),
            }
        );
        assert!(transport.seen().is_empty());
    }

    #[test]
    fn protocol_validates_response_shapes() {
        let (create_protocol, _) = protocol_with(vec![Ok(OpenCodeApiResponse::Accepted)]);
        let create_error = create_protocol
            .create_session(OpenCodeSessionConfig::default())
            .expect_err("unexpected create response should fail");
        assert_eq!(
            create_error,
            CrabError::InvariantViolation {
                context: "opencode_create_session_response",
                message: "expected SessionCreated response, got Accepted".to_string(),
            }
        );

        let (blank_id_protocol, _) = protocol_with(vec![Ok(OpenCodeApiResponse::SessionCreated {
            session_id: " ".to_string(),
        })]);
        let blank_id_error = blank_id_protocol
            .create_session(OpenCodeSessionConfig::default())
            .expect_err("blank session id should fail");
        assert_eq!(
            blank_id_error,
            CrabError::InvariantViolation {
                context: "opencode_create_session_response",
                message: "session_id must not be empty".to_string(),
            }
        );

        let (turn_protocol, _) = protocol_with(vec![Ok(OpenCodeApiResponse::SessionCreated {
            session_id: "session-x".to_string(),
        })]);
        let turn_error = turn_protocol
            .send_prompt("session-x", "hi", OpenCodeTurnConfig::default())
            .expect_err("unexpected turn response should fail");
        assert_eq!(
            turn_error,
            CrabError::InvariantViolation {
                context: "opencode_send_prompt_response",
                message: "expected TurnAccepted response, got SessionCreated { session_id: \"session-x\" }".to_string(),
            }
        );

        let (turn_blank_id_protocol, _) =
            protocol_with(vec![Ok(OpenCodeApiResponse::TurnAccepted {
                turn_id: " ".to_string(),
            })]);
        let turn_blank_id_error = turn_blank_id_protocol
            .send_prompt("session-x", "hi", OpenCodeTurnConfig::default())
            .expect_err("blank turn id should fail");
        assert_eq!(
            turn_blank_id_error,
            CrabError::InvariantViolation {
                context: "opencode_send_prompt_response",
                message: "turn_id must not be empty".to_string(),
            }
        );

        let (interrupt_protocol, _) = protocol_with(vec![Ok(OpenCodeApiResponse::TurnAccepted {
            turn_id: "turn-x".to_string(),
        })]);
        let interrupt_response_error = interrupt_protocol
            .interrupt_turn("session-y", "turn-z")
            .expect_err("unexpected interrupt response should fail");
        assert_eq!(
            interrupt_response_error,
            CrabError::InvariantViolation {
                context: "opencode_interrupt_response",
                message: "expected Accepted response, got TurnAccepted { turn_id: \"turn-x\" }"
                    .to_string(),
            }
        );

        let (end_protocol, _) = protocol_with(vec![Ok(OpenCodeApiResponse::SessionCreated {
            session_id: "session-y".to_string(),
        })]);
        let end_response_error = end_protocol
            .end_session("session-y")
            .expect_err("unexpected end response should fail");
        assert_eq!(
            end_response_error,
            CrabError::InvariantViolation {
                context: "opencode_end_session_response",
                message:
                    "expected Accepted response, got SessionCreated { session_id: \"session-y\" }"
                        .to_string(),
            }
        );
    }

    #[test]
    fn protocol_propagates_transport_errors_and_guard_errors() {
        let transport_error = CrabError::InvariantViolation {
            context: "fake_transport",
            message: "boom".to_string(),
        };
        let (send_protocol, _) = protocol_with(vec![Err(transport_error.clone())]);
        let send_error = send_protocol
            .send_prompt("session-ok", "hello", OpenCodeTurnConfig::default())
            .expect_err("transport failure should propagate");
        assert_eq!(send_error, transport_error);

        let interrupt_transport_error = CrabError::InvariantViolation {
            context: "fake_transport_interrupt",
            message: "boom".to_string(),
        };
        let (interrupt_protocol, _) = protocol_with(vec![Err(interrupt_transport_error.clone())]);
        let interrupt_error = interrupt_protocol
            .interrupt_turn("session-ok", "turn-ok")
            .expect_err("interrupt transport failure should propagate");
        assert_eq!(interrupt_error, interrupt_transport_error);

        let end_transport_error = CrabError::InvariantViolation {
            context: "fake_transport_end",
            message: "boom".to_string(),
        };
        let (end_protocol, _) = protocol_with(vec![Err(end_transport_error.clone())]);
        let end_error = end_protocol
            .end_session("session-ok")
            .expect_err("end transport failure should propagate");
        assert_eq!(end_error, end_transport_error);

        let (guard_protocol, _) = protocol_with(Vec::new());
        let guard_error = guard_protocol
            .create_session(OpenCodeSessionConfig::default())
            .expect_err("missing scripted response should be actionable");
        assert_eq!(
            guard_error,
            CrabError::InvariantViolation {
                context: "fake_opencode_protocol_transport",
                message: "missing scripted response".to_string(),
            }
        );
    }
}
