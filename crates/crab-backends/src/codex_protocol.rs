use std::collections::BTreeMap;

use crab_core::{CrabError, CrabResult};

use crate::ensure_non_empty_field;

const THREAD_ID_FIELD: &str = "threadId";
#[cfg(test)]
const TURN_ID_FIELD: &str = "turnId";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexRpcRequest {
    pub method: String,
    pub params: BTreeMap<String, String>,
    pub input: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CodexTurnConfig {
    pub model: Option<String>,
    pub effort: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CodexRpcResponse {
    pub fields: BTreeMap<String, String>,
}

pub trait CodexRpcTransport: Send + Sync {
    fn call(&self, request: CodexRpcRequest) -> CrabResult<CodexRpcResponse>;
}

#[derive(Debug, Clone)]
pub struct CodexProtocol<T: CodexRpcTransport> {
    transport: T,
}

impl<T: CodexRpcTransport> CodexProtocol<T> {
    #[must_use]
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    pub fn thread_start(&self) -> CrabResult<String> {
        let response = self.transport.call(CodexRpcRequest {
            method: "thread/start".to_string(),
            params: BTreeMap::new(),
            input: Vec::new(),
        })?;
        required_response_field(&response, "codex_thread_start_response", THREAD_ID_FIELD)
    }

    pub fn thread_resume(&self, thread_id: &str) -> CrabResult<String> {
        ensure_non_empty_field("codex_thread_resume_input", THREAD_ID_FIELD, thread_id)?;
        let response = self.transport.call(CodexRpcRequest {
            method: "thread/resume".to_string(),
            params: BTreeMap::from([(THREAD_ID_FIELD.to_string(), thread_id.to_string())]),
            input: Vec::new(),
        })?;
        required_response_field(&response, "codex_thread_resume_response", THREAD_ID_FIELD)
    }

    #[cfg(test)]
    fn turn_start(
        &self,
        thread_id: &str,
        input: &[String],
        config: CodexTurnConfig,
    ) -> CrabResult<String> {
        ensure_non_empty_field("codex_turn_start_input", THREAD_ID_FIELD, thread_id)?;
        validate_turn_config(&config)?;
        if input.is_empty() {
            return Err(CrabError::InvariantViolation {
                context: "codex_turn_start_input",
                message: "input must include at least one user message".to_string(),
            });
        }
        for (index, message) in input.iter().enumerate() {
            ensure_non_empty_field("codex_turn_start_input", "input message", message).map_err(
                |_| CrabError::InvariantViolation {
                    context: "codex_turn_start_input",
                    message: format!("input message at index {index} must not be empty"),
                },
            )?;
        }

        let mut params = BTreeMap::from([(THREAD_ID_FIELD.to_string(), thread_id.to_string())]);
        if let Some(model) = config.model {
            params.insert("model".to_string(), model);
        }
        if let Some(effort) = config.effort {
            params.insert("effort".to_string(), effort);
        }

        let response = self.transport.call(CodexRpcRequest {
            method: "turn/start".to_string(),
            params,
            input: input.to_vec(),
        })?;
        required_response_field(&response, "codex_turn_start_response", TURN_ID_FIELD)
    }

    #[cfg(test)]
    fn turn_interrupt(&self, thread_id: &str, turn_id: &str) -> CrabResult<()> {
        ensure_non_empty_field("codex_turn_interrupt_input", THREAD_ID_FIELD, thread_id)?;
        ensure_non_empty_field("codex_turn_interrupt_input", TURN_ID_FIELD, turn_id)?;
        self.transport.call(CodexRpcRequest {
            method: "turn/interrupt".to_string(),
            params: BTreeMap::from([
                (THREAD_ID_FIELD.to_string(), thread_id.to_string()),
                (TURN_ID_FIELD.to_string(), turn_id.to_string()),
            ]),
            input: Vec::new(),
        })?;
        Ok(())
    }
}

#[cfg(test)]
fn validate_turn_config(config: &CodexTurnConfig) -> CrabResult<()> {
    for (field_name, value) in [
        ("model", config.model.as_deref()),
        ("effort", config.effort.as_deref()),
    ] {
        if let Some(value) = value {
            ensure_non_empty_field("codex_turn_start_input", field_name, value)?;
        }
    }
    Ok(())
}

fn required_response_field(
    response: &CodexRpcResponse,
    context: &'static str,
    field_name: &'static str,
) -> CrabResult<String> {
    let value = response
        .fields
        .get(field_name)
        .ok_or_else(|| CrabError::InvariantViolation {
            context,
            message: format!("response missing required field {field_name}"),
        })?;
    ensure_non_empty_field(context, field_name, value)?;
    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use crab_core::{CrabError, CrabResult};

    use super::{
        CodexProtocol, CodexRpcRequest, CodexRpcResponse, CodexRpcTransport, CodexTurnConfig,
    };

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    struct FakeTransportStats {
        call_count: usize,
        requests: Vec<CodexRpcRequest>,
    }

    #[derive(Debug, Clone)]
    struct FakeTransport {
        state: Arc<Mutex<FakeTransportState>>,
    }

    #[derive(Debug, Clone)]
    struct FakeTransportState {
        scripted_responses: VecDeque<CrabResult<CodexRpcResponse>>,
        stats: FakeTransportStats,
    }

    impl FakeTransport {
        fn with_responses(responses: Vec<CrabResult<CodexRpcResponse>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeTransportState {
                    scripted_responses: VecDeque::from(responses),
                    stats: FakeTransportStats::default(),
                })),
            }
        }

        fn stats(&self) -> FakeTransportStats {
            self.state
                .lock()
                .expect("lock should succeed")
                .stats
                .clone()
        }
    }

    impl CodexRpcTransport for FakeTransport {
        fn call(&self, request: CodexRpcRequest) -> CrabResult<CodexRpcResponse> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.call_count += 1;
            state.stats.requests.push(request);
            state.scripted_responses.pop_front().unwrap_or_else(|| {
                Err(CrabError::InvariantViolation {
                    context: "fake_codex_transport_call",
                    message: "missing scripted response".to_string(),
                })
            })
        }
    }

    fn response(fields: &[(&str, &str)]) -> CodexRpcResponse {
        let mut map = BTreeMap::new();
        for (key, value) in fields {
            map.insert((*key).to_string(), (*value).to_string());
        }
        CodexRpcResponse { fields: map }
    }

    fn assert_single_request(transport: &FakeTransport, expected: CodexRpcRequest) {
        let stats = transport.stats();
        assert_eq!(stats.call_count, 1);
        assert_eq!(stats.requests, vec![expected]);
    }

    fn make_protocol(
        responses: Vec<CrabResult<CodexRpcResponse>>,
    ) -> (CodexProtocol<FakeTransport>, FakeTransport) {
        let transport = FakeTransport::with_responses(responses);
        (CodexProtocol::new(transport.clone()), transport)
    }

    #[test]
    fn thread_start_sends_contract_request_and_returns_thread_id() {
        let (protocol, transport) = make_protocol(vec![Ok(response(&[("threadId", "thread-1")]))]);
        let thread_id = protocol
            .thread_start()
            .expect("thread/start should parse thread id");
        assert_eq!(thread_id, "thread-1");
        assert_single_request(
            &transport,
            CodexRpcRequest {
                method: "thread/start".to_string(),
                params: BTreeMap::new(),
                input: Vec::new(),
            },
        );
    }

    #[test]
    fn thread_resume_sends_contract_request_and_returns_thread_id() {
        let (protocol, transport) = make_protocol(vec![Ok(response(&[("threadId", "thread-2")]))]);
        let thread_id = protocol
            .thread_resume("thread-2")
            .expect("thread/resume should parse thread id");
        assert_eq!(thread_id, "thread-2");
        assert_single_request(
            &transport,
            CodexRpcRequest {
                method: "thread/resume".to_string(),
                params: BTreeMap::from([("threadId".to_string(), "thread-2".to_string())]),
                input: Vec::new(),
            },
        );
    }

    #[test]
    fn turn_start_sends_contract_request_and_returns_turn_id() {
        let (protocol, transport) = make_protocol(vec![Ok(response(&[("turnId", "turn-7")]))]);
        let input = vec!["hello".to_string(), "status".to_string()];
        let turn_id = protocol
            .turn_start("thread-7", &input, CodexTurnConfig::default())
            .expect("turn/start should parse turn id");
        assert_eq!(turn_id, "turn-7");
        assert_single_request(
            &transport,
            CodexRpcRequest {
                method: "turn/start".to_string(),
                params: BTreeMap::from([("threadId".to_string(), "thread-7".to_string())]),
                input,
            },
        );
    }

    #[test]
    fn turn_start_includes_optional_model_and_effort_when_provided() {
        let (protocol, transport) = make_protocol(vec![Ok(response(&[("turnId", "turn-8")]))]);
        let input = vec!["hello".to_string()];
        let turn_id = protocol
            .turn_start(
                "thread-8",
                &input,
                CodexTurnConfig {
                    model: Some("gpt-5-codex".to_string()),
                    effort: Some("high".to_string()),
                },
            )
            .expect("turn/start should parse turn id");
        assert_eq!(turn_id, "turn-8");
        assert_single_request(
            &transport,
            CodexRpcRequest {
                method: "turn/start".to_string(),
                params: BTreeMap::from([
                    ("effort".to_string(), "high".to_string()),
                    ("model".to_string(), "gpt-5-codex".to_string()),
                    ("threadId".to_string(), "thread-8".to_string()),
                ]),
                input,
            },
        );
    }

    #[test]
    fn turn_interrupt_sends_contract_request() {
        let (protocol, transport) = make_protocol(vec![Ok(CodexRpcResponse::default())]);
        protocol
            .turn_interrupt("thread-3", "turn-9")
            .expect("turn/interrupt should succeed");
        assert_single_request(
            &transport,
            CodexRpcRequest {
                method: "turn/interrupt".to_string(),
                params: BTreeMap::from([
                    ("threadId".to_string(), "thread-3".to_string()),
                    ("turnId".to_string(), "turn-9".to_string()),
                ]),
                input: Vec::new(),
            },
        );
    }

    #[test]
    fn protocol_validates_required_inputs() {
        let (protocol, transport) = make_protocol(vec![Ok(CodexRpcResponse::default())]);

        let resume_err = protocol
            .thread_resume(" ")
            .expect_err("blank thread id should fail");
        assert_eq!(
            resume_err,
            CrabError::InvariantViolation {
                context: "codex_thread_resume_input",
                message: "threadId must not be empty".to_string(),
            }
        );

        let empty_input_err = protocol
            .turn_start("thread-1", &[], CodexTurnConfig::default())
            .expect_err("empty turn input should fail");
        assert_eq!(
            empty_input_err,
            CrabError::InvariantViolation {
                context: "codex_turn_start_input",
                message: "input must include at least one user message".to_string(),
            }
        );

        let blank_message_err = protocol
            .turn_start("thread-1", &[" ".to_string()], CodexTurnConfig::default())
            .expect_err("blank turn message should fail");
        assert_eq!(
            blank_message_err,
            CrabError::InvariantViolation {
                context: "codex_turn_start_input",
                message: "input message at index 0 must not be empty".to_string(),
            }
        );

        let turn_start_thread_err = protocol
            .turn_start(" ", &["hello".to_string()], CodexTurnConfig::default())
            .expect_err("blank turn_start thread id should fail");
        assert_eq!(
            turn_start_thread_err,
            CrabError::InvariantViolation {
                context: "codex_turn_start_input",
                message: "threadId must not be empty".to_string(),
            }
        );

        let interrupt_thread_err = protocol
            .turn_interrupt(" ", "turn-1")
            .expect_err("blank interrupt thread id should fail");
        assert_eq!(
            interrupt_thread_err,
            CrabError::InvariantViolation {
                context: "codex_turn_interrupt_input",
                message: "threadId must not be empty".to_string(),
            }
        );

        let interrupt_err = protocol
            .turn_interrupt("thread-1", " ")
            .expect_err("blank turn id should fail");
        assert_eq!(
            interrupt_err,
            CrabError::InvariantViolation {
                context: "codex_turn_interrupt_input",
                message: "turnId must not be empty".to_string(),
            }
        );

        let blank_model_err = protocol
            .turn_start(
                "thread-1",
                &["hello".to_string()],
                CodexTurnConfig {
                    model: Some(" ".to_string()),
                    effort: None,
                },
            )
            .expect_err("blank model should fail");
        assert_eq!(
            blank_model_err,
            CrabError::InvariantViolation {
                context: "codex_turn_start_input",
                message: "model must not be empty".to_string(),
            }
        );

        let blank_effort_err = protocol
            .turn_start(
                "thread-1",
                &["hello".to_string()],
                CodexTurnConfig {
                    model: None,
                    effort: Some(" ".to_string()),
                },
            )
            .expect_err("blank effort should fail");
        assert_eq!(
            blank_effort_err,
            CrabError::InvariantViolation {
                context: "codex_turn_start_input",
                message: "effort must not be empty".to_string(),
            }
        );

        assert_eq!(transport.stats().call_count, 0);
    }

    #[test]
    fn protocol_validates_required_response_fields() {
        let (protocol, _) = make_protocol(vec![Ok(CodexRpcResponse::default())]);
        let err = protocol
            .thread_start()
            .expect_err("missing thread id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "codex_thread_start_response",
                message: "response missing required field threadId".to_string(),
            }
        );

        let (protocol, _) = make_protocol(vec![Ok(response(&[("turnId", " ")]))]);
        let err = protocol
            .turn_start(
                "thread-1",
                &["hello".to_string()],
                CodexTurnConfig::default(),
            )
            .expect_err("blank turn id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "codex_turn_start_response",
                message: "turnId must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn protocol_propagates_transport_errors() {
        let transport_error = CrabError::InvariantViolation {
            context: "fake_transport",
            message: "boom".to_string(),
        };

        let (protocol, _) = make_protocol(vec![Err(transport_error.clone())]);
        let thread_start_err = protocol
            .thread_start()
            .expect_err("transport error should propagate");
        assert_eq!(thread_start_err, transport_error);

        let (protocol, _) = make_protocol(vec![Err(transport_error.clone())]);
        let thread_resume_err = protocol
            .thread_resume("thread-1")
            .expect_err("transport error should propagate for thread resume");
        assert_eq!(thread_resume_err, transport_error);

        let (protocol, _) = make_protocol(vec![Err(transport_error.clone())]);
        let turn_start_err = protocol
            .turn_start(
                "thread-1",
                &["hello".to_string()],
                CodexTurnConfig::default(),
            )
            .expect_err("transport error should propagate for turn start");
        assert_eq!(turn_start_err, transport_error);

        let (protocol, _) = make_protocol(vec![Err(transport_error.clone())]);
        let turn_interrupt_err = protocol
            .turn_interrupt("thread-1", "turn-1")
            .expect_err("transport error should propagate for turn interrupt");
        assert_eq!(turn_interrupt_err, transport_error);
    }

    #[test]
    fn fake_transport_requires_scripted_responses() {
        let (protocol, _) = make_protocol(Vec::new());
        let err = protocol
            .thread_start()
            .expect_err("missing scripted response should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "fake_codex_transport_call",
                message: "missing scripted response".to_string(),
            }
        );
    }
}
