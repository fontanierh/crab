#[cfg(test)]
use crate::IngressMessage;
use crate::{ensure_non_empty_field, GatewayIngress, GatewayMessage};
use crab_core::{CrabError, CrabResult};

pub const DEFAULT_DISCORD_RETRY_MAX_ATTEMPTS: u8 = 3;
pub const DEFAULT_DISCORD_RETRY_BACKOFF_MS: u64 = 500;
pub const DEFAULT_DISCORD_RETRY_MAX_BACKOFF_MS: u64 = 30_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiscordRetryPolicy {
    pub max_attempts: u8,
    pub retry_backoff_ms: u64,
    pub max_retry_backoff_ms: u64,
}

impl Default for DiscordRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_DISCORD_RETRY_MAX_ATTEMPTS,
            retry_backoff_ms: DEFAULT_DISCORD_RETRY_BACKOFF_MS,
            max_retry_backoff_ms: DEFAULT_DISCORD_RETRY_MAX_BACKOFF_MS,
        }
    }
}

impl DiscordRetryPolicy {
    pub fn validate(self) -> CrabResult<Self> {
        if self.max_attempts == 0 {
            return Err(CrabError::InvalidConfig {
                key: "discord_retry_max_attempts",
                value: self.max_attempts.to_string(),
                reason: "must be greater than 0",
            });
        }
        if self.retry_backoff_ms == 0 {
            return Err(CrabError::InvalidConfig {
                key: "discord_retry_backoff_ms",
                value: self.retry_backoff_ms.to_string(),
                reason: "must be greater than 0",
            });
        }
        if self.max_retry_backoff_ms == 0 {
            return Err(CrabError::InvalidConfig {
                key: "discord_retry_max_backoff_ms",
                value: self.max_retry_backoff_ms.to_string(),
                reason: "must be greater than 0",
            });
        }
        if self.retry_backoff_ms > self.max_retry_backoff_ms {
            return Err(CrabError::InvalidConfig {
                key: "discord_retry_backoff_ms",
                value: self.retry_backoff_ms.to_string(),
                reason: "must be less than or equal to discord_retry_max_backoff_ms",
            });
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscordPostedMessage {
    pub message_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscordRuntimeEvent {
    MessageCreate(GatewayMessage),
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscordTransportError {
    Retryable {
        message: String,
    },
    RateLimited {
        retry_after_ms: Option<u64>,
        message: String,
    },
    Fatal {
        message: String,
    },
}

impl DiscordTransportError {
    fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable { .. } | Self::RateLimited { .. })
    }

    fn message(&self) -> &str {
        match self {
            Self::Retryable { message }
            | Self::RateLimited { message, .. }
            | Self::Fatal { message } => message,
        }
    }
}

pub trait DiscordRuntimeTransport {
    fn next_event(&mut self) -> CrabResult<Option<DiscordRuntimeEvent>>;
    fn send_message(
        &mut self,
        channel_id: &str,
        content: &str,
    ) -> Result<DiscordPostedMessage, DiscordTransportError>;
    fn edit_message(
        &mut self,
        channel_id: &str,
        message_id: &str,
        content: &str,
    ) -> Result<(), DiscordTransportError>;
    fn wait_for_retry(&mut self, backoff_ms: u64) -> CrabResult<()>;
}

#[derive(Debug, Clone)]
pub struct DiscordRuntimeAdapter<T: DiscordRuntimeTransport> {
    _ingress: GatewayIngress,
    transport: T,
    retry_policy: DiscordRetryPolicy,
}

impl<T: DiscordRuntimeTransport> DiscordRuntimeAdapter<T> {
    pub fn new(
        bot_user_id: impl Into<String>,
        transport: T,
        retry_policy: DiscordRetryPolicy,
    ) -> CrabResult<Self> {
        Ok(Self {
            _ingress: GatewayIngress::new(bot_user_id)?,
            transport,
            retry_policy: retry_policy.validate()?,
        })
    }

    #[cfg(test)]
    fn next_ingress_message(&mut self) -> CrabResult<Option<IngressMessage>> {
        let Some(event) = self.transport.next_event()? else {
            return Ok(None);
        };

        match event {
            DiscordRuntimeEvent::MessageCreate(message) => self._ingress.ingest(message),
            DiscordRuntimeEvent::Other => Ok(None),
        }
    }

    pub fn post_message(
        &mut self,
        channel_id: &str,
        content: &str,
    ) -> CrabResult<DiscordPostedMessage> {
        ensure_non_empty_field("discord_runtime_post_message", "channel_id", channel_id)?;
        ensure_non_empty_field("discord_runtime_post_message", "content", content)?;

        let posted = self.run_with_retry("discord_runtime_post_message", |transport| {
            transport.send_message(channel_id, content)
        })?;

        ensure_non_empty_field(
            "discord_runtime_post_message",
            "message_id",
            &posted.message_id,
        )?;
        Ok(posted)
    }

    pub fn edit_message(
        &mut self,
        channel_id: &str,
        message_id: &str,
        content: &str,
    ) -> CrabResult<()> {
        ensure_non_empty_field("discord_runtime_edit_message", "channel_id", channel_id)?;
        ensure_non_empty_field("discord_runtime_edit_message", "message_id", message_id)?;
        ensure_non_empty_field("discord_runtime_edit_message", "content", content)?;

        self.run_with_retry("discord_runtime_edit_message", |transport| {
            transport.edit_message(channel_id, message_id, content)
        })
    }

    fn run_with_retry<R, F>(&mut self, context: &'static str, mut operation: F) -> CrabResult<R>
    where
        F: FnMut(&mut T) -> Result<R, DiscordTransportError>,
    {
        let mut attempt = 1_u32;
        let max_attempts = u32::from(self.retry_policy.max_attempts);

        loop {
            match operation(&mut self.transport) {
                Ok(output) => return Ok(output),
                Err(error) => {
                    if !error.is_retryable() {
                        return Err(CrabError::InvariantViolation {
                            context,
                            message: format!(
                                "fatal discord transport error on attempt {attempt}: {}",
                                error.message()
                            ),
                        });
                    }

                    if attempt >= max_attempts {
                        return Err(CrabError::InvariantViolation {
                            context,
                            message: format!(
                                "discord transport retries exhausted after {attempt} attempts: {}",
                                error.message()
                            ),
                        });
                    }

                    let backoff_ms = self.retry_backoff_ms(&error);
                    self.transport
                        .wait_for_retry(backoff_ms)
                        .map_err(|wait_error| CrabError::InvariantViolation {
                            context,
                            message: format!(
                                "failed to wait before retry on attempt {attempt}: {wait_error}"
                            ),
                        })?;
                    attempt += 1;
                }
            }
        }
    }

    fn retry_backoff_ms(&self, error: &DiscordTransportError) -> u64 {
        let requested = match error {
            DiscordTransportError::RateLimited {
                retry_after_ms: Some(retry_after_ms),
                ..
            } if *retry_after_ms > 0 => *retry_after_ms,
            _ => self.retry_policy.retry_backoff_ms,
        };
        requested.min(self.retry_policy.max_retry_backoff_ms)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use crate::{GatewayConversationKind, RoutingKey};

    use super::{
        DiscordPostedMessage, DiscordRetryPolicy, DiscordRuntimeAdapter, DiscordRuntimeEvent,
        DiscordRuntimeTransport, DiscordTransportError, DEFAULT_DISCORD_RETRY_BACKOFF_MS,
        DEFAULT_DISCORD_RETRY_MAX_ATTEMPTS,
    };
    use crab_core::{CrabError, CrabResult};

    #[derive(Debug, Default, Clone)]
    struct ScriptedTransportState {
        events: VecDeque<CrabResult<Option<DiscordRuntimeEvent>>>,
        post_results: VecDeque<Result<DiscordPostedMessage, DiscordTransportError>>,
        edit_results: VecDeque<Result<(), DiscordTransportError>>,
        wait_results: VecDeque<CrabResult<()>>,
        wait_calls: Vec<u64>,
        post_calls: usize,
        edit_calls: usize,
    }

    #[derive(Debug, Clone)]
    struct ScriptedTransport {
        state: Arc<Mutex<ScriptedTransportState>>,
    }

    impl ScriptedTransport {
        fn with_state(state: ScriptedTransportState) -> Self {
            Self {
                state: Arc::new(Mutex::new(state)),
            }
        }

        fn state(&self) -> ScriptedTransportState {
            self.state.lock().expect("lock should succeed").clone()
        }
    }

    impl DiscordRuntimeTransport for ScriptedTransport {
        fn next_event(&mut self) -> CrabResult<Option<DiscordRuntimeEvent>> {
            self.state
                .lock()
                .expect("lock should succeed")
                .events
                .pop_front()
                .unwrap_or(Ok(None))
        }

        fn send_message(
            &mut self,
            _channel_id: &str,
            _content: &str,
        ) -> Result<DiscordPostedMessage, DiscordTransportError> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.post_calls += 1;
            state
                .post_results
                .pop_front()
                .unwrap_or(Ok(DiscordPostedMessage {
                    message_id: "m-default".to_string(),
                }))
        }

        fn edit_message(
            &mut self,
            _channel_id: &str,
            _message_id: &str,
            _content: &str,
        ) -> Result<(), DiscordTransportError> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.edit_calls += 1;
            state.edit_results.pop_front().unwrap_or(Ok(()))
        }

        fn wait_for_retry(&mut self, backoff_ms: u64) -> CrabResult<()> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.wait_calls.push(backoff_ms);
            state.wait_results.pop_front().unwrap_or(Ok(()))
        }
    }

    fn message(author_is_bot: bool) -> crate::GatewayMessage {
        crate::GatewayMessage {
            message_id: "m-1".to_string(),
            author_id: "u-1".to_string(),
            author_is_bot,
            channel_id: "c-1".to_string(),
            guild_id: Some("g-1".to_string()),
            thread_id: None,
            content: "hello".to_string(),
            conversation_kind: GatewayConversationKind::GuildChannel,
        }
    }

    fn adapter_with_transport(
        transport: ScriptedTransport,
        policy: DiscordRetryPolicy,
    ) -> CrabResult<DiscordRuntimeAdapter<ScriptedTransport>> {
        DiscordRuntimeAdapter::new("bot-999", transport, policy)
    }

    fn execute_post(
        initial_state: ScriptedTransportState,
        policy: DiscordRetryPolicy,
    ) -> (
        Result<DiscordPostedMessage, CrabError>,
        ScriptedTransportState,
    ) {
        let transport = ScriptedTransport::with_state(initial_state);
        let state_handle = transport.clone();
        let mut adapter = adapter_with_transport(transport, policy).expect("adapter should build");
        let result = adapter.post_message("c-1", "hello");
        (result, state_handle.state())
    }

    fn execute_edit(
        initial_state: ScriptedTransportState,
        policy: DiscordRetryPolicy,
    ) -> (Result<(), CrabError>, ScriptedTransportState) {
        let transport = ScriptedTransport::with_state(initial_state);
        let state_handle = transport.clone();
        let mut adapter = adapter_with_transport(transport, policy).expect("adapter should build");
        let result = adapter.edit_message("c-1", "m-1", "updated");
        (result, state_handle.state())
    }

    #[test]
    fn retry_policy_default_is_valid() {
        let validated = DiscordRetryPolicy::default()
            .validate()
            .expect("default policy should validate");
        assert_eq!(validated.max_attempts, DEFAULT_DISCORD_RETRY_MAX_ATTEMPTS);
    }

    #[test]
    fn retry_policy_validation_rejects_invalid_values() {
        let zero_attempts = DiscordRetryPolicy {
            max_attempts: 0,
            ..DiscordRetryPolicy::default()
        }
        .validate()
        .expect_err("zero attempts should fail");
        assert_eq!(
            zero_attempts,
            CrabError::InvalidConfig {
                key: "discord_retry_max_attempts",
                value: "0".to_string(),
                reason: "must be greater than 0"
            }
        );

        let zero_backoff = DiscordRetryPolicy {
            retry_backoff_ms: 0,
            ..DiscordRetryPolicy::default()
        }
        .validate()
        .expect_err("zero backoff should fail");
        assert_eq!(
            zero_backoff,
            CrabError::InvalidConfig {
                key: "discord_retry_backoff_ms",
                value: "0".to_string(),
                reason: "must be greater than 0"
            }
        );

        let zero_max_backoff = DiscordRetryPolicy {
            max_retry_backoff_ms: 0,
            ..DiscordRetryPolicy::default()
        }
        .validate()
        .expect_err("zero max backoff should fail");
        assert_eq!(
            zero_max_backoff,
            CrabError::InvalidConfig {
                key: "discord_retry_max_backoff_ms",
                value: "0".to_string(),
                reason: "must be greater than 0"
            }
        );

        let invalid_ratio = DiscordRetryPolicy {
            retry_backoff_ms: 1_000,
            max_retry_backoff_ms: 10,
            ..DiscordRetryPolicy::default()
        }
        .validate()
        .expect_err("backoff greater than max should fail");
        assert_eq!(
            invalid_ratio,
            CrabError::InvalidConfig {
                key: "discord_retry_backoff_ms",
                value: "1000".to_string(),
                reason: "must be less than or equal to discord_retry_max_backoff_ms"
            }
        );
    }

    #[test]
    fn adapter_creation_requires_valid_bot_user_id_and_policy() {
        let err = DiscordRuntimeAdapter::new(
            "   ",
            ScriptedTransport::with_state(ScriptedTransportState::default()),
            DiscordRetryPolicy::default(),
        )
        .expect_err("blank bot id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "gateway_ingress_new",
                message: "bot_user_id must not be empty".to_string()
            }
        );

        let invalid_policy = DiscordRuntimeAdapter::new(
            "bot-1",
            ScriptedTransport::with_state(ScriptedTransportState::default()),
            DiscordRetryPolicy {
                max_attempts: 0,
                ..DiscordRetryPolicy::default()
            },
        )
        .expect_err("invalid retry policy should fail");
        assert_eq!(
            invalid_policy,
            CrabError::InvalidConfig {
                key: "discord_retry_max_attempts",
                value: "0".to_string(),
                reason: "must be greater than 0"
            }
        );
    }

    #[test]
    fn next_ingress_message_returns_none_when_event_queue_is_empty() {
        let transport = ScriptedTransport::with_state(ScriptedTransportState::default());
        let mut adapter = adapter_with_transport(transport, DiscordRetryPolicy::default())
            .expect("adapter should build");
        assert!(adapter
            .next_ingress_message()
            .expect("no events should be accepted")
            .is_none());
    }

    #[test]
    fn next_ingress_message_skips_non_message_events() {
        let transport = ScriptedTransport::with_state(ScriptedTransportState {
            events: VecDeque::from([Ok(Some(DiscordRuntimeEvent::Other))]),
            ..ScriptedTransportState::default()
        });
        let mut adapter = adapter_with_transport(transport, DiscordRetryPolicy::default())
            .expect("adapter should build");
        assert!(adapter
            .next_ingress_message()
            .expect("other event should be ignored")
            .is_none());
    }

    #[test]
    fn next_ingress_message_filters_bot_messages() {
        let transport = ScriptedTransport::with_state(ScriptedTransportState {
            events: VecDeque::from([Ok(Some(DiscordRuntimeEvent::MessageCreate(message(true))))]),
            ..ScriptedTransportState::default()
        });
        let mut adapter = adapter_with_transport(transport, DiscordRetryPolicy::default())
            .expect("adapter should build");
        assert!(adapter
            .next_ingress_message()
            .expect("bot events should be ignored")
            .is_none());
    }

    #[test]
    fn next_ingress_message_returns_valid_ingress_message() {
        let transport = ScriptedTransport::with_state(ScriptedTransportState {
            events: VecDeque::from([Ok(Some(DiscordRuntimeEvent::MessageCreate(message(false))))]),
            ..ScriptedTransportState::default()
        });
        let mut adapter = adapter_with_transport(transport, DiscordRetryPolicy::default())
            .expect("adapter should build");
        let ingress = adapter
            .next_ingress_message()
            .expect("ingress should succeed")
            .expect("user message should produce ingress");
        assert_eq!(ingress.message_id, "m-1");
        assert_eq!(ingress.author_id, "u-1");
        assert_eq!(
            ingress.routing_key,
            RoutingKey::Channel {
                channel_id: "c-1".to_string()
            }
        );
    }

    #[test]
    fn next_ingress_message_propagates_transport_errors() {
        let transport = ScriptedTransport::with_state(ScriptedTransportState {
            events: VecDeque::from([Err(CrabError::InvariantViolation {
                context: "discord_transport_next_event",
                message: "socket closed".to_string(),
            })]),
            ..ScriptedTransportState::default()
        });
        let mut adapter = adapter_with_transport(transport, DiscordRetryPolicy::default())
            .expect("adapter should build");
        let error = adapter
            .next_ingress_message()
            .expect_err("transport errors should propagate");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "discord_transport_next_event",
                message: "socket closed".to_string()
            }
        );
    }

    #[test]
    fn post_message_validates_inputs_and_output_shape() {
        let transport = ScriptedTransport::with_state(ScriptedTransportState::default());
        let mut adapter = adapter_with_transport(transport.clone(), DiscordRetryPolicy::default())
            .expect("adapter should build");

        let blank_channel = adapter
            .post_message(" ", "hello")
            .expect_err("blank channel should fail");
        assert_eq!(
            blank_channel,
            CrabError::InvariantViolation {
                context: "discord_runtime_post_message",
                message: "channel_id must not be empty".to_string()
            }
        );

        let blank_content = adapter
            .post_message("c-1", " ")
            .expect_err("blank content should fail");
        assert_eq!(
            blank_content,
            CrabError::InvariantViolation {
                context: "discord_runtime_post_message",
                message: "content must not be empty".to_string()
            }
        );

        {
            let mut state = transport.state.lock().expect("lock should succeed");
            state.post_results = VecDeque::from([Ok(DiscordPostedMessage {
                message_id: "   ".to_string(),
            })]);
        }

        let blank_output = adapter
            .post_message("c-1", "hello")
            .expect_err("blank message id from transport should fail");
        assert_eq!(
            blank_output,
            CrabError::InvariantViolation {
                context: "discord_runtime_post_message",
                message: "message_id must not be empty".to_string()
            }
        );
    }

    #[test]
    fn post_message_succeeds_without_retry() {
        let (result, state) = execute_post(
            ScriptedTransportState {
                post_results: VecDeque::from([Ok(DiscordPostedMessage {
                    message_id: "m-42".to_string(),
                })]),
                ..ScriptedTransportState::default()
            },
            DiscordRetryPolicy::default(),
        );
        let posted = result.expect("post should succeed");
        assert_eq!(posted.message_id, "m-42");
        assert_eq!(state.post_calls, 1);
        assert!(state.wait_calls.is_empty());
    }

    #[test]
    fn post_message_retries_retryable_errors_with_default_backoff() {
        let (result, state) = execute_post(
            ScriptedTransportState {
                post_results: VecDeque::from([
                    Err(DiscordTransportError::Retryable {
                        message: "temporary network error".to_string(),
                    }),
                    Ok(DiscordPostedMessage {
                        message_id: "m-2".to_string(),
                    }),
                ]),
                ..ScriptedTransportState::default()
            },
            DiscordRetryPolicy::default(),
        );
        let posted = result.expect("post should retry and succeed");
        assert_eq!(posted.message_id, "m-2");
        assert_eq!(state.post_calls, 2);
        assert_eq!(state.wait_calls, vec![DEFAULT_DISCORD_RETRY_BACKOFF_MS]);
    }

    #[test]
    fn post_message_rate_limit_backoff_uses_retry_after_and_clamps_to_max() {
        let (result, state) = execute_post(
            ScriptedTransportState {
                post_results: VecDeque::from([
                    Err(DiscordTransportError::RateLimited {
                        retry_after_ms: Some(60_000),
                        message: "too many requests".to_string(),
                    }),
                    Ok(DiscordPostedMessage {
                        message_id: "m-3".to_string(),
                    }),
                ]),
                ..ScriptedTransportState::default()
            },
            DiscordRetryPolicy {
                max_attempts: 3,
                retry_backoff_ms: 500,
                max_retry_backoff_ms: 2_000,
            },
        );
        let posted = result.expect("post should retry and succeed");
        assert_eq!(posted.message_id, "m-3");
        assert_eq!(state.wait_calls, vec![2_000]);
    }

    #[test]
    fn post_message_rate_limit_with_missing_or_zero_retry_after_uses_default_backoff() {
        let (result, state) = execute_post(
            ScriptedTransportState {
                post_results: VecDeque::from([
                    Err(DiscordTransportError::RateLimited {
                        retry_after_ms: None,
                        message: "rate limit bucket".to_string(),
                    }),
                    Err(DiscordTransportError::RateLimited {
                        retry_after_ms: Some(0),
                        message: "rate limit zero".to_string(),
                    }),
                    Ok(DiscordPostedMessage {
                        message_id: "m-4".to_string(),
                    }),
                ]),
                ..ScriptedTransportState::default()
            },
            DiscordRetryPolicy {
                max_attempts: 4,
                retry_backoff_ms: 111,
                max_retry_backoff_ms: 999,
            },
        );
        let posted = result.expect("post should retry and succeed");
        assert_eq!(posted.message_id, "m-4");
        assert_eq!(state.wait_calls, vec![111, 111]);
    }

    #[test]
    fn post_message_propagates_retry_wait_failures() {
        let (result, _state) = execute_post(
            ScriptedTransportState {
                post_results: VecDeque::from([Err(DiscordTransportError::Retryable {
                    message: "socket timeout".to_string(),
                })]),
                wait_results: VecDeque::from([Err(CrabError::InvariantViolation {
                    context: "discord_wait_retry",
                    message: "timer failure".to_string(),
                })]),
                ..ScriptedTransportState::default()
            },
            DiscordRetryPolicy::default(),
        );
        let error = result.expect_err("wait failure should surface");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "discord_runtime_post_message",
                message: "failed to wait before retry on attempt 1: discord_wait_retry invariant violation: timer failure".to_string()
            }
        );
    }

    #[test]
    fn post_message_fails_fast_for_fatal_errors() {
        let (result, state) = execute_post(
            ScriptedTransportState {
                post_results: VecDeque::from([Err(DiscordTransportError::Fatal {
                    message: "unauthorized".to_string(),
                })]),
                ..ScriptedTransportState::default()
            },
            DiscordRetryPolicy::default(),
        );
        let error = result.expect_err("fatal errors should not retry");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "discord_runtime_post_message",
                message: "fatal discord transport error on attempt 1: unauthorized".to_string()
            }
        );

        assert_eq!(state.post_calls, 1);
        assert!(state.wait_calls.is_empty());
    }

    #[test]
    fn post_message_reports_retry_exhaustion() {
        let (result, state) = execute_post(
            ScriptedTransportState {
                post_results: VecDeque::from([
                    Err(DiscordTransportError::Retryable {
                        message: "transient-1".to_string(),
                    }),
                    Err(DiscordTransportError::Retryable {
                        message: "transient-2".to_string(),
                    }),
                ]),
                ..ScriptedTransportState::default()
            },
            DiscordRetryPolicy {
                max_attempts: 2,
                retry_backoff_ms: 10,
                max_retry_backoff_ms: 10,
            },
        );
        let error = result.expect_err("retry exhaustion should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "discord_runtime_post_message",
                message: "discord transport retries exhausted after 2 attempts: transient-2"
                    .to_string()
            }
        );

        assert_eq!(state.post_calls, 2);
        assert_eq!(state.wait_calls, vec![10]);
    }

    #[test]
    fn post_message_reports_rate_limit_exhaustion() {
        let (result, _state) = execute_post(
            ScriptedTransportState {
                post_results: VecDeque::from([Err(DiscordTransportError::RateLimited {
                    retry_after_ms: Some(25),
                    message: "rate limited".to_string(),
                })]),
                ..ScriptedTransportState::default()
            },
            DiscordRetryPolicy {
                max_attempts: 1,
                retry_backoff_ms: 10,
                max_retry_backoff_ms: 10,
            },
        );
        let error = result.expect_err("single-attempt policy should exhaust immediately");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "discord_runtime_post_message",
                message: "discord transport retries exhausted after 1 attempts: rate limited"
                    .to_string()
            }
        );
    }

    #[test]
    fn edit_message_validates_inputs() {
        let transport = ScriptedTransport::with_state(ScriptedTransportState::default());
        let mut adapter = adapter_with_transport(transport, DiscordRetryPolicy::default())
            .expect("adapter should build");

        let blank_channel = adapter
            .edit_message(" ", "m-1", "hello")
            .expect_err("blank channel should fail");
        assert_eq!(
            blank_channel,
            CrabError::InvariantViolation {
                context: "discord_runtime_edit_message",
                message: "channel_id must not be empty".to_string()
            }
        );

        let blank_message_id = adapter
            .edit_message("c-1", " ", "hello")
            .expect_err("blank message_id should fail");
        assert_eq!(
            blank_message_id,
            CrabError::InvariantViolation {
                context: "discord_runtime_edit_message",
                message: "message_id must not be empty".to_string()
            }
        );

        let blank_content = adapter
            .edit_message("c-1", "m-1", " ")
            .expect_err("blank content should fail");
        assert_eq!(
            blank_content,
            CrabError::InvariantViolation {
                context: "discord_runtime_edit_message",
                message: "content must not be empty".to_string()
            }
        );
    }

    #[test]
    fn edit_message_supports_retry_and_success() {
        let (result, state) = execute_edit(
            ScriptedTransportState {
                edit_results: VecDeque::from([
                    Err(DiscordTransportError::Retryable {
                        message: "temporary edit failure".to_string(),
                    }),
                    Ok(()),
                ]),
                ..ScriptedTransportState::default()
            },
            DiscordRetryPolicy::default(),
        );
        result.expect("edit should retry then succeed");
        assert_eq!(state.edit_calls, 2);
        assert_eq!(state.wait_calls, vec![DEFAULT_DISCORD_RETRY_BACKOFF_MS]);
    }

    #[test]
    fn edit_message_propagates_fatal_transport_errors() {
        let (result, state) = execute_edit(
            ScriptedTransportState {
                edit_results: VecDeque::from([Err(DiscordTransportError::Fatal {
                    message: "message deleted".to_string(),
                })]),
                ..ScriptedTransportState::default()
            },
            DiscordRetryPolicy::default(),
        );
        let error = result.expect_err("fatal errors should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "discord_runtime_edit_message",
                message: "fatal discord transport error on attempt 1: message deleted".to_string()
            }
        );

        assert_eq!(state.edit_calls, 1);
        assert!(state.wait_calls.is_empty());
    }
}
