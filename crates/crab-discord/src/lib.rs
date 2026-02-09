//! Discord transport integration for Crab.

use crab_core::{CrabError, CrabResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayConversationKind {
    GuildChannel,
    Thread,
    DirectMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayMessage {
    pub message_id: String,
    pub author_id: String,
    pub author_is_bot: bool,
    pub channel_id: String,
    pub guild_id: Option<String>,
    pub thread_id: Option<String>,
    pub content: String,
    pub conversation_kind: GatewayConversationKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingKey {
    Channel { channel_id: String },
    Thread { thread_id: String },
    DirectMessage { user_id: String },
}

impl RoutingKey {
    #[must_use]
    pub fn provider_scoped_id(&self) -> &str {
        match self {
            Self::Channel { channel_id } => channel_id,
            Self::Thread { thread_id } => thread_id,
            Self::DirectMessage { user_id } => user_id,
        }
    }

    pub fn logical_session_id(&self) -> CrabResult<String> {
        match self {
            Self::Channel { channel_id } => {
                build_logical_session_id("discord:channel", "channel_id", channel_id)
            }
            Self::Thread { thread_id } => {
                build_logical_session_id("discord:thread", "thread_id", thread_id)
            }
            Self::DirectMessage { user_id } => {
                build_logical_session_id("discord:dm", "user_id", user_id)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressMessage {
    pub message_id: String,
    pub author_id: String,
    pub content: String,
    pub routing_key: RoutingKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayIngress {
    bot_user_id: String,
}

impl GatewayIngress {
    pub fn new(bot_user_id: impl Into<String>) -> CrabResult<Self> {
        let bot_user_id = bot_user_id.into();
        ensure_non_empty_field("gateway_ingress_new", "bot_user_id", &bot_user_id)?;
        Ok(Self { bot_user_id })
    }

    pub fn ingest(&self, message: GatewayMessage) -> CrabResult<Option<IngressMessage>> {
        if message.author_is_bot || message.author_id == self.bot_user_id {
            return Ok(None);
        }

        let routing_key = extract_routing_key(&message)?;
        Ok(Some(IngressMessage {
            message_id: message.message_id,
            author_id: message.author_id,
            content: message.content,
            routing_key,
        }))
    }
}

pub fn extract_routing_key(message: &GatewayMessage) -> CrabResult<RoutingKey> {
    validate_gateway_message(message)?;
    match message.conversation_kind {
        GatewayConversationKind::GuildChannel => Ok(RoutingKey::Channel {
            channel_id: message.channel_id.trim().to_string(),
        }),
        GatewayConversationKind::Thread => {
            let thread_id = trimmed_option(message.thread_id.as_deref())
                .unwrap_or_else(|| message.channel_id.trim());
            Ok(RoutingKey::Thread {
                thread_id: thread_id.to_string(),
            })
        }
        GatewayConversationKind::DirectMessage => Ok(RoutingKey::DirectMessage {
            user_id: message.author_id.trim().to_string(),
        }),
    }
}

fn validate_gateway_message(message: &GatewayMessage) -> CrabResult<()> {
    ensure_non_empty_field(
        "gateway_message_validate",
        "message_id",
        &message.message_id,
    )?;
    ensure_non_empty_field("gateway_message_validate", "author_id", &message.author_id)?;
    ensure_non_empty_field(
        "gateway_message_validate",
        "channel_id",
        &message.channel_id,
    )?;
    ensure_optional_field_not_blank("gateway_message_validate", "guild_id", &message.guild_id)?;
    ensure_optional_field_not_blank("gateway_message_validate", "thread_id", &message.thread_id)?;

    match message.conversation_kind {
        GatewayConversationKind::GuildChannel => {
            if message.guild_id.is_none() {
                return Err(CrabError::InvariantViolation {
                    context: "gateway_message_validate",
                    message: "guild channel messages must include guild_id".to_string(),
                });
            }
            if message.thread_id.is_some() {
                return Err(CrabError::InvariantViolation {
                    context: "gateway_message_validate",
                    message: "guild channel messages must not include thread_id; use thread kind"
                        .to_string(),
                });
            }
        }
        GatewayConversationKind::Thread => {
            if message.guild_id.is_none() {
                return Err(CrabError::InvariantViolation {
                    context: "gateway_message_validate",
                    message: "thread messages must include guild_id".to_string(),
                });
            }
        }
        GatewayConversationKind::DirectMessage => {
            if message.guild_id.is_some() {
                return Err(CrabError::InvariantViolation {
                    context: "gateway_message_validate",
                    message: "direct messages must not include guild_id".to_string(),
                });
            }
            if message.thread_id.is_some() {
                return Err(CrabError::InvariantViolation {
                    context: "gateway_message_validate",
                    message: "direct messages must not include thread_id".to_string(),
                });
            }
        }
    }

    Ok(())
}

fn trimmed_option(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn ensure_non_empty_field(
    context: &'static str,
    field_name: &'static str,
    value: &str,
) -> CrabResult<()> {
    if value.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context,
            message: format!("{field_name} must not be empty"),
        });
    }

    Ok(())
}

fn ensure_optional_field_not_blank(
    context: &'static str,
    field_name: &'static str,
    value: &Option<String>,
) -> CrabResult<()> {
    if let Some(value) = value.as_deref() {
        ensure_non_empty_field(context, field_name, value)?;
    }
    Ok(())
}

fn build_logical_session_id(
    prefix: &str,
    field_name: &'static str,
    value: &str,
) -> CrabResult<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "session_route",
            message: format!("{field_name} must not be empty"),
        });
    }

    Ok(format!("{prefix}:{trimmed}"))
}

#[cfg(test)]
mod tests {
    use crab_core::CrabError;

    use super::{
        extract_routing_key, GatewayConversationKind, GatewayIngress, GatewayMessage, RoutingKey,
    };

    fn sample_message(kind: GatewayConversationKind) -> GatewayMessage {
        GatewayMessage {
            message_id: "m-1".to_string(),
            author_id: "u-1".to_string(),
            author_is_bot: false,
            channel_id: "c-1".to_string(),
            guild_id: Some("g-1".to_string()),
            thread_id: None,
            content: "hello".to_string(),
            conversation_kind: kind,
        }
    }

    #[test]
    fn extracts_channel_routing_key() {
        let message = sample_message(GatewayConversationKind::GuildChannel);
        let key = extract_routing_key(&message).expect("routing key should extract");
        assert_eq!(
            key,
            RoutingKey::Channel {
                channel_id: "c-1".to_string()
            }
        );
    }

    #[test]
    fn extracts_thread_routing_key_from_thread_id() {
        let mut message = sample_message(GatewayConversationKind::Thread);
        message.thread_id = Some("thread-9".to_string());

        let key = extract_routing_key(&message).expect("routing key should extract");
        assert_eq!(
            key,
            RoutingKey::Thread {
                thread_id: "thread-9".to_string()
            }
        );
    }

    #[test]
    fn extracts_thread_routing_key_from_channel_fallback() {
        let message = sample_message(GatewayConversationKind::Thread);
        let key = extract_routing_key(&message).expect("routing key should extract");
        assert_eq!(
            key,
            RoutingKey::Thread {
                thread_id: "c-1".to_string()
            }
        );
    }

    #[test]
    fn extracts_dm_routing_key_from_author() {
        let mut message = sample_message(GatewayConversationKind::DirectMessage);
        message.guild_id = None;

        let key = extract_routing_key(&message).expect("routing key should extract");
        assert_eq!(
            key,
            RoutingKey::DirectMessage {
                user_id: "u-1".to_string()
            }
        );
    }

    #[test]
    fn provider_scoped_id_uses_underlying_identifier() {
        assert_eq!(
            RoutingKey::Channel {
                channel_id: "c".to_string()
            }
            .provider_scoped_id(),
            "c"
        );
        assert_eq!(
            RoutingKey::Thread {
                thread_id: "t".to_string()
            }
            .provider_scoped_id(),
            "t"
        );
        assert_eq!(
            RoutingKey::DirectMessage {
                user_id: "u".to_string()
            }
            .provider_scoped_id(),
            "u"
        );
    }

    #[test]
    fn maps_channel_routing_key_to_logical_session_id() {
        let session_id = RoutingKey::Channel {
            channel_id: "123".to_string(),
        }
        .logical_session_id()
        .expect("mapping should succeed");
        assert_eq!(session_id, "discord:channel:123");
    }

    #[test]
    fn maps_thread_routing_key_to_logical_session_id() {
        let session_id = RoutingKey::Thread {
            thread_id: "456".to_string(),
        }
        .logical_session_id()
        .expect("mapping should succeed");
        assert_eq!(session_id, "discord:thread:456");
    }

    #[test]
    fn maps_dm_routing_key_to_logical_session_id() {
        let session_id = RoutingKey::DirectMessage {
            user_id: "789".to_string(),
        }
        .logical_session_id()
        .expect("mapping should succeed");
        assert_eq!(session_id, "discord:dm:789");
    }

    #[test]
    fn logical_session_id_mapping_is_stable() {
        let key = RoutingKey::Thread {
            thread_id: "thread-stable".to_string(),
        };

        let first = key
            .logical_session_id()
            .expect("first mapping should succeed");
        let second = key
            .logical_session_id()
            .expect("second mapping should succeed");
        assert_eq!(first, second);
    }

    #[test]
    fn logical_session_id_mapping_trims_whitespace() {
        let channel = RoutingKey::Channel {
            channel_id: "  channel-9  ".to_string(),
        };
        let dm = RoutingKey::DirectMessage {
            user_id: "  user-9  ".to_string(),
        };

        assert_eq!(
            channel
                .logical_session_id()
                .expect("channel mapping should succeed"),
            "discord:channel:channel-9"
        );
        assert_eq!(
            dm.logical_session_id().expect("dm mapping should succeed"),
            "discord:dm:user-9"
        );
    }

    #[test]
    fn rejects_blank_channel_routing_key_for_logical_session_id() {
        let err = RoutingKey::Channel {
            channel_id: " ".to_string(),
        }
        .logical_session_id()
        .expect_err("blank channel id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "session_route",
                message: "channel_id must not be empty".to_string()
            }
        );
    }

    #[test]
    fn rejects_blank_thread_routing_key_for_logical_session_id() {
        let err = RoutingKey::Thread {
            thread_id: " ".to_string(),
        }
        .logical_session_id()
        .expect_err("blank thread id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "session_route",
                message: "thread_id must not be empty".to_string()
            }
        );
    }

    #[test]
    fn rejects_blank_dm_routing_key_for_logical_session_id() {
        let err = RoutingKey::DirectMessage {
            user_id: " ".to_string(),
        }
        .logical_session_id()
        .expect_err("blank user id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "session_route",
                message: "user_id must not be empty".to_string()
            }
        );
    }

    #[test]
    fn rejects_empty_message_id() {
        let mut message = sample_message(GatewayConversationKind::GuildChannel);
        message.message_id = " ".to_string();
        let err = extract_routing_key(&message).expect_err("blank message id should be rejected");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "gateway_message_validate",
                message: "message_id must not be empty".to_string()
            }
        );
    }

    #[test]
    fn rejects_empty_author_id() {
        let mut message = sample_message(GatewayConversationKind::GuildChannel);
        message.author_id = " ".to_string();
        let err = extract_routing_key(&message).expect_err("blank author id should be rejected");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "gateway_message_validate",
                message: "author_id must not be empty".to_string()
            }
        );
    }

    #[test]
    fn rejects_empty_channel_id() {
        let mut message = sample_message(GatewayConversationKind::GuildChannel);
        message.channel_id = " ".to_string();
        let err = extract_routing_key(&message).expect_err("blank channel id should be rejected");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "gateway_message_validate",
                message: "channel_id must not be empty".to_string()
            }
        );
    }

    #[test]
    fn rejects_blank_optional_id() {
        let mut message = sample_message(GatewayConversationKind::GuildChannel);
        message.guild_id = Some(" ".to_string());
        let err = extract_routing_key(&message).expect_err("blank guild id should be rejected");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "gateway_message_validate",
                message: "guild_id must not be empty".to_string()
            }
        );
    }

    #[test]
    fn rejects_blank_thread_id() {
        let mut message = sample_message(GatewayConversationKind::Thread);
        message.thread_id = Some(" ".to_string());
        let err = extract_routing_key(&message).expect_err("blank thread id should be rejected");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "gateway_message_validate",
                message: "thread_id must not be empty".to_string()
            }
        );
    }

    #[test]
    fn rejects_guild_channel_without_guild_id() {
        let mut message = sample_message(GatewayConversationKind::GuildChannel);
        message.guild_id = None;
        let err = extract_routing_key(&message).expect_err("guild channel should require guild");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "gateway_message_validate",
                message: "guild channel messages must include guild_id".to_string()
            }
        );
    }

    #[test]
    fn rejects_guild_channel_with_thread_id() {
        let mut message = sample_message(GatewayConversationKind::GuildChannel);
        message.thread_id = Some("t-1".to_string());
        let err = extract_routing_key(&message)
            .expect_err("guild channel should reject thread identifier");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "gateway_message_validate",
                message: "guild channel messages must not include thread_id; use thread kind"
                    .to_string()
            }
        );
    }

    #[test]
    fn rejects_thread_without_guild_id() {
        let mut message = sample_message(GatewayConversationKind::Thread);
        message.guild_id = None;
        let err = extract_routing_key(&message).expect_err("thread should require guild id");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "gateway_message_validate",
                message: "thread messages must include guild_id".to_string()
            }
        );
    }

    #[test]
    fn rejects_dm_with_guild_id() {
        let message = sample_message(GatewayConversationKind::DirectMessage);
        let err = extract_routing_key(&message).expect_err("dm should not include guild id");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "gateway_message_validate",
                message: "direct messages must not include guild_id".to_string()
            }
        );
    }

    #[test]
    fn rejects_dm_with_thread_id() {
        let mut message = sample_message(GatewayConversationKind::DirectMessage);
        message.guild_id = None;
        message.thread_id = Some("t-3".to_string());
        let err = extract_routing_key(&message).expect_err("dm should not include thread id");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "gateway_message_validate",
                message: "direct messages must not include thread_id".to_string()
            }
        );
    }

    #[test]
    fn gateway_ingress_requires_non_empty_bot_user_id() {
        let err = GatewayIngress::new(" ").expect_err("blank bot user id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "gateway_ingress_new",
                message: "bot_user_id must not be empty".to_string()
            }
        );
    }

    #[test]
    fn gateway_ingress_ignores_bot_messages() {
        let ingress = GatewayIngress::new("bot-self").expect("ingress should build");
        let mut message = sample_message(GatewayConversationKind::GuildChannel);
        message.author_is_bot = true;

        let result = ingress
            .ingest(message)
            .expect("ingest should succeed for ignored messages");
        assert!(result.is_none());
    }

    #[test]
    fn gateway_ingress_ignores_self_messages() {
        let ingress = GatewayIngress::new("bot-self").expect("ingress should build");
        let mut message = sample_message(GatewayConversationKind::GuildChannel);
        message.author_id = "bot-self".to_string();

        let result = ingress
            .ingest(message)
            .expect("ingest should succeed for ignored messages");
        assert!(result.is_none());
    }

    #[test]
    fn gateway_ingress_accepts_user_messages() {
        let ingress = GatewayIngress::new("bot-self").expect("ingress should build");
        let message = sample_message(GatewayConversationKind::GuildChannel);

        let accepted = ingress
            .ingest(message)
            .expect("ingest should succeed")
            .expect("message should be accepted");
        assert_eq!(accepted.message_id, "m-1");
        assert_eq!(accepted.author_id, "u-1");
        assert_eq!(accepted.content, "hello");
        assert_eq!(
            accepted.routing_key,
            RoutingKey::Channel {
                channel_id: "c-1".to_string()
            }
        );
    }

    #[test]
    fn gateway_ingress_returns_validation_errors() {
        let ingress = GatewayIngress::new("bot-self").expect("ingress should build");
        let mut message = sample_message(GatewayConversationKind::GuildChannel);
        message.thread_id = Some("thread-9".to_string());

        let err = ingress
            .ingest(message)
            .expect_err("invalid message should be rejected");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "gateway_message_validate",
                message: "guild channel messages must not include thread_id; use thread kind"
                    .to_string()
            }
        );
    }
}
