//! Discord transport integration for Crab.

mod runtime_adapter;

use crab_core::{CrabError, CrabResult, OutboundRecord};
use crab_store::OutboundRecordStore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub use runtime_adapter::{
    DiscordPostedMessage, DiscordRetryPolicy, DiscordRuntimeAdapter, DiscordRuntimeEvent,
    DiscordRuntimeTransport, DiscordTransportError,
};

pub const DISCORD_MESSAGE_CHAR_LIMIT: usize = 2000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayConversationKind {
    GuildChannel,
    Thread,
    DirectMessage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// JSONL IPC frame emitted by `crabd` (stdout) and consumed by `crab-discord-connector`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum CrabdOutboundOp {
    #[serde(rename = "post")]
    Post {
        op_id: String,
        channel_id: String,
        delivery_id: String,
        content: String,
    },
    #[serde(rename = "edit")]
    Edit {
        op_id: String,
        channel_id: String,
        delivery_id: String,
        content: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrabdOutboundReceiptStatus {
    Ok,
    Error,
}

/// JSONL IPC receipt emitted by `crab-discord-connector` (stdin) and consumed by `crabd`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrabdOutboundReceipt {
    pub op_id: String,
    pub status: CrabdOutboundReceiptStatus,
    pub channel_id: String,
    pub delivery_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discord_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

/// JSONL IPC frame emitted by `crab-discord-connector` (stdin) and consumed by `crabd`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum CrabdInboundFrame {
    GatewayMessage(GatewayMessage),
    OutboundReceipt(CrabdOutboundReceipt),
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
pub enum StreamingDeliveryOp {
    PostNew {
        content: String,
        chunk_index: u32,
    },
    EditExisting {
        message_id: String,
        content: String,
        chunk_index: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscordMessageChunk {
    pub chunk_index: u32,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamingDelivery {
    rendered_text: String,
    message_id: Option<String>,
    awaiting_post_ack: bool,
    last_sent_text: String,
}

impl StreamingDelivery {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_delta(&mut self, delta: &str) -> Option<StreamingDeliveryOp> {
        if delta.is_empty() {
            return None;
        }
        self.rendered_text.push_str(delta);

        if let Some(message_id) = self.message_id.as_ref() {
            self.last_sent_text = self.rendered_text.clone();
            return Some(StreamingDeliveryOp::EditExisting {
                message_id: message_id.clone(),
                content: self.rendered_text.clone(),
                chunk_index: 0,
            });
        }

        if self.awaiting_post_ack {
            return None;
        }

        self.awaiting_post_ack = true;
        self.last_sent_text = self.rendered_text.clone();
        Some(StreamingDeliveryOp::PostNew {
            content: self.rendered_text.clone(),
            chunk_index: 0,
        })
    }

    pub fn acknowledge_post(
        &mut self,
        message_id: impl Into<String>,
    ) -> CrabResult<Option<StreamingDeliveryOp>> {
        let message_id = message_id.into();
        ensure_non_empty_field("streaming_delivery_ack", "message_id", &message_id)?;

        if let Some(existing) = self.message_id.as_ref() {
            if existing == &message_id {
                return Ok(None);
            }
            return Err(CrabError::InvariantViolation {
                context: "streaming_delivery_ack",
                message: format!(
                    "message id already bound to {existing}; cannot rebind to {message_id}"
                ),
            });
        }

        if !self.awaiting_post_ack {
            return Err(CrabError::InvariantViolation {
                context: "streaming_delivery_ack",
                message: "cannot acknowledge post before initial message send".to_string(),
            });
        }

        self.awaiting_post_ack = false;
        self.message_id = Some(message_id.clone());

        if self.last_sent_text == self.rendered_text {
            return Ok(None);
        }
        self.last_sent_text = self.rendered_text.clone();

        Ok(Some(StreamingDeliveryOp::EditExisting {
            message_id,
            content: self.rendered_text.clone(),
            chunk_index: 0,
        }))
    }

    #[must_use]
    pub fn rendered_text(&self) -> &str {
        &self.rendered_text
    }

    #[must_use]
    pub fn message_id(&self) -> Option<&str> {
        self.message_id.as_deref()
    }

    #[must_use]
    pub fn rendered_chunks(&self) -> Vec<DiscordMessageChunk> {
        split_discord_message(&self.rendered_text)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayIngress {
    bot_user_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryAttempt {
    pub logical_session_id: String,
    pub run_id: String,
    pub channel_id: String,
    pub message_id: String,
    pub edit_generation: u32,
    pub content: String,
    pub delivered_at_epoch_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShouldSendDecision {
    Send,
    SkipDuplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkSentDecision {
    Recorded,
    AlreadyRecorded,
}

#[derive(Debug, Clone)]
pub struct IdempotentDeliveryLedger {
    store: OutboundRecordStore,
}

impl IdempotentDeliveryLedger {
    #[must_use]
    pub fn new(store: OutboundRecordStore) -> Self {
        Self { store }
    }

    pub fn should_send(&self, attempt: &DeliveryAttempt) -> CrabResult<ShouldSendDecision> {
        validate_delivery_attempt(attempt)?;
        let attempt_hash = content_sha256_hex(&attempt.content);
        let records = self
            .store
            .list_run_records(&attempt.logical_session_id, &attempt.run_id)?;

        for record in records {
            if !same_delivery_target(&record, attempt) {
                continue;
            }

            if record.content_sha256 == attempt_hash {
                return Ok(ShouldSendDecision::SkipDuplicate);
            }
            return Err(CrabError::InvariantViolation {
                context: "idempotent_delivery_should_send",
                message: format!(
                    "conflicting content hash for channel={}, message={}, edit_generation={}",
                    attempt.channel_id, attempt.message_id, attempt.edit_generation
                ),
            });
        }

        Ok(ShouldSendDecision::Send)
    }

    pub fn mark_sent(&self, attempt: &DeliveryAttempt) -> CrabResult<MarkSentDecision> {
        validate_delivery_attempt(attempt)?;
        let record = build_outbound_record(attempt);
        let wrote_new_record = self.store.record_or_skip_duplicate(&record)?;
        if wrote_new_record {
            return Ok(MarkSentDecision::Recorded);
        }

        Ok(MarkSentDecision::AlreadyRecorded)
    }
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

pub(crate) fn extract_routing_key(message: &GatewayMessage) -> CrabResult<RoutingKey> {
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

fn validate_delivery_attempt(attempt: &DeliveryAttempt) -> CrabResult<()> {
    ensure_non_empty_field(
        "idempotent_delivery_validate",
        "logical_session_id",
        &attempt.logical_session_id,
    )?;
    ensure_non_empty_field("idempotent_delivery_validate", "run_id", &attempt.run_id)?;
    ensure_non_empty_field(
        "idempotent_delivery_validate",
        "channel_id",
        &attempt.channel_id,
    )?;
    ensure_non_empty_field(
        "idempotent_delivery_validate",
        "message_id",
        &attempt.message_id,
    )?;
    ensure_non_empty_field("idempotent_delivery_validate", "content", &attempt.content)?;
    if attempt.delivered_at_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: "idempotent_delivery_validate",
            message: "delivered_at_epoch_ms must be greater than 0".to_string(),
        });
    }
    Ok(())
}

fn build_outbound_record(attempt: &DeliveryAttempt) -> OutboundRecord {
    OutboundRecord {
        record_id: delivery_record_id(attempt),
        logical_session_id: attempt.logical_session_id.trim().to_string(),
        run_id: attempt.run_id.trim().to_string(),
        channel_id: attempt.channel_id.trim().to_string(),
        message_id: attempt.message_id.trim().to_string(),
        edit_generation: attempt.edit_generation,
        content_sha256: content_sha256_hex(&attempt.content),
        delivered_at_epoch_ms: attempt.delivered_at_epoch_ms,
    }
}

fn delivery_record_id(attempt: &DeliveryAttempt) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        attempt.logical_session_id.trim(),
        attempt.run_id.trim(),
        attempt.channel_id.trim(),
        attempt.message_id.trim(),
        attempt.edit_generation
    )
}

fn same_delivery_target(record: &OutboundRecord, attempt: &DeliveryAttempt) -> bool {
    record.channel_id == attempt.channel_id.trim()
        && record.message_id == attempt.message_id.trim()
        && record.edit_generation == attempt.edit_generation
}

fn content_sha256_hex(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    let digest = hasher.finalize();
    hex_encode(&digest)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: [char; 16] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    ];

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let upper = usize::from(byte >> 4);
        let lower = usize::from(byte & 0x0f);
        output.push(HEX[upper]);
        output.push(HEX[lower]);
    }
    output
}

#[must_use]
pub(crate) fn split_discord_message(content: &str) -> Vec<DiscordMessageChunk> {
    if content.is_empty() {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut current_chunk = String::new();
    let mut current_count = 0_usize;

    for ch in content.chars() {
        if current_count == DISCORD_MESSAGE_CHAR_LIMIT {
            chunks.push(current_chunk);
            current_chunk = String::new();
            current_count = 0;
        }
        current_chunk.push(ch);
        current_count += 1;
    }

    chunks.push(current_chunk);

    chunks
        .into_iter()
        .enumerate()
        .map(|(chunk_index, content)| DiscordMessageChunk {
            chunk_index: chunk_index as u32,
            content,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use crab_core::CrabError;
    use crab_store::OutboundRecordStore;

    use super::{
        extract_routing_key, split_discord_message, DeliveryAttempt, DiscordMessageChunk,
        GatewayConversationKind, GatewayIngress, GatewayMessage, IdempotentDeliveryLedger,
        MarkSentDecision, RoutingKey, ShouldSendDecision, StreamingDelivery, StreamingDeliveryOp,
        DISCORD_MESSAGE_CHAR_LIMIT,
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

    fn sample_attempt() -> DeliveryAttempt {
        DeliveryAttempt {
            logical_session_id: "discord:channel:abc".to_string(),
            run_id: "run-1".to_string(),
            channel_id: "abc".to_string(),
            message_id: "message-1".to_string(),
            edit_generation: 0,
            content: "hello".to_string(),
            delivered_at_epoch_ms: 1,
        }
    }

    fn temp_root(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("crab-discord-{prefix}-{nanos}"))
    }

    fn cleanup(path: &Path) {
        let _ = fs::remove_dir_all(path);
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

    #[test]
    fn streaming_delivery_posts_first_chunk() {
        let mut streaming = StreamingDelivery::new();
        let op = streaming
            .push_delta("hello")
            .expect("first delta should emit post");

        assert_eq!(
            op,
            StreamingDeliveryOp::PostNew {
                content: "hello".to_string(),
                chunk_index: 0
            }
        );
        assert_eq!(streaming.rendered_text(), "hello");
        assert_eq!(streaming.message_id(), None);
    }

    #[test]
    fn streaming_delivery_coalesces_deltas_while_waiting_for_ack() {
        let mut streaming = StreamingDelivery::new();
        let first = streaming
            .push_delta("hel")
            .expect("first delta should emit post");
        assert_eq!(
            first,
            StreamingDeliveryOp::PostNew {
                content: "hel".to_string(),
                chunk_index: 0
            }
        );

        let second = streaming.push_delta("lo");
        assert_eq!(second, None);
        assert_eq!(streaming.rendered_text(), "hello");
    }

    #[test]
    fn streaming_delivery_ack_emits_catch_up_edit_when_buffer_advanced() {
        let mut streaming = StreamingDelivery::new();
        let _ = streaming
            .push_delta("hel")
            .expect("first delta should emit post");
        let _ = streaming.push_delta("lo");

        let ack = streaming
            .acknowledge_post("discord-msg-1")
            .expect("ack should succeed")
            .expect("ack should emit catch-up edit");
        assert_eq!(
            ack,
            StreamingDeliveryOp::EditExisting {
                message_id: "discord-msg-1".to_string(),
                content: "hello".to_string(),
                chunk_index: 0
            }
        );
        assert_eq!(streaming.message_id(), Some("discord-msg-1"));
    }

    #[test]
    fn streaming_delivery_ack_without_new_delta_has_no_edit() {
        let mut streaming = StreamingDelivery::new();
        let _ = streaming
            .push_delta("hello")
            .expect("first delta should emit post");

        let ack = streaming
            .acknowledge_post("discord-msg-1")
            .expect("ack should succeed");
        assert_eq!(ack, None);
        assert_eq!(streaming.message_id(), Some("discord-msg-1"));
    }

    #[test]
    fn streaming_delivery_edits_in_place_after_ack() {
        let mut streaming = StreamingDelivery::new();
        let _ = streaming
            .push_delta("hello")
            .expect("first delta should emit post");
        let _ = streaming
            .acknowledge_post("discord-msg-1")
            .expect("ack should succeed");

        let edit = streaming
            .push_delta(" world")
            .expect("delta after ack should emit edit");
        assert_eq!(
            edit,
            StreamingDeliveryOp::EditExisting {
                message_id: "discord-msg-1".to_string(),
                content: "hello world".to_string(),
                chunk_index: 0
            }
        );
    }

    #[test]
    fn streaming_delivery_ignores_empty_delta() {
        let mut streaming = StreamingDelivery::new();
        assert_eq!(streaming.push_delta(""), None);
        assert_eq!(streaming.rendered_text(), "");
    }

    #[test]
    fn streaming_delivery_rejects_blank_ack_message_id() {
        let mut streaming = StreamingDelivery::new();
        let _ = streaming
            .push_delta("hello")
            .expect("first delta should emit post");

        let err = streaming
            .acknowledge_post(" ")
            .expect_err("blank ack message id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "streaming_delivery_ack",
                message: "message_id must not be empty".to_string()
            }
        );
    }

    #[test]
    fn streaming_delivery_rejects_ack_before_post() {
        let mut streaming = StreamingDelivery::new();
        let err = streaming
            .acknowledge_post("discord-msg-1")
            .expect_err("ack before first post should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "streaming_delivery_ack",
                message: "cannot acknowledge post before initial message send".to_string()
            }
        );
    }

    #[test]
    fn streaming_delivery_allows_idempotent_ack_for_same_message() {
        let mut streaming = StreamingDelivery::new();
        let _ = streaming
            .push_delta("hello")
            .expect("first delta should emit post");
        let _ = streaming
            .acknowledge_post("discord-msg-1")
            .expect("first ack should succeed");

        let second = streaming
            .acknowledge_post("discord-msg-1")
            .expect("same ack should be idempotent");
        assert_eq!(second, None);
    }

    #[test]
    fn streaming_delivery_rejects_ack_rebind_to_different_message() {
        let mut streaming = StreamingDelivery::new();
        let _ = streaming
            .push_delta("hello")
            .expect("first delta should emit post");
        let _ = streaming
            .acknowledge_post("discord-msg-1")
            .expect("first ack should succeed");

        let err = streaming
            .acknowledge_post("discord-msg-2")
            .expect_err("rebind should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "streaming_delivery_ack",
                message:
                    "message id already bound to discord-msg-1; cannot rebind to discord-msg-2"
                        .to_string()
            }
        );
    }

    #[test]
    fn split_discord_message_returns_empty_for_empty_content() {
        assert!(split_discord_message("").is_empty());
    }

    #[test]
    fn split_discord_message_keeps_short_content_in_single_chunk() {
        let chunks = split_discord_message("hello");
        assert_eq!(
            chunks,
            vec![DiscordMessageChunk {
                chunk_index: 0,
                content: "hello".to_string()
            }]
        );
    }

    #[test]
    fn split_discord_message_keeps_exact_limit_in_single_chunk() {
        let content = "a".repeat(DISCORD_MESSAGE_CHAR_LIMIT);
        let chunks = split_discord_message(&content);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(
            chunks[0].content.chars().count(),
            DISCORD_MESSAGE_CHAR_LIMIT
        );
    }

    #[test]
    fn split_discord_message_splits_when_over_limit() {
        let content = "a".repeat(DISCORD_MESSAGE_CHAR_LIMIT + 1);
        let chunks = split_discord_message(&content);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[1].chunk_index, 1);
        assert_eq!(
            chunks[0].content.chars().count(),
            DISCORD_MESSAGE_CHAR_LIMIT
        );
        assert_eq!(chunks[1].content.chars().count(), 1);
    }

    #[test]
    fn split_discord_message_handles_unicode_by_character_count() {
        let content = "🙂".repeat(DISCORD_MESSAGE_CHAR_LIMIT + 1);
        let chunks = split_discord_message(&content);
        assert_eq!(chunks.len(), 2);
        assert_eq!(
            chunks[0].content.chars().count(),
            DISCORD_MESSAGE_CHAR_LIMIT
        );
        assert_eq!(chunks[1].content.chars().count(), 1);
    }

    #[test]
    fn split_discord_message_is_deterministic() {
        let content = "abc".repeat(900);
        let first = split_discord_message(&content);
        let second = split_discord_message(&content);
        assert_eq!(first, second);
    }

    #[test]
    fn split_discord_message_uses_contiguous_chunk_indices() {
        let content = "x".repeat((DISCORD_MESSAGE_CHAR_LIMIT * 2) + 500);
        let chunks = split_discord_message(&content);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[1].chunk_index, 1);
        assert_eq!(chunks[2].chunk_index, 2);
        assert_eq!(
            chunks[0].content.chars().count(),
            DISCORD_MESSAGE_CHAR_LIMIT
        );
        assert_eq!(
            chunks[1].content.chars().count(),
            DISCORD_MESSAGE_CHAR_LIMIT
        );
        assert_eq!(chunks[2].content.chars().count(), 500);
    }

    #[test]
    fn streaming_delivery_exposes_rendered_chunks() {
        let mut streaming = StreamingDelivery::new();
        let _ = streaming.push_delta(&"a".repeat(DISCORD_MESSAGE_CHAR_LIMIT + 5));
        let chunks = streaming.rendered_chunks();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[1].chunk_index, 1);
        assert_eq!(
            chunks[0].content.chars().count(),
            DISCORD_MESSAGE_CHAR_LIMIT
        );
        assert_eq!(chunks[1].content.chars().count(), 5);
    }

    #[test]
    fn idempotent_delivery_skips_retry_after_recorded_send() {
        let root = temp_root("idempotent-retry");
        let ledger = IdempotentDeliveryLedger::new(OutboundRecordStore::new(&root));
        let attempt = sample_attempt();

        assert_eq!(
            ledger
                .should_send(&attempt)
                .expect("pre-send check should succeed"),
            ShouldSendDecision::Send
        );
        assert_eq!(
            ledger
                .mark_sent(&attempt)
                .expect("mark sent should succeed"),
            MarkSentDecision::Recorded
        );
        assert_eq!(
            ledger
                .should_send(&attempt)
                .expect("retry check should succeed"),
            ShouldSendDecision::SkipDuplicate
        );

        cleanup(&root);
    }

    #[test]
    fn idempotent_delivery_detects_conflicting_content_for_same_target() {
        let root = temp_root("idempotent-conflict");
        let ledger = IdempotentDeliveryLedger::new(OutboundRecordStore::new(&root));
        let attempt = sample_attempt();
        ledger
            .mark_sent(&attempt)
            .expect("initial mark sent should succeed");

        let mut conflicting = sample_attempt();
        conflicting.content = "different content".to_string();

        let err = ledger
            .should_send(&conflicting)
            .expect_err("conflicting content should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "idempotent_delivery_should_send",
                message:
                    "conflicting content hash for channel=abc, message=message-1, edit_generation=0"
                        .to_string()
            }
        );

        cleanup(&root);
    }

    #[test]
    fn idempotent_delivery_allows_new_edit_generation() {
        let root = temp_root("idempotent-generation");
        let ledger = IdempotentDeliveryLedger::new(OutboundRecordStore::new(&root));
        let attempt = sample_attempt();
        ledger
            .mark_sent(&attempt)
            .expect("initial mark sent should succeed");

        let mut next = sample_attempt();
        next.edit_generation = 1;
        next.content = "hello world".to_string();

        assert_eq!(
            ledger
                .should_send(&next)
                .expect("new edit generation should be sendable"),
            ShouldSendDecision::Send
        );

        cleanup(&root);
    }

    #[test]
    fn idempotent_delivery_mark_sent_is_idempotent() {
        let root = temp_root("idempotent-mark");
        let ledger = IdempotentDeliveryLedger::new(OutboundRecordStore::new(&root));
        let attempt = sample_attempt();

        let first = ledger
            .mark_sent(&attempt)
            .expect("first mark should succeed");
        let second = ledger
            .mark_sent(&attempt)
            .expect("second mark should be idempotent");

        assert_eq!(first, MarkSentDecision::Recorded);
        assert_eq!(second, MarkSentDecision::AlreadyRecorded);

        cleanup(&root);
    }

    #[test]
    fn idempotent_delivery_rejects_blank_logical_session_id() {
        let root = temp_root("idempotent-blank-session");
        let ledger = IdempotentDeliveryLedger::new(OutboundRecordStore::new(&root));
        let mut attempt = sample_attempt();
        attempt.logical_session_id = " ".to_string();

        let err = ledger
            .should_send(&attempt)
            .expect_err("blank logical session id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "idempotent_delivery_validate",
                message: "logical_session_id must not be empty".to_string()
            }
        );

        cleanup(&root);
    }

    #[test]
    fn idempotent_delivery_rejects_zero_delivery_timestamp() {
        let root = temp_root("idempotent-zero-delivered");
        let ledger = IdempotentDeliveryLedger::new(OutboundRecordStore::new(&root));
        let mut attempt = sample_attempt();
        attempt.delivered_at_epoch_ms = 0;

        let err = ledger
            .mark_sent(&attempt)
            .expect_err("zero delivery timestamp should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "idempotent_delivery_validate",
                message: "delivered_at_epoch_ms must be greater than 0".to_string()
            }
        );

        cleanup(&root);
    }

    #[test]
    fn idempotent_delivery_rejects_blank_run_id() {
        let root = temp_root("idempotent-blank-run");
        let ledger = IdempotentDeliveryLedger::new(OutboundRecordStore::new(&root));
        let mut attempt = sample_attempt();
        attempt.run_id = " ".to_string();

        let err = ledger
            .should_send(&attempt)
            .expect_err("blank run id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "idempotent_delivery_validate",
                message: "run_id must not be empty".to_string()
            }
        );

        cleanup(&root);
    }

    #[test]
    fn idempotent_delivery_rejects_blank_channel_id() {
        let root = temp_root("idempotent-blank-channel");
        let ledger = IdempotentDeliveryLedger::new(OutboundRecordStore::new(&root));
        let mut attempt = sample_attempt();
        attempt.channel_id = " ".to_string();

        let err = ledger
            .should_send(&attempt)
            .expect_err("blank channel id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "idempotent_delivery_validate",
                message: "channel_id must not be empty".to_string()
            }
        );

        cleanup(&root);
    }

    #[test]
    fn idempotent_delivery_rejects_blank_message_id() {
        let root = temp_root("idempotent-blank-message");
        let ledger = IdempotentDeliveryLedger::new(OutboundRecordStore::new(&root));
        let mut attempt = sample_attempt();
        attempt.message_id = " ".to_string();

        let err = ledger
            .should_send(&attempt)
            .expect_err("blank message id should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "idempotent_delivery_validate",
                message: "message_id must not be empty".to_string()
            }
        );

        cleanup(&root);
    }

    #[test]
    fn idempotent_delivery_rejects_blank_content() {
        let root = temp_root("idempotent-blank-content");
        let ledger = IdempotentDeliveryLedger::new(OutboundRecordStore::new(&root));
        let mut attempt = sample_attempt();
        attempt.content = " ".to_string();

        let err = ledger
            .should_send(&attempt)
            .expect_err("blank content should fail");
        assert_eq!(
            err,
            CrabError::InvariantViolation {
                context: "idempotent_delivery_validate",
                message: "content must not be empty".to_string()
            }
        );

        cleanup(&root);
    }

    #[test]
    fn idempotent_delivery_should_send_surfaces_store_errors() {
        let root = temp_root("idempotent-should-send-store-error");
        fs::write(&root, "not-a-directory").expect("file setup should succeed");
        let ledger = IdempotentDeliveryLedger::new(OutboundRecordStore::new(&root));
        let attempt = sample_attempt();

        let err = ledger
            .should_send(&attempt)
            .expect_err("store error should propagate");
        assert!(matches!(
            err,
            CrabError::Io {
                context: "outbound_record_store_layout",
                ..
            }
        ));

        let _ = fs::remove_file(&root);
    }

    #[test]
    fn idempotent_delivery_mark_sent_surfaces_store_errors() {
        let root = temp_root("idempotent-mark-store-error");
        fs::write(&root, "not-a-directory").expect("file setup should succeed");
        let ledger = IdempotentDeliveryLedger::new(OutboundRecordStore::new(&root));
        let attempt = sample_attempt();

        let err = ledger
            .mark_sent(&attempt)
            .expect_err("store error should propagate");
        assert!(matches!(
            err,
            CrabError::Io {
                context: "outbound_record_store_layout",
                ..
            }
        ));

        let _ = fs::remove_file(&root);
    }
}
