use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::profile::ProfileValueSource;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendKind {
    Claude,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningLevel {
    None,
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
}

impl ReasoningLevel {
    #[must_use]
    pub const fn as_token(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
        }
    }

    #[must_use]
    pub fn parse_token(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::None),
            "minimal" => Some(Self::Minimal),
            "low" => Some(Self::Low),
            "medium" => Some(Self::Medium),
            "high" => Some(Self::High),
            "xhigh" => Some(Self::XHigh),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LaneState {
    Idle,
    Running,
    Cancelling,
    Rotating,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventSource {
    Backend,
    System,
    Discord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    TextDelta,
    ToolCall,
    ToolResult,
    ApprovalRequest,
    ApprovalDecision,
    RunNote,
    RunState,
    Heartbeat,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferenceProfile {
    pub backend: BackendKind,
    pub model: String,
    pub reasoning_level: ReasoningLevel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunProfileTelemetry {
    pub requested_profile: Option<InferenceProfile>,
    pub resolved_profile: InferenceProfile,
    pub backend_source: ProfileValueSource,
    pub model_source: ProfileValueSource,
    pub reasoning_level_source: ProfileValueSource,
    pub fallback_applied: bool,
    pub fallback_notes: Vec<String>,
    pub sender_id: String,
    pub sender_is_owner: bool,
    pub resolved_owner_profile: Option<OwnerProfileMetadata>,
}

impl RunProfileTelemetry {
    #[must_use]
    pub fn profile_source_token(&self) -> &'static str {
        if self.fallback_applied {
            return "fallback";
        }

        let mut strongest_source = self.backend_source;
        for source in [self.model_source, self.reasoning_level_source] {
            if profile_source_rank(source) < profile_source_rank(strongest_source) {
                strongest_source = source;
            }
        }
        profile_source_token(strongest_source)
    }
}

const fn profile_source_rank(source: ProfileValueSource) -> u8 {
    match source {
        ProfileValueSource::TurnOverride => 0,
        ProfileValueSource::SessionProfile => 1,
        ProfileValueSource::ChannelOverride => 2,
        ProfileValueSource::BackendDefault => 3,
        ProfileValueSource::GlobalDefault => 4,
    }
}

const fn profile_source_token(source: ProfileValueSource) -> &'static str {
    match source {
        ProfileValueSource::TurnOverride => "turn",
        ProfileValueSource::SessionProfile => "session",
        ProfileValueSource::ChannelOverride => "channel",
        ProfileValueSource::BackendDefault => "backend_default",
        ProfileValueSource::GlobalDefault => "global_default",
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerProfileMetadata {
    pub machine_location: Option<String>,
    pub machine_timezone: Option<String>,
    pub default_backend: Option<BackendKind>,
    pub default_model: Option<String>,
    pub default_reasoning_level: Option<ReasoningLevel>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenAccounting {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
}

impl TokenAccounting {
    /// Real context window consumption: cached + non-cached input tokens.
    #[must_use]
    pub fn context_window_tokens(&self) -> u64 {
        self.cache_read_input_tokens
            .saturating_add(self.cache_creation_input_tokens)
            .saturating_add(self.input_tokens)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalSession {
    pub id: String,
    pub active_backend: BackendKind,
    pub active_profile: InferenceProfile,
    pub active_physical_session_id: Option<String>,
    pub last_successful_checkpoint_id: Option<String>,
    pub lane_state: LaneState,
    pub queued_run_count: u32,
    pub last_activity_epoch_ms: u64,
    pub token_accounting: TokenAccounting,
    /// Tracks whether bootstrap context has been injected into the current physical session.
    /// Reset to `false` on rotation (physical session teardown).
    #[serde(default)]
    pub has_injected_bootstrap: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalSession {
    pub id: String,
    pub logical_session_id: String,
    pub backend: BackendKind,
    pub backend_session_id: String,
    pub created_at_epoch_ms: u64,
    pub last_turn_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Run {
    pub id: String,
    pub logical_session_id: String,
    pub physical_session_id: Option<String>,
    pub status: RunStatus,
    pub user_input: String,
    /// Discord channel id to deliver replies into. For DMs this differs from the logical session id
    /// (which is `discord:dm:<user_id>`); Discord delivery must target the DM channel id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_channel_id: Option<String>,
    pub profile: RunProfileTelemetry,
    pub queued_at_epoch_ms: u64,
    pub started_at_epoch_ms: Option<u64>,
    pub completed_at_epoch_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub id: String,
    pub logical_session_id: String,
    pub run_id: String,
    pub created_at_epoch_ms: u64,
    pub summary: String,
    pub memory_digest: String,
    pub state: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: String,
    pub run_id: String,
    pub turn_id: Option<String>,
    pub lane_id: Option<String>,
    pub logical_session_id: String,
    pub physical_session_id: Option<String>,
    pub backend: Option<BackendKind>,
    pub resolved_model: Option<String>,
    pub resolved_reasoning_level: Option<String>,
    pub profile_source: Option<String>,
    pub sequence: u64,
    pub emitted_at_epoch_ms: u64,
    pub source: EventSource,
    pub kind: EventKind,
    pub payload: BTreeMap<String, String>,
    pub profile: Option<RunProfileTelemetry>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundRecord {
    pub record_id: String,
    pub logical_session_id: String,
    pub run_id: String,
    pub channel_id: String,
    pub message_id: String,
    pub edit_generation: u32,
    pub content_sha256: String,
    pub delivered_at_epoch_ms: u64,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fmt::Debug;

    use serde::de::DeserializeOwned;
    use serde::Serialize;

    use super::{
        BackendKind, Checkpoint, EventEnvelope, EventKind, EventSource, InferenceProfile,
        LaneState, LogicalSession, OutboundRecord, OwnerProfileMetadata, PhysicalSession,
        ReasoningLevel, Run, RunProfileTelemetry, RunStatus, TokenAccounting,
    };
    use crate::ProfileValueSource;

    fn assert_json_round_trip<T>(value: &T)
    where
        T: Serialize + DeserializeOwned + PartialEq + Debug,
    {
        let encoded = serde_json::to_vec(value).expect("json encode should succeed");
        let decoded: T = serde_json::from_slice(&encoded).expect("json decode should succeed");
        assert_eq!(*value, decoded);
    }

    fn sample_profile() -> InferenceProfile {
        InferenceProfile {
            backend: BackendKind::Claude,
            model: "claude-sonnet".to_string(),
            reasoning_level: ReasoningLevel::High,
        }
    }

    fn sample_token_accounting() -> TokenAccounting {
        TokenAccounting {
            input_tokens: 1200,
            output_tokens: 300,
            total_tokens: 1500,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
        }
    }

    fn sample_run_profile_telemetry() -> RunProfileTelemetry {
        RunProfileTelemetry {
            requested_profile: Some(InferenceProfile {
                backend: BackendKind::Claude,
                model: "legacy".to_string(),
                reasoning_level: ReasoningLevel::XHigh,
            }),
            resolved_profile: sample_profile(),
            backend_source: ProfileValueSource::SessionProfile,
            model_source: ProfileValueSource::BackendDefault,
            reasoning_level_source: ProfileValueSource::TurnOverride,
            fallback_applied: true,
            fallback_notes: vec!["fallback profile applied".to_string()],
            sender_id: "123456789012345678".to_string(),
            sender_is_owner: true,
            resolved_owner_profile: Some(OwnerProfileMetadata {
                machine_location: Some("Paris, France".to_string()),
                machine_timezone: Some("Europe/Paris".to_string()),
                default_backend: Some(BackendKind::Claude),
                default_model: Some("claude-sonnet".to_string()),
                default_reasoning_level: Some(ReasoningLevel::High),
            }),
        }
    }

    #[test]
    fn logical_session_round_trip() {
        let session = LogicalSession {
            id: "discord:channel:123".to_string(),
            active_backend: BackendKind::Claude,
            active_profile: sample_profile(),
            active_physical_session_id: Some("thread_abc".to_string()),
            last_successful_checkpoint_id: Some("ckpt_9".to_string()),
            lane_state: LaneState::Running,
            queued_run_count: 2,
            last_activity_epoch_ms: 1_739_173_200_000,
            token_accounting: sample_token_accounting(),
            has_injected_bootstrap: false,
        };
        assert_json_round_trip(&session);
    }

    #[test]
    fn physical_session_round_trip() {
        let physical = PhysicalSession {
            id: "physical_1".to_string(),
            logical_session_id: "discord:thread:777".to_string(),
            backend: BackendKind::Claude,
            backend_session_id: "claude_resume_id".to_string(),
            created_at_epoch_ms: 1_739_173_200_123,
            last_turn_id: Some("turn_9".to_string()),
        };
        assert_json_round_trip(&physical);
    }

    #[test]
    fn run_round_trip() {
        let run = Run {
            id: "run_001".to_string(),
            logical_session_id: "discord:channel:123".to_string(),
            physical_session_id: Some("physical_1".to_string()),
            status: RunStatus::Succeeded,
            user_input: "Please summarize the queue state.".to_string(),
            delivery_channel_id: None,
            profile: sample_run_profile_telemetry(),
            queued_at_epoch_ms: 1_739_173_200_200,
            started_at_epoch_ms: Some(1_739_173_200_250),
            completed_at_epoch_ms: Some(1_739_173_200_500),
        };
        assert_json_round_trip(&run);
    }

    #[test]
    fn checkpoint_round_trip() {
        let state = BTreeMap::from([
            ("lane_state".to_string(), "idle".to_string()),
            ("last_checkpoint_turn".to_string(), "84".to_string()),
        ]);

        let checkpoint = Checkpoint {
            id: "ckpt_42".to_string(),
            logical_session_id: "discord:channel:123".to_string(),
            run_id: "run_001".to_string(),
            created_at_epoch_ms: 1_739_173_201_000,
            summary: "Session compacted with pending queue preserved.".to_string(),
            memory_digest: "mem:users/42/2026-02-09".to_string(),
            state,
        };
        assert_json_round_trip(&checkpoint);
    }

    #[test]
    fn event_envelope_round_trip() {
        let payload = BTreeMap::from([
            ("text".to_string(), "Working on WS1-T1".to_string()),
            ("token_delta".to_string(), "42".to_string()),
        ]);

        let event = EventEnvelope {
            event_id: "evt_001".to_string(),
            run_id: "run_001".to_string(),
            turn_id: Some("turn_001".to_string()),
            lane_id: Some("discord:channel:123".to_string()),
            logical_session_id: "discord:channel:123".to_string(),
            physical_session_id: Some("physical_1".to_string()),
            backend: Some(BackendKind::Claude),
            resolved_model: Some("claude-sonnet".to_string()),
            resolved_reasoning_level: Some("high".to_string()),
            profile_source: Some("fallback".to_string()),
            sequence: 7,
            emitted_at_epoch_ms: 1_739_173_201_010,
            source: EventSource::Backend,
            kind: EventKind::TextDelta,
            payload,
            profile: Some(sample_run_profile_telemetry()),
            idempotency_key: Some("run_001:7".to_string()),
        };
        assert_json_round_trip(&event);
    }

    #[test]
    fn run_profile_source_token_prefers_highest_precedence_source() {
        let profile = RunProfileTelemetry {
            requested_profile: None,
            resolved_profile: sample_profile(),
            backend_source: ProfileValueSource::GlobalDefault,
            model_source: ProfileValueSource::SessionProfile,
            reasoning_level_source: ProfileValueSource::TurnOverride,
            fallback_applied: false,
            fallback_notes: Vec::new(),
            sender_id: "123456789012345678".to_string(),
            sender_is_owner: false,
            resolved_owner_profile: None,
        };
        assert_eq!(profile.profile_source_token(), "turn");
    }

    #[test]
    fn run_profile_source_token_maps_all_source_levels() {
        let cases = [
            (ProfileValueSource::TurnOverride, "turn"),
            (ProfileValueSource::SessionProfile, "session"),
            (ProfileValueSource::ChannelOverride, "channel"),
            (ProfileValueSource::BackendDefault, "backend_default"),
            (ProfileValueSource::GlobalDefault, "global_default"),
        ];

        for (source, expected) in cases {
            let profile = RunProfileTelemetry {
                requested_profile: None,
                resolved_profile: sample_profile(),
                backend_source: source,
                model_source: source,
                reasoning_level_source: source,
                fallback_applied: false,
                fallback_notes: Vec::new(),
                sender_id: "123456789012345678".to_string(),
                sender_is_owner: false,
                resolved_owner_profile: None,
            };
            assert_eq!(profile.profile_source_token(), expected);
        }
    }

    #[test]
    fn run_profile_source_token_reports_fallback_when_applied() {
        let profile = sample_run_profile_telemetry();
        assert_eq!(profile.profile_source_token(), "fallback");
    }

    #[test]
    fn outbound_record_round_trip() {
        let outbound = OutboundRecord {
            record_id: "out_99".to_string(),
            logical_session_id: "discord:channel:123".to_string(),
            run_id: "run_001".to_string(),
            channel_id: "123".to_string(),
            message_id: "456".to_string(),
            edit_generation: 3,
            content_sha256: "abc123".to_string(),
            delivered_at_epoch_ms: 1_739_173_201_100,
        };
        assert_json_round_trip(&outbound);
    }

    #[test]
    fn token_accounting_context_window_tokens_helper() {
        let accounting = TokenAccounting {
            input_tokens: 5,
            output_tokens: 10,
            total_tokens: 15,
            cache_read_input_tokens: 40_000,
            cache_creation_input_tokens: 7_000,
        };
        assert_eq!(accounting.context_window_tokens(), 47_005);
    }

    #[test]
    fn token_accounting_context_window_tokens_saturates_on_overflow() {
        let accounting = TokenAccounting {
            input_tokens: 1,
            output_tokens: 0,
            total_tokens: 1,
            cache_read_input_tokens: u64::MAX,
            cache_creation_input_tokens: 1,
        };
        assert_eq!(accounting.context_window_tokens(), u64::MAX);
    }

    #[test]
    fn token_accounting_backward_compat_deserialization() {
        let json = r#"{"input_tokens":10,"output_tokens":5,"total_tokens":15}"#;
        let accounting: TokenAccounting =
            serde_json::from_str(json).expect("legacy JSON should deserialize");
        assert_eq!(accounting.input_tokens, 10);
        assert_eq!(accounting.output_tokens, 5);
        assert_eq!(accounting.total_tokens, 15);
        assert_eq!(accounting.cache_read_input_tokens, 0);
        assert_eq!(accounting.cache_creation_input_tokens, 0);
        assert_eq!(accounting.context_window_tokens(), 10);
    }
}
