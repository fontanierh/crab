use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::profile::ProfileValueSource;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendKind {
    Claude,
    Codex,
    OpenCode,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenAccounting {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
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
    pub logical_session_id: String,
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
        LaneState, LogicalSession, OutboundRecord, PhysicalSession, ReasoningLevel, Run,
        RunProfileTelemetry, RunStatus, TokenAccounting,
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
            backend: BackendKind::Codex,
            model: "gpt-5-codex".to_string(),
            reasoning_level: ReasoningLevel::High,
        }
    }

    fn sample_token_accounting() -> TokenAccounting {
        TokenAccounting {
            input_tokens: 1200,
            output_tokens: 300,
            total_tokens: 1500,
        }
    }

    fn sample_run_profile_telemetry() -> RunProfileTelemetry {
        RunProfileTelemetry {
            requested_profile: Some(InferenceProfile {
                backend: BackendKind::Codex,
                model: "legacy".to_string(),
                reasoning_level: ReasoningLevel::XHigh,
            }),
            resolved_profile: sample_profile(),
            backend_source: ProfileValueSource::SessionProfile,
            model_source: ProfileValueSource::BackendDefault,
            reasoning_level_source: ProfileValueSource::TurnOverride,
            fallback_applied: true,
            fallback_notes: vec!["fallback profile applied".to_string()],
        }
    }

    #[test]
    fn logical_session_round_trip() {
        let session = LogicalSession {
            id: "discord:channel:123".to_string(),
            active_backend: BackendKind::Codex,
            active_profile: sample_profile(),
            active_physical_session_id: Some("thread_abc".to_string()),
            last_successful_checkpoint_id: Some("ckpt_9".to_string()),
            lane_state: LaneState::Running,
            queued_run_count: 2,
            last_activity_epoch_ms: 1_739_173_200_000,
            token_accounting: sample_token_accounting(),
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
            logical_session_id: "discord:channel:123".to_string(),
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
}
