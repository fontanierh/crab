use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crab_core::{
    config::{HeartbeatConfig, StartupReconciliationConfig},
    ensure_state_schema_version, CrabError, CrabResult, RuntimeConfig, StateSchemaMigrationOutcome,
};
use crab_discord::{GatewayIngress, IdempotentDeliveryLedger};
use crab_scheduler::LaneScheduler;
use crab_store::{CheckpointStore, EventStore, OutboundRecordStore, RunStore, SessionStore};

use crate::{initialize_runtime_startup, AppStartupOutcome};

pub const DEFAULT_LANE_QUEUE_LIMIT: usize = 128;
const STATE_DIRECTORY_NAME: &str = "state";

#[derive(Debug, Clone)]
pub struct AppStateStores {
    pub root: PathBuf,
    pub session_store: SessionStore,
    pub run_store: RunStore,
    pub event_store: EventStore,
    pub checkpoint_store: CheckpointStore,
    pub outbound_record_store: OutboundRecordStore,
}

impl AppStateStores {
    #[must_use]
    pub fn new(root: &Path) -> Self {
        let root = root.to_path_buf();
        Self {
            root: root.clone(),
            session_store: SessionStore::new(root.clone()),
            run_store: RunStore::new(root.clone()),
            event_store: EventStore::new(root.clone()),
            checkpoint_store: CheckpointStore::new(root.clone()),
            outbound_record_store: OutboundRecordStore::new(root),
        }
    }
}

pub struct AppComposition {
    pub startup: AppStartupOutcome,
    pub startup_reconciliation_policy: StartupReconciliationConfig,
    pub heartbeat_policy: HeartbeatConfig,
    pub state_schema_migration: StateSchemaMigrationOutcome,
    pub state_stores: AppStateStores,
    pub scheduler: LaneScheduler,
    pub gateway_ingress: GatewayIngress,
    pub delivery_ledger: IdempotentDeliveryLedger,
}

#[cfg(test)]
pub(crate) fn compose_runtime(
    config: &RuntimeConfig,
    bot_user_id: &str,
) -> CrabResult<AppComposition> {
    compose_runtime_with_queue_limit(config, bot_user_id, DEFAULT_LANE_QUEUE_LIMIT)
}

pub fn compose_runtime_with_queue_limit(
    config: &RuntimeConfig,
    bot_user_id: &str,
    lane_queue_limit: usize,
) -> CrabResult<AppComposition> {
    let startup = initialize_runtime_startup(config)?;
    let state_root = startup.workspace_root.join(STATE_DIRECTORY_NAME);
    ensure_state_root(&state_root)?;
    let migration_now_epoch_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX);
    let state_schema_migration = ensure_state_schema_version(&state_root, migration_now_epoch_ms)?;

    let state_stores = AppStateStores::new(&state_root);
    let scheduler = LaneScheduler::new(config.max_concurrent_lanes, lane_queue_limit)?;
    let gateway_ingress = GatewayIngress::new(bot_user_id.to_string())?;
    let delivery_ledger = IdempotentDeliveryLedger::new(state_stores.outbound_record_store.clone());

    Ok(AppComposition {
        startup,
        startup_reconciliation_policy: config.startup_reconciliation,
        heartbeat_policy: config.heartbeat,
        state_schema_migration,
        state_stores,
        scheduler,
        gateway_ingress,
        delivery_ledger,
    })
}

fn ensure_state_root(state_root: &Path) -> CrabResult<()> {
    fs::create_dir_all(state_root).map_err(|error| CrabError::InvariantViolation {
        context: "app_composition_root",
        message: format!(
            "failed to create state root {}: {error}",
            state_root.to_string_lossy()
        ),
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crab_core::{CrabError, CURRENT_STATE_SCHEMA_VERSION};
    use crab_discord::{
        DeliveryAttempt, GatewayConversationKind, GatewayMessage, MarkSentDecision, RoutingKey,
        ShouldSendDecision,
    };
    use crab_scheduler::QueuedRun;

    use super::{
        compose_runtime, compose_runtime_with_queue_limit, AppComposition, DEFAULT_LANE_QUEUE_LIMIT,
    };
    use crate::test_support::{runtime_config_for_workspace_with_lanes, TempWorkspace};

    fn compose_default(workspace: &TempWorkspace) -> crab_core::CrabResult<AppComposition> {
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 3);
        compose_runtime(&config, "999")
    }

    #[test]
    fn composition_root_wires_stores_scheduler_discord_and_backends() {
        let workspace = TempWorkspace::new("composition", "wiring");
        let mut composition = compose_default(&workspace).expect("composition should succeed");

        assert_eq!(composition.state_stores.root, workspace.path.join("state"));
        assert!(composition.state_stores.root.is_dir());
        assert_eq!(
            composition.state_schema_migration.target_version,
            CURRENT_STATE_SCHEMA_VERSION
        );
        assert_eq!(composition.scheduler.active_lane_count(), 0);

        composition
            .scheduler
            .enqueue(
                "discord:channel:1",
                QueuedRun {
                    run_id: "run-1".to_string(),
                },
            )
            .expect("enqueue should succeed");
        let dispatched = composition
            .scheduler
            .try_dispatch_next()
            .expect("queued run should dispatch");
        assert_eq!(dispatched.logical_session_id, "discord:channel:1");
        assert_eq!(dispatched.run.run_id, "run-1");

        let ingested = composition
            .gateway_ingress
            .ingest(GatewayMessage {
                message_id: "msg-1".to_string(),
                author_id: "111".to_string(),
                author_is_bot: false,
                channel_id: "777".to_string(),
                guild_id: Some("555".to_string()),
                thread_id: None,
                content: "ping".to_string(),
                conversation_kind: GatewayConversationKind::GuildChannel,
                attachments: vec![],
            })
            .expect("ingress should succeed")
            .expect("non-bot message should be accepted");
        assert_eq!(
            ingested.routing_key,
            RoutingKey::Channel {
                channel_id: "777".to_string()
            }
        );

        let attempt = DeliveryAttempt {
            logical_session_id: "discord:channel:777".to_string(),
            run_id: "run-1".to_string(),
            channel_id: "777".to_string(),
            message_id: "msg-42".to_string(),
            edit_generation: 0,
            content: "hello from crab".to_string(),
            delivered_at_epoch_ms: 1_739_173_200_123,
        };
        assert_eq!(
            composition
                .delivery_ledger
                .should_send(&attempt)
                .expect("should_send should succeed"),
            ShouldSendDecision::Send
        );
        assert_eq!(
            composition
                .delivery_ledger
                .mark_sent(&attempt)
                .expect("mark_sent should succeed"),
            MarkSentDecision::Recorded
        );
        let records = composition
            .state_stores
            .outbound_record_store
            .list_run_records(&attempt.logical_session_id, &attempt.run_id)
            .expect("store should list written record");
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn composition_root_validates_queue_limit_and_bot_identity() {
        let workspace = TempWorkspace::new("composition", "validation");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 3);

        let queue_result = compose_runtime_with_queue_limit(&config, "999", 0);
        let queue_error = queue_result.err().expect("queue limit 0 should fail");
        assert_eq!(
            queue_error,
            CrabError::InvalidConfig {
                key: "CRAB_LANE_QUEUE_LIMIT",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );

        let bot_result = compose_runtime(&config, " ");
        let bot_error = bot_result.err().expect("blank bot id should fail");
        assert_eq!(
            bot_error,
            CrabError::InvariantViolation {
                context: "gateway_ingress_new",
                message: "bot_user_id must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn composition_root_propagates_workspace_startup_failures() {
        let workspace = TempWorkspace::new("composition", "startup-failure");
        fs::write(&workspace.path, "workspace root as file")
            .expect("fixture file should be writable");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 3);

        let result = compose_runtime(&config, "999");
        let error = result.err().expect("workspace file root should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_layout",
                message: format!("{} must be a directory", workspace.path.to_string_lossy()),
            }
        );
    }

    #[test]
    fn composition_root_reports_state_root_creation_failures() {
        let workspace = TempWorkspace::new("composition", "state-root-failure");
        fs::create_dir_all(&workspace.path).expect("workspace root should be creatable");
        fs::write(workspace.path.join("state"), "file blocks directory")
            .expect("state collision file should be writable");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 3);

        let error = compose_runtime(&config, "999")
            .err()
            .expect("state root collision should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "app_composition_root",
                ..
            }
        ));
        assert!(error.to_string().contains("failed to create state root"));
    }

    #[test]
    fn default_lane_queue_limit_is_stable() {
        assert_eq!(DEFAULT_LANE_QUEUE_LIMIT, 128);
    }
}
