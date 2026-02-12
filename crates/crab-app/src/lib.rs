//! Application entry surfaces for Crab runtime wiring.

mod composition;
mod daemon;
mod installer;
mod maintenance;
mod memory_cli;
mod startup;
#[cfg(test)]
mod test_support;
mod turn_executor;

pub use composition::{
    compose_runtime_with_processes, compose_runtime_with_processes_and_queue_limit,
    AppBackendManagers, AppComposition, AppStateStores, DEFAULT_LANE_QUEUE_LIMIT,
};
pub use daemon::{
    run_daemon_loop_with_transport, DaemonClaudeProcess, DaemonConfig, DaemonDiscordIo,
    DaemonLoopControl, DaemonLoopStats, DaemonTurnRuntime, SystemDaemonLoopControl,
    DEFAULT_DAEMON_TICK_INTERVAL_MS,
};
pub use installer::run_installer_cli;
pub use maintenance::{
    boot_runtime_with_processes, boot_runtime_with_processes_and_queue_limit, run_heartbeat_if_due,
    run_startup_reconciliation_on_boot, BootRuntime, HeartbeatLoopState,
};
pub use memory_cli::{run_memory_get_cli, run_memory_search_cli};
pub use startup::{initialize_runtime_startup, AppStartupOutcome};
pub use turn_executor::{DispatchedTurn, QueuedTurn, TurnExecutor, TurnExecutorRuntime};
