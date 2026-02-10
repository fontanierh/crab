//! Application entry surfaces for Crab runtime wiring.

mod composition;
mod memory_cli;
mod startup;
#[cfg(test)]
mod test_support;

pub use composition::{
    compose_runtime_with_processes, compose_runtime_with_processes_and_queue_limit,
    AppBackendManagers, AppComposition, AppStateStores, DEFAULT_LANE_QUEUE_LIMIT,
};
pub use memory_cli::{run_memory_get_cli, run_memory_search_cli};
pub use startup::{initialize_runtime_startup, AppStartupOutcome};
