//! Application entry surfaces for Crab runtime wiring.

mod memory_cli;
mod startup;

pub use memory_cli::{run_memory_get_cli, run_memory_search_cli};
pub use startup::{initialize_runtime_startup, AppStartupOutcome};
