#![forbid(unsafe_code)]

use std::{env, process::ExitCode};

const USAGE: &str =
    "Usage: crab-v2-sub-agent-mcp\n\nServe Crab's native sub-agent tools over MCP stdio.";

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    match arguments.as_slice() {
        [] => {}
        [argument] if argument == "--help" || argument == "-h" => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        _ => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    }
    match crab_v2_runtime::run_sub_agent_mcp_stdio().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("crab-v2-sub-agent-mcp: {error}");
            ExitCode::FAILURE
        }
    }
}
