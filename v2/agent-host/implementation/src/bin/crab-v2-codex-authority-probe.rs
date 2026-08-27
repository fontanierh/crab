#![forbid(unsafe_code)]

mod authority_probe_support;

use std::env;

use authority_probe_support::ProbeDefinition;

const DEFINITION: ProbeDefinition = ProbeDefinition {
    probe_name: "crab-v2-codex-authority-probe",
    adapter_name: "Codex",
    adapter_package: "@agentclientprotocol/codex-acp@1.6.2",
    adapter_version: "1.6.2",
    adapter_version_output: "@agentclientprotocol/codex-acp 1.6.2",
    network_endpoint: "chatgpt.com:443",
    require_non_root: false,
};

fn main() {
    match authority_probe_support::run(&DEFINITION, env::args_os().skip(1)) {
        Ok(report) => println!("{report}"),
        Err(error) => {
            eprintln!("codex authority probe failed: {error}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFINITION, authority_probe_support::adapter_invocation};

    #[test]
    fn default_adapter_stays_exactly_pinned() {
        let invocation =
            adapter_invocation(&DEFINITION, std::iter::empty()).expect("default invocation");
        assert_eq!(invocation.executable, std::path::Path::new("npx"));
        assert_eq!(
            invocation.arguments,
            ["--yes", "@agentclientprotocol/codex-acp@1.6.2", "--version"]
        );
        assert_eq!(invocation.source, "pinned-npx");
    }
}
