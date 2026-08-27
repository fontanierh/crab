#![forbid(unsafe_code)]

mod authority_probe_support;

use std::env;

use authority_probe_support::{ProbeDefinition, run};

const DEFINITION: ProbeDefinition = ProbeDefinition {
    probe_name: "crab-v2-claude-authority-probe",
    adapter_name: "Claude",
    adapter_package: "@agentclientprotocol/claude-agent-acp@0.70.0",
    adapter_version: "0.70.0",
    adapter_version_output: "0.70.0",
    network_endpoint: "api.anthropic.com:443",
    require_non_root: true,
};

fn main() {
    match run(&DEFINITION, env::args_os().skip(1)) {
        Ok(report) => println!("{report}"),
        Err(error) => {
            eprintln!("claude authority probe failed: {error}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::{
        DEFINITION,
        authority_probe_support::{adapter_invocation, sandbox_is_disabled},
    };

    #[test]
    fn sandbox_policy_requires_the_exact_unsandboxed_record() {
        assert!(sandbox_is_disabled("pid = 42\nsandboxed = no\n"));
        assert!(!sandbox_is_disabled("sandboxed = yes\n"));
        assert!(!sandbox_is_disabled("sandboxed = no-ish\n"));
        assert!(!sandbox_is_disabled(""));
    }

    #[test]
    fn default_adapter_stays_exactly_pinned() {
        let invocation =
            adapter_invocation(&DEFINITION, std::iter::empty()).expect("default invocation");
        assert_eq!(invocation.executable, std::path::Path::new("npx"));
        assert_eq!(
            invocation.arguments,
            [
                "--yes",
                "@agentclientprotocol/claude-agent-acp@0.70.0",
                "--version"
            ]
        );
        assert_eq!(invocation.source, "pinned-npx");
    }

    #[test]
    fn bundled_adapter_must_be_one_relative_path() {
        assert!(adapter_invocation(&DEFINITION, [OsString::from("--other")].into_iter()).is_err());
        assert!(
            adapter_invocation(
                &DEFINITION,
                ["--adapter-relative-to-probe", "/absolute/adapter"]
                    .into_iter()
                    .map(OsString::from)
            )
            .is_err()
        );
        let invocation = adapter_invocation(
            &DEFINITION,
            [
                "--adapter-relative-to-probe",
                "../agents/claude/node_modules/.bin/claude-agent-acp",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .expect("relative adapter path");
        assert!(invocation.executable.is_absolute());
        assert!(
            invocation
                .executable
                .ends_with("agents/claude/node_modules/.bin/claude-agent-acp")
        );
        assert_eq!(invocation.arguments, ["--version"]);
        assert_eq!(invocation.source, "bundle-relative");
    }
}
