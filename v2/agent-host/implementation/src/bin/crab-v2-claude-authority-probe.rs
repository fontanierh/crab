#![forbid(unsafe_code)]

use std::{
    env,
    ffi::OsString,
    fs::{self, OpenOptions},
    io::Write,
    net::{TcpStream, ToSocketAddrs},
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use serde_json::json;
use uuid::Uuid;

const ADAPTER_PACKAGE: &str = "@agentclientprotocol/claude-agent-acp@0.70.0";
const ADAPTER_VERSION: &str = "0.70.0";
const NETWORK_ENDPOINT: &str = "api.anthropic.com:443";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

struct AdapterInvocation {
    executable: PathBuf,
    arguments: Vec<OsString>,
    source: &'static str,
}

fn main() {
    match adapter_invocation(env::args_os().skip(1)).and_then(|adapter| verify(&adapter)) {
        Ok(report) => println!("{report}"),
        Err(error) => {
            eprintln!("claude authority probe failed: {error}");
            std::process::exit(1);
        }
    }
}

fn verify(adapter: &AdapterInvocation) -> Result<serde_json::Value, &'static str> {
    let uid = verify_non_root_uid()?;
    verify_adapter_version(adapter)?;
    verify_macos_sandbox()?;
    verify_write_scope(&env::temp_dir())?;
    let home = env::var_os("HOME").ok_or("HOME is unavailable")?;
    verify_write_scope(Path::new(&home))?;
    verify_network()?;

    Ok(json!({
        "sandboxDisabled": true,
        "permissionBypass": true,
        "unrestrictedFilesystem": true,
        "unrestrictedNetwork": true,
        "evidence": {
            "probe": "crab-v2-claude-authority-probe",
            "probeVersion": env!("CARGO_PKG_VERSION"),
            "adapterPackage": ADAPTER_PACKAGE,
            "adapterVersion": ADAPTER_VERSION,
            "adapterSource": adapter.source,
            "uid": uid,
            "sandbox": "launchctl:sandboxed=no",
            "filesystemScopes": ["home", "temporary-directory"],
            "networkEndpoint": NETWORK_ENDPOINT
        }
    }))
}

fn verify_non_root_uid() -> Result<u32, &'static str> {
    let output = Command::new("id")
        .arg("-u")
        .output()
        .map_err(|_| "id could not run")?;
    if !output.status.success() {
        return Err("id failed");
    }
    let uid = std::str::from_utf8(&output.stdout)
        .map_err(|_| "id returned non-UTF-8")?
        .trim()
        .parse::<u32>()
        .map_err(|_| "id returned an invalid uid")?;
    if uid == 0 {
        return Err("Claude bypass mode is unavailable at EUID 0");
    }
    Ok(uid)
}

fn adapter_invocation(
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<AdapterInvocation, &'static str> {
    let Some(first) = arguments.next() else {
        return Ok(AdapterInvocation {
            executable: "npx".into(),
            arguments: ["--yes", ADAPTER_PACKAGE, "--version"]
                .into_iter()
                .map(OsString::from)
                .collect(),
            source: "pinned-npx",
        });
    };
    if first != "--adapter-relative-to-probe" {
        return Err("authority probe arguments are invalid");
    }
    let relative = PathBuf::from(
        arguments
            .next()
            .ok_or("authority probe adapter path is missing")?,
    );
    if relative.is_absolute() || arguments.next().is_some() {
        return Err("authority probe adapter path must be one relative path");
    }
    let executable = env::current_exe()
        .map_err(|_| "authority probe executable path is unavailable")?
        .parent()
        .ok_or("authority probe executable directory is unavailable")?
        .join(relative);
    Ok(AdapterInvocation {
        executable,
        arguments: vec![OsString::from("--version")],
        source: "bundle-relative",
    })
}

fn verify_adapter_version(adapter: &AdapterInvocation) -> Result<(), &'static str> {
    let output = Command::new(&adapter.executable)
        .args(&adapter.arguments)
        .output()
        .map_err(|_| "pinned Claude ACP adapter could not run")?;
    if !output.status.success() {
        return Err("pinned Claude ACP adapter failed");
    }
    let version = std::str::from_utf8(&output.stdout)
        .map_err(|_| "Claude ACP adapter returned non-UTF-8")?
        .trim();
    if !adapter_version_is_exact(version) {
        return Err("Claude ACP adapter version did not match the pin");
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn verify_macos_sandbox() -> Result<(), &'static str> {
    let pid = std::process::id().to_string();
    let output = Command::new("sudo")
        .args(["-n", "launchctl", "procinfo", &pid])
        .output()
        .map_err(|_| "macOS process policy could not be inspected")?;
    if !output.status.success() {
        return Err("macOS process policy inspection failed");
    }
    let stdout = std::str::from_utf8(&output.stdout)
        .map_err(|_| "macOS process policy returned non-UTF-8")?;
    if !sandbox_is_disabled(stdout) {
        return Err("macOS reports that the probe is sandboxed");
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn verify_macos_sandbox() -> Result<(), &'static str> {
    Err("the first-party Claude authority probe currently requires macOS")
}

fn verify_write_scope(directory: &Path) -> Result<(), &'static str> {
    let path = directory.join(format!(".crab-v2-authority-{}", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|_| "authority file could not be created")?;
    let result = file
        .write_all(b"crab-v2-authority\n")
        .map_err(|_| "authority file could not be written");
    drop(file);
    let removed = fs::remove_file(path).map_err(|_| "authority file could not be removed");
    result.and(removed)
}

fn verify_network() -> Result<(), &'static str> {
    let addresses = NETWORK_ENDPOINT
        .to_socket_addrs()
        .map_err(|_| "Anthropic endpoint could not be resolved")?;
    for address in addresses {
        if TcpStream::connect_timeout(&address, CONNECT_TIMEOUT).is_ok() {
            return Ok(());
        }
    }
    Err("Anthropic endpoint could not be reached")
}

fn adapter_version_is_exact(version: &str) -> bool {
    version == ADAPTER_VERSION
}

fn sandbox_is_disabled(output: &str) -> bool {
    output.lines().any(|line| line.trim() == "sandboxed = no")
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::{adapter_invocation, adapter_version_is_exact, sandbox_is_disabled};

    #[test]
    fn adapter_version_match_is_exact() {
        assert!(adapter_version_is_exact("0.70.0"));
        assert!(!adapter_version_is_exact("v0.70.0"));
        assert!(!adapter_version_is_exact("0.70.1"));
        assert!(!adapter_version_is_exact("0.70.0\nextra"));
    }

    #[test]
    fn sandbox_policy_requires_the_exact_unsandboxed_record() {
        assert!(sandbox_is_disabled("pid = 42\nsandboxed = no\n"));
        assert!(!sandbox_is_disabled("sandboxed = yes\n"));
        assert!(!sandbox_is_disabled("sandboxed = no-ish\n"));
        assert!(!sandbox_is_disabled(""));
    }

    #[test]
    fn default_adapter_stays_exactly_pinned() {
        let invocation = adapter_invocation(std::iter::empty()).expect("default invocation");
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
        assert!(adapter_invocation([OsString::from("--other")].into_iter()).is_err());
        assert!(
            adapter_invocation(
                ["--adapter-relative-to-probe", "/absolute/adapter"]
                    .into_iter()
                    .map(OsString::from)
            )
            .is_err()
        );
        let invocation = adapter_invocation(
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
