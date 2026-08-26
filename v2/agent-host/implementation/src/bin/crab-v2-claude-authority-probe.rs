#![forbid(unsafe_code)]

use std::{
    env,
    fs::{self, OpenOptions},
    io::Write,
    net::{TcpStream, ToSocketAddrs},
    path::Path,
    process::Command,
    time::Duration,
};

use serde_json::json;
use uuid::Uuid;

const ADAPTER_PACKAGE: &str = "@agentclientprotocol/claude-agent-acp@0.70.0";
const ADAPTER_VERSION: &str = "0.70.0";
const NETWORK_ENDPOINT: &str = "api.anthropic.com:443";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

fn main() {
    match verify() {
        Ok(report) => println!("{report}"),
        Err(error) => {
            eprintln!("claude authority probe failed: {error}");
            std::process::exit(1);
        }
    }
}

fn verify() -> Result<serde_json::Value, &'static str> {
    let uid = verify_non_root_uid()?;
    verify_adapter_version()?;
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

fn verify_adapter_version() -> Result<(), &'static str> {
    let output = Command::new("npx")
        .args(["--yes", ADAPTER_PACKAGE, "--version"])
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
    use super::{adapter_version_is_exact, sandbox_is_disabled};

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
}
