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

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) struct ProbeDefinition {
    pub(crate) probe_name: &'static str,
    pub(crate) adapter_name: &'static str,
    pub(crate) adapter_package: &'static str,
    pub(crate) adapter_version: &'static str,
    pub(crate) adapter_version_output: &'static str,
    pub(crate) network_endpoint: &'static str,
    pub(crate) require_non_root: bool,
}

pub(crate) struct AdapterInvocation {
    pub(crate) executable: PathBuf,
    pub(crate) arguments: Vec<OsString>,
    pub(crate) source: &'static str,
}

pub(crate) fn run(
    definition: &ProbeDefinition,
    arguments: impl Iterator<Item = OsString>,
) -> Result<serde_json::Value, String> {
    let adapter = adapter_invocation(definition, arguments)?;
    let uid = verify_uid(definition)?;
    verify_adapter_version(definition, &adapter)?;
    verify_macos_sandbox()?;
    verify_write_scope(&env::temp_dir())?;
    let home = env::var_os("HOME").ok_or_else(|| "HOME is unavailable".to_owned())?;
    verify_write_scope(Path::new(&home))?;
    verify_network(definition)?;

    Ok(json!({
        "sandboxDisabled": true,
        "permissionBypass": true,
        "unrestrictedFilesystem": true,
        "unrestrictedNetwork": true,
        "evidence": {
            "probe": definition.probe_name,
            "probeVersion": env!("CARGO_PKG_VERSION"),
            "adapterPackage": definition.adapter_package,
            "adapterVersion": definition.adapter_version,
            "adapterSource": adapter.source,
            "uid": uid,
            "sandbox": "launchctl:sandboxed=no",
            "filesystemScopes": ["home", "temporary-directory"],
            "networkEndpoint": definition.network_endpoint
        }
    }))
}

fn verify_uid(definition: &ProbeDefinition) -> Result<u32, String> {
    let output = Command::new("id")
        .arg("-u")
        .output()
        .map_err(|_| "id could not run".to_owned())?;
    if !output.status.success() {
        return Err("id failed".into());
    }
    let uid = std::str::from_utf8(&output.stdout)
        .map_err(|_| "id returned non-UTF-8".to_owned())?
        .trim()
        .parse::<u32>()
        .map_err(|_| "id returned an invalid uid".to_owned())?;
    if definition.require_non_root && uid == 0 {
        return Err(format!(
            "{} unrestricted mode is unavailable at EUID 0",
            definition.adapter_name
        ));
    }
    Ok(uid)
}

pub(crate) fn adapter_invocation(
    definition: &ProbeDefinition,
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<AdapterInvocation, String> {
    let Some(first) = arguments.next() else {
        return Ok(AdapterInvocation {
            executable: "npx".into(),
            arguments: ["--yes", definition.adapter_package, "--version"]
                .into_iter()
                .map(OsString::from)
                .collect(),
            source: "pinned-npx",
        });
    };
    if first != "--adapter-relative-to-probe" {
        return Err("authority probe arguments are invalid".into());
    }
    let relative = PathBuf::from(
        arguments
            .next()
            .ok_or_else(|| "authority probe adapter path is missing".to_owned())?,
    );
    if relative.is_absolute() || arguments.next().is_some() {
        return Err("authority probe adapter path must be one relative path".into());
    }
    let executable = env::current_exe()
        .map_err(|_| "authority probe executable path is unavailable".to_owned())?
        .parent()
        .ok_or_else(|| "authority probe executable directory is unavailable".to_owned())?
        .join(relative);
    Ok(AdapterInvocation {
        executable,
        arguments: vec![OsString::from("--version")],
        source: "bundle-relative",
    })
}

fn verify_adapter_version(
    definition: &ProbeDefinition,
    adapter: &AdapterInvocation,
) -> Result<(), String> {
    let output = Command::new(&adapter.executable)
        .args(&adapter.arguments)
        .output()
        .map_err(|_| {
            format!(
                "pinned {} ACP adapter could not run",
                definition.adapter_name
            )
        })?;
    if !output.status.success() {
        return Err(format!(
            "pinned {} ACP adapter failed",
            definition.adapter_name
        ));
    }
    let version = std::str::from_utf8(&output.stdout)
        .map_err(|_| format!("{} ACP adapter returned non-UTF-8", definition.adapter_name))?
        .trim();
    if version != definition.adapter_version_output {
        return Err(format!(
            "{} ACP adapter version did not match the pin",
            definition.adapter_name
        ));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn verify_macos_sandbox() -> Result<(), String> {
    let pid = std::process::id().to_string();
    let output = Command::new("sudo")
        .args(["-n", "launchctl", "procinfo", &pid])
        .output()
        .map_err(|_| "macOS process policy could not be inspected".to_owned())?;
    if !output.status.success() {
        return Err("macOS process policy inspection failed".into());
    }
    let stdout = std::str::from_utf8(&output.stdout)
        .map_err(|_| "macOS process policy returned non-UTF-8".to_owned())?;
    if !sandbox_is_disabled(stdout) {
        return Err("macOS reports that the probe is sandboxed".into());
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn verify_macos_sandbox() -> Result<(), String> {
    Err("the first-party authority probes currently require macOS".into())
}

fn verify_write_scope(directory: &Path) -> Result<(), String> {
    let path = directory.join(format!(".crab-v2-authority-{}", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|_| "authority file could not be created".to_owned())?;
    let result = file
        .write_all(b"crab-v2-authority\n")
        .map_err(|_| "authority file could not be written".to_owned());
    drop(file);
    let removed =
        fs::remove_file(path).map_err(|_| "authority file could not be removed".to_owned());
    result.and(removed)
}

fn verify_network(definition: &ProbeDefinition) -> Result<(), String> {
    let addresses = definition
        .network_endpoint
        .to_socket_addrs()
        .map_err(|_| format!("{} endpoint could not be resolved", definition.adapter_name))?;
    for address in addresses {
        if TcpStream::connect_timeout(&address, CONNECT_TIMEOUT).is_ok() {
            return Ok(());
        }
    }
    Err(format!(
        "{} endpoint could not be reached",
        definition.adapter_name
    ))
}

pub(crate) fn sandbox_is_disabled(output: &str) -> bool {
    output.lines().any(|line| line.trim() == "sandboxed = no")
}
