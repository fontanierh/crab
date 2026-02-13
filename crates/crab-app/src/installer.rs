use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;

use crab_core::{
    evaluate_state_schema_compatibility, StateSchemaCompatibilityReport,
    StateSchemaCompatibilityStatus,
};

const USAGE: &str = "Usage:
  crabctl install --target <macos|linux> [--dry-run] [--root-prefix <path>] [--release-id <id>] [--crabd-bin <path>] [--connector-bin <path>] [--workspace-root <path>] [--service-user <name>] [--service-group <name>]
  crabctl upgrade --target <macos|linux> --release-id <id> [--dry-run] [--root-prefix <path>] [--crabd-bin <path>] [--connector-bin <path>] [--workspace-root <path>] [--service-user <name>] [--service-group <name>]
  crabctl rollback --target <macos|linux> [--dry-run] [--root-prefix <path>] [--workspace-root <path>] [--service-user <name>] [--service-group <name>]
  crabctl doctor --target <macos|linux> [--root-prefix <path>] [--workspace-root <path>]

Flags:
  --target          required install target (`macos` or `linux`)
  --dry-run         render deterministic plan with no host mutations
  --root-prefix     optional root for install/provision paths (default: /)
  --release-id      release identifier token (required for upgrade, default install: bootstrap)
  --crabd-bin       source path for crabd binary (default: ./target/release/crabd)
  --connector-bin   source path for connector binary (default: ./target/release/crab-discord-connector)
  --workspace-root  runtime workspace root inside target host layout (default: /var/lib/crab/workspace)
  --service-user    service account user token (default: crab)
  --service-group   service account group token (default: crab)
  --help            show help";

const DEFAULT_ROOT_PREFIX: &str = "/";
const DEFAULT_RELEASE_ID: &str = "bootstrap";
const DEFAULT_WORKSPACE_ROOT: &str = "/var/lib/crab/workspace";
const DEFAULT_SERVICE_USER: &str = "crab";
const DEFAULT_SERVICE_GROUP: &str = "crab";
const DEFAULT_CRABD_BIN: &str = "./target/release/crabd";
const DEFAULT_CONNECTOR_BIN: &str = "./target/release/crab-discord-connector";
const EXIT_CODE_USAGE: i32 = 2;
const EXIT_CODE_RUNTIME_ERROR: i32 = 1;
const EXIT_CODE_UPGRADE_BLOCKED_STATE_COMPATIBILITY: i32 = 3;
const VALUE_FLAGS: [&str; 8] = [
    "--target",
    "--root-prefix",
    "--release-id",
    "--crabd-bin",
    "--connector-bin",
    "--workspace-root",
    "--service-user",
    "--service-group",
];
const ENV_FILE_TEMPLATE: &str = "# Crab runtime environment
# Fill required values before enabling service.
CRAB_DISCORD_TOKEN=
CRAB_BOT_USER_ID=
CRAB_WORKSPACE_ROOT=/var/lib/crab/workspace
CRAB_MAX_CONCURRENT_LANES=4
CRAB_COMPACTION_TOKEN_THRESHOLD=120000
CRAB_INACTIVITY_TIMEOUT_SECS=1800
CRAB_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS=90
CRAB_HEARTBEAT_INTERVAL_SECS=10
CRAB_RUN_STALL_TIMEOUT_SECS=600
CRAB_BACKEND_STALL_TIMEOUT_SECS=600
CRAB_DISPATCHER_STALL_TIMEOUT_SECS=20
CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED=false
CRAB_WORKSPACE_GIT_BRANCH=main
CRAB_WORKSPACE_GIT_COMMIT_NAME=Crab Workspace Bot
CRAB_WORKSPACE_GIT_COMMIT_EMAIL=crab@localhost
CRAB_WORKSPACE_GIT_PUSH_POLICY=on-commit
";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstallerCommandKind {
    Install,
    Upgrade,
    Rollback,
    Doctor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstallTarget {
    Macos,
    Linux,
}

impl InstallTarget {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "macos" => Some(Self::Macos),
            "linux" => Some(Self::Linux),
            _ => None,
        }
    }

    fn as_token(self) -> &'static str {
        match self {
            Self::Macos => "macos",
            Self::Linux => "linux",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InstallerOptions {
    command: InstallerCommandKind,
    target: InstallTarget,
    dry_run: bool,
    root_prefix: PathBuf,
    release_id: String,
    crabd_bin: PathBuf,
    connector_bin: PathBuf,
    workspace_root: String,
    service_user: String,
    service_group: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedFlags {
    values: BTreeMap<String, String>,
    switches: BTreeSet<String>,
}

impl ParsedFlags {
    fn new() -> Self {
        Self {
            values: BTreeMap::new(),
            switches: BTreeSet::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InstallLayout {
    root_prefix: PathBuf,
    opt_root: PathBuf,
    bin_dir: PathBuf,
    releases_dir: PathBuf,
    current_link: PathBuf,
    etc_crab_dir: PathBuf,
    env_file: PathBuf,
    var_lib_root: PathBuf,
    install_state_dir: PathBuf,
    previous_release_file: PathBuf,
    workspace_root: PathBuf,
    log_dir: PathBuf,
    runtime_log: PathBuf,
    service_file: PathBuf,
}

impl InstallLayout {
    fn from_options(options: &InstallerOptions) -> Self {
        let root_prefix = normalize_root_prefix(&options.root_prefix);
        let opt_root = rooted(&root_prefix, "/opt/crab");
        let bin_dir = opt_root.join("bin");
        let releases_dir = opt_root.join("releases");
        let current_link = opt_root.join("current");
        let etc_crab_dir = rooted(&root_prefix, "/etc/crab");
        let env_file = etc_crab_dir.join("crab.env");
        let var_lib_root = rooted(&root_prefix, "/var/lib/crab");
        let install_state_dir = var_lib_root.join("install-state");
        let previous_release_file = install_state_dir.join("previous_release.txt");
        let workspace_root = rooted(&root_prefix, &options.workspace_root);
        let log_dir = rooted(&root_prefix, "/var/log/crab");
        let runtime_log = log_dir.join("runtime.log");
        let service_file = match options.target {
            InstallTarget::Linux => rooted(&root_prefix, "/etc/systemd/system/crab.service"),
            InstallTarget::Macos => rooted(
                &root_prefix,
                "/Library/LaunchDaemons/com.crab.runtime.plist",
            ),
        };

        Self {
            root_prefix,
            opt_root,
            bin_dir,
            releases_dir,
            current_link,
            etc_crab_dir,
            env_file,
            var_lib_root,
            install_state_dir,
            previous_release_file,
            workspace_root,
            log_dir,
            runtime_log,
            service_file,
        }
    }

    fn release_dir(&self, release_id: &str) -> PathBuf {
        self.releases_dir.join(release_id)
    }

    fn release_bin_dir(&self, release_id: &str) -> PathBuf {
        self.release_dir(release_id).join("bin")
    }

    fn shim_crabd_link(&self) -> PathBuf {
        self.bin_dir.join("crabd")
    }

    fn shim_connector_link(&self) -> PathBuf {
        self.bin_dir.join("crab-discord-connector")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CommandInvocation {
    program: String,
    args: Vec<String>,
}

impl CommandInvocation {
    fn display(&self) -> String {
        if self.args.is_empty() {
            return self.program.clone();
        }
        format!("{} {}", self.program, self.args.join(" "))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActionLog {
    lines: Vec<String>,
}

impl ActionLog {
    fn new() -> Self {
        Self { lines: Vec::new() }
    }

    fn push(&mut self, line: impl Into<String>) {
        self.lines.push(line.into());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DoctorCheck {
    name: String,
    ok: bool,
    detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StateCompatibilityPreflight {
    report: StateSchemaCompatibilityReport,
    blocked: bool,
    reason: String,
    remediation: Vec<String>,
}

trait CommandRuntime {
    fn command_exists(&mut self, name: &str) -> Result<bool, String>;
    fn command_version(&mut self, name: &str, args: &[&str]) -> Result<String, String>;
    fn run_command(&mut self, invocation: &CommandInvocation) -> Result<(), String>;
}

#[derive(Debug, Clone, Default)]
struct SystemCommandRuntime;

impl CommandRuntime for SystemCommandRuntime {
    fn command_exists(&mut self, name: &str) -> Result<bool, String> {
        if name.contains('/') {
            let candidate = Path::new(name);
            return Ok(candidate.is_file());
        }

        let path_value = std::env::var_os("PATH").unwrap_or_default();
        for segment in std::env::split_paths(&path_value) {
            let candidate = segment.join(name);
            if candidate.is_file() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn command_version(&mut self, name: &str, args: &[&str]) -> Result<String, String> {
        let output = Command::new(name)
            .args(args)
            .output()
            .map_err(|error| format!("failed to run `{name}` for version: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "`{name}` version command failed with status {}",
                output.status
            ));
        }

        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !stdout.is_empty() {
            return Ok(stdout);
        }
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if !stderr.is_empty() {
            return Ok(stderr);
        }
        Err(format!("`{name}` produced empty version output"))
    }

    fn run_command(&mut self, invocation: &CommandInvocation) -> Result<(), String> {
        let output = Command::new(&invocation.program)
            .args(&invocation.args)
            .output()
            .map_err(|error| format!("failed to execute `{}`: {error}", invocation.display()))?;
        if output.status.success() {
            return Ok(());
        }
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        Err(format!(
            "`{}` failed with status {} (stdout={:?}, stderr={:?})",
            invocation.display(),
            output.status,
            stdout,
            stderr
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnsureState {
    Created,
    Updated,
    Unchanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PrerequisiteSpec {
    command: &'static str,
    version_args: &'static [&'static str],
    install_macos: &'static [&'static str],
    install_linux: &'static [&'static str],
}

const PREREQUISITES: [PrerequisiteSpec; 8] = [
    PrerequisiteSpec {
        command: "tmux",
        version_args: &["-V"],
        install_macos: &["brew", "install", "tmux"],
        install_linux: &["apt-get", "install", "-y", "tmux"],
    },
    PrerequisiteSpec {
        command: "git",
        version_args: &["--version"],
        install_macos: &["brew", "install", "git"],
        install_linux: &["apt-get", "install", "-y", "git"],
    },
    PrerequisiteSpec {
        command: "gh",
        version_args: &["--version"],
        install_macos: &["brew", "install", "gh"],
        install_linux: &["apt-get", "install", "-y", "gh"],
    },
    PrerequisiteSpec {
        command: "jq",
        version_args: &["--version"],
        install_macos: &["brew", "install", "jq"],
        install_linux: &["apt-get", "install", "-y", "jq"],
    },
    PrerequisiteSpec {
        command: "rg",
        version_args: &["--version"],
        install_macos: &["brew", "install", "ripgrep"],
        install_linux: &["apt-get", "install", "-y", "ripgrep"],
    },
    PrerequisiteSpec {
        command: "node",
        version_args: &["--version"],
        install_macos: &["brew", "install", "node"],
        install_linux: &["apt-get", "install", "-y", "nodejs", "npm"],
    },
    PrerequisiteSpec {
        command: "cargo",
        version_args: &["--version"],
        install_macos: &["brew", "install", "rust"],
        install_linux: &["apt-get", "install", "-y", "cargo", "rustc"],
    },
    PrerequisiteSpec {
        command: "cargo-llvm-cov",
        version_args: &["--version"],
        install_macos: &[
            "cargo",
            "install",
            "cargo-llvm-cov",
            "--version",
            "0.6.21",
            "--locked",
        ],
        install_linux: &[
            "cargo",
            "install",
            "cargo-llvm-cov",
            "--version",
            "0.6.21",
            "--locked",
        ],
    },
];

pub fn run_installer_cli(args: &[String], stdout: &mut dyn Write, stderr: &mut dyn Write) -> i32 {
    let argv = args.to_vec();
    if argv.len() <= 1 || is_help_request(argv.get(1..).unwrap_or(&[])) {
        let _ = writeln!(stdout, "{}", USAGE);
        return 0;
    }

    let parsed = parse_options(&argv).and_then(validate_options);
    let options = match parsed {
        Ok(options) => options,
        Err(message) => {
            let _ = writeln!(stderr, "error: {}", message);
            let _ = writeln!(stderr, "{}", USAGE);
            return EXIT_CODE_USAGE;
        }
    };

    let mut runtime = SystemCommandRuntime;
    match run_with_runtime(&options, &mut runtime) {
        Ok((lines, exit_code)) => {
            for line in lines {
                let _ = writeln!(stdout, "{line}");
            }
            exit_code
        }
        Err(message) => {
            let _ = writeln!(stderr, "error: {message}");
            EXIT_CODE_RUNTIME_ERROR
        }
    }
}

fn run_with_runtime(
    options: &InstallerOptions,
    runtime: &mut dyn CommandRuntime,
) -> Result<(Vec<String>, i32), String> {
    match options.command {
        InstallerCommandKind::Install => {
            let mut log = ActionLog::new();
            let layout = InstallLayout::from_options(options);
            ensure_prerequisites(options, runtime, &mut log)?;
            ensure_runtime_layout(options, &layout, &mut log)?;
            stage_release(options, &layout, &mut log)?;
            install_service_definition(options, &layout, &mut log)?;
            Ok((log.lines, 0))
        }
        InstallerCommandKind::Upgrade => {
            let mut log = ActionLog::new();
            let layout = InstallLayout::from_options(options);
            let preflight = evaluate_state_compatibility_preflight(options, &layout)?;
            if preflight.blocked {
                log.push("upgrade preflight: blocked".to_string());
                log.push(format!("upgrade preflight reason: {}", preflight.reason));
                for line in preflight.remediation {
                    log.push(format!("upgrade preflight remediation: {line}"));
                }
                return Ok((log.lines, EXIT_CODE_UPGRADE_BLOCKED_STATE_COMPATIBILITY));
            }
            log.push(format!(
                "upgrade preflight: compatible (state_version={}, supported_range={}..={})",
                preflight.report.detected_version,
                preflight.report.supported_min_version,
                preflight.report.supported_max_version
            ));
            ensure_prerequisites(options, runtime, &mut log)?;
            ensure_runtime_layout(options, &layout, &mut log)?;
            preserve_previous_release(&layout, options.dry_run, &mut log)?;
            stage_release(options, &layout, &mut log)?;
            install_service_definition(options, &layout, &mut log)?;
            Ok((log.lines, 0))
        }
        InstallerCommandKind::Rollback => {
            let mut log = ActionLog::new();
            let layout = InstallLayout::from_options(options);
            ensure_runtime_layout(options, &layout, &mut log)?;
            rollback_release(options, &layout, &mut log)?;
            install_service_definition(options, &layout, &mut log)?;
            Ok((log.lines, 0))
        }
        InstallerCommandKind::Doctor => {
            let layout = InstallLayout::from_options(options);
            let checks = run_doctor_checks(options, &layout, runtime);
            let mut lines = Vec::new();
            lines.push(format!(
                "doctor target={} root_prefix={}",
                options.target.as_token(),
                display_path(&layout.root_prefix)
            ));
            let mut healthy = true;
            for check in &checks {
                let status = if check.ok { "PASS" } else { "FAIL" };
                lines.push(format!("{status} {} :: {}", check.name, check.detail));
                healthy &= check.ok;
            }
            match evaluate_state_compatibility_preflight(options, &layout) {
                Ok(preflight) => {
                    let status = if preflight.blocked { "FAIL" } else { "PASS" };
                    lines.push(format!(
                        "{status} state_schema_compatibility :: {} (state_version={}, supported_range={}..={})",
                        preflight.reason,
                        preflight.report.detected_version,
                        preflight.report.supported_min_version,
                        preflight.report.supported_max_version
                    ));
                    if preflight.blocked {
                        healthy = false;
                        for line in preflight.remediation {
                            lines.push(format!("state_schema_compatibility remediation: {line}"));
                        }
                    }
                }
                Err(error) => {
                    healthy = false;
                    lines.push(format!(
                        "FAIL state_schema_compatibility :: failed to evaluate preflight ({error})"
                    ));
                }
            }
            if healthy {
                lines.push("doctor summary: healthy".to_string());
                Ok((lines, 0))
            } else {
                lines.push("doctor summary: unhealthy".to_string());
                Ok((lines, 1))
            }
        }
    }
}

fn ensure_prerequisites(
    options: &InstallerOptions,
    runtime: &mut dyn CommandRuntime,
    log: &mut ActionLog,
) -> Result<(), String> {
    for spec in PREREQUISITES {
        let exists = runtime.command_exists(spec.command)?;
        if exists {
            let version = runtime
                .command_version(spec.command, spec.version_args)
                .unwrap_or_else(|_| "version-unavailable".to_string());
            log.push(format!("prerequisite ok {} ({version})", spec.command));
            continue;
        }

        let invocation = install_invocation(options.target, spec)?;
        if options.dry_run {
            log.push(format!(
                "plan prerequisite install {} -> {}",
                spec.command,
                invocation.display()
            ));
            continue;
        }

        log.push(format!(
            "install prerequisite {} via {}",
            spec.command,
            invocation.display()
        ));
        runtime.run_command(&invocation)?;
        if !runtime.command_exists(spec.command)? {
            return Err(format!(
                "prerequisite {} still missing after running {}",
                spec.command,
                invocation.display()
            ));
        }
        let version = runtime
            .command_version(spec.command, spec.version_args)
            .unwrap_or_else(|_| "version-unavailable".to_string());
        log.push(format!(
            "prerequisite installed {} ({version})",
            spec.command
        ));
    }

    Ok(())
}

fn ensure_runtime_layout(
    options: &InstallerOptions,
    layout: &InstallLayout,
    log: &mut ActionLog,
) -> Result<(), String> {
    for path in [
        &layout.opt_root,
        &layout.bin_dir,
        &layout.releases_dir,
        &layout.etc_crab_dir,
        &layout.var_lib_root,
        &layout.install_state_dir,
        &layout.workspace_root,
        &layout.log_dir,
    ] {
        let status = ensure_directory(path, options.dry_run)?;
        log_ensure_status(log, "directory", path, status);
    }

    let env_status = ensure_file_if_missing(&layout.env_file, ENV_FILE_TEMPLATE, options.dry_run)?;
    log_ensure_status(log, "env_file", &layout.env_file, env_status);

    Ok(())
}

fn stage_release(
    options: &InstallerOptions,
    layout: &InstallLayout,
    log: &mut ActionLog,
) -> Result<(), String> {
    let release_dir = layout.release_dir(&options.release_id);
    let release_bin_dir = layout.release_bin_dir(&options.release_id);

    let release_dir_status = ensure_directory(&release_dir, options.dry_run)?;
    log_ensure_status(log, "release_dir", &release_dir, release_dir_status);
    let release_bin_status = ensure_directory(&release_bin_dir, options.dry_run)?;
    log_ensure_status(log, "release_bin_dir", &release_bin_dir, release_bin_status);

    let crabd_target = release_bin_dir.join("crabd");
    let connector_target = release_bin_dir.join("crab-discord-connector");
    let crabd_copy_status = ensure_binary_copy(
        &options.crabd_bin,
        &crabd_target,
        options.dry_run,
        "crabd",
        log,
    )?;
    log_ensure_status(log, "binary", &crabd_target, crabd_copy_status);

    let connector_copy_status = ensure_binary_copy(
        &options.connector_bin,
        &connector_target,
        options.dry_run,
        "crab-discord-connector",
        log,
    )?;
    log_ensure_status(log, "binary", &connector_target, connector_copy_status);

    let current_status = ensure_symlink(&layout.current_link, &release_dir, options.dry_run)?;
    log_ensure_status(log, "symlink", &layout.current_link, current_status);

    let shim_crabd_target = layout.current_link.join("bin").join("crabd");
    let shim_connector_target = layout
        .current_link
        .join("bin")
        .join("crab-discord-connector");
    let shim_crabd_status = ensure_symlink(
        &layout.shim_crabd_link(),
        &shim_crabd_target,
        options.dry_run,
    )?;
    log_ensure_status(log, "symlink", &layout.shim_crabd_link(), shim_crabd_status);
    let shim_connector_status = ensure_symlink(
        &layout.shim_connector_link(),
        &shim_connector_target,
        options.dry_run,
    )?;
    log_ensure_status(
        log,
        "symlink",
        &layout.shim_connector_link(),
        shim_connector_status,
    );

    Ok(())
}

fn preserve_previous_release(
    layout: &InstallLayout,
    dry_run: bool,
    log: &mut ActionLog,
) -> Result<(), String> {
    let Some(current_target) = read_symlink_target(&layout.current_link)? else {
        let status = ensure_file_with_content(&layout.previous_release_file, "", dry_run)?;
        log_ensure_status(
            log,
            "state_previous_release_reset",
            &layout.previous_release_file,
            status,
        );
        return Ok(());
    };

    let value = format!("{}\n", display_path(&current_target));
    let status = ensure_file_with_content(&layout.previous_release_file, &value, dry_run)?;
    log_ensure_status(
        log,
        "state_previous_release",
        &layout.previous_release_file,
        status,
    );
    Ok(())
}

fn rollback_release(
    options: &InstallerOptions,
    layout: &InstallLayout,
    log: &mut ActionLog,
) -> Result<(), String> {
    let previous_release = read_previous_release_target(layout)?;
    if !previous_release.is_dir() {
        return Err(format!(
            "cannot rollback: previous release target {} does not exist",
            display_path(&previous_release)
        ));
    }

    let previous_current = read_symlink_target(&layout.current_link)?;
    let current_status = ensure_symlink(&layout.current_link, &previous_release, options.dry_run)?;
    log_ensure_status(
        log,
        "rollback_current",
        &layout.current_link,
        current_status,
    );

    if let Some(old_current) = previous_current {
        let value = format!("{}\n", display_path(&old_current));
        let status =
            ensure_file_with_content(&layout.previous_release_file, &value, options.dry_run)?;
        log_ensure_status(
            log,
            "rollback_previous_swap",
            &layout.previous_release_file,
            status,
        );
    }

    let shim_crabd_target = layout.current_link.join("bin").join("crabd");
    let shim_connector_target = layout
        .current_link
        .join("bin")
        .join("crab-discord-connector");
    let shim_crabd_status = ensure_symlink(
        &layout.shim_crabd_link(),
        &shim_crabd_target,
        options.dry_run,
    )?;
    log_ensure_status(log, "symlink", &layout.shim_crabd_link(), shim_crabd_status);
    let shim_connector_status = ensure_symlink(
        &layout.shim_connector_link(),
        &shim_connector_target,
        options.dry_run,
    )?;
    log_ensure_status(
        log,
        "symlink",
        &layout.shim_connector_link(),
        shim_connector_status,
    );

    Ok(())
}

fn install_service_definition(
    options: &InstallerOptions,
    layout: &InstallLayout,
    log: &mut ActionLog,
) -> Result<(), String> {
    let content = match options.target {
        InstallTarget::Linux => render_systemd_service(layout, options),
        InstallTarget::Macos => render_launchd_service(layout, options),
    };
    let service_status = ensure_file_with_content(&layout.service_file, &content, options.dry_run)?;
    log_ensure_status(log, "service_file", &layout.service_file, service_status);

    #[cfg(unix)]
    {
        let env_mode_status = ensure_mode_if_exists(&layout.env_file, 0o600, options.dry_run)?;
        log_ensure_status(log, "env_mode", &layout.env_file, env_mode_status);
    }

    Ok(())
}

fn run_doctor_checks(
    options: &InstallerOptions,
    layout: &InstallLayout,
    runtime: &mut dyn CommandRuntime,
) -> Vec<DoctorCheck> {
    let mut checks = Vec::new();
    for path in [
        ("opt root", &layout.opt_root),
        ("bin dir", &layout.bin_dir),
        ("releases dir", &layout.releases_dir),
        ("etc dir", &layout.etc_crab_dir),
        ("workspace dir", &layout.workspace_root),
        ("log dir", &layout.log_dir),
    ] {
        let ok = path.1.is_dir();
        checks.push(DoctorCheck {
            name: format!("path:{}", path.0),
            ok,
            detail: display_path(path.1),
        });
    }

    checks.push(DoctorCheck {
        name: "env_file".to_string(),
        ok: layout.env_file.is_file(),
        detail: display_path(&layout.env_file),
    });

    checks.push(DoctorCheck {
        name: "service_file".to_string(),
        ok: layout.service_file.is_file(),
        detail: display_path(&layout.service_file),
    });

    let current_target = read_symlink_target(&layout.current_link)
        .ok()
        .flatten()
        .filter(|target| target.exists());
    checks.push(DoctorCheck {
        name: "current_link".to_string(),
        ok: current_target.is_some(),
        detail: format!(
            "{} -> {}",
            display_path(&layout.current_link),
            current_target
                .as_ref()
                .map_or_else(|| "(missing)".to_string(), |target| display_path(target))
        ),
    });

    let shim_checks = [
        ("shim_crabd", layout.shim_crabd_link()),
        ("shim_connector", layout.shim_connector_link()),
    ];
    for (name, shim_path) in shim_checks {
        let shim_target = read_symlink_target(&shim_path).ok().flatten();
        let ok = shim_target.as_ref().is_some_and(|target| target.exists());
        checks.push(DoctorCheck {
            name: name.to_string(),
            ok,
            detail: format!(
                "{} -> {}",
                display_path(&shim_path),
                shim_target
                    .as_ref()
                    .map_or_else(|| "(missing)".to_string(), |target| display_path(target))
            ),
        });
    }

    checks.push(DoctorCheck {
        name: "runtime_log_parent".to_string(),
        ok: layout
            .runtime_log
            .parent()
            .is_some_and(|parent| parent.is_dir()),
        detail: display_path(&layout.runtime_log),
    });

    for spec in PREREQUISITES {
        let exists = runtime.command_exists(spec.command).unwrap_or(false);
        let detail = if exists {
            runtime
                .command_version(spec.command, spec.version_args)
                .unwrap_or_else(|_| "version-unavailable".to_string())
        } else {
            "missing".to_string()
        };
        checks.push(DoctorCheck {
            name: format!("prerequisite:{}", spec.command),
            ok: exists,
            detail,
        });
    }

    #[cfg(unix)]
    {
        let mode_ok = fs::metadata(&layout.env_file)
            .ok()
            .map(|metadata| metadata.permissions().mode() & 0o777 == 0o600)
            .unwrap_or(false);
        checks.push(DoctorCheck {
            name: "env_mode_0600".to_string(),
            ok: mode_ok,
            detail: display_path(&layout.env_file),
        });
    }

    if options.target == InstallTarget::Linux {
        checks.push(DoctorCheck {
            name: "service_target".to_string(),
            ok: layout
                .service_file
                .ends_with(Path::new("etc/systemd/system/crab.service")),
            detail: display_path(&layout.service_file),
        });
    } else {
        checks.push(DoctorCheck {
            name: "service_target".to_string(),
            ok: layout
                .service_file
                .ends_with(Path::new("Library/LaunchDaemons/com.crab.runtime.plist")),
            detail: display_path(&layout.service_file),
        });
    }

    checks
}

fn evaluate_state_compatibility_preflight(
    options: &InstallerOptions,
    layout: &InstallLayout,
) -> Result<StateCompatibilityPreflight, String> {
    let state_root = compatibility_state_root(layout);
    let report = evaluate_state_schema_compatibility(&state_root).map_err(|error| {
        format!(
            "state compatibility preflight failed at {}: {error}",
            display_path(&state_root)
        )
    })?;

    let mut remediation = Vec::new();
    let (blocked, reason) = match report.status {
        StateSchemaCompatibilityStatus::Compatible => {
            (false, "compatible with current binary".to_string())
        }
        StateSchemaCompatibilityStatus::UnsupportedTooOld => {
            remediation.push(format!(
                "upgrade through a release that supports state schema {} and rerun upgrade",
                report.detected_version
            ));
            remediation.push(render_crabctl_upgrade_command_hint(
                options,
                "<intermediate-release-id>",
            ));
            remediation.push(render_crabctl_doctor_command_hint(options));
            (
                true,
                format!(
                    "state schema version {} is below minimum supported {}",
                    report.detected_version, report.supported_min_version
                ),
            )
        }
        StateSchemaCompatibilityStatus::UnsupportedTooNew => {
            remediation.push(
                "roll back to the previous release or deploy a newer Crab binary that supports this state schema"
                    .to_string(),
            );
            remediation.push(render_crabctl_rollback_command_hint(options));
            remediation.push(render_crabctl_doctor_command_hint(options));
            (
                true,
                format!(
                    "state schema version {} is newer than maximum supported {}",
                    report.detected_version, report.supported_max_version
                ),
            )
        }
    };

    Ok(StateCompatibilityPreflight {
        report,
        blocked,
        reason,
        remediation,
    })
}

fn compatibility_state_root(layout: &InstallLayout) -> PathBuf {
    layout.workspace_root.join("state")
}

fn render_crabctl_upgrade_command_hint(options: &InstallerOptions, release_id: &str) -> String {
    let mut command = format!(
        "crabctl upgrade --target {} --release-id {}",
        options.target.as_token(),
        release_id
    );
    if options.root_prefix != Path::new(DEFAULT_ROOT_PREFIX) {
        command.push_str(&format!(
            " --root-prefix {}",
            display_path(&options.root_prefix)
        ));
    }
    if options.workspace_root != DEFAULT_WORKSPACE_ROOT {
        command.push_str(&format!(" --workspace-root {}", options.workspace_root));
    }
    command
}

fn render_crabctl_rollback_command_hint(options: &InstallerOptions) -> String {
    let mut command = format!("crabctl rollback --target {}", options.target.as_token());
    if options.root_prefix != Path::new(DEFAULT_ROOT_PREFIX) {
        command.push_str(&format!(
            " --root-prefix {}",
            display_path(&options.root_prefix)
        ));
    }
    if options.workspace_root != DEFAULT_WORKSPACE_ROOT {
        command.push_str(&format!(" --workspace-root {}", options.workspace_root));
    }
    command
}

fn render_crabctl_doctor_command_hint(options: &InstallerOptions) -> String {
    let mut command = format!("crabctl doctor --target {}", options.target.as_token());
    if options.root_prefix != Path::new(DEFAULT_ROOT_PREFIX) {
        command.push_str(&format!(
            " --root-prefix {}",
            display_path(&options.root_prefix)
        ));
    }
    if options.workspace_root != DEFAULT_WORKSPACE_ROOT {
        command.push_str(&format!(" --workspace-root {}", options.workspace_root));
    }
    command
}

fn ensure_binary_copy(
    source: &Path,
    destination: &Path,
    dry_run: bool,
    binary_name: &str,
    log: &mut ActionLog,
) -> Result<EnsureState, String> {
    if !source.is_file() {
        if dry_run {
            log.push(format!(
                "plan binary {} source missing: {}",
                binary_name,
                display_path(source)
            ));
            return Ok(EnsureState::Unchanged);
        }
        return Err(format!(
            "missing {} binary source at {}; pass --{}-bin with a valid path",
            binary_name,
            display_path(source),
            if binary_name == "crabd" {
                "crabd"
            } else {
                "connector"
            }
        ));
    }

    let parent = destination.parent().ok_or_else(|| {
        format!(
            "destination {} has no parent directory",
            display_path(destination)
        )
    })?;
    if !parent.is_dir() {
        return Err(format!(
            "destination parent {} is not a directory",
            display_path(parent)
        ));
    }

    if dry_run {
        return Ok(if destination.exists() {
            EnsureState::Updated
        } else {
            EnsureState::Created
        });
    }

    let existed_before = destination.exists();
    if let Err(error) = fs::copy(source, destination) {
        return Err(format!(
            "failed to copy {binary_name} from {} to {}: {error}",
            display_path(source),
            display_path(destination)
        ));
    }
    #[cfg(unix)]
    {
        let permissions = fs::Permissions::from_mode(0o755);
        let _ = fs::set_permissions(destination, permissions);
    }

    Ok(if existed_before {
        EnsureState::Updated
    } else {
        EnsureState::Created
    })
}

fn ensure_directory(path: &Path, dry_run: bool) -> Result<EnsureState, String> {
    if path.is_dir() {
        return Ok(EnsureState::Unchanged);
    }
    if path.exists() {
        return Err(format!(
            "path conflict: {} exists and is not a directory (remove or move it)",
            display_path(path)
        ));
    }

    if dry_run {
        return Ok(EnsureState::Created);
    }

    fs::create_dir_all(path).map_err(|error| {
        format!(
            "failed to create directory {}: {}",
            display_path(path),
            error
        )
    })?;
    Ok(EnsureState::Created)
}

fn ensure_file_if_missing(
    path: &Path,
    content: &str,
    dry_run: bool,
) -> Result<EnsureState, String> {
    if path.is_file() {
        return Ok(EnsureState::Unchanged);
    }
    if path.exists() {
        return Err(format!(
            "path conflict: {} exists and is not a file",
            display_path(path)
        ));
    }
    if dry_run {
        return Ok(EnsureState::Created);
    }
    let parent = path
        .parent()
        .ok_or_else(|| format!("file {} has no parent directory", display_path(path)))?;
    fs::create_dir_all(parent).map_err(|error| {
        format!(
            "failed to create parent directory {}: {}",
            display_path(parent),
            error
        )
    })?;
    fs::write(path, content)
        .map_err(|error| format!("failed to write file {}: {}", display_path(path), error))?;
    Ok(EnsureState::Created)
}

fn ensure_file_with_content(
    path: &Path,
    content: &str,
    dry_run: bool,
) -> Result<EnsureState, String> {
    if path.exists() && !path.is_file() {
        return Err(format!(
            "path conflict: {} exists and is not a file",
            display_path(path)
        ));
    }
    if path.is_file() {
        let existing = fs::read_to_string(path).map_err(|error| {
            format!(
                "failed to read existing file {}: {error}",
                display_path(path)
            )
        })?;
        if existing == content {
            return Ok(EnsureState::Unchanged);
        }
        if dry_run {
            return Ok(EnsureState::Updated);
        }
        fs::write(path, content)
            .map_err(|error| format!("failed to update file {}: {}", display_path(path), error))?;
        return Ok(EnsureState::Updated);
    }

    if dry_run {
        return Ok(EnsureState::Created);
    }
    let parent = path
        .parent()
        .ok_or_else(|| format!("file {} has no parent directory", display_path(path)))?;
    fs::create_dir_all(parent).map_err(|error| {
        format!(
            "failed to create parent directory {}: {}",
            display_path(parent),
            error
        )
    })?;
    fs::write(path, content)
        .map_err(|error| format!("failed to write file {}: {}", display_path(path), error))?;
    Ok(EnsureState::Created)
}

fn ensure_symlink(path: &Path, target: &Path, dry_run: bool) -> Result<EnsureState, String> {
    if path.exists() {
        let metadata = fs::symlink_metadata(path).map_err(|error| {
            format!(
                "failed to stat symlink path {}: {error}",
                display_path(path)
            )
        })?;
        if metadata.file_type().is_symlink() {
            let existing_target = fs::read_link(path).map_err(|error| {
                format!(
                    "failed to read existing symlink {}: {error}",
                    display_path(path)
                )
            })?;
            if existing_target == target {
                return Ok(EnsureState::Unchanged);
            }
            if dry_run {
                return Ok(EnsureState::Updated);
            }
            fs::remove_file(path).map_err(|error| {
                format!("failed to remove symlink {}: {error}", display_path(path))
            })?;
            create_symlink(target, path)?;
            return Ok(EnsureState::Updated);
        }
        return Err(format!(
            "path conflict: {} exists and is not a symlink",
            display_path(path)
        ));
    }

    if dry_run {
        return Ok(EnsureState::Created);
    }

    let parent = path
        .parent()
        .ok_or_else(|| format!("symlink {} has no parent directory", display_path(path)))?;
    fs::create_dir_all(parent).map_err(|error| {
        format!(
            "failed to create symlink parent directory {}: {}",
            display_path(parent),
            error
        )
    })?;
    create_symlink(target, path)?;
    Ok(EnsureState::Created)
}

fn read_previous_release_target(layout: &InstallLayout) -> Result<PathBuf, String> {
    let raw = fs::read_to_string(&layout.previous_release_file).map_err(|error| {
        format!(
            "cannot rollback: missing previous release state at {} ({})",
            display_path(&layout.previous_release_file),
            error
        )
    })?;
    let normalized = raw.trim();
    if normalized.is_empty() {
        return Err(format!(
            "cannot rollback: previous release state {} is empty",
            display_path(&layout.previous_release_file)
        ));
    }
    Ok(PathBuf::from(normalized))
}

fn read_symlink_target(path: &Path) -> Result<Option<PathBuf>, String> {
    if !path.exists() {
        return Ok(None);
    }
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("failed to stat {}: {}", display_path(path), error))?;
    if !metadata.file_type().is_symlink() {
        return Err(format!(
            "path conflict: {} exists and is not a symlink",
            display_path(path)
        ));
    }
    let target = fs::read_link(path)
        .map_err(|error| format!("failed to read symlink {}: {}", display_path(path), error))?;
    Ok(Some(target))
}

#[cfg(unix)]
fn ensure_mode_if_exists(path: &Path, mode: u32, dry_run: bool) -> Result<EnsureState, String> {
    if !path.exists() {
        return Ok(EnsureState::Unchanged);
    }
    if !path.is_file() {
        return Err(format!(
            "expected {} to be a file before setting mode",
            display_path(path)
        ));
    }
    let metadata = fs::metadata(path)
        .map_err(|error| format!("failed to read metadata {}: {}", display_path(path), error))?;
    if metadata.permissions().mode() & 0o777 == mode {
        return Ok(EnsureState::Unchanged);
    }
    if dry_run {
        return Ok(EnsureState::Updated);
    }
    let permissions = fs::Permissions::from_mode(mode);
    let _ = fs::set_permissions(path, permissions);
    Ok(EnsureState::Updated)
}

fn render_systemd_service(layout: &InstallLayout, options: &InstallerOptions) -> String {
    format!(
        "[Unit]
Description=Crab Discord Runtime
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User={}
Group={}
EnvironmentFile={}
WorkingDirectory={}
ExecStart={} --crabd {}
Restart=always
RestartSec=2
TimeoutStopSec=30
StandardOutput=append:{}
StandardError=append:{}

[Install]
WantedBy=multi-user.target
",
        options.service_user,
        options.service_group,
        display_path(&layout.env_file),
        display_path(&layout.current_link),
        display_path(&layout.shim_connector_link()),
        display_path(&layout.shim_crabd_link()),
        display_path(&layout.runtime_log),
        display_path(&layout.runtime_log)
    )
}

fn render_launchd_service(layout: &InstallLayout, _options: &InstallerOptions) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">
<plist version=\"1.0\">
  <dict>
    <key>Label</key>
    <string>com.crab.runtime</string>
    <key>ProgramArguments</key>
    <array>
      <string>{}</string>
      <string>--crabd</string>
      <string>{}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{}</string>
    <key>StandardErrorPath</key>
    <string>{}</string>
  </dict>
</plist>
",
        display_path(&layout.shim_connector_link()),
        display_path(&layout.shim_crabd_link()),
        display_path(&layout.runtime_log),
        display_path(&layout.runtime_log)
    )
}

fn install_invocation(
    target: InstallTarget,
    spec: PrerequisiteSpec,
) -> Result<CommandInvocation, String> {
    let parts = match target {
        InstallTarget::Macos => spec.install_macos,
        InstallTarget::Linux => spec.install_linux,
    };
    let Some((program, args)) = parts.split_first() else {
        return Err(format!(
            "no install invocation configured for prerequisite {}",
            spec.command
        ));
    };
    Ok(CommandInvocation {
        program: (*program).to_string(),
        args: args.iter().map(|value| (*value).to_string()).collect(),
    })
}

fn parse_options(argv: &[String]) -> Result<InstallerOptions, String> {
    let command = parse_command_token(
        argv.get(1)
            .ok_or_else(|| "missing command token".to_string())?,
    )?;
    let parsed_flags = parse_flags(argv.get(2..).unwrap_or(&[]), &VALUE_FLAGS, &["--dry-run"])?;

    let target_raw = require_flag(&parsed_flags, "--target")?;
    let target = InstallTarget::parse(&target_raw)
        .ok_or_else(|| format!("--target must be macos or linux, got {:?}", target_raw))?;
    let root_prefix = parsed_flags
        .values
        .get("--root-prefix")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_ROOT_PREFIX));

    let release_id = parsed_flags
        .values
        .get("--release-id")
        .cloned()
        .unwrap_or_else(|| DEFAULT_RELEASE_ID.to_string());
    let crabd_bin = parsed_flags
        .values
        .get("--crabd-bin")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CRABD_BIN));
    let connector_bin = parsed_flags
        .values
        .get("--connector-bin")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONNECTOR_BIN));
    let workspace_root = parsed_flags
        .values
        .get("--workspace-root")
        .cloned()
        .unwrap_or_else(|| DEFAULT_WORKSPACE_ROOT.to_string());
    let service_user = parsed_flags
        .values
        .get("--service-user")
        .cloned()
        .unwrap_or_else(|| DEFAULT_SERVICE_USER.to_string());
    let service_group = parsed_flags
        .values
        .get("--service-group")
        .cloned()
        .unwrap_or_else(|| DEFAULT_SERVICE_GROUP.to_string());

    Ok(InstallerOptions {
        command,
        target,
        dry_run: parsed_flags.switches.contains("--dry-run"),
        root_prefix,
        release_id,
        crabd_bin,
        connector_bin,
        workspace_root,
        service_user,
        service_group,
    })
}

fn validate_options(options: InstallerOptions) -> Result<InstallerOptions, String> {
    if options.root_prefix.as_os_str().is_empty() {
        return Err("--root-prefix must not be empty".to_string());
    }
    if matches!(options.command, InstallerCommandKind::Upgrade)
        && options.release_id == DEFAULT_RELEASE_ID
    {
        return Err("--release-id is required for upgrade".to_string());
    }
    if !is_release_token_safe(&options.release_id) {
        return Err(format!(
            "--release-id must use only ASCII letters, digits, '.', '-' or '_' (got {:?})",
            options.release_id
        ));
    }
    if options.workspace_root.trim().is_empty() {
        return Err("--workspace-root must not be empty".to_string());
    }
    if !is_simple_token(&options.service_user) {
        return Err("--service-user must be a non-empty token with no whitespace".to_string());
    }
    if !is_simple_token(&options.service_group) {
        return Err("--service-group must be a non-empty token with no whitespace".to_string());
    }
    Ok(options)
}

fn parse_command_token(token: &str) -> Result<InstallerCommandKind, String> {
    match token {
        "install" => Ok(InstallerCommandKind::Install),
        "upgrade" => Ok(InstallerCommandKind::Upgrade),
        "rollback" => Ok(InstallerCommandKind::Rollback),
        "doctor" => Ok(InstallerCommandKind::Doctor),
        other => Err(format!("unsupported command {:?}", other)),
    }
}

fn parse_flags(
    args: &[String],
    value_flags: &[&str],
    switch_flags: &[&str],
) -> Result<ParsedFlags, String> {
    let mut parsed = ParsedFlags::new();
    let mut index = 0usize;

    while let Some(token) = args.get(index) {
        if !token.starts_with("--") {
            return Err(format!("unexpected positional argument {:?}", token));
        }
        if switch_flags.contains(&token.as_str()) {
            if !parsed.switches.insert(token.clone()) {
                return Err(format!("duplicate flag {}", token));
            }
            index = index.saturating_add(1);
            continue;
        }
        if !value_flags.contains(&token.as_str()) {
            return Err(format!("unknown flag {}", token));
        }
        let Some(value) = args.get(index.saturating_add(1)) else {
            return Err(format!("missing value for flag {}", token));
        };
        if value.starts_with("--") {
            return Err(format!("missing value for flag {}", token));
        }
        if parsed.values.insert(token.clone(), value.clone()).is_some() {
            return Err(format!("duplicate flag {}", token));
        }
        index = index.saturating_add(2);
    }

    Ok(parsed)
}

fn require_flag(parsed: &ParsedFlags, flag: &'static str) -> Result<String, String> {
    let value = parsed
        .values
        .get(flag)
        .ok_or_else(|| format!("missing required flag {}", flag))?;
    if value.trim().is_empty() {
        return Err(format!("{} must not be empty", flag));
    }
    Ok(value.clone())
}

fn is_help_request(args: &[String]) -> bool {
    args.iter().any(|value| value == "--help" || value == "-h")
}

fn is_release_token_safe(value: &str) -> bool {
    !value.is_empty()
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_')
        })
}

fn is_simple_token(value: &str) -> bool {
    let normalized = value.trim();
    !normalized.is_empty()
        && normalized
            .chars()
            .all(|character| !character.is_ascii_whitespace())
}

fn normalize_root_prefix(root_prefix: &Path) -> PathBuf {
    if root_prefix.as_os_str().is_empty() {
        return PathBuf::from(DEFAULT_ROOT_PREFIX);
    }
    root_prefix.to_path_buf()
}

fn rooted(root_prefix: &Path, raw_path: &str) -> PathBuf {
    let raw = Path::new(raw_path);
    if raw.is_absolute() {
        if raw == Path::new("/") {
            return root_prefix.to_path_buf();
        }
        let without_root = raw
            .strip_prefix(Path::new("/"))
            .expect("absolute paths should strip /");
        return root_prefix.join(without_root);
    }
    root_prefix.join(raw)
}

fn display_path(path: &Path) -> String {
    path.as_os_str().to_string_lossy().to_string()
}

fn log_ensure_status(log: &mut ActionLog, kind: &str, path: &Path, status: EnsureState) {
    let verb = match status {
        EnsureState::Created => "created",
        EnsureState::Updated => "updated",
        EnsureState::Unchanged => "unchanged",
    };
    log.push(format!("{verb} {kind} {}", display_path(path)));
}

#[cfg(unix)]
fn create_symlink(target: &Path, path: &Path) -> Result<(), String> {
    symlink(target, path).map_err(|error| {
        format!(
            "failed to create symlink {} -> {}: {}",
            display_path(path),
            display_path(target),
            error
        )
    })
}

#[cfg(not(unix))]
fn create_symlink(_target: &Path, _path: &Path) -> Result<(), String> {
    Err("symlink operations are supported only on unix targets".to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{
        display_path, ensure_binary_copy, ensure_directory, ensure_file_if_missing,
        ensure_file_with_content, ensure_mode_if_exists, ensure_prerequisites, ensure_symlink,
        install_invocation, is_help_request, parse_flags, parse_options,
        read_previous_release_target, read_symlink_target, rooted, run_installer_cli,
        run_with_runtime, validate_options, ActionLog, CommandInvocation, CommandRuntime,
        EnsureState, InstallLayout, InstallTarget, InstallerCommandKind, InstallerOptions,
        ParsedFlags, PrerequisiteSpec, SystemCommandRuntime, DEFAULT_ROOT_PREFIX,
        DEFAULT_SERVICE_GROUP, DEFAULT_SERVICE_USER, DEFAULT_WORKSPACE_ROOT,
        EXIT_CODE_UPGRADE_BLOCKED_STATE_COMPATIBILITY,
    };

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug)]
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let suffix = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "crab-installer-{label}-{}-{suffix}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).expect("temp dir should be creatable");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[derive(Debug, Default)]
    struct FakeRuntime {
        existing: HashSet<String>,
        versions: HashMap<String, String>,
        commands: Vec<CommandInvocation>,
        fail_on_run: HashSet<String>,
    }

    impl FakeRuntime {
        fn with_prerequisites() -> Self {
            let mut runtime = Self::default();
            for command in [
                "tmux",
                "git",
                "gh",
                "jq",
                "rg",
                "node",
                "cargo",
                "cargo-llvm-cov",
            ] {
                runtime.existing.insert(command.to_string());
                runtime
                    .versions
                    .insert(command.to_string(), format!("{command} 1.0.0"));
            }
            runtime
        }
    }

    impl CommandRuntime for FakeRuntime {
        fn command_exists(&mut self, name: &str) -> Result<bool, String> {
            Ok(self.existing.contains(name))
        }

        fn command_version(&mut self, name: &str, _args: &[&str]) -> Result<String, String> {
            self.versions
                .get(name)
                .cloned()
                .ok_or_else(|| format!("missing version for {name}"))
        }

        fn run_command(&mut self, invocation: &CommandInvocation) -> Result<(), String> {
            if self.fail_on_run.contains(&invocation.program) {
                return Err(format!("forced failure for {}", invocation.program));
            }
            self.commands.push(invocation.clone());
            match invocation.program.as_str() {
                "brew" => {
                    if invocation.args.len() == 2 && invocation.args[0] == "install" {
                        let tool = match invocation.args[1].as_str() {
                            "ripgrep" => "rg",
                            "node" => "node",
                            "rust" => "cargo",
                            other => other,
                        };
                        self.existing.insert(tool.to_string());
                        self.versions
                            .insert(tool.to_string(), format!("{tool} installed"));
                    }
                }
                "apt-get" => {
                    for tool in &invocation.args {
                        match tool.as_str() {
                            "tmux" | "git" | "gh" | "jq" => {
                                self.existing.insert(tool.to_string());
                                self.versions
                                    .insert(tool.to_string(), format!("{tool} installed"));
                            }
                            "ripgrep" => {
                                self.existing.insert("rg".to_string());
                                self.versions
                                    .insert("rg".to_string(), "rg installed".to_string());
                            }
                            "nodejs" => {
                                self.existing.insert("node".to_string());
                                self.versions
                                    .insert("node".to_string(), "node installed".to_string());
                            }
                            "cargo" | "rustc" => {
                                self.existing.insert("cargo".to_string());
                                self.versions
                                    .insert("cargo".to_string(), "cargo installed".to_string());
                            }
                            _ => {}
                        }
                    }
                }
                "cargo" => {
                    if invocation.args.first().map(String::as_str) == Some("install")
                        && invocation.args.get(1).map(String::as_str) == Some("cargo-llvm-cov")
                    {
                        self.existing.insert("cargo-llvm-cov".to_string());
                        self.versions.insert(
                            "cargo-llvm-cov".to_string(),
                            "cargo-llvm-cov installed".to_string(),
                        );
                    }
                }
                _ => {}
            }
            Ok(())
        }
    }

    fn options_for(
        command: InstallerCommandKind,
        target: InstallTarget,
        root_prefix: &Path,
    ) -> InstallerOptions {
        InstallerOptions {
            command,
            target,
            dry_run: false,
            root_prefix: root_prefix.to_path_buf(),
            release_id: "r1".to_string(),
            crabd_bin: root_prefix.join("sources/crabd"),
            connector_bin: root_prefix.join("sources/crab-discord-connector"),
            workspace_root: "/var/lib/crab/workspace".to_string(),
            service_user: "crab".to_string(),
            service_group: "crab".to_string(),
        }
    }

    fn argv(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    fn write_dummy_binary(path: &Path) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("parent should be created");
        }
        fs::write(path, "#!/bin/sh\necho crab\n").expect("dummy binary should be written");
        #[cfg(unix)]
        {
            let permissions = fs::Permissions::from_mode(0o755);
            fs::set_permissions(path, permissions).expect("binary should be executable");
        }
    }

    fn write_executable(path: &Path, body: &str) {
        fs::write(path, body).expect("script should be written");
        #[cfg(unix)]
        {
            let permissions = fs::Permissions::from_mode(0o755);
            fs::set_permissions(path, permissions).expect("script should be executable");
        }
    }

    #[test]
    fn collect_args_and_help_detection_work() {
        let args = argv(&["crabctl", "install", "--help"]);
        assert_eq!(args[1], "install");
        assert!(is_help_request(args.get(2..).expect("slice should exist")));
        assert!(!is_help_request(&["install".to_string()]));
    }

    #[test]
    fn parse_flags_rejects_unknown_and_duplicates() {
        let unknown = parse_flags(&["--bad".to_string()], &["--target"], &["--dry-run"])
            .expect_err("unknown flag should fail");
        assert!(unknown.contains("unknown flag"));

        let duplicate = parse_flags(
            &[
                "--target".to_string(),
                "linux".to_string(),
                "--target".to_string(),
                "macos".to_string(),
            ],
            &["--target"],
            &["--dry-run"],
        )
        .expect_err("duplicate flag should fail");
        assert!(duplicate.contains("duplicate flag"));
    }

    #[test]
    fn parse_options_and_validation_cover_required_cases() {
        let err = parse_options(&argv(&["crabctl", "bad", "--target", "linux"]))
            .expect_err("bad command should fail");
        assert!(err.contains("unsupported command"));

        let err =
            parse_options(&argv(&["crabctl", "install"])).expect_err("missing target should fail");
        assert!(err.contains("missing required flag --target"));

        let parsed = parse_options(&argv(&[
            "crabctl",
            "install",
            "--target",
            "linux",
            "--release-id",
            "v1",
            "--dry-run",
            "--workspace-root",
            "/srv/crab/workspace",
        ]))
        .expect("install options should parse");
        assert_eq!(parsed.command, InstallerCommandKind::Install);
        assert_eq!(parsed.target, InstallTarget::Linux);
        assert!(parsed.dry_run);
        assert_eq!(parsed.release_id, "v1");
        assert_eq!(parsed.workspace_root, "/srv/crab/workspace");
    }

    #[test]
    fn install_layout_uses_target_specific_service_paths() {
        let root = TempDir::new("layout");
        let linux = InstallLayout::from_options(&options_for(
            InstallerCommandKind::Install,
            InstallTarget::Linux,
            &root.path,
        ));
        let macos = InstallLayout::from_options(&options_for(
            InstallerCommandKind::Install,
            InstallTarget::Macos,
            &root.path,
        ));
        assert!(display_path(&linux.service_file).ends_with("etc/systemd/system/crab.service"));
        assert!(display_path(&macos.service_file)
            .ends_with("Library/LaunchDaemons/com.crab.runtime.plist"));
    }

    #[test]
    fn run_installer_cli_help_and_error_paths_are_explicit() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let args = argv(&["crabctl", "--help"]);
        let code = run_installer_cli(&args, &mut out, &mut err);
        assert_eq!(code, 0);
        assert!(String::from_utf8(out).expect("utf8").contains("Usage:"));
        assert!(String::from_utf8(err).expect("utf8").is_empty());

        let mut out = Vec::new();
        let mut err = Vec::new();
        let args = argv(&["crabctl", "install"]);
        let code = run_installer_cli(&args, &mut out, &mut err);
        assert_eq!(code, 2);
        let stderr = String::from_utf8(err).expect("utf8");
        assert!(stderr.contains("missing required flag --target"));
    }

    #[test]
    fn install_dry_run_generates_plan_without_mutation() {
        let root = TempDir::new("dry-run");
        let mut runtime = FakeRuntime::default();
        let mut options = options_for(
            InstallerCommandKind::Install,
            InstallTarget::Linux,
            &root.path,
        );
        options.dry_run = true;

        let (lines, code) = run_with_runtime(&options, &mut runtime).expect("dry run should work");
        assert_eq!(code, 0);
        assert!(lines
            .iter()
            .any(|line| line.contains("plan prerequisite install tmux")));
        assert!(!InstallLayout::from_options(&options).opt_root.exists());
        assert!(runtime.commands.is_empty());
    }

    #[test]
    fn install_applies_layout_and_release_symlinks() {
        let root = TempDir::new("install");
        let mut runtime = FakeRuntime::with_prerequisites();
        let options = options_for(
            InstallerCommandKind::Install,
            InstallTarget::Linux,
            &root.path,
        );
        write_dummy_binary(&options.crabd_bin);
        write_dummy_binary(&options.connector_bin);

        let (_lines, code) = run_with_runtime(&options, &mut runtime).expect("install should work");
        assert_eq!(code, 0);

        let layout = InstallLayout::from_options(&options);
        let release_bin = layout.release_bin_dir(&options.release_id);
        assert!(release_bin.join("crabd").is_file());
        assert!(release_bin.join("crab-discord-connector").is_file());
        assert!(layout.current_link.exists());
        assert!(layout.shim_crabd_link().exists());
        assert!(layout.service_file.is_file());
        assert!(layout.env_file.is_file());
    }

    #[test]
    fn install_is_idempotent_on_second_run() {
        let root = TempDir::new("idempotent");
        let mut runtime = FakeRuntime::with_prerequisites();
        let options = options_for(
            InstallerCommandKind::Install,
            InstallTarget::Linux,
            &root.path,
        );
        write_dummy_binary(&options.crabd_bin);
        write_dummy_binary(&options.connector_bin);

        let (_lines, code) = run_with_runtime(&options, &mut runtime).expect("first run");
        assert_eq!(code, 0);
        let (lines, code) = run_with_runtime(&options, &mut runtime).expect("second run");
        assert_eq!(code, 0);
        assert!(lines
            .iter()
            .any(|line| line.contains("unchanged directory")));
    }

    #[test]
    fn install_reports_conflict_when_directory_path_is_file() {
        let root = TempDir::new("conflict");
        let mut runtime = FakeRuntime::with_prerequisites();
        let options = options_for(
            InstallerCommandKind::Install,
            InstallTarget::Linux,
            &root.path,
        );
        write_dummy_binary(&options.crabd_bin);
        write_dummy_binary(&options.connector_bin);

        let layout = InstallLayout::from_options(&options);
        fs::create_dir_all(
            layout
                .opt_root
                .parent()
                .expect("opt_root should have parent"),
        )
        .expect("parent should be created");
        fs::write(&layout.opt_root, "not a dir").expect("conflict file should be written");

        let error = run_with_runtime(&options, &mut runtime).expect_err("should fail on conflict");
        assert!(error.contains("path conflict"));
    }

    #[test]
    fn install_without_binary_sources_fails_when_not_dry_run() {
        let root = TempDir::new("missing-binary");
        let mut runtime = FakeRuntime::with_prerequisites();
        let options = options_for(
            InstallerCommandKind::Install,
            InstallTarget::Linux,
            &root.path,
        );

        let error =
            run_with_runtime(&options, &mut runtime).expect_err("missing binaries should fail");
        assert!(error.contains("missing crabd binary source"));
    }

    #[test]
    fn upgrade_writes_previous_release_and_switches_current() {
        let root = TempDir::new("upgrade");
        let mut runtime = FakeRuntime::with_prerequisites();

        let mut install_options = options_for(
            InstallerCommandKind::Install,
            InstallTarget::Linux,
            &root.path,
        );
        install_options.release_id = "r1".to_string();
        write_dummy_binary(&install_options.crabd_bin);
        write_dummy_binary(&install_options.connector_bin);
        run_with_runtime(&install_options, &mut runtime).expect("install should work");

        let mut upgrade_options = options_for(
            InstallerCommandKind::Upgrade,
            InstallTarget::Linux,
            &root.path,
        );
        upgrade_options.release_id = "r2".to_string();
        write_dummy_binary(&upgrade_options.crabd_bin);
        write_dummy_binary(&upgrade_options.connector_bin);

        run_with_runtime(&upgrade_options, &mut runtime).expect("upgrade should work");
        let layout = InstallLayout::from_options(&upgrade_options);
        let previous =
            fs::read_to_string(&layout.previous_release_file).expect("state should exist");
        assert!(previous.contains("r1"));
        let current = fs::read_link(&layout.current_link).expect("current should be symlink");
        assert!(display_path(&current).contains("r2"));
    }

    #[test]
    fn upgrade_blocks_when_state_schema_is_too_new() {
        let root = TempDir::new("upgrade-blocked-schema");
        let mut runtime = FakeRuntime::with_prerequisites();
        let mut upgrade_options = options_for(
            InstallerCommandKind::Upgrade,
            InstallTarget::Linux,
            &root.path,
        );
        upgrade_options.release_id = "r2".to_string();
        write_dummy_binary(&upgrade_options.crabd_bin);
        write_dummy_binary(&upgrade_options.connector_bin);

        let layout = InstallLayout::from_options(&upgrade_options);
        let state_root = layout.workspace_root.join("state");
        fs::create_dir_all(&state_root).expect("state root should be creatable");
        fs::write(
            state_root.join("schema_version.json"),
            r#"{"version":999,"updated_at_epoch_ms":1739173200000}"#,
        )
        .expect("schema marker should be writable");

        let (lines, code) =
            run_with_runtime(&upgrade_options, &mut runtime).expect("upgrade should run");
        assert_eq!(code, EXIT_CODE_UPGRADE_BLOCKED_STATE_COMPATIBILITY);
        assert!(lines
            .iter()
            .any(|line| line.contains("upgrade preflight: blocked")));
        assert!(lines
            .iter()
            .any(|line| line.contains("state schema version 999 is newer than maximum supported")));
        assert!(lines
            .iter()
            .any(|line| line.contains("upgrade preflight remediation: crabctl rollback")));
        assert!(!layout.previous_release_file.exists());
        assert!(!layout.current_link.exists());
    }

    #[test]
    fn rollback_swaps_to_previous_release() {
        let root = TempDir::new("rollback");
        let mut runtime = FakeRuntime::with_prerequisites();

        let mut install_options = options_for(
            InstallerCommandKind::Install,
            InstallTarget::Linux,
            &root.path,
        );
        install_options.release_id = "r1".to_string();
        write_dummy_binary(&install_options.crabd_bin);
        write_dummy_binary(&install_options.connector_bin);
        run_with_runtime(&install_options, &mut runtime).expect("install should work");

        let mut upgrade_options = options_for(
            InstallerCommandKind::Upgrade,
            InstallTarget::Linux,
            &root.path,
        );
        upgrade_options.release_id = "r2".to_string();
        write_dummy_binary(&upgrade_options.crabd_bin);
        write_dummy_binary(&upgrade_options.connector_bin);
        run_with_runtime(&upgrade_options, &mut runtime).expect("upgrade should work");

        let rollback_options = options_for(
            InstallerCommandKind::Rollback,
            InstallTarget::Linux,
            &root.path,
        );
        run_with_runtime(&rollback_options, &mut runtime).expect("rollback should work");
        let layout = InstallLayout::from_options(&rollback_options);
        let current = fs::read_link(&layout.current_link).expect("current should be symlink");
        assert!(display_path(&current).contains("r1"));
    }

    #[test]
    fn rollback_fails_without_previous_release_state() {
        let root = TempDir::new("rollback-missing");
        let mut runtime = FakeRuntime::with_prerequisites();
        let options = options_for(
            InstallerCommandKind::Rollback,
            InstallTarget::Linux,
            &root.path,
        );

        let error = run_with_runtime(&options, &mut runtime).expect_err("rollback should fail");
        assert!(error.contains("cannot rollback"));
    }

    #[test]
    fn doctor_reports_unhealthy_without_install_and_healthy_after_install() {
        let root = TempDir::new("doctor");
        let mut runtime = FakeRuntime::with_prerequisites();
        let doctor_options = options_for(
            InstallerCommandKind::Doctor,
            InstallTarget::Linux,
            &root.path,
        );

        let (lines, code) =
            run_with_runtime(&doctor_options, &mut runtime).expect("doctor should run");
        assert_eq!(code, 1);
        assert!(lines
            .iter()
            .any(|line| line.contains("doctor summary: unhealthy")));

        let mut install_options = options_for(
            InstallerCommandKind::Install,
            InstallTarget::Linux,
            &root.path,
        );
        install_options.release_id = "r1".to_string();
        write_dummy_binary(&install_options.crabd_bin);
        write_dummy_binary(&install_options.connector_bin);
        run_with_runtime(&install_options, &mut runtime).expect("install should work");

        let (lines, code) =
            run_with_runtime(&doctor_options, &mut runtime).expect("doctor should run");
        assert_eq!(code, 0);
        assert!(lines
            .iter()
            .any(|line| line.contains("doctor summary: healthy")));
    }

    #[test]
    fn doctor_reports_state_schema_preflight_failure_when_state_is_too_new() {
        let root = TempDir::new("doctor-schema-preflight");
        let mut runtime = FakeRuntime::with_prerequisites();
        let mut install_options = options_for(
            InstallerCommandKind::Install,
            InstallTarget::Linux,
            &root.path,
        );
        install_options.release_id = "r1".to_string();
        write_dummy_binary(&install_options.crabd_bin);
        write_dummy_binary(&install_options.connector_bin);
        run_with_runtime(&install_options, &mut runtime).expect("install should work");

        let layout = InstallLayout::from_options(&install_options);
        let state_root = layout.workspace_root.join("state");
        fs::create_dir_all(&state_root).expect("state root should be creatable");
        fs::write(
            state_root.join("schema_version.json"),
            r#"{"version":999,"updated_at_epoch_ms":1739173200000}"#,
        )
        .expect("schema marker should be writable");

        let doctor_options = options_for(
            InstallerCommandKind::Doctor,
            InstallTarget::Linux,
            &root.path,
        );
        let (lines, code) =
            run_with_runtime(&doctor_options, &mut runtime).expect("doctor should run");
        assert_eq!(code, 1);
        assert!(lines
            .iter()
            .any(|line| line.contains("FAIL state_schema_compatibility")));
        assert!(lines.iter().any(|line| {
            line.contains("state_schema_compatibility remediation: crabctl rollback")
        }));
    }

    #[test]
    fn prerequisites_install_path_runs_commands_for_missing_tools() {
        let root = TempDir::new("prereq-install");
        let mut runtime = FakeRuntime::default();
        let mut options = options_for(
            InstallerCommandKind::Install,
            InstallTarget::Macos,
            &root.path,
        );
        options.dry_run = false;
        write_dummy_binary(&options.crabd_bin);
        write_dummy_binary(&options.connector_bin);

        run_with_runtime(&options, &mut runtime).expect("install should bootstrap prerequisites");
        assert!(runtime
            .commands
            .iter()
            .any(|command| command.program == "brew"));
        assert!(runtime.existing.contains("cargo-llvm-cov"));
    }

    #[test]
    fn prerequisite_install_failure_surfaces_actionable_error() {
        let root = TempDir::new("prereq-fail");
        let mut runtime = FakeRuntime::default();
        runtime.fail_on_run.insert("apt-get".to_string());
        let options = options_for(
            InstallerCommandKind::Install,
            InstallTarget::Linux,
            &root.path,
        );
        write_dummy_binary(&options.crabd_bin);
        write_dummy_binary(&options.connector_bin);

        let error = run_with_runtime(&options, &mut runtime).expect_err("install should fail");
        assert!(error.contains("forced failure"));
    }

    #[test]
    fn parse_flags_supports_switches_and_values() {
        let parsed = parse_flags(
            &[
                "--target".to_string(),
                "linux".to_string(),
                "--dry-run".to_string(),
            ],
            &["--target"],
            &["--dry-run"],
        )
        .expect("flags should parse");
        assert_eq!(
            parsed,
            ParsedFlags {
                values: BTreeMap::from([("--target".to_string(), "linux".to_string())]),
                switches: BTreeSet::from(["--dry-run".to_string()]),
            }
        );
    }

    #[test]
    fn parse_options_rejects_unsafe_release_token() {
        let options = parse_options(&argv(&[
            "crabctl",
            "install",
            "--target",
            "linux",
            "--release-id",
            "bad/token",
        ]))
        .expect("raw parse should succeed");
        let error = super::validate_options(options).expect_err("validation should fail");
        assert!(error.contains("--release-id"));
    }

    #[test]
    fn parse_options_rejects_upgrade_without_release_id() {
        let options = parse_options(&argv(&["crabctl", "upgrade", "--target", "linux"]))
            .expect("raw parse should succeed");
        let error = validate_options(options).expect_err("validation should fail");
        assert!(error.contains("required for upgrade"));
    }

    #[test]
    fn target_token_and_command_invocation_helpers_cover_edge_paths() {
        assert_eq!(InstallTarget::parse("MACOS"), Some(InstallTarget::Macos));
        assert_eq!(InstallTarget::parse("linux"), Some(InstallTarget::Linux));
        assert_eq!(InstallTarget::parse("other"), None);
        assert_eq!(InstallTarget::Macos.as_token(), "macos");
        assert_eq!(InstallTarget::Linux.as_token(), "linux");

        let no_args = CommandInvocation {
            program: "echo".to_string(),
            args: Vec::new(),
        };
        assert_eq!(no_args.display(), "echo");
        let with_args = CommandInvocation {
            program: "echo".to_string(),
            args: vec!["a".to_string(), "b".to_string()],
        };
        assert_eq!(with_args.display(), "echo a b");
    }

    #[test]
    fn system_runtime_covers_command_and_version_and_run_branches() {
        let mut runtime = SystemCommandRuntime;
        let root = TempDir::new("sys-runtime");
        let executable = root.path.join("exists.sh");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        assert!(runtime
            .command_exists(&display_path(&executable))
            .expect("absolute check should work"));

        let missing = root.path.join("missing.sh");
        assert!(!runtime
            .command_exists(&display_path(&missing))
            .expect("missing absolute check should work"));

        assert!(!runtime
            .command_exists("definitely-not-found")
            .expect("missing command should return false"));
        assert!(runtime
            .command_exists("sh")
            .expect("known command should be discovered in PATH"));

        let stdout_version = runtime
            .command_version("/bin/sh", &["-c", "echo out"])
            .expect("stdout version should parse");
        assert_eq!(stdout_version, "out");

        let stderr_version = runtime
            .command_version("/bin/sh", &["-c", "echo err 1>&2"])
            .expect("stderr version should parse");
        assert_eq!(stderr_version, "err");

        let empty = runtime
            .command_version("/bin/sh", &["-c", ":"])
            .expect_err("empty output should fail");
        assert!(empty.contains("empty version output"));

        let failing = runtime
            .command_version("/bin/sh", &["-c", "exit 7"])
            .expect_err("non-zero exit should fail");
        assert!(failing.contains("version command failed"));

        let spawn = runtime
            .command_version("definitely-no-command", &["--version"])
            .expect_err("spawn should fail");
        assert!(spawn.contains("failed to run"));

        runtime
            .run_command(&CommandInvocation {
                program: "/bin/sh".to_string(),
                args: vec!["-c".to_string(), "exit 0".to_string()],
            })
            .expect("run command success path should work");
        let run_fail = runtime
            .run_command(&CommandInvocation {
                program: "/bin/sh".to_string(),
                args: vec!["-c".to_string(), "echo bad 1>&2; exit 3".to_string()],
            })
            .expect_err("run command failure path should fail");
        assert!(run_fail.contains("failed with status"));
        let run_spawn = runtime
            .run_command(&CommandInvocation {
                program: "definitely-no-command".to_string(),
                args: Vec::new(),
            })
            .expect_err("spawn failure should fail");
        assert!(run_spawn.contains("failed to execute"));
    }

    #[test]
    fn run_installer_cli_success_and_runtime_error_paths_are_covered() {
        let root = TempDir::new("cli-system");

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let args = argv(&[
            "crabctl",
            "install",
            "--target",
            "linux",
            "--root-prefix",
            &display_path(&root.path),
            "--dry-run",
        ]);
        let code = run_installer_cli(&args, &mut stdout, &mut stderr);
        assert_eq!(code, 0);
        assert!(String::from_utf8(stderr).expect("utf8").is_empty());
        let rendered = String::from_utf8(stdout).expect("utf8");
        assert!(rendered.contains("prerequisite"));

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let args = argv(&[
            "crabctl",
            "rollback",
            "--target",
            "linux",
            "--root-prefix",
            &display_path(&root.path.join("fresh")),
        ]);
        let code = run_installer_cli(&args, &mut stdout, &mut stderr);
        assert_eq!(code, 1);
        let error = String::from_utf8(stderr).expect("utf8");
        assert!(error.contains("cannot rollback"));
    }

    #[test]
    fn helper_functions_cover_file_directory_symlink_and_parse_edges() {
        let root = TempDir::new("helpers");
        let mut log = ActionLog::new();

        let missing_source = root.path.join("no-crabd");
        let binary_target = root.path.join("bin/crabd");
        fs::create_dir_all(binary_target.parent().expect("parent")).expect("parent should exist");
        let dry_missing =
            ensure_binary_copy(&missing_source, &binary_target, true, "crabd", &mut log)
                .expect("dry run missing source should pass");
        assert_eq!(dry_missing, EnsureState::Unchanged);
        assert!(log.lines.iter().any(|line| line.contains("source missing")));

        let connector_error = ensure_binary_copy(
            &missing_source,
            &binary_target,
            false,
            "crab-discord-connector",
            &mut log,
        )
        .expect_err("missing source should fail");
        assert!(connector_error.contains("--connector-bin"));

        let parent_file = root.path.join("parent-file");
        fs::write(&parent_file, "x").expect("parent file should exist");
        let directory_error = ensure_directory(&parent_file.join("child"), false)
            .expect_err("directory create under file should fail");
        assert!(directory_error.contains("failed to create directory"));

        let existing_dir = root.path.join("existing-dir");
        fs::create_dir_all(&existing_dir).expect("dir should exist");
        assert_eq!(
            ensure_directory(&existing_dir, false).expect("existing dir should be unchanged"),
            EnsureState::Unchanged
        );

        let file_if_missing = root.path.join("etc/crab.env");
        let created = ensure_file_if_missing(&file_if_missing, "A=1\n", false)
            .expect("file should be created");
        assert_eq!(created, EnsureState::Created);
        let unchanged = ensure_file_if_missing(&file_if_missing, "B=2\n", false)
            .expect("existing file should be unchanged");
        assert_eq!(unchanged, EnsureState::Unchanged);

        let update_target = root.path.join("updated.txt");
        fs::write(&update_target, "old").expect("file should exist");
        let dry_updated = ensure_file_with_content(&update_target, "new", true)
            .expect("dry update should be reported");
        assert_eq!(dry_updated, EnsureState::Updated);
        let updated =
            ensure_file_with_content(&update_target, "new", false).expect("update should apply");
        assert_eq!(updated, EnsureState::Updated);
        assert_eq!(fs::read_to_string(&update_target).expect("read"), "new");

        let symlink_target_a = root.path.join("target-a");
        let symlink_target_b = root.path.join("target-b");
        fs::write(&symlink_target_a, "a").expect("target a");
        fs::write(&symlink_target_b, "b").expect("target b");
        let symlink_path = root.path.join("link");
        let created_link =
            ensure_symlink(&symlink_path, &symlink_target_a, false).expect("symlink create");
        assert_eq!(created_link, EnsureState::Created);
        let updated_link =
            ensure_symlink(&symlink_path, &symlink_target_b, false).expect("symlink update");
        assert_eq!(updated_link, EnsureState::Updated);
        let unchanged_link =
            ensure_symlink(&symlink_path, &symlink_target_b, false).expect("symlink unchanged");
        assert_eq!(unchanged_link, EnsureState::Unchanged);

        let non_symlink = root.path.join("non-symlink");
        fs::write(&non_symlink, "file").expect("file should exist");
        let symlink_conflict = ensure_symlink(&non_symlink, &symlink_target_b, false)
            .expect_err("non-symlink should fail");
        assert!(symlink_conflict.contains("not a symlink"));

        let missing_target = read_symlink_target(&root.path.join("does-not-exist"))
            .expect("missing path should return none");
        assert!(missing_target.is_none());
        let non_symlink_error = read_symlink_target(&non_symlink).expect_err("file should fail");
        assert!(non_symlink_error.contains("not a symlink"));

        let options = options_for(
            InstallerCommandKind::Rollback,
            InstallTarget::Linux,
            &root.path,
        );
        let layout = InstallLayout::from_options(&options);
        fs::create_dir_all(&layout.install_state_dir).expect("state dir should exist");
        fs::write(&layout.previous_release_file, "\n").expect("empty state");
        let empty_previous = read_previous_release_target(&layout).expect_err("empty should fail");
        assert!(empty_previous.contains("is empty"));

        #[cfg(unix)]
        {
            let mode_file = root.path.join("mode-file");
            fs::write(&mode_file, "mode").expect("mode file");
            let changed = ensure_mode_if_exists(&mode_file, 0o600, false).expect("mode set");
            assert_eq!(changed, EnsureState::Updated);
            let unchanged_mode =
                ensure_mode_if_exists(&mode_file, 0o600, false).expect("mode unchanged");
            assert_eq!(unchanged_mode, EnsureState::Unchanged);
            let mode_error = ensure_mode_if_exists(&root.path.join("existing-dir"), 0o600, false)
                .expect_err("non-file should fail");
            assert!(mode_error.contains("expected"));
        }

        let invalid_spec = PrerequisiteSpec {
            command: "x",
            version_args: &["--version"],
            install_macos: &[],
            install_linux: &[],
        };
        let invocation_error =
            install_invocation(InstallTarget::Linux, invalid_spec).expect_err("invalid spec");
        assert!(invocation_error.contains("no install invocation configured"));

        assert_eq!(rooted(&root.path, "/"), root.path);
        assert_eq!(
            rooted(&root.path, "relative/path"),
            root.path.join("relative/path")
        );
    }

    #[test]
    fn validate_and_parse_helpers_cover_remaining_error_branches() {
        let options = InstallerOptions {
            command: InstallerCommandKind::Install,
            target: InstallTarget::Linux,
            dry_run: false,
            root_prefix: PathBuf::new(),
            release_id: "v1".to_string(),
            crabd_bin: PathBuf::from("x"),
            connector_bin: PathBuf::from("y"),
            workspace_root: DEFAULT_WORKSPACE_ROOT.to_string(),
            service_user: DEFAULT_SERVICE_USER.to_string(),
            service_group: DEFAULT_SERVICE_GROUP.to_string(),
        };
        let error = validate_options(options).expect_err("blank root prefix should fail");
        assert!(error.contains("--root-prefix"));

        let blank_workspace = InstallerOptions {
            command: InstallerCommandKind::Install,
            target: InstallTarget::Linux,
            dry_run: false,
            root_prefix: PathBuf::from(DEFAULT_ROOT_PREFIX),
            release_id: "v1".to_string(),
            crabd_bin: PathBuf::from("x"),
            connector_bin: PathBuf::from("y"),
            workspace_root: "   ".to_string(),
            service_user: DEFAULT_SERVICE_USER.to_string(),
            service_group: DEFAULT_SERVICE_GROUP.to_string(),
        };
        let error = validate_options(blank_workspace).expect_err("blank workspace should fail");
        assert!(error.contains("--workspace-root"));

        let bad_user = InstallerOptions {
            command: InstallerCommandKind::Install,
            target: InstallTarget::Linux,
            dry_run: false,
            root_prefix: PathBuf::from(DEFAULT_ROOT_PREFIX),
            release_id: "v1".to_string(),
            crabd_bin: PathBuf::from("x"),
            connector_bin: PathBuf::from("y"),
            workspace_root: DEFAULT_WORKSPACE_ROOT.to_string(),
            service_user: "bad user".to_string(),
            service_group: DEFAULT_SERVICE_GROUP.to_string(),
        };
        let error = validate_options(bad_user).expect_err("bad user should fail");
        assert!(error.contains("--service-user"));

        let bad_group = InstallerOptions {
            command: InstallerCommandKind::Install,
            target: InstallTarget::Linux,
            dry_run: false,
            root_prefix: PathBuf::from(DEFAULT_ROOT_PREFIX),
            release_id: "v1".to_string(),
            crabd_bin: PathBuf::from("x"),
            connector_bin: PathBuf::from("y"),
            workspace_root: DEFAULT_WORKSPACE_ROOT.to_string(),
            service_user: DEFAULT_SERVICE_USER.to_string(),
            service_group: "bad group".to_string(),
        };
        let error = validate_options(bad_group).expect_err("bad group should fail");
        assert!(error.contains("--service-group"));

        let positional = parse_flags(&["oops".to_string()], &["--target"], &["--dry-run"])
            .expect_err("positional should fail");
        assert!(positional.contains("unexpected positional argument"));

        let missing_value = parse_flags(&["--target".to_string()], &["--target"], &["--dry-run"])
            .expect_err("missing value should fail");
        assert!(missing_value.contains("missing value"));

        let blank_required = ParsedFlags {
            values: BTreeMap::from([("--target".to_string(), " ".to_string())]),
            switches: BTreeSet::new(),
        };
        let required_error = super::require_flag(&blank_required, "--target")
            .expect_err("blank required value should fail");
        assert!(required_error.contains("must not be empty"));
    }

    #[test]
    fn parse_options_rejects_missing_command_token() {
        let error =
            parse_options(&argv(&["crabctl"])).expect_err("missing command token should fail");
        assert!(error.contains("missing command token"));
    }

    #[test]
    fn runtime_prerequisite_recheck_error_and_version_fallback_are_covered() {
        #[derive(Default)]
        struct StuckRuntime {
            called: bool,
        }

        impl CommandRuntime for StuckRuntime {
            fn command_exists(&mut self, _name: &str) -> Result<bool, String> {
                Ok(false)
            }

            fn command_version(&mut self, _name: &str, _args: &[&str]) -> Result<String, String> {
                Err("version unavailable".to_string())
            }

            fn run_command(&mut self, _invocation: &CommandInvocation) -> Result<(), String> {
                self.called = true;
                Ok(())
            }
        }

        let root = TempDir::new("stuck-runtime");
        let mut options = options_for(
            InstallerCommandKind::Install,
            InstallTarget::Linux,
            &root.path,
        );
        options.dry_run = false;
        let mut log = ActionLog::new();
        let mut runtime = StuckRuntime::default();
        let command_version_error = runtime
            .command_version("tmux", &["-V"])
            .expect_err("explicit command version call should fail");
        assert!(command_version_error.contains("version unavailable"));
        let error = ensure_prerequisites(&options, &mut runtime, &mut log)
            .expect_err("stuck runtime should fail after install");
        assert!(error.contains("still missing"));
        assert!(runtime.called);

        let mut fallback_runtime = FakeRuntime::with_prerequisites();
        fallback_runtime.versions.clear();
        write_dummy_binary(&options.crabd_bin);
        write_dummy_binary(&options.connector_bin);
        let (lines, code) =
            run_with_runtime(&options, &mut fallback_runtime).expect("fallback run should pass");
        assert_eq!(code, 0);
        assert!(lines
            .iter()
            .any(|line| line.contains("version-unavailable")));
    }

    #[test]
    fn ensure_mode_dry_run_and_fake_runtime_default_match_branches_are_covered() {
        let root = TempDir::new("mode-dry-run-and-fakeruntime-default");
        let mode_file = root.path.join("mode-file");
        fs::write(&mode_file, "mode").expect("mode file should exist");
        #[cfg(unix)]
        {
            let permissions = fs::Permissions::from_mode(0o644);
            fs::set_permissions(&mode_file, permissions).expect("mode should be set");
            let changed = ensure_mode_if_exists(&mode_file, 0o600, true).expect("dry run");
            assert_eq!(changed, EnsureState::Updated);
        }

        let mut runtime = FakeRuntime::default();
        runtime
            .run_command(&CommandInvocation {
                program: "brew".to_string(),
                args: vec!["install".to_string(), "jq".to_string()],
            })
            .expect("brew branch should run");
        assert!(runtime.existing.contains("jq"));
        runtime
            .run_command(&CommandInvocation {
                program: "brew".to_string(),
                args: vec!["install".to_string(), "rust".to_string()],
            })
            .expect("brew rust mapping branch should run");
        assert!(runtime.existing.contains("cargo"));
        runtime
            .run_command(&CommandInvocation {
                program: "brew".to_string(),
                args: vec!["list".to_string()],
            })
            .expect("brew non-install branch should run");

        runtime
            .run_command(&CommandInvocation {
                program: "noop".to_string(),
                args: Vec::new(),
            })
            .expect("default branch should run");
        assert!(runtime
            .commands
            .iter()
            .any(|command| command.program == "noop"));
    }

    #[test]
    fn linux_prerequisite_bootstrap_exercises_fake_apt_get_branch() {
        let root = TempDir::new("linux-prereq");
        let mut runtime = FakeRuntime::default();
        let options = options_for(
            InstallerCommandKind::Install,
            InstallTarget::Linux,
            &root.path,
        );
        write_dummy_binary(&options.crabd_bin);
        write_dummy_binary(&options.connector_bin);

        run_with_runtime(&options, &mut runtime).expect("linux prereq bootstrap should work");
        assert!(runtime
            .commands
            .iter()
            .any(|command| command.program == "apt-get"));
        assert!(runtime.existing.contains("tmux"));
        assert!(runtime.existing.contains("rg"));
        assert!(runtime.existing.contains("node"));
        assert!(runtime.existing.contains("cargo"));
    }

    #[test]
    fn preserve_previous_release_and_rollback_missing_target_paths_are_covered() {
        let root = TempDir::new("preserve-rollback");
        let options = options_for(
            InstallerCommandKind::Upgrade,
            InstallTarget::Linux,
            &root.path,
        );
        let layout = InstallLayout::from_options(&options);
        fs::create_dir_all(&layout.install_state_dir).expect("state dir should exist");
        let mut log = ActionLog::new();

        super::preserve_previous_release(&layout, false, &mut log)
            .expect("preserve without current should write reset state");
        assert!(layout.previous_release_file.is_file());
        assert!(log
            .lines
            .iter()
            .any(|line| line.contains("state_previous_release_reset")));

        fs::write(
            &layout.previous_release_file,
            display_path(&layout.releases_dir.join("missing-release")),
        )
        .expect("previous release file should exist");
        let rollback_error = super::rollback_release(
            &options_for(
                InstallerCommandKind::Rollback,
                InstallTarget::Linux,
                &root.path,
            ),
            &layout,
            &mut ActionLog::new(),
        )
        .expect_err("rollback with missing release directory should fail");
        assert!(rollback_error.contains("does not exist"));

        let rollback_without_current = TempDir::new("rollback-no-current");
        let rollback_options = options_for(
            InstallerCommandKind::Rollback,
            InstallTarget::Linux,
            &rollback_without_current.path,
        );
        let rollback_layout = InstallLayout::from_options(&rollback_options);
        fs::create_dir_all(rollback_layout.releases_dir.join("r1/bin"))
            .expect("release should exist");
        fs::create_dir_all(&rollback_layout.install_state_dir).expect("state dir should exist");
        fs::write(
            &rollback_layout.previous_release_file,
            display_path(&rollback_layout.releases_dir.join("r1")),
        )
        .expect("previous release should be written");
        super::rollback_release(&rollback_options, &rollback_layout, &mut ActionLog::new())
            .expect("rollback without current symlink should still succeed");
    }

    #[test]
    fn doctor_macos_branch_and_helper_edge_paths_are_covered() {
        let root = TempDir::new("doctor-macos");
        let mut runtime = FakeRuntime::with_prerequisites();
        let options = options_for(
            InstallerCommandKind::Doctor,
            InstallTarget::Macos,
            &root.path,
        );
        let layout = InstallLayout::from_options(&options);
        fs::create_dir_all(&layout.opt_root).expect("opt root");
        fs::create_dir_all(&layout.bin_dir).expect("bin");
        fs::create_dir_all(&layout.releases_dir).expect("releases");
        fs::create_dir_all(&layout.etc_crab_dir).expect("etc");
        fs::create_dir_all(&layout.workspace_root).expect("workspace");
        fs::create_dir_all(&layout.log_dir).expect("log");
        fs::create_dir_all(&layout.install_state_dir).expect("state");
        fs::create_dir_all(
            layout
                .service_file
                .parent()
                .expect("service file should have parent"),
        )
        .expect("service parent");
        fs::write(&layout.env_file, "TOKEN=1").expect("env");
        fs::write(&layout.service_file, "plist").expect("service");

        let checks = super::run_doctor_checks(&options, &layout, &mut runtime);
        assert!(checks
            .iter()
            .any(|check| check.name == "service_target" && check.ok));

        let mut missing_runtime = FakeRuntime::default();
        let missing_checks = super::run_doctor_checks(&options, &layout, &mut missing_runtime);
        assert!(missing_checks
            .iter()
            .any(|check| check.name == "prerequisite:tmux" && check.detail == "missing"));
    }

    #[test]
    fn macos_install_path_covers_launchd_render_and_stage_helpers() {
        let root = TempDir::new("macos-install");
        let mut runtime = FakeRuntime::with_prerequisites();
        let options = options_for(
            InstallerCommandKind::Install,
            InstallTarget::Macos,
            &root.path,
        );
        write_dummy_binary(&options.crabd_bin);
        write_dummy_binary(&options.connector_bin);
        let (lines, code) = run_with_runtime(&options, &mut runtime).expect("macos install");
        assert_eq!(code, 0);
        assert!(lines
            .iter()
            .any(|line| line.contains("service_file") && line.contains("com.crab.runtime.plist")));
    }

    #[test]
    fn low_level_error_branches_cover_copy_file_symlink_and_parse_variants() {
        let root = TempDir::new("low-level-errors");
        let source = root.path.join("source-bin");
        write_dummy_binary(&source);
        let mut log = ActionLog::new();

        let no_parent_error = ensure_binary_copy(&source, Path::new(""), false, "crabd", &mut log)
            .expect_err("empty destination path should fail");
        assert!(no_parent_error.contains("no parent directory"));

        let parent_file = root.path.join("parent-file");
        fs::write(&parent_file, "x").expect("parent file");
        let parent_not_dir = ensure_binary_copy(
            &source,
            &parent_file.join("child"),
            false,
            "crabd",
            &mut log,
        )
        .expect_err("non-directory parent should fail");
        assert!(parent_not_dir.contains("is not a directory"));

        let copy_target = root.path.join("copy/target");
        fs::create_dir_all(copy_target.parent().expect("parent")).expect("copy parent");
        fs::write(&copy_target, "exists").expect("existing target");
        let dry_update = ensure_binary_copy(&source, &copy_target, true, "crabd", &mut log)
            .expect("dry-run should report update");
        assert_eq!(dry_update, EnsureState::Updated);
        let dry_create = ensure_binary_copy(
            &source,
            &root.path.join("copy/new-target"),
            true,
            "crabd",
            &mut log,
        )
        .expect("dry-run should report create");
        assert_eq!(dry_create, EnsureState::Created);
        fs::create_dir_all(root.path.join("copy-fail-target")).expect("copy-fail target");
        let copy_fail = ensure_binary_copy(
            &source,
            &root.path.join("copy-fail-target"),
            false,
            "crabd",
            &mut log,
        )
        .expect_err("copy to directory destination should fail");
        assert!(copy_fail.contains("failed to copy"));

        let file_conflict = root.path.join("file-conflict");
        fs::create_dir_all(&file_conflict).expect("conflict dir");
        let if_missing_error = ensure_file_if_missing(&file_conflict, "x", false)
            .expect_err("existing directory path conflict should fail");
        assert!(if_missing_error.contains("not a file"));

        let file_parent_error = ensure_file_if_missing(&parent_file.join("child"), "x", false)
            .expect_err("parent file should fail mkdir");
        assert!(file_parent_error.contains("failed to create parent directory"));

        let regular_file = root.path.join("regular-file");
        fs::write(&regular_file, "old").expect("regular file");
        let with_content_conflict = ensure_file_with_content(&regular_file, "x", false)
            .expect("existing file should update");
        assert_eq!(with_content_conflict, EnsureState::Updated);
        let with_content_dir_conflict = ensure_file_with_content(&file_conflict, "x", false)
            .expect_err("directory should fail for ensure_file_with_content");
        assert!(with_content_dir_conflict.contains("not a file"));
        let with_content_parent_error =
            ensure_file_with_content(&parent_file.join("child2"), "x", false)
                .expect_err("parent file should fail");
        assert!(with_content_parent_error.contains("failed to create parent directory"));

        let symlink_parent_error = ensure_symlink(&parent_file.join("child-link"), &source, false)
            .expect_err("parent file should fail symlink parent creation");
        assert!(symlink_parent_error.contains("failed to create symlink parent directory"));

        let symlink_target_a = root.path.join("symlink-target-a");
        let symlink_target_b = root.path.join("symlink-target-b");
        fs::write(&symlink_target_a, "a").expect("symlink target a");
        fs::write(&symlink_target_b, "b").expect("symlink target b");
        let dry_link = root.path.join("dry-link");
        super::create_symlink(&symlink_target_a, &dry_link).expect("initial symlink should exist");
        let dry_link_updated = ensure_symlink(&dry_link, &symlink_target_b, true)
            .expect("dry-run symlink update should pass");
        assert_eq!(dry_link_updated, EnsureState::Updated);

        let create_symlink_error = super::create_symlink(&source, &parent_file.join("child-link2"))
            .expect_err("direct symlink create should fail when parent is file");
        assert!(create_symlink_error.contains("failed to create symlink"));

        let duplicate_switch = parse_flags(
            &["--dry-run".to_string(), "--dry-run".to_string()],
            &["--target"],
            &["--dry-run"],
        )
        .expect_err("duplicate switch should fail");
        assert!(duplicate_switch.contains("duplicate flag"));

        let missing_value_from_flag = parse_flags(
            &["--target".to_string(), "--dry-run".to_string()],
            &["--target"],
            &["--dry-run"],
        )
        .expect_err("missing value should fail");
        assert!(missing_value_from_flag.contains("missing value"));

        assert_eq!(
            super::normalize_root_prefix(Path::new("")),
            PathBuf::from("/")
        );
    }
}
