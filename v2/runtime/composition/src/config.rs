use std::{
    collections::{BTreeMap, HashSet},
    env, fmt, fs,
    path::{Path, PathBuf},
};

use agent_host_implementation::{AgentProtocol, AuthorityProbeConfig, ConfiguredAgent};
use bridge_host_contract::{AuthenticationMethod, BridgeIngressMode, BridgeSpec};
use serde::Deserialize;
use serde_json::{Map, Value};

const CONFIG_SCHEMA: u64 = 1;

/// Secret-free runtime topology loaded from JSON.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Configuration schema. Only version 1 is accepted.
    pub schema: u64,
    /// ACP harness commands known to the runtime.
    pub agents: Vec<AgentConfig>,
    /// Logical native channels and their initial ACP sessions.
    pub channels: Vec<ChannelConfig>,
    /// Durable trigger lanes drained by background workers.
    pub lanes: Vec<LaneConfig>,
    /// Package-defined bridges reconciled against durable registrations at startup.
    #[serde(default)]
    pub bridges: Vec<BridgeConfig>,
}

/// One ACP harness command. Environment values are always read by name at runtime.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentConfig {
    /// Stable agent identifier.
    pub agent_id: String,
    /// Human-readable catalog label.
    pub display_name: String,
    /// Executable name or path, invoked without a shell.
    pub executable: PathBuf,
    /// Exact executable arguments.
    #[serde(default)]
    pub arguments: Vec<String>,
    /// Ambient environment variable names copied into explicit process configuration.
    #[serde(default)]
    pub environment_from: Vec<String>,
    /// Required ACP session configuration values verified before the session becomes ready.
    #[serde(default)]
    pub session_options: BTreeMap<String, String>,
    /// ACP wire profile required from the command.
    pub protocol: ProtocolConfig,
    /// Agent-specific command that proves yolo/no-sandbox authority.
    pub authority_probe: CommandConfig,
}

/// ACP profile selected in configuration.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ProtocolConfig {
    /// Stable ACP v1; active work only supports turn-boundary queueing.
    V1,
    /// Draft ACP v2; active work may support concurrent steering.
    V2,
}

/// A shell-free command with environment values sourced only by name.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CommandConfig {
    /// Executable name or path.
    pub executable: PathBuf,
    /// Exact executable arguments.
    #[serde(default)]
    pub arguments: Vec<String>,
    /// Ambient environment variable names copied to the command.
    #[serde(default)]
    pub environment_from: Vec<String>,
}

/// One configured logical channel and the ACP session opened for it at startup.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChannelConfig {
    /// Stable route target used by bridges, schedules and native adapters.
    pub channel_id: String,
    /// Stable native UI/adapter identifier.
    pub adapter_id: String,
    /// Configured agent identifier used for the session.
    pub agent_id: String,
    /// Working directory supplied to ACP and authority preflight.
    pub working_directory: PathBuf,
    /// Optional bootstrap prompt file. Relative paths resolve beside the config file.
    #[serde(default)]
    pub bootstrap_prompt_file: Option<PathBuf>,
    /// Adapter metadata retained losslessly in the binding.
    pub native_channel: Value,
    /// Additional non-secret session metadata.
    #[serde(default = "empty_object")]
    pub session_metadata: Value,
    /// Durable trigger lane routed to this channel.
    pub lane: String,
}

/// One continuously drained trigger lane.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LaneConfig {
    /// Lane name used by trigger producers and routes.
    pub lane: String,
    /// Stable trigger lease worker identifier.
    pub worker_id: String,
    /// Maximum triggers leased per drain.
    pub batch_limit: u64,
    /// Trigger lease duration.
    pub lease_duration_ms: u64,
    /// Delay before a transient delivery is claimable again.
    pub retry_delay_ms: u64,
    /// Maximum delivery attempts before dead-lettering.
    pub max_attempts: u64,
    /// Idle delay between bounded drain calls.
    pub poll_interval_ms: u64,
}

/// One bridge package and its immutable ingress/supervision policy.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BridgeConfig {
    /// Stable registration identity.
    pub bridge_id: String,
    /// Stable package identity.
    pub package_id: String,
    /// Operator-facing label.
    pub display_name: String,
    /// Absolute path or path relative to the runtime configuration file.
    pub executable: PathBuf,
    /// Exact executable arguments without shell evaluation.
    #[serde(default)]
    pub arguments: Vec<String>,
    /// Environment values are copied at launch by name and never serialized into durable state.
    #[serde(default)]
    pub environment_from: Vec<String>,
    /// Absolute path or path relative to the runtime configuration file.
    pub working_directory: PathBuf,
    /// Non-secret package configuration.
    #[serde(default = "empty_object")]
    pub configuration: Value,
    /// Authentication presentations supported by the package.
    #[serde(default)]
    pub authentication_methods: Vec<BridgeAuthenticationConfig>,
    /// Fixed ingress behavior for this bridge generation.
    pub ingress_mode: BridgeIngressConfig,
    /// Whether the package should be supervised.
    pub desired_running: bool,
    /// Delay between live health probes.
    pub health_interval_ms: u64,
    /// Delay between active credential validations.
    pub credential_validation_interval_ms: u64,
    /// Maximum process starts within the restart window.
    pub restart_limit: u64,
    /// Restart-budget window and maximum backoff.
    pub restart_window_ms: u64,
}

/// Authentication presentation selected from the generic bridge contract.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum BridgeAuthenticationConfig {
    /// Renderable QR payload.
    QrCode,
    /// Phone-number link code.
    PhoneCode,
    /// OAuth redirect flow.
    OAuth,
    /// Browser-owned flow.
    Browser,
    /// Terminal-owned flow.
    Terminal,
    /// Package-defined manual flow.
    Manual,
}

/// How an inbound bridge event contributes to agent work.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum BridgeIngressConfig {
    /// Wait in FIFO order.
    Queue,
    /// Contribute to compatible active work.
    Steer,
    /// Cancel active work before steering immediately.
    InterruptAndSteer,
}

/// Safe configuration failures. Values from referenced environment variables are never retained.
#[derive(Debug)]
pub enum RuntimeConfigError {
    /// The JSON file could not be read.
    Read(std::io::Error),
    /// The file was not valid strict JSON configuration.
    Decode(serde_json::Error),
    /// The schema version is unsupported.
    UnsupportedSchema,
    /// Required identities, references or numeric bounds are invalid.
    InvalidTopology,
    /// A named environment variable was absent or not Unicode.
    MissingEnvironment(String),
    /// A bootstrap prompt file could not be read.
    Bootstrap(std::io::Error),
}

impl fmt::Display for RuntimeConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(_) => formatter.write_str("runtime config could not be read"),
            Self::Decode(_) => formatter.write_str("runtime config is not valid schema-v1 JSON"),
            Self::UnsupportedSchema => formatter.write_str("runtime config schema is unsupported"),
            Self::InvalidTopology => formatter.write_str("runtime config topology is invalid"),
            Self::MissingEnvironment(name) => {
                write!(
                    formatter,
                    "runtime environment variable {name} is unavailable"
                )
            }
            Self::Bootstrap(_) => formatter.write_str("channel bootstrap prompt could not be read"),
        }
    }
}

impl std::error::Error for RuntimeConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Read(error) | Self::Bootstrap(error) => Some(error),
            Self::Decode(error) => Some(error),
            _ => None,
        }
    }
}

impl RuntimeConfig {
    /// Load, resolve relative filesystem paths and validate one strict JSON file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, RuntimeConfigError> {
        let path = absolute_config_path(path.as_ref())?;
        let bytes = fs::read(&path).map_err(RuntimeConfigError::Read)?;
        let mut config: Self =
            serde_json::from_slice(&bytes).map_err(RuntimeConfigError::Decode)?;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        config.resolve_paths(base);
        config.validate()?;
        Ok(config)
    }

    pub(crate) fn configured_agents(&self) -> Result<Vec<ConfiguredAgent>, RuntimeConfigError> {
        self.agents
            .iter()
            .map(|agent| {
                let environment = environment_values(&agent.environment_from)?;
                let probe_environment =
                    environment_values(&agent.authority_probe.environment_from)?;
                Ok(ConfiguredAgent::new(
                    &agent.agent_id,
                    &agent.display_name,
                    &agent.executable,
                    match agent.protocol {
                        ProtocolConfig::V1 => AgentProtocol::V1,
                        ProtocolConfig::V2 => AgentProtocol::V2,
                    },
                    AuthorityProbeConfig::new(&agent.authority_probe.executable)
                        .arguments(agent.authority_probe.arguments.clone())
                        .environment(probe_environment),
                )
                .arguments(agent.arguments.clone())
                .environment(environment)
                .session_options(agent.session_options.clone()))
            })
            .collect()
    }

    pub(crate) fn bootstrap_prompt(
        &self,
        channel: &ChannelConfig,
    ) -> Result<Option<String>, RuntimeConfigError> {
        channel
            .bootstrap_prompt_file
            .as_ref()
            .map(|path| fs::read_to_string(path).map_err(RuntimeConfigError::Bootstrap))
            .transpose()
    }

    pub(crate) fn bridge_specs(&self) -> Result<Vec<BridgeSpec>, RuntimeConfigError> {
        self.bridges.iter().map(BridgeConfig::spec).collect()
    }

    fn resolve_paths(&mut self, base: &Path) {
        for agent in &mut self.agents {
            resolve_command_path(base, &mut agent.executable);
            resolve_command_path(base, &mut agent.authority_probe.executable);
        }
        for channel in &mut self.channels {
            resolve_path(base, &mut channel.working_directory);
            if let Some(path) = &mut channel.bootstrap_prompt_file {
                resolve_path(base, path);
            }
        }
        for bridge in &mut self.bridges {
            resolve_command_path(base, &mut bridge.executable);
            resolve_path(base, &mut bridge.working_directory);
        }
    }

    pub(crate) fn validate(&self) -> Result<(), RuntimeConfigError> {
        if self.schema != CONFIG_SCHEMA {
            return Err(RuntimeConfigError::UnsupportedSchema);
        }
        if self.agents.is_empty() || self.channels.is_empty() || self.lanes.is_empty() {
            return Err(RuntimeConfigError::InvalidTopology);
        }
        let mut agent_ids = HashSet::new();
        for agent in &self.agents {
            if agent.agent_id.trim().is_empty()
                || agent.display_name.trim().is_empty()
                || agent.executable.as_os_str().is_empty()
                || agent.authority_probe.executable.as_os_str().is_empty()
                || !agent_ids.insert(agent.agent_id.as_str())
                || !valid_environment_names(&agent.environment_from)
                || !valid_environment_names(&agent.authority_probe.environment_from)
                || agent
                    .session_options
                    .iter()
                    .any(|(name, value)| name.trim().is_empty() || value.trim().is_empty())
            {
                return Err(RuntimeConfigError::InvalidTopology);
            }
        }
        let mut lanes = HashSet::new();
        for lane in &self.lanes {
            if lane.lane.trim().is_empty()
                || lane.worker_id.trim().is_empty()
                || lane.batch_limit == 0
                || lane.batch_limit > 1_000
                || lane.lease_duration_ms == 0
                || lane.retry_delay_ms == 0
                || lane.max_attempts == 0
                || lane.poll_interval_ms == 0
                || !lanes.insert(lane.lane.as_str())
            {
                return Err(RuntimeConfigError::InvalidTopology);
            }
        }
        let mut channels = HashSet::new();
        for channel in &self.channels {
            if channel.channel_id.trim().is_empty()
                || channel.adapter_id.trim().is_empty()
                || channel.working_directory.as_os_str().is_empty()
                || !agent_ids.contains(channel.agent_id.as_str())
                || !lanes.contains(channel.lane.as_str())
                || !channel.native_channel.is_object()
                || !channel.session_metadata.is_object()
                || !channels.insert(channel.channel_id.as_str())
            {
                return Err(RuntimeConfigError::InvalidTopology);
            }
        }
        let mut bridge_ids = HashSet::new();
        for bridge in &self.bridges {
            let unique_methods = bridge
                .authentication_methods
                .iter()
                .copied()
                .collect::<HashSet<_>>();
            if bridge.bridge_id.trim().is_empty()
                || bridge.package_id.trim().is_empty()
                || bridge.display_name.trim().is_empty()
                || bridge.executable.as_os_str().is_empty()
                || !bridge.executable.is_absolute()
                || bridge.working_directory.as_os_str().is_empty()
                || !bridge.working_directory.is_absolute()
                || !bridge.configuration.is_object()
                || !valid_environment_names(&bridge.environment_from)
                || unique_methods.len() != bridge.authentication_methods.len()
                || bridge.health_interval_ms == 0
                || bridge.credential_validation_interval_ms == 0
                || bridge.restart_limit == 0
                || bridge.restart_window_ms == 0
                || !bridge_ids.insert(bridge.bridge_id.as_str())
            {
                return Err(RuntimeConfigError::InvalidTopology);
            }
            if let Some(target) = bridge.configuration.get("targetChannelId")
                && (!target.is_string() || !channels.contains(target.as_str().unwrap_or_default()))
            {
                return Err(RuntimeConfigError::InvalidTopology);
            }
        }
        Ok(())
    }
}

fn absolute_config_path(path: &Path) -> Result<PathBuf, RuntimeConfigError> {
    if path.is_absolute() {
        return Ok(path.to_owned());
    }
    env::current_dir()
        .map(|directory| directory.join(path))
        .map_err(RuntimeConfigError::Read)
}

impl BridgeConfig {
    fn spec(&self) -> Result<BridgeSpec, RuntimeConfigError> {
        let launch_json = serde_json::to_string(&serde_json::json!({
            "executable": self.executable,
            "arguments": self.arguments,
            "workingDirectory": self.working_directory,
            "environmentNames": self.environment_from,
        }))
        .map_err(|_| RuntimeConfigError::InvalidTopology)?;
        let configuration_json = serde_json::to_string(&self.configuration)
            .map_err(|_| RuntimeConfigError::InvalidTopology)?;
        Ok(BridgeSpec {
            bridge_id: self.bridge_id.clone(),
            package_id: self.package_id.clone(),
            display_name: self.display_name.clone(),
            launch_json,
            configuration_json,
            authentication_methods: self
                .authentication_methods
                .iter()
                .map(|method| match method {
                    BridgeAuthenticationConfig::QrCode => AuthenticationMethod::QrCode,
                    BridgeAuthenticationConfig::PhoneCode => AuthenticationMethod::PhoneCode,
                    BridgeAuthenticationConfig::OAuth => AuthenticationMethod::OAuth,
                    BridgeAuthenticationConfig::Browser => AuthenticationMethod::Browser,
                    BridgeAuthenticationConfig::Terminal => AuthenticationMethod::Terminal,
                    BridgeAuthenticationConfig::Manual => AuthenticationMethod::Manual,
                })
                .collect(),
            ingress_mode: match self.ingress_mode {
                BridgeIngressConfig::Queue => BridgeIngressMode::Queue,
                BridgeIngressConfig::Steer => BridgeIngressMode::Steer,
                BridgeIngressConfig::InterruptAndSteer => BridgeIngressMode::InterruptAndSteer,
            },
            desired_running: self.desired_running,
            health_interval_ms: self.health_interval_ms,
            credential_validation_interval_ms: self.credential_validation_interval_ms,
            restart_limit: self.restart_limit,
            restart_window_ms: self.restart_window_ms,
        })
    }
}

fn empty_object() -> Value {
    Value::Object(Map::new())
}

fn resolve_command_path(base: &Path, path: &mut PathBuf) {
    if path.is_relative() && path.components().count() > 1 {
        resolve_path(base, path);
    }
}

fn resolve_path(base: &Path, path: &mut PathBuf) {
    if path.is_relative() {
        *path = base.join(&*path);
    }
}

fn valid_environment_names(names: &[String]) -> bool {
    let mut unique = HashSet::new();
    names
        .iter()
        .all(|name| !name.trim().is_empty() && !name.contains(['=', '\0']) && unique.insert(name))
}

fn environment_values(names: &[String]) -> Result<BTreeMap<String, String>, RuntimeConfigError> {
    names
        .iter()
        .map(|name| {
            env::var(name)
                .map(|value| (name.clone(), value))
                .map_err(|_| RuntimeConfigError::MissingEnvironment(name.clone()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{RuntimeConfig, RuntimeConfigError, absolute_config_path};

    #[test]
    fn relative_config_paths_gain_an_absolute_resolution_base() {
        let resolved = absolute_config_path(std::path::Path::new("runtime/example.json"))
            .expect("current directory is available");
        assert!(resolved.is_absolute());
        assert!(resolved.ends_with("runtime/example.json"));
    }

    #[test]
    fn strict_config_resolves_paths_and_references_without_secret_values() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config_path = directory.path().join("runtime.json");
        std::fs::write(
            &config_path,
            r#"{
              "schema": 1,
              "agents": [{
                "agentId": "fixture", "displayName": "Fixture", "executable": "bin/acp",
                "arguments": ["--acp"], "environmentFrom": [],
                "sessionOptions": {"mode":"bypassPermissions","model":"opus"},
                "protocol": "v2",
                "authorityProbe": { "executable": "bin/probe", "arguments": [], "environmentFrom": [] }
              }],
              "channels": [{
                "channelId": "primary", "adapterId": "native", "agentId": "fixture",
                "workingDirectory": "workspace", "bootstrapPromptFile": "bootstrap.md",
                "nativeChannel": {"title":"Jim"}, "sessionMetadata": {}, "lane": "primary"
              }],
              "lanes": [{
                "lane": "primary", "workerId": "runtime-1", "batchLimit": 16,
                "leaseDurationMs": 30000, "retryDelayMs": 1000, "maxAttempts": 3,
                "pollIntervalMs": 25
              }],
              "bridges": [{
                "bridgeId": "whatsapp", "packageId": "crab.whatsapp",
                "displayName": "WhatsApp", "executable": "../bridges/whatsapp/src/index.js",
                "arguments": [], "environmentFrom": ["PATH"],
                "workingDirectory": "../bridges/whatsapp",
                "configuration": {"targetChannelId":"primary","browserName":"Crab"},
                "authenticationMethods": ["qrCode", "phoneCode"],
                "ingressMode": "queue", "desiredRunning": true,
                "healthIntervalMs": 5000, "credentialValidationIntervalMs": 3600000,
                "restartLimit": 5, "restartWindowMs": 300000
              }]
            }"#,
        )
        .expect("config writes");
        let config = RuntimeConfig::load(&config_path).expect("config loads");
        assert_eq!(
            config.agents[0].executable,
            directory.path().join("bin/acp")
        );
        assert_eq!(
            config.agents[0]
                .session_options
                .get("mode")
                .map(String::as_str),
            Some("bypassPermissions")
        );
        assert_eq!(
            config.channels[0].working_directory,
            directory.path().join("workspace")
        );
        assert_eq!(config.lanes[0].max_attempts, 3);
        assert_eq!(
            config.bridges[0].executable,
            directory.path().join("../bridges/whatsapp/src/index.js")
        );
        let spec = &config.bridge_specs().expect("bridge specs encode")[0];
        assert!(spec.launch_json.contains("PATH"));
        assert!(!spec.launch_json.contains("secret"));
        assert_eq!(spec.authentication_methods.len(), 2);
    }

    #[test]
    fn unknown_fields_fail_closed() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let unknown = directory.path().join("unknown.json");
        std::fs::write(
            &unknown,
            r#"{"schema":1,"agents":[],"channels":[],"lanes":[],"secret":"no"}"#,
        )
        .expect("config writes");
        assert!(matches!(
            RuntimeConfig::load(&unknown),
            Err(RuntimeConfigError::Decode(_))
        ));
    }

    #[test]
    fn claude_opus_preset_pins_adapter_and_required_session_policy() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../runtime.claude-opus.example.json");
        let config = RuntimeConfig::load(path).expect("committed Claude preset loads");
        let agent = &config.agents[0];
        assert_eq!(
            agent.arguments,
            ["--yes", "@agentclientprotocol/claude-agent-acp@0.70.0"]
        );
        assert_eq!(agent.protocol, super::ProtocolConfig::V1);
        assert_eq!(
            agent.session_options.get("mode").map(String::as_str),
            Some("bypassPermissions")
        );
        assert_eq!(
            agent.session_options.get("model").map(String::as_str),
            Some("opus[1m]")
        );
        assert_eq!(
            agent.authority_probe.executable,
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/release/crab-v2-claude-authority-probe")
        );
    }
}
