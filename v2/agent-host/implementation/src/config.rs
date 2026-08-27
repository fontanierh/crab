use std::{collections::BTreeMap, path::PathBuf};

use agent_client_protocol::AcpAgentConfig;

use crate::{AcpProtocolProfile, AgentDescriptor, AgentHostError, AgentLifecycle};

/// Runtime state directory made available to every configured session MCP server.
pub const CRAB_STATE_DIRECTORY_ENV: &str = "CRAB_V2_STATE_DIRECTORY";
/// Crab session identity made available to every configured session MCP server.
pub const CRAB_SESSION_ID_ENV: &str = "CRAB_V2_SESSION_ID";
/// Configured agent identity made available to every session MCP server.
pub const CRAB_AGENT_ID_ENV: &str = "CRAB_V2_AGENT_ID";
/// Canonical ACP workspace made available to every session MCP server.
pub const CRAB_WORKING_DIRECTORY_ENV: &str = "CRAB_V2_WORKING_DIRECTORY";
/// Sub-agent identity made available when the ACP session belongs to a Crab child.
pub const CRAB_SUB_AGENT_ID_ENV: &str = "CRAB_V2_SUB_AGENT_ID";
/// Parent identity made available when the ACP session belongs to a Crab child.
pub const CRAB_PARENT_SESSION_ID_ENV: &str = "CRAB_V2_PARENT_SESSION_ID";

const RESERVED_MCP_ENVIRONMENT: [&str; 6] = [
    CRAB_STATE_DIRECTORY_ENV,
    CRAB_SESSION_ID_ENV,
    CRAB_AGENT_ID_ENV,
    CRAB_WORKING_DIRECTORY_ENV,
    CRAB_SUB_AGENT_ID_ENV,
    CRAB_PARENT_SESSION_ID_ENV,
];

/// ACP protocol implementation required from a configured agent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentProtocol {
    /// Stable ACP v1. Inputs submitted during work can only be queued.
    V1,
    /// Draft ACP v2. Prompts are acknowledged immediately and may steer active work.
    V2,
}

/// Optional agent extension negotiated on top of a stable ACP profile.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentSteeringExtension {
    /// Canonical ACP v1 `_session/steering` request with host-owned idle fallback.
    SessionSteeringV1,
}

impl AgentProtocol {
    pub(crate) fn profile(self) -> AcpProtocolProfile {
        match self {
            Self::V1 => AcpProtocolProfile::V1Stable,
            Self::V2 => AcpProtocolProfile::V2Draft,
        }
    }
}

/// A command whose output attests the agent-specific authority policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorityProbeConfig {
    /// Executable invoked without a shell.
    pub executable: PathBuf,
    /// Explicit command arguments.
    pub arguments: Vec<String>,
    /// Explicit environment additions. Values never appear in agent discovery.
    pub environment: BTreeMap<String, String>,
}

/// One stdio MCP server attached by Crab to every session of a configured agent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredMcpServer {
    /// Human-readable name published in the ACP session request.
    pub name: String,
    /// Absolute executable path invoked by the ACP agent.
    pub executable: PathBuf,
    /// Exact executable arguments.
    pub arguments: Vec<String>,
    /// Explicit non-context environment values supplied to the MCP subprocess.
    pub environment: BTreeMap<String, String>,
}

impl ConfiguredMcpServer {
    /// Create a stdio MCP server configuration without arguments or environment additions.
    #[must_use]
    pub fn new(name: impl Into<String>, executable: impl Into<PathBuf>) -> Self {
        Self {
            name: name.into(),
            executable: executable.into(),
            arguments: Vec::new(),
            environment: BTreeMap::new(),
        }
    }

    /// Replace the exact MCP executable arguments.
    #[must_use]
    pub fn arguments<I, S>(mut self, arguments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.arguments = arguments.into_iter().map(Into::into).collect();
        self
    }

    /// Add explicit environment values without inheriting the agent process environment.
    #[must_use]
    pub fn environment<I, K, V>(mut self, environment: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.environment = environment
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect();
        self
    }

    fn validate(&self) -> Result<(), AgentHostError> {
        if self.name.trim().is_empty()
            || !self.executable.is_absolute()
            || self.environment.keys().any(|name| {
                name.trim().is_empty()
                    || name.contains(['=', '\0'])
                    || RESERVED_MCP_ENVIRONMENT.contains(&name.as_str())
            })
        {
            return Err(AgentHostError::InvalidConfiguration);
        }
        Ok(())
    }
}

impl AuthorityProbeConfig {
    /// Create an authority probe command.
    #[must_use]
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            arguments: Vec::new(),
            environment: BTreeMap::new(),
        }
    }

    /// Append command arguments.
    #[must_use]
    pub fn arguments<I, S>(mut self, arguments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.arguments = arguments.into_iter().map(Into::into).collect();
        self
    }

    /// Add environment values for the probe process.
    #[must_use]
    pub fn environment<I, K, V>(mut self, environment: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.environment = environment
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect();
        self
    }
}

/// Complete launch configuration for one ACP-compatible agent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredAgent {
    /// Stable Crab identifier.
    pub agent_id: String,
    /// Operator-facing label.
    pub display_name: String,
    /// ACP executable invoked without a shell.
    pub executable: PathBuf,
    /// Explicit ACP executable arguments.
    pub arguments: Vec<String>,
    /// Environment values inherited by the ACP subprocess.
    pub environment: BTreeMap<String, String>,
    /// Required ACP session configuration values negotiated before readiness.
    pub session_options: BTreeMap<String, String>,
    /// Stdio MCP servers that Crab requires on every session.
    pub session_mcp_servers: Vec<ConfiguredMcpServer>,
    /// Protocol profile required from the subprocess.
    pub protocol: AgentProtocol,
    /// Optional steering extension. Crab still verifies the matching initialize metadata.
    pub steering_extension: Option<AgentSteeringExtension>,
    /// Agent-specific no-sandbox and permission-bypass probe.
    pub authority_probe: AuthorityProbeConfig,
}

impl ConfiguredAgent {
    /// Create a configured ACP agent.
    #[must_use]
    pub fn new(
        agent_id: impl Into<String>,
        display_name: impl Into<String>,
        executable: impl Into<PathBuf>,
        protocol: AgentProtocol,
        authority_probe: AuthorityProbeConfig,
    ) -> Self {
        Self {
            agent_id: agent_id.into(),
            display_name: display_name.into(),
            executable: executable.into(),
            arguments: Vec::new(),
            environment: BTreeMap::new(),
            session_options: BTreeMap::new(),
            session_mcp_servers: Vec::new(),
            protocol,
            steering_extension: None,
            authority_probe,
        }
    }

    /// Append ACP executable arguments.
    #[must_use]
    pub fn arguments<I, S>(mut self, arguments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.arguments = arguments.into_iter().map(Into::into).collect();
        self
    }

    /// Add ACP subprocess environment values.
    #[must_use]
    pub fn environment<I, K, V>(mut self, environment: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.environment = environment
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect();
        self
    }

    /// Require ACP session configuration values before accepting the agent.
    #[must_use]
    pub fn session_options<I, K, V>(mut self, options: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.session_options = options
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect();
        self
    }

    /// Attach the exact stdio MCP server set to every new ACP session.
    #[must_use]
    pub fn session_mcp_servers<I>(mut self, servers: I) -> Self
    where
        I: IntoIterator<Item = ConfiguredMcpServer>,
    {
        self.session_mcp_servers = servers.into_iter().collect();
        self
    }

    /// Require one explicitly supported steering extension.
    #[must_use]
    pub fn steering_extension(mut self, extension: AgentSteeringExtension) -> Self {
        self.steering_extension = Some(extension);
        self
    }

    pub(crate) fn validate(&self) -> Result<(), AgentHostError> {
        if self.agent_id.trim().is_empty()
            || self.display_name.trim().is_empty()
            || self.executable.as_os_str().is_empty()
            || self.authority_probe.executable.as_os_str().is_empty()
            || self.environment.keys().any(|name| name.trim().is_empty())
            || self
                .session_options
                .iter()
                .any(|(name, value)| name.trim().is_empty() || value.trim().is_empty())
            || self
                .session_mcp_servers
                .iter()
                .any(|server| server.validate().is_err())
            || self
                .session_mcp_servers
                .iter()
                .map(|server| server.name.as_str())
                .collect::<std::collections::HashSet<_>>()
                .len()
                != self.session_mcp_servers.len()
            || (self.steering_extension.is_some() && self.protocol != AgentProtocol::V1)
        {
            return Err(AgentHostError::InvalidConfiguration);
        }
        Ok(())
    }

    pub(crate) fn descriptor(&self, lifecycle: AgentLifecycle) -> AgentDescriptor {
        AgentDescriptor {
            agent_id: self.agent_id.clone(),
            display_name: self.display_name.clone(),
            executable: self.executable.to_string_lossy().into_owned(),
            arguments: self.arguments.clone(),
            environment_names: self.environment.keys().cloned().collect(),
            mcp_server_names: self
                .session_mcp_servers
                .iter()
                .map(|server| server.name.clone())
                .collect(),
            lifecycle,
        }
    }

    pub(crate) fn process_config(&self) -> AcpAgentConfig {
        AcpAgentConfig::new(self.executable.clone())
            .args(self.arguments.clone())
            .envs(self.environment.clone())
    }
}
