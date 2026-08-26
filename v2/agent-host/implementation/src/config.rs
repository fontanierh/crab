use std::{collections::BTreeMap, path::PathBuf};

use agent_client_protocol::AcpAgentConfig;

use crate::{AcpProtocolProfile, AgentDescriptor, AgentHostError, AgentLifecycle};

/// ACP protocol implementation required from a configured agent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentProtocol {
    /// Stable ACP v1. Inputs submitted during work can only be queued.
    V1,
    /// Draft ACP v2. Prompts are acknowledged immediately and may steer active work.
    V2,
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
    /// Protocol profile required from the subprocess.
    pub protocol: AgentProtocol,
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
            protocol,
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
            lifecycle,
        }
    }

    pub(crate) fn process_config(&self) -> AcpAgentConfig {
        AcpAgentConfig::new(self.executable.clone())
            .args(self.arguments.clone())
            .envs(self.environment.clone())
    }
}
