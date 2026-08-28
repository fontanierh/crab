use std::{
    collections::HashMap,
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

use agent_host_contract::{AgentLifecycle, SessionReference};
use bridge_host_contract::{
    BridgeLifecycle, BridgeManagement, BridgeReference, CredentialLifecycle,
};
use native_channel_contract::{ChannelLifecycle, ListChannelBindingsRequest};
use serde::Serialize;

use crate::{
    ChannelIpcClient, ChannelIpcClientError, RuntimeConfig, RuntimeConfigError,
    channel_ipc::complete_active_bridge_catalog_request,
};

const HEALTH_SCHEMA: u64 = 2;
const MAX_BINDING_CATALOG: u64 = 256;

/// A failure to read the authenticated runtime health surface.
#[derive(Debug)]
pub enum RuntimeHealthError {
    /// The local Boxology IPC could not provide its catalogs.
    Ipc(ChannelIpcClientError),
    /// The system clock cannot produce a portable Unix timestamp.
    ClockUnavailable,
    /// The expected semantic configuration could not be fingerprinted.
    Configuration(RuntimeConfigError),
}

impl fmt::Display for RuntimeHealthError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ipc(error) => error.fmt(formatter),
            Self::ClockUnavailable => formatter.write_str("system clock is unavailable"),
            Self::Configuration(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for RuntimeHealthError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Ipc(error) => Some(error),
            Self::Configuration(error) => Some(error),
            Self::ClockUnavailable => None,
        }
    }
}

impl From<ChannelIpcClientError> for RuntimeHealthError {
    fn from(error: ChannelIpcClientError) -> Self {
        Self::Ipc(error)
    }
}

/// Owner-only, configuration-aware evidence about the running topology.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeHealthReport {
    schema_version: u64,
    observed_at_ms: u64,
    ready: bool,
    healthy: bool,
    runtime: RuntimeAttestationHealth,
    channels: Vec<ChannelHealth>,
    bridges: Vec<BridgeHealth>,
    errors: Vec<String>,
    needs_action: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeAttestationHealth {
    expected_configuration_fingerprint: String,
    loaded_configuration_fingerprint: Option<String>,
    started_at_ms: u64,
    process_id: u64,
    ready: bool,
    error: Option<String>,
}

impl RuntimeHealthReport {
    /// Whether every configured component is present and structurally usable.
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        self.ready
    }

    /// Whether every configured component is fully healthy without operator action.
    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        self.healthy
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ChannelHealth {
    channel_id: String,
    adapter_id: String,
    agent_id: String,
    binding_id: Option<String>,
    session_id: Option<String>,
    binding_lifecycle: Option<&'static str>,
    agent_lifecycle: Option<&'static str>,
    published_sequence: Option<u64>,
    pending_input_count: Option<u64>,
    ready: bool,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeHealth {
    bridge_id: String,
    management: Option<&'static str>,
    desired_running: bool,
    lifecycle: Option<&'static str>,
    generation: Option<u64>,
    process_alive: Option<bool>,
    service_connected: Option<bool>,
    can_receive: Option<bool>,
    can_send: Option<bool>,
    credential_lifecycle: Option<&'static str>,
    consecutive_failures: Option<u64>,
    ready: bool,
    healthy: bool,
    error: Option<String>,
}

/// Aggregate the configured topology through generated owner-authenticated Boxology calls.
pub async fn inspect_runtime_health(
    client: &ChannelIpcClient,
    config: &RuntimeConfig,
) -> Result<RuntimeHealthReport, RuntimeHealthError> {
    let observed_at_ms = system_time_ms()?;
    let expected_configuration_fingerprint = config
        .configuration_fingerprint()
        .map_err(RuntimeHealthError::Configuration)?;
    let attestation = client.runtime_status().await?;
    let binding_catalog = client
        .list_channel_bindings(ListChannelBindingsRequest {
            limit: MAX_BINDING_CATALOG,
        })
        .await?;
    let bridge_catalog = client
        .list_bridge_page(complete_active_bridge_catalog_request())
        .await?;
    let mut errors = Vec::new();
    let mut needs_action = Vec::new();
    let runtime_error = match attestation.configuration_fingerprint.as_deref() {
        Some(loaded) if loaded == expected_configuration_fingerprint => None,
        Some(_) => Some("running runtime configuration does not match runtime.json".to_owned()),
        None => Some("running runtime has no configured topology attestation".to_owned()),
    };
    if let Some(error) = &runtime_error {
        errors.push(error.clone());
    }
    let runtime = RuntimeAttestationHealth {
        expected_configuration_fingerprint,
        loaded_configuration_fingerprint: attestation.configuration_fingerprint,
        started_at_ms: attestation.started_at_ms,
        process_id: attestation.process_id,
        ready: runtime_error.is_none(),
        error: runtime_error,
    };

    if binding_catalog.total_bindings
        > u64::try_from(binding_catalog.bindings.len()).unwrap_or(u64::MAX)
    {
        errors.push(format!(
            "native channel catalog is truncated at {MAX_BINDING_CATALOG} bindings"
        ));
    }
    let bindings = binding_catalog
        .bindings
        .into_iter()
        .filter(|binding| !matches!(binding.lifecycle, ChannelLifecycle::Detached))
        .map(|binding| {
            (
                (binding.channel_id.clone(), binding.adapter_id.clone()),
                binding,
            )
        })
        .collect::<HashMap<_, _>>();

    let mut channels = Vec::with_capacity(config.channels.len());
    for expected in &config.channels {
        let key = (expected.channel_id.clone(), expected.adapter_id.clone());
        let Some(binding) = bindings.get(&key) else {
            let error = format!(
                "configured channel {}/{} has no durable binding",
                expected.adapter_id, expected.channel_id
            );
            errors.push(error.clone());
            channels.push(ChannelHealth {
                channel_id: expected.channel_id.clone(),
                adapter_id: expected.adapter_id.clone(),
                agent_id: expected.agent_id.clone(),
                binding_id: None,
                session_id: None,
                binding_lifecycle: None,
                agent_lifecycle: None,
                published_sequence: None,
                pending_input_count: None,
                ready: false,
                error: Some(error),
            });
            continue;
        };
        let binding_state = channel_lifecycle_name(&binding.lifecycle);
        let session = client
            .agent_session_status(SessionReference {
                session_id: binding.session_id.clone(),
            })
            .await;
        let (agent_state, agent_ready, session_error) = match session {
            Ok(status) => {
                let ready = matches!(
                    status.lifecycle,
                    AgentLifecycle::Ready | AgentLifecycle::Busy
                );
                (
                    Some(agent_lifecycle_name(&status.lifecycle)),
                    ready,
                    (!ready).then(|| {
                        format!(
                            "configured channel {} agent session is {}",
                            expected.channel_id,
                            agent_lifecycle_name(&status.lifecycle)
                        )
                    }),
                )
            }
            Err(error) => (
                None,
                false,
                Some(format!(
                    "configured channel {} agent session is unavailable: {error}",
                    expected.channel_id
                )),
            ),
        };
        let binding_ready = matches!(binding.lifecycle, ChannelLifecycle::Attached);
        let binding_error = (!binding_ready).then(|| {
            format!(
                "configured channel {} binding is {binding_state}",
                expected.channel_id
            )
        });
        let error = binding_error.or(session_error);
        if let Some(error) = &error {
            errors.push(error.clone());
        }
        channels.push(ChannelHealth {
            channel_id: expected.channel_id.clone(),
            adapter_id: expected.adapter_id.clone(),
            agent_id: expected.agent_id.clone(),
            binding_id: Some(binding.binding_id.clone()),
            session_id: Some(binding.session_id.clone()),
            binding_lifecycle: Some(binding_state),
            agent_lifecycle: agent_state,
            published_sequence: Some(binding.published_sequence),
            pending_input_count: Some(binding.pending_input_count),
            ready: binding_ready && agent_ready,
            error,
        });
    }

    let bridge_records = bridge_catalog
        .bridges
        .into_iter()
        .map(|bridge| (bridge.bridge_id.clone(), bridge))
        .collect::<HashMap<_, _>>();
    let mut bridges = Vec::with_capacity(config.bridges.len());
    for expected in &config.bridges {
        let Some(record) = bridge_records.get(&expected.bridge_id) else {
            let error = format!("configured bridge {} is not registered", expected.bridge_id);
            errors.push(error.clone());
            bridges.push(BridgeHealth::missing(
                expected.bridge_id.clone(),
                expected.desired_running,
                error,
            ));
            continue;
        };
        let desired_matches = record.desired_running == expected.desired_running;
        let management_matches = matches!(record.management, BridgeManagement::RuntimeConfigured);
        let status = client
            .bridge_status(BridgeReference {
                bridge_id: expected.bridge_id.clone(),
            })
            .await;
        let component = match status {
            Ok(status) => {
                let lifecycle = bridge_lifecycle_name(&status.lifecycle);
                let ready = desired_matches
                    && management_matches
                    && bridge_is_ready(expected.desired_running, &status.lifecycle);
                let healthy = desired_matches
                    && management_matches
                    && bridge_is_healthy(expected.desired_running, &status.lifecycle);
                if expected.desired_running
                    && desired_matches
                    && management_matches
                    && matches!(status.lifecycle, BridgeLifecycle::AwaitingAuthentication)
                {
                    needs_action.push(format!("authenticate bridge {}", expected.bridge_id));
                } else if expected.desired_running
                    && desired_matches
                    && management_matches
                    && matches!(status.lifecycle, BridgeLifecycle::Degraded)
                {
                    needs_action.push(format!("inspect degraded bridge {}", expected.bridge_id));
                }
                let error = if !desired_matches {
                    Some(format!(
                        "configured bridge {} desired state does not match durable registration",
                        expected.bridge_id
                    ))
                } else if !management_matches {
                    Some(format!(
                        "configured bridge {} is not runtime-managed",
                        expected.bridge_id
                    ))
                } else if !ready {
                    Some(format!(
                        "configured bridge {} is {lifecycle}",
                        expected.bridge_id
                    ))
                } else {
                    None
                };
                let observation = status.last_health.as_ref();
                BridgeHealth {
                    bridge_id: expected.bridge_id.clone(),
                    management: Some(bridge_management_name(&record.management)),
                    desired_running: expected.desired_running,
                    lifecycle: Some(lifecycle),
                    generation: Some(status.generation),
                    process_alive: observation.map(|health| health.process_alive),
                    service_connected: observation.map(|health| health.service_connected),
                    can_receive: observation.map(|health| health.can_receive),
                    can_send: observation.map(|health| health.can_send),
                    credential_lifecycle: observation
                        .map(|health| credential_lifecycle_name(&health.credential_lifecycle)),
                    consecutive_failures: Some(status.consecutive_failures),
                    ready,
                    healthy,
                    error,
                }
            }
            Err(error) => BridgeHealth::missing(
                expected.bridge_id.clone(),
                expected.desired_running,
                format!(
                    "configured bridge {} status is unavailable: {error}",
                    expected.bridge_id
                ),
            ),
        };
        if let Some(error) = &component.error {
            errors.push(error.clone());
        }
        bridges.push(component);
    }

    let ready = runtime.ready
        && errors.is_empty()
        && channels.iter().all(|channel| channel.ready)
        && bridges.iter().all(|bridge| bridge.ready);
    let healthy = ready
        && channels.iter().all(|channel| channel.ready)
        && bridges.iter().all(|bridge| bridge.healthy)
        && needs_action.is_empty();
    Ok(RuntimeHealthReport {
        schema_version: HEALTH_SCHEMA,
        observed_at_ms,
        ready,
        healthy,
        runtime,
        channels,
        bridges,
        errors,
        needs_action,
    })
}

impl BridgeHealth {
    fn missing(bridge_id: String, desired_running: bool, error: String) -> Self {
        Self {
            bridge_id,
            management: None,
            desired_running,
            lifecycle: None,
            generation: None,
            process_alive: None,
            service_connected: None,
            can_receive: None,
            can_send: None,
            credential_lifecycle: None,
            consecutive_failures: None,
            ready: false,
            healthy: false,
            error: Some(error),
        }
    }
}

fn bridge_is_ready(desired_running: bool, lifecycle: &BridgeLifecycle) -> bool {
    if desired_running {
        matches!(
            lifecycle,
            BridgeLifecycle::Healthy
                | BridgeLifecycle::AwaitingAuthentication
                | BridgeLifecycle::Degraded
        )
    } else {
        matches!(
            lifecycle,
            BridgeLifecycle::Registered | BridgeLifecycle::Stopped
        )
    }
}

fn bridge_is_healthy(desired_running: bool, lifecycle: &BridgeLifecycle) -> bool {
    if desired_running {
        matches!(lifecycle, BridgeLifecycle::Healthy)
    } else {
        matches!(
            lifecycle,
            BridgeLifecycle::Registered | BridgeLifecycle::Stopped
        )
    }
}

fn system_time_ms() -> Result<u64, RuntimeHealthError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RuntimeHealthError::ClockUnavailable)?;
    u64::try_from(duration.as_millis()).map_err(|_| RuntimeHealthError::ClockUnavailable)
}

fn channel_lifecycle_name(value: &ChannelLifecycle) -> &'static str {
    match value {
        ChannelLifecycle::Binding => "binding",
        ChannelLifecycle::Attached => "attached",
        ChannelLifecycle::Replaying => "replaying",
        ChannelLifecycle::Detached => "detached",
        ChannelLifecycle::Failed => "failed",
        ChannelLifecycle::Unknown { .. } => "unknown",
    }
}

fn agent_lifecycle_name(value: &AgentLifecycle) -> &'static str {
    match value {
        AgentLifecycle::Discovered => "discovered",
        AgentLifecycle::Starting => "starting",
        AgentLifecycle::Ready => "ready",
        AgentLifecycle::Busy => "busy",
        AgentLifecycle::Detaching => "detaching",
        AgentLifecycle::Detached => "detached",
        AgentLifecycle::Stopping => "stopping",
        AgentLifecycle::Stopped => "stopped",
        AgentLifecycle::Failed => "failed",
        AgentLifecycle::Unknown { .. } => "unknown",
    }
}

fn bridge_lifecycle_name(value: &BridgeLifecycle) -> &'static str {
    match value {
        BridgeLifecycle::Registered => "registered",
        BridgeLifecycle::Starting => "starting",
        BridgeLifecycle::AwaitingAuthentication => "awaiting-authentication",
        BridgeLifecycle::Healthy => "healthy",
        BridgeLifecycle::Degraded => "degraded",
        BridgeLifecycle::BackingOff => "backing-off",
        BridgeLifecycle::Stopped => "stopped",
        BridgeLifecycle::Unregistered => "unregistered",
        BridgeLifecycle::Failed => "failed",
        BridgeLifecycle::Unknown { .. } => "unknown",
    }
}

fn bridge_management_name(value: &BridgeManagement) -> &'static str {
    match value {
        BridgeManagement::RuntimeConfigured => "runtime-configured",
        BridgeManagement::AgentManaged => "agent-managed",
        BridgeManagement::Unknown { .. } => "unknown",
    }
}

fn credential_lifecycle_name(value: &CredentialLifecycle) -> &'static str {
    match value {
        CredentialLifecycle::Missing => "missing",
        CredentialLifecycle::Challenged => "challenged",
        CredentialLifecycle::Validating => "validating",
        CredentialLifecycle::Valid => "valid",
        CredentialLifecycle::Expiring => "expiring",
        CredentialLifecycle::Rejected => "rejected",
        CredentialLifecycle::Revoked => "revoked",
        CredentialLifecycle::Unknown { .. } => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use bridge_host_contract::BridgeLifecycle;

    use super::{bridge_is_healthy, bridge_is_ready};

    #[test]
    fn bridge_readiness_allows_auth_bootstrap_but_health_does_not() {
        assert!(bridge_is_ready(
            true,
            &BridgeLifecycle::AwaitingAuthentication
        ));
        assert!(!bridge_is_healthy(
            true,
            &BridgeLifecycle::AwaitingAuthentication
        ));
        assert!(bridge_is_ready(false, &BridgeLifecycle::Stopped));
        assert!(bridge_is_healthy(false, &BridgeLifecycle::Stopped));
        assert!(bridge_is_ready(true, &BridgeLifecycle::Degraded));
        assert!(!bridge_is_ready(true, &BridgeLifecycle::BackingOff));
        assert!(!bridge_is_healthy(true, &BridgeLifecycle::Degraded));
    }
}
