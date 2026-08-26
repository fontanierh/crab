//! Executable assembly for the Crab v2 contract graph.
//!
//! Domain policy remains inside the six boxes. This composition loads deployment topology,
//! restores durable channel routes and supervises the bounded trigger-lane workers.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

#[cfg(test)]
extern crate agent_host_contract as boxology_generated_contract;

use std::{path::Path, sync::Arc};

mod config;
mod configured;

pub use config::*;
pub use configured::*;

use agent_host_implementation::{
    AgentHost, AgentHostError, ConfiguredAgent, generated as agent_host,
};
use boxology_contract::ExposureLevel;
use boxology_runtime::{
    AssemblyErrors, Composition, CompositionBuilder, test_support::StubTransport,
};
use bridge_host_implementation::{
    BridgeHostError, BridgeHostState, CredentialStore, CredentialStoreError, FileCredentialStore,
    InMemoryCredentialStore, ProcessBridgePackageFactory, generated as bridge_host,
};
use native_channel_implementation::{
    NativeChannelError, NativeChannelState, generated as native_channel,
};
use sub_agent_host_implementation::{
    SubAgentHostError, SubAgentHostState, generated as sub_agent_host,
};
use trigger_inbox_implementation::{TriggerInbox, TriggerInboxError, generated as trigger_inbox};
use turn_router_implementation::{TurnRouterError, TurnRouterState, generated as turn_router};

/// A started draft graph and its in-process binding.
pub struct DraftRuntime {
    /// Keeps the Boxology graph alive.
    pub composition: Composition,
    /// Lets contract tests inspect and dispatch the exposed generated capabilities.
    pub in_process: Arc<StubTransport>,
    agent_host: agent_host_contract::AgentHostHandle,
    bridge_host: bridge_host_contract::BridgeHostHandle,
    native_channel: native_channel_contract::NativeChannelHandle,
    sub_agent_host: sub_agent_host_contract::SubAgentHostHandle,
    turn_router: turn_router_contract::TurnRouterHandle,
    trigger_inbox: trigger_inbox_contract::TriggerInboxHandle,
}

impl DraftRuntime {
    /// Returns the ordinary typed handle for the implemented ACP agent host.
    pub fn agent_host(&self) -> &agent_host_contract::AgentHostHandle {
        &self.agent_host
    }

    /// Returns the ordinary typed handle for the implemented bridge host.
    pub fn bridge_host(&self) -> &bridge_host_contract::BridgeHostHandle {
        &self.bridge_host
    }

    /// Returns the ordinary typed handle for the implemented native-channel router.
    pub fn native_channel(&self) -> &native_channel_contract::NativeChannelHandle {
        &self.native_channel
    }

    /// Returns the ordinary typed handle for Crab-owned ACP sub-agent orchestration.
    pub fn sub_agent_host(&self) -> &sub_agent_host_contract::SubAgentHostHandle {
        &self.sub_agent_host
    }

    /// Returns the typed durable ingress router.
    pub fn turn_router(&self) -> &turn_router_contract::TurnRouterHandle {
        &self.turn_router
    }

    /// Returns the ordinary typed handle for the implemented trigger inbox.
    pub fn trigger_inbox(&self) -> &trigger_inbox_contract::TriggerInboxHandle {
        &self.trigger_inbox
    }
}

/// Failures while assembling and restoring the v2 runtime.
#[derive(Debug)]
pub enum RuntimeStartError {
    /// Runtime topology configuration could not be loaded or resolved.
    Configuration(RuntimeConfigError),
    /// The concrete ACP agent host could not start.
    AgentHost(AgentHostError),
    /// The concrete bridge-host state could not start.
    BridgeHost(BridgeHostError),
    /// The private bridge credential provider could not start.
    CredentialStore(CredentialStoreError),
    /// The concrete native-channel state could not start.
    NativeChannel(NativeChannelError),
    /// The concrete sub-agent-host state could not start.
    SubAgentHost(SubAgentHostError),
    /// The concrete turn-router state could not start.
    TurnRouter(TurnRouterError),
    /// The concrete trigger store could not start.
    TriggerInbox(TriggerInboxError),
    /// Session metadata could not be encoded.
    SessionMetadata(serde_json::Error),
    /// The configured ACP session could not be opened.
    OpenSession(boxology_contract::CallError<agent_host_contract::AgentHostError>),
    /// A persisted route could not be resolved.
    ResolveRoute(boxology_contract::CallError<turn_router_contract::TurnRouterError>),
    /// A persisted native binding could not be inspected.
    InspectBinding(boxology_contract::CallError<native_channel_contract::NativeChannelError>),
    /// A crash-orphaned native binding could not be located.
    FindBinding(boxology_contract::CallError<native_channel_contract::NativeChannelError>),
    /// A configured native binding could not be created.
    BindChannel(boxology_contract::CallError<native_channel_contract::NativeChannelError>),
    /// A persisted native binding could not receive its fresh ACP session.
    ReplaceSession(boxology_contract::CallError<native_channel_contract::NativeChannelError>),
    /// A stale native binding could not be detached.
    UnbindChannel(boxology_contract::CallError<native_channel_contract::NativeChannelError>),
    /// A durable channel route could not be registered.
    PutRoute(boxology_contract::CallError<turn_router_contract::TurnRouterError>),
    /// Durable bridge registrations could not be listed.
    ListBridges(boxology_contract::CallError<bridge_host_contract::BridgeHostError>),
    /// A configured bridge could not be registered.
    RegisterBridge(boxology_contract::CallError<bridge_host_contract::BridgeHostError>),
    /// A changed configured bridge generation could not be installed.
    ReplaceBridge(boxology_contract::CallError<bridge_host_contract::BridgeHostError>),
    /// A bridge removed from configuration could not be stopped.
    StopBridge(boxology_contract::CallError<bridge_host_contract::BridgeHostError>),
    /// The durable state directory could not be created.
    StateDirectory(std::io::Error),
    /// Boxology rejected the composition graph.
    Assembly(AssemblyErrors),
}

impl From<AssemblyErrors> for RuntimeStartError {
    fn from(error: AssemblyErrors) -> Self {
        Self::Assembly(error)
    }
}

impl From<RuntimeConfigError> for RuntimeStartError {
    fn from(error: RuntimeConfigError) -> Self {
        Self::Configuration(error)
    }
}

/// Assemble all v2 boxes through generated Boxology adapters.
pub fn start_draft() -> Result<DraftRuntime, RuntimeStartError> {
    let agent_host = AgentHost::open_in_memory(Vec::new()).map_err(RuntimeStartError::AgentHost)?;
    let bridge_host = BridgeHostState::open_in_memory().map_err(RuntimeStartError::BridgeHost)?;
    let native_channel =
        NativeChannelState::open_in_memory().map_err(RuntimeStartError::NativeChannel)?;
    let sub_agent_host =
        SubAgentHostState::open_in_memory().map_err(RuntimeStartError::SubAgentHost)?;
    let turn_router = TurnRouterState::open_in_memory().map_err(RuntimeStartError::TurnRouter)?;
    let trigger_store = TriggerInbox::open_in_memory().map_err(RuntimeStartError::TriggerInbox)?;
    assemble_draft(
        agent_host,
        bridge_host,
        Arc::new(InMemoryCredentialStore::default()),
        native_channel,
        sub_agent_host,
        turn_router,
        trigger_store,
    )
}

/// Assemble the draft graph with a durable trigger inbox at `path`.
pub fn start_draft_with_trigger_store(
    path: impl AsRef<Path>,
) -> Result<DraftRuntime, RuntimeStartError> {
    let agent_host = AgentHost::open_in_memory(Vec::new()).map_err(RuntimeStartError::AgentHost)?;
    let bridge_host = BridgeHostState::open_in_memory().map_err(RuntimeStartError::BridgeHost)?;
    let native_channel =
        NativeChannelState::open_in_memory().map_err(RuntimeStartError::NativeChannel)?;
    let sub_agent_host =
        SubAgentHostState::open_in_memory().map_err(RuntimeStartError::SubAgentHost)?;
    let turn_router = TurnRouterState::open_in_memory().map_err(RuntimeStartError::TurnRouter)?;
    let trigger_store = TriggerInbox::open(path).map_err(RuntimeStartError::TriggerInbox)?;
    assemble_draft(
        agent_host,
        bridge_host,
        Arc::new(InMemoryCredentialStore::default()),
        native_channel,
        sub_agent_host,
        turn_router,
        trigger_store,
    )
}

/// Assemble every implemented box with durable state beneath one private runtime directory.
pub fn start_draft_with_state_directory(
    path: impl AsRef<Path>,
) -> Result<DraftRuntime, RuntimeStartError> {
    start_runtime_with_state_directory(path.as_ref(), Vec::new())
}

fn start_runtime_with_state_directory(
    path: &Path,
    agents: Vec<ConfiguredAgent>,
) -> Result<DraftRuntime, RuntimeStartError> {
    std::fs::create_dir_all(path).map_err(RuntimeStartError::StateDirectory)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(RuntimeStartError::StateDirectory)?;
    }
    let agent_host = AgentHost::open(path.join("agent-host.sqlite"), agents)
        .map_err(RuntimeStartError::AgentHost)?;
    let bridge_host = BridgeHostState::open(path.join("bridge-host.sqlite"))
        .map_err(RuntimeStartError::BridgeHost)?;
    let credentials = FileCredentialStore::open(path.join("bridge-credentials"))
        .map_err(RuntimeStartError::CredentialStore)?;
    let native_channel = NativeChannelState::open(path.join("native-channel.sqlite"))
        .map_err(RuntimeStartError::NativeChannel)?;
    let sub_agent_host = SubAgentHostState::open(path.join("sub-agent-host.sqlite"))
        .map_err(RuntimeStartError::SubAgentHost)?;
    let turn_router = TurnRouterState::open(path.join("turn-router.sqlite"))
        .map_err(RuntimeStartError::TurnRouter)?;
    let trigger_store = TriggerInbox::open(path.join("trigger-inbox.sqlite"))
        .map_err(RuntimeStartError::TriggerInbox)?;
    assemble_draft(
        agent_host,
        bridge_host,
        Arc::new(credentials),
        native_channel,
        sub_agent_host,
        turn_router,
        trigger_store,
    )
}

fn assemble_draft(
    agent_host_box: AgentHost,
    bridge_host_state: BridgeHostState,
    credential_store: Arc<dyn CredentialStore>,
    native_channel_state: NativeChannelState,
    sub_agent_host_state: SubAgentHostState,
    turn_router_state: TurnRouterState,
    trigger_store: TriggerInbox,
) -> Result<DraftRuntime, RuntimeStartError> {
    let in_process = Arc::new(StubTransport::new());
    let mut builder = CompositionBuilder::new();

    let agent_host = agent_host::register(&mut builder, agent_host_box);
    let agent_host_handle = builder.handle::<agent_host_contract::AgentHostHandle>(&agent_host);
    builder.expose_all(&agent_host, in_process.clone(), ExposureLevel::CodeOnly);

    let native_channel = native_channel::register(&mut builder, move |imports| {
        native_channel_state.connect(imports.agent_host)
    });
    builder.connect(&native_channel, &agent_host);
    let native_channel_handle =
        builder.handle::<native_channel_contract::NativeChannelHandle>(&native_channel);
    builder.expose_all(&native_channel, in_process.clone(), ExposureLevel::CodeOnly);

    let sub_agent_host = sub_agent_host::register(&mut builder, move |imports| {
        sub_agent_host_state.connect(imports.agent_host)
    });
    builder.connect(&sub_agent_host, &agent_host);
    let sub_agent_host_handle =
        builder.handle::<sub_agent_host_contract::SubAgentHostHandle>(&sub_agent_host);
    builder.expose_all(&sub_agent_host, in_process.clone(), ExposureLevel::CodeOnly);

    let trigger_inbox_box = trigger_inbox::register(&mut builder, trigger_store);
    let trigger_inbox =
        builder.handle::<trigger_inbox_contract::TriggerInboxHandle>(&trigger_inbox_box);
    builder.expose_all(
        &trigger_inbox_box,
        in_process.clone(),
        ExposureLevel::CodeOnly,
    );

    let turn_router = turn_router::register(&mut builder, move |imports| {
        turn_router_state.connect(imports.trigger_inbox, imports.native_channel)
    });
    builder.connect(&turn_router, &trigger_inbox_box);
    builder.connect(&turn_router, &native_channel);
    let turn_router_handle = builder.handle::<turn_router_contract::TurnRouterHandle>(&turn_router);
    builder.expose_all(&turn_router, in_process.clone(), ExposureLevel::CodeOnly);

    let bridge_host = bridge_host::register(&mut builder, move |imports| {
        bridge_host_state.connect(
            imports.trigger_inbox,
            Arc::new(ProcessBridgePackageFactory),
            credential_store,
        )
    });
    builder.connect(&bridge_host, &trigger_inbox_box);
    let bridge_host_handle = builder.handle::<bridge_host_contract::BridgeHostHandle>(&bridge_host);
    builder.expose_all(&bridge_host, in_process.clone(), ExposureLevel::CodeOnly);

    let composition = builder.start()?;
    Ok(DraftRuntime {
        composition,
        in_process,
        agent_host: agent_host_handle,
        bridge_host: bridge_host_handle,
        native_channel: native_channel_handle,
        sub_agent_host: sub_agent_host_handle,
        turn_router: turn_router_handle,
        trigger_inbox,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use boxology_contract::{CallContext, Caller, CancelToken, TraceContext};
    use boxology_manifest::{Kind, Manifest, RelativePath};
    use trigger_inbox_contract::{
        EnqueueTrigger, TriggerMode, TriggerReference, TriggerSource, TriggerState,
    };

    use super::{start_draft, start_draft_with_state_directory, start_draft_with_trigger_store};

    const MANIFEST: &str = include_str!("../../boxology.toml");

    fn context() -> CallContext {
        CallContext::new(
            Caller::Anonymous,
            None,
            CancelToken::new(),
            TraceContext::empty(),
            None,
        )
    }

    #[test]
    fn manifest_and_runtime_expose_the_same_six_box_graph() {
        let manifest = Manifest::parse(
            RelativePath::new("boxology.toml").expect("manifest path is valid"),
            MANIFEST.as_bytes(),
        )
        .expect("runtime manifest is valid");
        assert_eq!(manifest.kind(), Kind::Composition);

        let runtime = start_draft().expect("draft graph assembles");
        let binding = runtime
            .in_process
            .runtime()
            .expect("in-process binding starts");
        let counts = binding.exposures().iter().fold(
            BTreeMap::<String, usize>::new(),
            |mut counts, exposure| {
                *counts
                    .entry(exposure.descriptor().id().box_id().as_str().to_owned())
                    .or_default() += 1;
                counts
            },
        );

        assert_eq!(
            counts.keys().map(String::as_str).collect::<Vec<_>>(),
            [
                "agent-host",
                "bridge-host",
                "native-channel",
                "sub-agent-host",
                "trigger-inbox",
                "turn-router"
            ]
        );
        assert!(counts.values().all(|count| *count > 0));
        assert_eq!(
            manifest
                .composition()
                .expect("composition exists")
                .boxes()
                .len(),
            6
        );
    }

    #[test]
    fn durable_runtime_uses_one_explicit_state_layout() {
        let directory = tempfile::tempdir().expect("temporary state directory is created");
        let state = directory.path().join("runtime");
        let _runtime = start_draft_with_state_directory(&state).expect("durable graph assembles");
        for expected in [
            "agent-host.sqlite",
            "bridge-host.sqlite",
            "native-channel.sqlite",
            "sub-agent-host.sqlite",
            "turn-router.sqlite",
            "trigger-inbox.sqlite",
        ] {
            assert!(state.join(expected).is_file(), "missing {expected}");
        }
        assert!(state.join("bridge-credentials").is_dir());
    }

    #[tokio::test]
    async fn assembled_runtime_recovers_triggers_across_restart() {
        let directory = tempfile::tempdir().expect("temporary state directory is created");
        let path = directory.path().join("triggers.sqlite");
        let receipt = {
            let runtime =
                start_draft_with_trigger_store(&path).expect("file-backed graph assembles");
            runtime
                .trigger_inbox()
                .enqueue(
                    context(),
                    EnqueueTrigger {
                        source: TriggerSource::Operator,
                        source_id: "composition-test".into(),
                        deduplication_key: "turn-1".into(),
                        target_channel_id: "channel-1".into(),
                        lane: "primary".into(),
                        mode: TriggerMode::Queue,
                        not_before_ms: 0,
                        message_json: r#"{"text":"hello"}"#.into(),
                        attachments: Vec::new(),
                    },
                )
                .await
                .expect("generated handle dispatches enqueue")
        };
        assert_eq!(receipt.state, TriggerState::Pending);

        let restarted = start_draft_with_trigger_store(&path).expect("file-backed graph restarts");
        let record = restarted
            .trigger_inbox()
            .inspect(
                context(),
                TriggerReference {
                    trigger_id: receipt.trigger_id,
                },
            )
            .await
            .expect("generated handle dispatches inspect");
        assert_eq!(record.deduplication_key, "turn-1");
    }
}
