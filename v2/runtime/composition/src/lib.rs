//! Thin assembly proof for the Crab v2 contract draft.
//!
//! This crate deliberately contains no routing, auth or agent policy. Those decisions belong to
//! the five boxes. The composition only proves that generated adapters form one coherent graph.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

use std::{path::Path, sync::Arc};

use agent_host_implementation::{AgentHost, AgentHostError, generated as agent_host};
use boxology_contract::ExposureLevel;
use boxology_runtime::{
    AssemblyErrors, Composition, CompositionBuilder, test_support::StubTransport,
};
use bridge_host_implementation::{BridgeHostDraft, generated as bridge_host};
use native_channel_implementation::{NativeChannelDraft, generated as native_channel};
use sub_agent_host_implementation::{SubAgentHostDraft, generated as sub_agent_host};
use trigger_inbox_implementation::{TriggerInbox, TriggerInboxError, generated as trigger_inbox};

/// A started draft graph and its in-process binding.
pub struct DraftRuntime {
    /// Keeps the Boxology graph alive.
    pub composition: Composition,
    /// Lets contract tests inspect and dispatch the exposed generated capabilities.
    pub in_process: Arc<StubTransport>,
    agent_host: agent_host_contract::AgentHostHandle,
    trigger_inbox: trigger_inbox_contract::TriggerInboxHandle,
}

impl DraftRuntime {
    /// Returns the ordinary typed handle for the implemented ACP agent host.
    pub fn agent_host(&self) -> &agent_host_contract::AgentHostHandle {
        &self.agent_host
    }

    /// Returns the ordinary typed handle for the implemented trigger inbox.
    pub fn trigger_inbox(&self) -> &trigger_inbox_contract::TriggerInboxHandle {
        &self.trigger_inbox
    }
}

/// Failures while assembling the partially implemented v2 runtime.
#[derive(Debug)]
pub enum RuntimeStartError {
    /// The concrete ACP agent host could not start.
    AgentHost(AgentHostError),
    /// The concrete trigger store could not start.
    TriggerInbox(TriggerInboxError),
    /// Boxology rejected the composition graph.
    Assembly(AssemblyErrors),
}

impl From<AssemblyErrors> for RuntimeStartError {
    fn from(error: AssemblyErrors) -> Self {
        Self::Assembly(error)
    }
}

/// Assemble all v2 boxes through generated Boxology adapters.
pub fn start_draft() -> Result<DraftRuntime, RuntimeStartError> {
    let agent_host = AgentHost::open_in_memory(Vec::new()).map_err(RuntimeStartError::AgentHost)?;
    let trigger_store = TriggerInbox::open_in_memory().map_err(RuntimeStartError::TriggerInbox)?;
    assemble_draft(agent_host, trigger_store)
}

/// Assemble the draft graph with a durable trigger inbox at `path`.
pub fn start_draft_with_trigger_store(
    path: impl AsRef<Path>,
) -> Result<DraftRuntime, RuntimeStartError> {
    let agent_host = AgentHost::open_in_memory(Vec::new()).map_err(RuntimeStartError::AgentHost)?;
    let trigger_store = TriggerInbox::open(path).map_err(RuntimeStartError::TriggerInbox)?;
    assemble_draft(agent_host, trigger_store)
}

fn assemble_draft(
    agent_host_box: AgentHost,
    trigger_store: TriggerInbox,
) -> Result<DraftRuntime, RuntimeStartError> {
    let in_process = Arc::new(StubTransport::new());
    let mut builder = CompositionBuilder::new();

    let agent_host = agent_host::register(&mut builder, agent_host_box);
    let agent_host_handle = builder.handle::<agent_host_contract::AgentHostHandle>(&agent_host);
    builder.expose_all(&agent_host, in_process.clone(), ExposureLevel::CodeOnly);

    let native_channel = native_channel::register(&mut builder, NativeChannelDraft);
    builder.expose_all(&native_channel, in_process.clone(), ExposureLevel::CodeOnly);

    let bridge_host = bridge_host::register(&mut builder, BridgeHostDraft);
    builder.expose_all(&bridge_host, in_process.clone(), ExposureLevel::CodeOnly);

    let sub_agent_host = sub_agent_host::register(&mut builder, SubAgentHostDraft);
    builder.expose_all(&sub_agent_host, in_process.clone(), ExposureLevel::CodeOnly);

    let trigger_inbox_box = trigger_inbox::register(&mut builder, trigger_store);
    let trigger_inbox =
        builder.handle::<trigger_inbox_contract::TriggerInboxHandle>(&trigger_inbox_box);
    builder.expose_all(
        &trigger_inbox_box,
        in_process.clone(),
        ExposureLevel::CodeOnly,
    );

    let composition = builder.start()?;
    Ok(DraftRuntime {
        composition,
        in_process,
        agent_host: agent_host_handle,
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

    use super::{start_draft, start_draft_with_trigger_store};

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
    fn manifest_and_runtime_expose_the_same_five_box_graph() {
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
                "trigger-inbox"
            ]
        );
        assert!(counts.values().all(|count| *count > 0));
        assert_eq!(
            manifest
                .composition()
                .expect("composition exists")
                .boxes()
                .len(),
            5
        );
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
