//! Thin assembly proof for the Crab v2 contract draft.
//!
//! This crate deliberately contains no routing, auth or agent policy. Those decisions belong to
//! the five boxes. The composition only proves that generated adapters form one coherent graph.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

use std::sync::Arc;

use agent_host_implementation::{AgentHostDraft, generated as agent_host};
use boxology_contract::ExposureLevel;
use boxology_runtime::{
    AssemblyErrors, Composition, CompositionBuilder, test_support::StubTransport,
};
use bridge_host_implementation::{BridgeHostDraft, generated as bridge_host};
use native_channel_implementation::{NativeChannelDraft, generated as native_channel};
use sub_agent_host_implementation::{SubAgentHostDraft, generated as sub_agent_host};
use trigger_inbox_implementation::{TriggerInboxDraft, generated as trigger_inbox};

/// A started draft graph and its in-process binding.
pub struct DraftRuntime {
    /// Keeps the Boxology graph alive.
    pub composition: Composition,
    /// Lets contract tests inspect and dispatch the exposed generated capabilities.
    pub in_process: Arc<StubTransport>,
}

/// Assemble all v2 boxes through generated Boxology adapters.
pub fn start_draft() -> Result<DraftRuntime, AssemblyErrors> {
    let in_process = Arc::new(StubTransport::new());
    let mut builder = CompositionBuilder::new();

    let agent_host = agent_host::register(&mut builder, AgentHostDraft);
    builder.expose_all(&agent_host, in_process.clone(), ExposureLevel::CodeOnly);

    let native_channel = native_channel::register(&mut builder, NativeChannelDraft);
    builder.expose_all(&native_channel, in_process.clone(), ExposureLevel::CodeOnly);

    let bridge_host = bridge_host::register(&mut builder, BridgeHostDraft);
    builder.expose_all(&bridge_host, in_process.clone(), ExposureLevel::CodeOnly);

    let sub_agent_host = sub_agent_host::register(&mut builder, SubAgentHostDraft);
    builder.expose_all(&sub_agent_host, in_process.clone(), ExposureLevel::CodeOnly);

    let trigger_inbox = trigger_inbox::register(&mut builder, TriggerInboxDraft);
    builder.expose_all(&trigger_inbox, in_process.clone(), ExposureLevel::CodeOnly);

    let composition = builder.start()?;
    Ok(DraftRuntime {
        composition,
        in_process,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use boxology_manifest::{Kind, Manifest, RelativePath};

    use super::start_draft;

    const MANIFEST: &str = include_str!("../../boxology.toml");

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
}
